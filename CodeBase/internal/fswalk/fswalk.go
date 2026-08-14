package fswalk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// FileInfo информация о файле
type FileInfo struct {
	Path       string
	RelPath    string
	Extension  string
	Size       int64
	Hash       string
	ModifiedAt time.Time
	Encoding   string
	Language   string
	// Content — сырые байты файла, прочитанные один раз при обходе.
	// Используются и для хэша, и для парсинга, чтобы не читать файл повторно.
	Content []byte
}

// FileFingerprint — метаданные файла для pre-filter (Update only).
// Если size и modTime совпадают с предыдущей индексацией, файл считается неизменённым.
type FileFingerprint struct {
	Size    int64
	ModTime time.Time
}

// Walker обходчик файлов
type Walker struct {
	rootPath        string
	includePatterns []string
	excludePatterns []string
	includeRegexps  []*regexp.Regexp
	excludeRegexps  []*regexp.Regexp
	// preFilter — карта известных файлов для пропуска по mtime+size (Update only).
	// Ключ: нормализованный path (filepath.ToSlash). nil = pre-filter отключён.
	preFilter map[string]FileFingerprint
}

// NewWalker создаёт новый walker
func NewWalker(rootPath string, includePatterns, excludePatterns []string) *Walker {
	w := &Walker{
		rootPath:        rootPath,
		includePatterns: includePatterns,
		excludePatterns: excludePatterns,
	}

	// Паттерны компилируются один раз при создании Walker,
	// чтобы не тратить время на regexp.Compile для каждого файла.
	for _, p := range includePatterns {
		if re := patternToRegexp(p); re != nil {
			w.includeRegexps = append(w.includeRegexps, re)
		}
	}
	for _, p := range excludePatterns {
		if re := patternToRegexp(p); re != nil {
			w.excludeRegexps = append(w.excludeRegexps, re)
		}
	}

	return w
}

// patternToRegexp преобразует glob-паттерн в regexp
func patternToRegexp(pattern string) *regexp.Regexp {
	// Экранируем специальные символы regexp
	re := regexp.QuoteMeta(pattern)
	// Заменяем glob-символы на regexp
	re = strings.ReplaceAll(re, `\*`, `.*`)
	re = strings.ReplaceAll(re, `\?`, `.`)
	
	// Добавляем якоря
	if !strings.HasPrefix(re, "^") {
		re = "^" + re
	}
	if !strings.HasSuffix(re, "$") {
		re = re + "$"
	}
	re = "(?i)" + re
	
	r, err := regexp.Compile(re)
	if err != nil {
		return nil
	}
	return r
}

// SetPreFilter устанавливает карту известных файлов для пропуска по mtime+size.
// Вызывается только для Update (Init не имеет предыдущего состояния).
func (w *Walker) SetPreFilter(existing map[string]FileFingerprint) {
	w.preFilter = existing
}

// walkTask — метаданные файла для передачи в воркер чтения+хэширования.
type walkTask struct {
	path    string
	relPath string
	info    fs.FileInfo
	ext     string
}

// Walk обходит файлы в один поток (делегирует в WalkParallel с workers=1).
func (w *Walker) Walk() (<-chan FileInfo, <-chan error) {
	return w.WalkParallel(1)
}

// WalkParallel обходит файлы с параллельным чтением+хэшированием.
// WalkDir (обход каталогов) выполняется в одной горутине — это лёгкая операция.
// os.ReadFile + sha256 выполняются в workers горутинах — это тяжёлая I/O+CPU часть.
// При установленном preFilter файлы с совпадающими mtime+size отправляются напрямую
// в filesChan с пустым Hash (без чтения с диска).
func (w *Walker) WalkParallel(workers int) (<-chan FileInfo, <-chan error) {
	if workers < 1 {
		workers = 1
	}
	filesChan := make(chan FileInfo, workers*50)
	errorsChan := make(chan error, 100)
	fileQueue := make(chan walkTask, workers*50)

	// Обходчик каталогов — 1 горутина (только метаданные, без чтения файлов)
	go func() {
		defer close(fileQueue)

		err := filepath.WalkDir(w.rootPath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				errorsChan <- err
				return nil
			}

			if d.IsDir() {
				if strings.HasPrefix(d.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}

			relPath, err := filepath.Rel(w.rootPath, path)
			if err != nil {
				errorsChan <- fmt.Errorf("failed to get relative path: %w", err)
				return nil
			}
			relPath = filepath.ToSlash(relPath)

			if w.isExcluded(relPath) {
				return nil
			}
			if !w.isIncluded(relPath) {
				return nil
			}

			info, err := d.Info()
			if err != nil {
				errorsChan <- fmt.Errorf("failed to get file info for %s: %w", path, err)
				return nil
			}

			ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
			normalizedPath := filepath.ToSlash(path)

			// Pre-filter: если mtime+size совпадают — пропускаем чтение файла.
			// Отправляем метаданные с пустым Hash как маркер pre-filtered файла.
			if w.preFilter != nil {
				if fp, ok := w.preFilter[normalizedPath]; ok {
					if fp.Size == info.Size() && modTimeMatch(fp.ModTime, info.ModTime()) {
						encoding, language := getEncodingAndLanguage(ext)
						filesChan <- FileInfo{
							Path:       normalizedPath,
							RelPath:    relPath,
							Extension:  ext,
							Size:       info.Size(),
							Hash:       "", // маркер: файл не читался
							ModifiedAt: info.ModTime(),
							Encoding:   encoding,
							Language:   language,
						}
						return nil
					}
				}
			}

			fileQueue <- walkTask{path: path, relPath: relPath, info: info, ext: ext}
			return nil
		})

		if err != nil {
			errorsChan <- fmt.Errorf("walk error: %w", err)
		}
	}()

	// Воркеры чтения+хэширования — N горутин
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range fileQueue {
				content, err := os.ReadFile(task.path)
				if err != nil {
					errorsChan <- fmt.Errorf("failed to read %s: %w", task.path, err)
					continue
				}
				hash := computeHashBytes(content)
				encoding, language := getEncodingAndLanguage(task.ext)
				filesChan <- FileInfo{
					Path:       filepath.ToSlash(task.path),
					RelPath:    task.relPath,
					Extension:  task.ext,
					Size:       task.info.Size(),
					Hash:       hash,
					ModifiedAt: task.info.ModTime(),
					Encoding:   encoding,
					Language:   language,
					Content:    content,
				}
			}
		}()
	}

	// Закрытие channels после завершения всех воркеров
	go func() {
		wg.Wait()
		close(filesChan)
		close(errorsChan)
	}()

	return filesChan, errorsChan
}

// isExcluded проверяет, должен ли файл быть исключён
func (w *Walker) isExcluded(path string) bool {
	// Проверяем и полный относительный путь, и только basename,
	// чтобы работали паттерны обоих типов: "dir/*" и "*.bak".
	for _, re := range w.excludeRegexps {
		if re.MatchString(path) {
			return true
		}
		// Также проверяем только имя файла
		if re.MatchString(filepath.Base(path)) {
			return true
		}
	}
	return false
}

// isIncluded проверяет, должен ли файл быть включён
func (w *Walker) isIncluded(path string) bool {
	// При пустом списке include считаем, что ограничений нет.
	if len(w.includeRegexps) == 0 {
		return true
	}

	for _, re := range w.includeRegexps {
		if re.MatchString(path) || re.MatchString(filepath.Base(path)) {
			return true
		}
	}
	return false
}

// computeHashBytes вычисляет SHA256 хэш содержимого файла
func computeHashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// modTimeMatch сравнивает время модификации с допуском 1ms.
// PostgreSQL TIMESTAMPTZ хранит микросекунды, Windows os.Stat отдаёт 100ns.
// При сохранении в БД и чтении обратно точность теряется, поэтому точное
// сравнение time.Equal() не работает. Допуск 1ms покрывает разницу.
func modTimeMatch(a, b time.Time) bool {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Millisecond
}

// getEncodingAndLanguage возвращает кодировку и язык по расширению
func getEncodingAndLanguage(ext string) (string, string) {
	// Здесь зашито текущее знание о legacy-файлах Diasoft 5NT.
	// Это влияет и на чтение файлов, и на выбор parser implementation.
	switch ext {
	case "sql":
		return "CP866", "SQL"
	case "h":
		return "CP866", "H"
	case "pas":
		return "WIN1251", "PAS"
	case "inc":
		return "WIN1251", "INC"
	case "js":
		return "WIN1251", "JS"
	case "smf":
		return "WIN1251", "SMF"
	case "dfm":
		return "WIN1251", "DFM"
	case "tpr":
		return "CP866", "TPR"
	case "rpt":
		return "WIN1251", "RPT"
	case "xml":
		return "UTF8", "XML"
	case "t01":
		return "CP866", "T01"
	default:
		return "UTF8", "UNKNOWN"
	}
}

// GetSupportedExtensions возвращает список поддерживаемых расширений
func GetSupportedExtensions() []string {
	return []string{".sql", ".h", ".pas", ".inc", ".js", ".smf", ".dfm", ".tpr", ".rpt", ".xml", ".t01"}
}

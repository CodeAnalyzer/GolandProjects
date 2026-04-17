package fswalk

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
}

// Walker обходчик файлов
type Walker struct {
	rootPath        string
	includePatterns []string
	excludePatterns []string
	includeRegexps  []*regexp.Regexp
	excludeRegexps  []*regexp.Regexp
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

// Walk обходит файлы и возвращает канал FileInfo
func (w *Walker) Walk() (<-chan FileInfo, <-chan error) {
	filesChan := make(chan FileInfo, 100)
	errorsChan := make(chan error, 100)

	go func() {
		// Обход выполняется асинхронно: вызывающий код может параллельно читать
		// найденные файлы и передавать их дальше в indexing pipeline.
		defer close(filesChan)
		defer close(errorsChan)

		// Начинаем обход файлов в корневой директории
		err := filepath.Walk(w.rootPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				// Если произошла ошибка, отправляем ее в канал ошибок
				errorsChan <- err
				return nil // Продолжаем обход
			}

			// Пропускаем директории
			if info.IsDir() {
				// Пропускаем скрытые директории
				if strings.HasPrefix(info.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}

			// Получаем относительный путь
			relPath, err := filepath.Rel(w.rootPath, path)
			if err != nil {
				errorsChan <- fmt.Errorf("failed to get relative path: %w", err)
				return nil
			}

			// Нормализуем путь для Windows
			relPath = filepath.ToSlash(relPath)

			// Сначала применяем exclude-паттерны, чтобы быстро отсечь шумные файлы.
			if w.isExcluded(relPath) {
				return nil
			}

			// Затем применяем include-паттерны: только подходящие файлы уходят в индексатор.
			if !w.isIncluded(relPath) {
				return nil
			}

			// Вычисляем хэш
			hash, err := computeHash(path)
			if err != nil {
				errorsChan <- fmt.Errorf("failed to compute hash for %s: %w", path, err)
				return nil
			}

			// Расширение здесь выступает дешёвым классификатором языка и кодировки,
			// чтобы parser layer (слой парсеров) знал, как читать файл.
			ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
			encoding, language := getEncodingAndLanguage(ext)

			filesChan <- FileInfo{
				Path:       filepath.ToSlash(path),
				RelPath:    relPath,
				Extension:  ext,
				Size:       info.Size(),
				Hash:       hash,
				ModifiedAt: info.ModTime(),
				Encoding:   encoding,
				Language:   language,
			}

			return nil
		})

		if err != nil {
			errorsChan <- fmt.Errorf("walk error: %w", err)
		}
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

// computeHash вычисляет SHA256 хэш файла
func computeHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
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

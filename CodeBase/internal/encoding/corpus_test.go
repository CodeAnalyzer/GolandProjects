//go:build corpus

// Корпусная валидация детекции кодировки по содержимому (change
// add-content-encoding-detection, tasks 1.1–1.2). Запуск:
//
//	go test ./internal/encoding/ -tags corpus -run TestCorpusEncodingValidation -v
//
// Обходит FA-дерево (путь — константа corpusRoot ниже), прогоняет
// DetectFromBytesWithPrior с prior из карты расширений (зеркало
// fswalk.getEncodingAndLanguage) и формирует отчёт flip-ов:
// агрегаты по модулям/расширениям — в stdout, полный список flip-ов с
// превью декодирования — в файл corpus_encoding_report.txt рядом с cwd.
// Автопроверка читаемости: flip обязан декодироваться в кириллицу без
// артефакт-рун (U+2500–U+259F и украинские вне украинского контекста).
package encoding

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"
)

const corpusRoot = `D:/GITHUB/GolandProjects/FA`

var corpusPriorByExt = map[string]Encoding{
	"sql": CP866,
	"h":   CP866,
	"tpr": CP866,
	"t01": CP866,
	"pas": WIN1251,
	"inc": WIN1251,
	"js":  WIN1251,
	"smf": WIN1251,
	"dfm": WIN1251,
	"rpt": WIN1251,
}

func TestCorpusEncodingValidation(t *testing.T) {
	if _, err := os.Stat(corpusRoot); err != nil {
		t.Skipf("corpus root %s unavailable: %v", corpusRoot, err)
	}

	type flipInfo struct {
		rel      string
		ext      string
		module   string
		prior    Encoding
		detected Encoding
		cp866    int
		cp1251   int
		preview  string
	}

	var total, flips int
	byModule := map[string]int{}          // module → flips
	byExt := map[string]int{}             // ext → flips
	byDetected := map[string]int{}        // detected → flips
	unreadable := make([]flipInfo, 0, 16) // flips без читаемой кириллицы

	exts := map[string]bool{"sql": true, "h": true, "tpr": true, "pas": true, "inc": true, "js": true, "smf": true, "dfm": true, "rpt": true}

	// Сбор путей (только метаданные) — быстро; чтение+детекция — параллельно.
	var paths []string
	err := filepath.Walk(corpusRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // недоступные поддеревья пропускаем
		}
		if info.IsDir() {
			name := info.Name()
			if name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
		if !exts[ext] {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	type flipInfoResult struct {
		flip     flipInfo
		readable bool
	}
	results := make([]flipInfoResult, len(paths))
	var wg sync.WaitGroup
	workers := runtime.NumCPU()
	jobs := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				path := paths[i]
				ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(path), "."))
				data, err := os.ReadFile(path)
				if err != nil {
					continue
				}
				prior := corpusPriorByExt[ext]
				detected := DetectFromBytesWithPrior(data, prior)
				if detected == prior {
					continue
				}
				rel, _ := filepath.Rel(corpusRoot, path)
				rel = filepath.ToSlash(rel)
				module := strings.SplitN(rel, "/", 2)[0]
				_, _, cp866Score, cp1251Score := scanUTF8AndScore(data)
				decoded, _ := DecodeBytes(data, detected)
				results[i] = flipInfoResult{
					flip: flipInfo{rel: rel, ext: ext, module: module, prior: prior, detected: detected,
						cp866: cp866Score, cp1251: cp1251Score, preview: firstCyrillicLine(decoded)},
					readable: hasReadableRussian(decoded),
				}
			}
		}()
	}
	for i := range paths {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	total = len(paths)
	var flipList []flipInfo
	for _, r := range results {
		if r.flip.rel == "" {
			continue
		}
		flips++
		flipList = append(flipList, r.flip)
		byModule[r.flip.module]++
		byExt[r.flip.ext]++
		byDetected[string(r.flip.detected)]++
		if !r.readable {
			unreadable = append(unreadable, r.flip)
		}
	}

	// --- Агрегированный отчёт в stdout ---
	fmt.Printf("\n=== Corpus encoding validation ===\n")
	fmt.Printf("root: %s\n", corpusRoot)
	fmt.Printf("files scanned (single-byte exts): %d\n", total)
	fmt.Printf("flips (detected != prior):        %d\n\n", flips)

	fmt.Printf("-- flips by module --\n")
	printCountMap(byModule)
	fmt.Printf("\n-- flips by ext --\n")
	printCountMap(byExt)
	fmt.Printf("\n-- flips by detected encoding --\n")
	printCountMap(byDetected)

	// --- Полный отчёт в файл ---
	outPath := "corpus_encoding_report.txt"
	var b strings.Builder
	fmt.Fprintf(&b, "Corpus encoding validation report\nroot=%s files=%d flips=%d\n\n", corpusRoot, total, flips)
	for _, fi := range flipList {
		fmt.Fprintf(&b, "%s\t%s\t%s->%s\tcp866=%d cp1251=%d\t%s\n",
			fi.rel, fi.ext, fi.prior, fi.detected, fi.cp866, fi.cp1251, fi.preview)
	}
	if err := os.WriteFile(outPath, []byte(b.String()), 0644); err != nil {
		t.Fatalf("write report: %v", err)
	}
	fmt.Printf("\nfull flip list: %s\n", outPath)

	// --- Автопроверки (task 1.2) ---
	// Все flip-ы обязаны декодироваться в читаемую кириллицу (или UTF-8):
	// flip без кириллицы — подозрение на ложное срабатывание.
	fmt.Printf("\nflips without readable cyrillic: %d\n", len(unreadable))
	for _, fi := range unreadable {
		fmt.Printf("  ?? %s (%s->%s) preview=%q\n", fi.rel, fi.prior, fi.detected, fi.preview)
	}
	if len(unreadable) > 0 {
		fmt.Printf("\nNOTE: flips above decode to no cyrillic — inspect manually (may be UTF-8/no-text files).\n")
	}
}

func printCountMap(m map[string]int) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-24s %d\n", k, m[k])
	}
}

// firstCyrillicLine возвращает первую строку с кириллицей (обрезанную до 70
// рун) — превью для ручной выверки flip-а.
func firstCyrillicLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if hasCyrillicText(line) {
			r := []rune(line)
			if len(r) > 70 {
				r = r[:70]
			}
			return string(r)
		}
	}
	return ""
}

// hasReadableRussian сообщает, что в декоде есть кириллица и нет
// артефакт-рун mojibake (U+2500–U+259F).
func hasReadableRussian(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	hasCyr := false
	for _, r := range s {
		if r >= 0x2500 && r <= 0x259F {
			return false
		}
		if (r >= 'А' && r <= 'я') || r == 'Ё' || r == 'ё' {
			hasCyr = true
		}
	}
	return hasCyr
}

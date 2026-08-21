package fswalk

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestPatternToRegexp(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		match   string
		miss    string
	}{
		{name: "extension", pattern: "*.sql", match: "test.sql", miss: "test.pas"},
		{name: "directory", pattern: "temp/*", match: "temp/file.sql", miss: "other/file.sql"},
		{name: "question", pattern: "file?.h", match: "file1.h", miss: "file10.h"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			re := patternToRegexp(tt.pattern)
			if re == nil {
				t.Fatalf("patternToRegexp returned nil")
			}
			if !re.MatchString(tt.match) {
				t.Fatalf("regexp for %q must match %q", tt.pattern, tt.match)
			}
			if re.MatchString(tt.miss) {
				t.Fatalf("regexp for %q must not match %q", tt.pattern, tt.miss)
			}
		})
	}
}

func TestWalkerIncludeExclude(t *testing.T) {
	w := NewWalker("root", []string{"*.sql", "src/*.pas"}, []string{"*.bak", "temp/*"})

	if !w.isIncluded("query.sql") {
		t.Fatalf("expected query.sql to be included")
	}
	if !w.isIncluded("src/unit.pas") {
		t.Fatalf("expected src/unit.pas to be included")
	}
	if w.isIncluded("readme.txt") {
		t.Fatalf("expected readme.txt to be excluded by include list")
	}
	if !w.isExcluded("old.bak") {
		t.Fatalf("expected old.bak to be excluded")
	}
	if !w.isExcluded("temp/file.sql") {
		t.Fatalf("expected temp/file.sql to be excluded")
	}
}

func TestWalkerEmptyIncludeAllowsAll(t *testing.T) {
	w := NewWalker("root", nil, nil)
	if !w.isIncluded("readme.txt") {
		t.Fatalf("expected empty include list to allow file")
	}
}

func TestComputeHashBytes(t *testing.T) {
	got := computeHashBytes([]byte("abc"))
	want := "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("hash = %q, want %q", got, want)
	}
}

func TestGetEncodingAndLanguage(t *testing.T) {
	tests := []struct {
		ext          string
		wantEncoding string
		wantLanguage string
	}{
		{ext: "sql", wantEncoding: "CP866", wantLanguage: "SQL"},
		{ext: "h", wantEncoding: "CP866", wantLanguage: "H"},
		{ext: "pas", wantEncoding: "WIN1251", wantLanguage: "PAS"},
		{ext: "inc", wantEncoding: "WIN1251", wantLanguage: "INC"},
		{ext: "js", wantEncoding: "WIN1251", wantLanguage: "JS"},
		{ext: "smf", wantEncoding: "WIN1251", wantLanguage: "SMF"},
		{ext: "dfm", wantEncoding: "WIN1251", wantLanguage: "DFM"},
		{ext: "tpr", wantEncoding: "CP866", wantLanguage: "TPR"},
		{ext: "rpt", wantEncoding: "WIN1251", wantLanguage: "RPT"},
		{ext: "xml", wantEncoding: "UTF8", wantLanguage: "XML"},
		{ext: "t01", wantEncoding: "CP866", wantLanguage: "T01"},
		{ext: "txt", wantEncoding: "UTF8", wantLanguage: "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			gotEncoding, gotLanguage := getEncodingAndLanguage(tt.ext)
			if gotEncoding != tt.wantEncoding || gotLanguage != tt.wantLanguage {
				t.Fatalf("getEncodingAndLanguage(%q) = %q, %q; want %q, %q", tt.ext, gotEncoding, gotLanguage, tt.wantEncoding, tt.wantLanguage)
			}
		})
	}
}

func TestGetSupportedExtensions(t *testing.T) {
	want := []string{".sql", ".h", ".pas", ".inc", ".js", ".smf", ".dfm", ".tpr", ".rpt", ".xml", ".t01"}
	if got := GetSupportedExtensions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSupportedExtensions() = %v, want %v", got, want)
	}
}

func TestWalkReturnsIncludedFiles(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "query.sql"), "select 1")
	writeTestFile(t, filepath.Join(root, "unit.pas"), "unit Test;")
	writeTestFile(t, filepath.Join(root, "skip.txt"), "skip")
	writeTestFile(t, filepath.Join(root, "old.bak"), "skip")
	writeTestFile(t, filepath.Join(root, ".hidden", "hidden.sql"), "skip")

	w := NewWalker(root, []string{"*.sql", "*.pas"}, []string{"*.bak"})
	filesChan, errorsChan := w.Walk()

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}
	for err := range errorsChan {
		if err != nil {
			t.Fatalf("Walk returned error: %v", err)
		}
	}

	byRelPath := map[string]FileInfo{}
	for _, file := range files {
		byRelPath[file.RelPath] = file
	}
	if len(byRelPath) != 2 {
		t.Fatalf("walked files = %v, want 2 files", byRelPath)
	}
	if byRelPath["query.sql"].Encoding != "CP866" || byRelPath["query.sql"].Language != "SQL" {
		t.Fatalf("unexpected query.sql metadata: %+v", byRelPath["query.sql"])
	}
	if byRelPath["unit.pas"].Encoding != "WIN1251" || byRelPath["unit.pas"].Language != "PAS" {
		t.Fatalf("unexpected unit.pas metadata: %+v", byRelPath["unit.pas"])
	}
}

func writeTestFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("create dir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
}

func TestWalkParallelReturnsIncludedFiles(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "a.sql"), "select 1")
	writeTestFile(t, filepath.Join(root, "b.sql"), "select 2")
	writeTestFile(t, filepath.Join(root, "c.pas"), "unit C;")
	writeTestFile(t, filepath.Join(root, "skip.txt"), "skip")

	w := NewWalker(root, []string{"*.sql", "*.pas"}, nil)
	filesChan, errorsChan := w.WalkParallel(4)

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}
	for err := range errorsChan {
		if err != nil {
			t.Fatalf("WalkParallel returned error: %v", err)
		}
	}

	byRelPath := map[string]FileInfo{}
	for _, file := range files {
		byRelPath[file.RelPath] = file
	}
	if len(byRelPath) != 3 {
		t.Fatalf("walked files = %d, want 3", len(byRelPath))
	}
	for _, f := range files {
		if f.Hash == "" {
			t.Fatalf("file %s has empty hash (no pre-filter set)", f.RelPath)
		}
		if len(f.Content) == 0 {
			t.Fatalf("file %s has empty content", f.RelPath)
		}
	}
}

func TestWalkParallelPreFilterSkipsUnchanged(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "unchanged.sql"), "select 1")
	writeTestFile(t, filepath.Join(root, "modified.sql"), "select 2")

	// Получаем метаданные файла для pre-filter
	info1, _ := os.Stat(filepath.Join(root, "unchanged.sql"))

	// Pre-filter: unchanged.sql совпадает, modified.sql — нет (разный размер)
	normalizedPath := filepath.ToSlash(filepath.Join(root, "unchanged.sql"))
	w := NewWalker(root, []string{"*.sql"}, nil)
	w.SetPreFilter(map[string]FileFingerprint{
		normalizedPath: {Size: info1.Size(), ModTime: info1.ModTime()},
	})

	filesChan, errorsChan := w.WalkParallel(2)

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}
	for err := range errorsChan {
		if err != nil {
			t.Fatalf("WalkParallel returned error: %v", err)
		}
	}

	byRelPath := map[string]FileInfo{}
	for _, file := range files {
		byRelPath[file.RelPath] = file
	}
	if len(byRelPath) != 2 {
		t.Fatalf("walked files = %d, want 2", len(byRelPath))
	}

	// unchanged.sql — pre-filtered: Hash пустой, Content nil
	uc := byRelPath["unchanged.sql"]
	if uc.Hash != "" {
		t.Fatalf("unchanged.sql Hash = %q, want empty (pre-filtered)", uc.Hash)
	}
	if uc.Content != nil {
		t.Fatalf("unchanged.sql Content should be nil (pre-filtered)")
	}

	// modified.sql — прочитан нормально: Hash непустой, Content есть
	mod := byRelPath["modified.sql"]
	if mod.Hash == "" {
		t.Fatalf("modified.sql Hash is empty, expected real hash")
	}
	if len(mod.Content) == 0 {
		t.Fatalf("modified.sql Content is empty")
	}
}

func TestWalkParallelPreFilterSizeMismatchReadsFile(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "changed.sql"), "select 1")

	info, _ := os.Stat(filepath.Join(root, "changed.sql"))
	normalizedPath := filepath.ToSlash(filepath.Join(root, "changed.sql"))

	// Pre-filter с неверным размером — файл должен быть прочитан
	w := NewWalker(root, []string{"*.sql"}, nil)
	w.SetPreFilter(map[string]FileFingerprint{
		normalizedPath: {Size: info.Size() + 999, ModTime: info.ModTime()},
	})

	filesChan, _ := w.WalkParallel(1)

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Hash == "" {
		t.Fatalf("file should have real hash (size mismatch in pre-filter)")
	}
}

func TestWalkParallelPreFilterModTimeMismatchReadsFile(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "touched.sql"), "select 1")

	info, _ := os.Stat(filepath.Join(root, "touched.sql"))
	normalizedPath := filepath.ToSlash(filepath.Join(root, "touched.sql"))

	// Pre-filter с неверным mtime — файл должен быть прочитан
	w := NewWalker(root, []string{"*.sql"}, nil)
	w.SetPreFilter(map[string]FileFingerprint{
		normalizedPath: {Size: info.Size(), ModTime: info.ModTime().Add(-1 * time.Hour)},
	})

	filesChan, _ := w.WalkParallel(1)

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].Hash == "" {
		t.Fatalf("file should have real hash (mtime mismatch in pre-filter)")
	}
}

func TestWalkParallelPreFilterMicrosecondSkewMatches(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "db.sql"), "select 1")

	info, _ := os.Stat(filepath.Join(root, "db.sql"))
	normalizedPath := filepath.ToSlash(filepath.Join(root, "db.sql"))

	// Pre-filter с mtime сдвинутым на 500µs (имитация потери точности в PostgreSQL)
	w := NewWalker(root, []string{"*.sql"}, nil)
	w.SetPreFilter(map[string]FileFingerprint{
		normalizedPath: {Size: info.Size(), ModTime: info.ModTime().Add(500 * time.Microsecond)},
	})

	filesChan, _ := w.WalkParallel(1)

	var files []FileInfo
	for file := range filesChan {
		files = append(files, file)
	}

	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	// 500µs < 1ms tolerance → pre-filter должен сработать
	if files[0].Hash != "" {
		t.Fatalf("file should be pre-filtered (500µs skew < 1ms tolerance), got hash %q", files[0].Hash)
	}
}

func TestWalkParallelCtx_CancelStopsPipeline(t *testing.T) {
	root := t.TempDir()
	// Создаём много файлов, чтобы заполнить буфер каналов
	for i := 0; i < 200; i++ {
		writeTestFile(t, filepath.Join(root, fmt.Sprintf("file_%03d.sql", i)), "select 1")
	}

	ctx, cancel := context.WithCancel(context.Background())
	w := NewWalker(root, []string{"*.sql"}, nil)
	filesChan, errorsChan := w.WalkParallelCtx(ctx, 2)

	// Читаем первый файл, затем отменяем
	<-filesChan
	cancel()

	// Должны получить закрытые каналы (не зависнуть)
	done := make(chan struct{})
	go func() {
		for range filesChan {
		}
		for range errorsChan {
		}
		close(done)
	}()

	select {
	case <-done:
		// OK — каналы закрылись после отмены
	case <-time.After(5 * time.Second):
		t.Fatal("WalkParallelCtx hung after context cancellation")
	}
}

func TestWalkParallelCtx_PreFilterNoPanic(t *testing.T) {
	root := t.TempDir()
	// Создаём 100 файлов — все будут pre-filtered (mtime+size совпадают)
	for i := 0; i < 100; i++ {
		writeTestFile(t, filepath.Join(root, fmt.Sprintf("file_%03d.sql", i)), "select 1")
	}

	// Собираем fingerprints для всех файлов → все pre-filtered
	preFilter := make(map[string]FileFingerprint, 100)
	for i := 0; i < 100; i++ {
		p := filepath.Join(root, fmt.Sprintf("file_%03d.sql", i))
		info, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		preFilter[filepath.ToSlash(p)] = FileFingerprint{Size: info.Size(), ModTime: info.ModTime()}
	}

	w := NewWalker(root, []string{"*.sql"}, nil)
	w.SetPreFilter(preFilter)

	// 1 воркер — fileQueue будет пуст, read worker выйдет быстро.
	// WalkDir отправляет pre-filtered файлы напрямую в filesChan.
	// Без фикса: closer закрывает filesChan пока WalkDir ещё работает → panic.
	filesChan, errorsChan := w.WalkParallelCtx(context.Background(), 1)

	var count int
	for range filesChan {
		count++
	}
	for err := range errorsChan {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if count != 100 {
		t.Fatalf("expected 100 pre-filtered files, got %d", count)
	}
}

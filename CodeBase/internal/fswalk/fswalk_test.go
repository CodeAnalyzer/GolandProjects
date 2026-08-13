package fswalk

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
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

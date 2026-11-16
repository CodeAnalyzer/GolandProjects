package cmd

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/codebase/internal/review"
)

func TestReadReviewFileList(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "list.txt")
	content := "\ufeff# comment\r\n\r\n \"a.sql\" \r\n'b.sql'\r\n a.sql \r\nA.SQL\r\n#ignored\r\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write list: %v", err)
	}

	got, err := readReviewFileList(path)
	if err != nil {
		t.Fatalf("readReviewFileList: %v", err)
	}
	want := []string{"a.sql", "b.sql"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paths = %#v, want %#v", got, want)
	}
}

func TestReadReviewFileList_EmptyAndMissing(t *testing.T) {
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(empty, []byte("# only comments\n\n"), 0o644); err != nil {
		t.Fatalf("write empty list: %v", err)
	}
	got, err := readReviewFileList(empty)
	if err != nil {
		t.Fatalf("readReviewFileList(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("paths = %#v, want empty", got)
	}

	if _, err := readReviewFileList(filepath.Join(dir, "missing.txt")); err == nil {
		t.Fatal("expected error for missing list file")
	}
}

func TestTrimPairedQuotes(t *testing.T) {
	cases := map[string]string{
		`"a.sql"`:   "a.sql",
		`'b.sql'`:   "b.sql",
		`  c.sql  `: "  c.sql  ",
		`"a.sql`:    `"a.sql`,
		`a.sql"`:    `a.sql"`,
	}
	for in, want := range cases {
		if got := trimPairedQuotes(in); got != want {
			t.Fatalf("trimPairedQuotes(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewReviewBatchResponse_PartialSuccess(t *testing.T) {
	result := &review.BatchResult{
		Total:     2,
		Succeeded: 1,
		Failed:    1,
		Summary: review.Summary{
			Total:      1,
			ByRule:     map[review.RuleID]int{review.RuleUseSelectAll: 1},
			BySeverity: map[int]int{review.SeverityDeployStopper: 1},
		},
		Findings: []review.Finding{{
			Rule:     review.RuleUseSelectAll,
			Severity: review.SeverityDeployStopper,
			File:     "D:/repo/A.sql",
			Line:     3,
		}},
		Files: []review.BatchFileResult{
			{File: "A.sql", Status: "ok", Count: 1},
			{File: "B.sql", Status: "error", Error: &review.BatchError{Code: "not_indexed", Message: "file is not indexed: B.sql"}},
		},
	}

	resp := newReviewBatchResponse(result, []string{"useSelectAll"}, review.SeverityFineCode)
	if resp.Success {
		t.Fatal("Success = true, want false")
	}
	if resp.Mode != "batch" || resp.Command != "review" {
		t.Fatalf("mode/command = %q/%q", resp.Mode, resp.Command)
	}
	if resp.Count != 1 || len(resp.Items) != 1 {
		t.Fatalf("count/items = %d/%d, want 1/1", resp.Count, len(resp.Items))
	}
	if resp.Files.Total != 2 || resp.Files.Succeeded != 1 || resp.Files.Failed != 1 {
		t.Fatalf("files = %#v", resp.Files)
	}
	if len(resp.Files.Results) != 2 || resp.Files.Results[1].Error == nil || resp.Files.Results[1].Error.Code != "not_indexed" {
		t.Fatalf("results = %#v", resp.Files.Results)
	}
}

func TestNewReviewBatchResponse_NilSlicesBecomeEmptyArrays(t *testing.T) {
	resp := newReviewBatchResponse(&review.BatchResult{}, nil, review.SeverityFineCode)
	if resp.Items == nil {
		t.Fatal("Items is nil, want empty slice")
	}
	if resp.Files.Results == nil {
		t.Fatal("Files.Results is nil, want empty slice")
	}
	raw, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), `"items":null`) || strings.Contains(string(raw), `"results":null`) {
		t.Fatalf("nil slices serialized as null: %s", raw)
	}
}

func TestWriteReviewBatchResponseSingleDocument(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "review-*.json")
	if err != nil {
		t.Fatalf("create temp: %v", err)
	}
	defer file.Close()

	resp := newReviewBatchResponse(&review.BatchResult{
		Total:     1,
		Succeeded: 1,
		Files:     []review.BatchFileResult{{File: "A.sql", Status: "ok"}},
	}, nil, review.SeverityFineCode)
	if err := writeJSON(file, resp); err != nil {
		t.Fatalf("writeJSON: %v", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	decoder := json.NewDecoder(file)
	var first map[string]any
	if err := decoder.Decode(&first); err != nil {
		t.Fatalf("decode first document: %v", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("expected exactly one JSON document, second decode = %v", err)
	}
}

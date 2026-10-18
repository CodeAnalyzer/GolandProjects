package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/specfts"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = orig
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	return buf.String()
}

func TestPrintDescriptionResults_JSONEnvelope(t *testing.T) {
	origJSON, origLimit := outputJSON, limit
	outputJSON = true
	limit = 20
	defer func() { outputJSON, limit = origJSON, origLimit }()

	out := captureStdout(t, func() {
		if err := printDescriptionResults("query desc-search", map[string]string{"text": "тест"}, &query.DescriptionSearchOutput{Items: []query.DescriptionSearchResult{}}); err != nil {
			t.Errorf("print: %v", err)
		}
	})

	var response struct {
		Success       bool          `json:"success"`
		FormatVersion string        `json:"format_version"`
		Command       string        `json:"command"`
		Count         int           `json:"count"`
		Items         []interface{} `json:"items"`
		Meta          struct {
			Limit   int    `json:"limit"`
			Output  string `json:"output"`
			HasMore bool   `json:"has_more"`
		} `json:"meta"`
	}
	if err := json.Unmarshal([]byte(out), &response); err != nil {
		t.Fatalf("unmarshal: %v; out=%s", err, out)
	}
	if !response.Success || response.FormatVersion != "1.0" || response.Command != "query desc-search" {
		t.Fatalf("envelope = %+v", response)
	}
	if response.Count != 0 || response.Items == nil || len(response.Items) != 0 {
		t.Fatalf("count/items = %d/%v, want 0/[]", response.Count, response.Items)
	}
	if response.Meta.Limit != 20 || response.Meta.HasMore {
		t.Fatalf("meta = %+v", response.Meta)
	}
}

func TestPrintDescriptionResults_JSONHasMore(t *testing.T) {
	origJSON, origLimit := outputJSON, limit
	outputJSON = true
	limit = 1
	defer func() { outputJSON, limit = origJSON, origLimit }()

	items := []query.DescriptionSearchResult{
		{Kind: "procedure", Name: "P1", Source: "exact", Rank: 1, Snippet: "s"},
	}
	out := captureStdout(t, func() {
		if err := printDescriptionResults("query desc-search", nil, &query.DescriptionSearchOutput{Items: items, HasMore: true}); err != nil {
			t.Errorf("print: %v", err)
		}
	})
	if !strings.Contains(out, `"has_more": true`) {
		t.Fatalf("has_more отсутствует: %s", out)
	}
	if !strings.Contains(out, `"count": 1`) {
		t.Fatalf("count отсутствует: %s", out)
	}
}

func TestPrintDescriptionResults_JSONSemanticMeta(t *testing.T) {
	origJSON, origLimit := outputJSON, limit
	outputJSON = true
	limit = 20
	defer func() { outputJSON, limit = origJSON, origLimit }()

	coverage := 0.5
	threshold := 0.6
	out := captureStdout(t, func() {
		if err := printDescriptionResults("query desc-search", nil, &query.DescriptionSearchOutput{
			Items: []query.DescriptionSearchResult{},
			SemanticMeta: &specfts.SemanticDiagnostics{
				Reason:            "query_coverage",
				Coverage:          &coverage,
				CoverageThreshold: &threshold,
				OOVTerms:          []string{"asdkjhasd", "zzz"},
			},
		}); err != nil {
			t.Errorf("print: %v", err)
		}
	})

	var response struct {
		Meta struct {
			SemanticMeta *specfts.SemanticDiagnostics `json:"semantic_meta"`
		} `json:"meta"`
	}
	if err := json.Unmarshal([]byte(out), &response); err != nil {
		t.Fatalf("unmarshal: %v; out=%s", err, out)
	}
	meta := response.Meta.SemanticMeta
	if meta == nil || meta.Reason != "query_coverage" {
		t.Fatalf("semantic_meta = %+v", meta)
	}
	if meta.Coverage == nil || *meta.Coverage != 0.5 {
		t.Fatalf("coverage = %v, want 0.5", meta.Coverage)
	}
	if meta.CoverageThreshold == nil || *meta.CoverageThreshold != 0.6 {
		t.Fatalf("coverage_threshold = %v, want 0.6", meta.CoverageThreshold)
	}
}

func TestPrintDescriptionResults_TextTruncationNotice(t *testing.T) {
	origJSON, origLimit := outputJSON, limit
	outputJSON = false
	limit = 5
	defer func() { outputJSON, limit = origJSON, origLimit }()

	out := captureStdout(t, func() {
		if err := printDescriptionResults("query desc-search", nil, &query.DescriptionSearchOutput{Items: []query.DescriptionSearchResult{}, HasMore: true}); err != nil {
			t.Errorf("print: %v", err)
		}
	})
	if !strings.Contains(out, "truncated") {
		t.Fatalf("нет признака усечения в text-режиме: %s", out)
	}
}

package query

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/codebase/internal/specfts"
)

func TestBuildDescriptionSemanticMeta(t *testing.T) {
	vocab := specfts.BuildVocab([]specfts.Document{
		{ID: 1, Text: "клиенту банка"},
		{ID: 2, Text: "клиенту отчет"},
		{ID: 3, Text: "клиенту тариф"},
	}, 1, 1.0)

	t.Run("есть хиты и нет OOV — мета не возвращается", func(t *testing.T) {
		if meta := buildDescriptionSemanticMeta("клиенту", vocab, 3, 0.15, 5); meta != nil {
			t.Fatalf("expected nil meta, got %+v", meta)
		}
	})

	t.Run("пусто после порога, без OOV", func(t *testing.T) {
		meta := buildDescriptionSemanticMeta("клиенту", vocab, 12, 0.15, 0)
		if meta == nil || meta.Reason != "threshold" || meta.FilteredOut != 12 || meta.Threshold != 0.15 {
			t.Fatalf("unexpected meta: %+v", meta)
		}
		if meta.Hint != "" {
			t.Fatalf("no hint expected without oov, got %q", meta.Hint)
		}
	})

	t.Run("частичный OOV и пустая секция", func(t *testing.T) {
		meta := buildDescriptionSemanticMeta("заметка по клиенту", vocab, 12, 0.15, 0)
		if meta == nil || meta.Reason != "threshold" {
			t.Fatalf("unexpected meta: %+v", meta)
		}
		if len(meta.OOVTerms) != 1 || meta.OOVTerms[0] != "заметк" {
			t.Fatalf("expected oov ['заметк'], got %v", meta.OOVTerms)
		}
		if meta.Hint == "" {
			t.Fatal("expected hint referencing exact layer")
		}
	})

	t.Run("полный OOV — причина oov", func(t *testing.T) {
		meta := buildDescriptionSemanticMeta("заметка", vocab, 0, 0.15, 0)
		if meta == nil || meta.Reason != "oov" {
			t.Fatalf("unexpected meta: %+v", meta)
		}
		if meta.Hint == "" || meta.OOVTerms == nil {
			t.Fatalf("expected hint and oov terms: %+v", meta)
		}
	})
}

// MCP-контракт: desc-search возвращает объект {items, has_more, semantic_meta},
// а не массив; coverage 0.0 сохраняется в JSON.
func TestDescriptionSearchOutput_JSONShape(t *testing.T) {
	coverage := 0.0
	threshold := 0.6
	out := DescriptionSearchOutput{
		Items: []DescriptionSearchResult{},
		SemanticMeta: &specfts.SemanticDiagnostics{
			Reason:            "query_coverage",
			Coverage:          &coverage,
			CoverageThreshold: &threshold,
			OOVTerms:          []string{"asdkjhasd", "zzz"},
		},
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	jsonText := string(data)
	for _, want := range []string{`"items":[]`, `"has_more":false`, `"semantic_meta":`, `"reason":"query_coverage"`, `"coverage":0`} {
		if !strings.Contains(jsonText, want) {
			t.Fatalf("JSON %s не содержит %s", jsonText, want)
		}
	}
}

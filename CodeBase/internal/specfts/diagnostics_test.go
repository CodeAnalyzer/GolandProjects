package specfts

import (
	"encoding/json"
	"strings"
	"testing"
)

// Покрытие 0.0 сериализуется: указатель отличает «ноль» от «поле не задано».
func TestSemanticDiagnostics_ZeroCoverageJSON(t *testing.T) {
	coverage := 0.0
	threshold := 0.6
	meta := SemanticDiagnostics{
		Reason:            "query_coverage",
		Coverage:          &coverage,
		CoverageThreshold: &threshold,
		OOVTerms:          []string{"asdkjhasd"},
	}
	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := string(data)
	for _, want := range []string{`"coverage":0`, `"coverage_threshold":0.6`, `"reason":"query_coverage"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("JSON %s не содержит %s", out, want)
		}
	}
}

func TestOOVStems(t *testing.T) {
	vocab := BuildVocab([]Document{
		{ID: 1, Text: "клиенту банка"},
		{ID: 2, Text: "клиенту отчет"},
		{ID: 3, Text: "клиенту тариф"},
	}, 1, 1.0)

	oov := OOVStems("заметка по клиенту", vocab)
	if len(oov) != 1 || oov[0] != "заметк" {
		t.Fatalf("OOVStems = %v, want [заметк]", oov)
	}
	if got := OOVStems("клиенту", nil); got != nil {
		t.Fatalf("OOVStems(nil vocab) = %v, want nil", got)
	}
}

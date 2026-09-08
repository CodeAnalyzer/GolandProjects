package specsvc

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"

	"github.com/codebase/internal/errs"
)

func TestExecuteSpecSearchValidation(t *testing.T) {
	if _, err := ExecuteSpecSearch(context.Background(), nil, "", "", "", "both", 10); !errors.Is(err, errs.ErrSpecSearchEmpty) {
		t.Fatalf("empty query error = %v", err)
	}
	if _, err := ExecuteSpecSearch(context.Background(), nil, "query", "", "", "invalid", 10); err == nil {
		t.Fatal("invalid layer must return an error")
	}
}

func TestMergeSpecSearchHits(t *testing.T) {
	hits := []SpecSearchHit{
		{Level: "capability", EntityID: 1, Source: "tsvector"},
		{Level: "capability", EntityID: 1, Source: "trgm"},
		{Level: "requirement", EntityID: 2, Source: "trgm"},
	}
	got := mergeSpecSearchHits(hits, 2)
	if len(got) != 2 || got[0].Source != "tsvector" || got[1].EntityID != 2 {
		t.Fatalf("merged hits = %+v", got)
	}
}

func TestParsePGFloatArrayAndCosine(t *testing.T) {
	got := parsePGFloatArray("{1,2.5,-3}")
	if len(got) != 3 || got[0] != 1 || got[1] != 2.5 || got[2] != -3 {
		t.Fatalf("parsed array = %#v", got)
	}
	if similarity := cosineSim([]float64{1, 0}, []float64{1, 0}); math.Abs(similarity-1) > 1e-12 {
		t.Fatalf("cosine similarity = %v", similarity)
	}
	if similarity := cosineSim([]float64{1}, []float64{1, 0}); similarity != 0 {
		t.Fatalf("mismatched cosine similarity = %v", similarity)
	}
}

func TestCoverageMetric(t *testing.T) {
	if got := coverageMetric(sql.NullInt64{Int64: 0, Valid: true}, 17); got != 0 {
		t.Fatalf("stored zero metric = %d", got)
	}
	if got := coverageMetric(sql.NullInt64{}, 17); got != 17 {
		t.Fatalf("computed metric = %d", got)
	}
}

func TestNormalizeCoverageMode(t *testing.T) {
	if got, err := normalizeCoverageMode(" "); err != nil || got != "saved" {
		t.Fatalf("default mode = %q, %v", got, err)
	}
	if got, err := normalizeCoverageMode(" GAPS "); err != nil || got != "gaps" {
		t.Fatalf("gaps mode = %q, %v", got, err)
	}
	if _, err := normalizeCoverageMode("unknown"); err == nil {
		t.Fatal("invalid coverage mode must return an error")
	}
}

func TestNormalizeHistorySelectors(t *testing.T) {
	name, change, err := normalizeHistorySelectors(" capability ", "")
	if err != nil || name != "capability" || change != "" {
		t.Fatalf("capability selector = %q, %q, %v", name, change, err)
	}
	name, change, err = normalizeHistorySelectors("", " change ")
	if err != nil || name != "" || change != "change" {
		t.Fatalf("change selector = %q, %q, %v", name, change, err)
	}
	if _, _, err := normalizeHistorySelectors("", ""); err == nil {
		t.Fatal("missing history selector must return an error")
	}
	if _, _, err := normalizeHistorySelectors("capability", "change"); err == nil {
		t.Fatal("multiple history selectors must return an error")
	}
}

func TestSpecCoverageAndHistoryValidateBeforeDBAccess(t *testing.T) {
	ctx := context.Background()
	if _, err := ExecuteSpecCoverage(ctx, nil, "capability", "invalid"); err == nil {
		t.Fatal("invalid coverage mode must return an error")
	}
	if _, err := ExecuteSpecHistory(ctx, nil, ""); !errors.Is(err, errs.ErrSpecSearchEmpty) {
		t.Fatalf("legacy empty history selector error = %v", err)
	}
	if _, err := ExecuteSpecHistory(ctx, nil, "", ""); err == nil {
		t.Fatal("missing history selector must return an error")
	}
	if _, err := ExecuteSpecHistory(ctx, nil, "capability", "change"); err == nil {
		t.Fatal("multiple history selectors must return an error")
	}
}

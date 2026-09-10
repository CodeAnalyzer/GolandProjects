package specsvc

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/store"
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
	if _, err := ExecuteSpecCoverage(ctx, nil, "", "", ""); !errors.Is(err, errs.ErrSpecSearchEmpty) {
		t.Fatalf("empty product error = %v", err)
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

func TestBuildSpecConfigHierarchy_FlatAndDeep(t *testing.T) {
	rows := []store.SpecConfigHierarchyRow{
		{ID: 1, CapabilityName: "a", Title: "A"},
		{ID: 2, CapabilityName: "b", Title: "B"},
		{ID: 3, CapabilityName: "c", Title: "C"},
	}
	got := buildSpecConfigHierarchy(rows, 2)
	if len(got) != 3 {
		t.Fatalf("flat: len = %d, want 3", len(got))
	}
	for _, n := range got {
		if n.ChildrenCount != 0 || n.IsContainer {
			t.Fatalf("flat node %+v: unexpected fields", n)
		}
	}
}

func TestBuildSpecConfigHierarchy_DepthLimit(t *testing.T) {
	rows := []store.SpecConfigHierarchyRow{
		{ID: 1, CapabilityName: "root", Title: "Root"},
		{ID: 2, ParentID: sql.NullInt64{Int64: 1, Valid: true}, CapabilityName: "root/child", Title: "Child"},
		{ID: 3, ParentID: sql.NullInt64{Int64: 2, Valid: true}, CapabilityName: "root/child/grand", Title: "Grand"},
	}
	// depth=2: grandchild не раскрывается
	got := buildSpecConfigHierarchy(rows, 2)
	if len(got) != 1 || got[0].CapabilityName != "root" {
		t.Fatalf("depth=2: unexpected root: %+v", got)
	}
	child := got[0].Children[0]
	if child.CapabilityName != "root/child" {
		t.Fatalf("depth=2: child = %q", child.CapabilityName)
	}
	if child.ChildrenCount != 1 {
		t.Fatalf("depth=2: child children_count = %d, want 1", child.ChildrenCount)
	}
	if len(child.Children) != 0 {
		t.Fatalf("depth=2: child children = %d, want 0", len(child.Children))
	}
	// depth=3: grandchild раскрывается
	got3 := buildSpecConfigHierarchy(rows, 3)
	grand := got3[0].Children[0].Children[0]
	if grand.CapabilityName != "root/child/grand" {
		t.Fatalf("depth=3: grand = %q", grand.CapabilityName)
	}
}

func TestBuildSpecConfigHierarchy_Container(t *testing.T) {
	rows := []store.SpecConfigHierarchyRow{
		{ID: 1, CapabilityName: "billing", Title: ""},
		{ID: 2, ParentID: sql.NullInt64{Int64: 1, Valid: true}, CapabilityName: "billing/invoicing", Title: "Invoicing"},
	}
	got := buildSpecConfigHierarchy(rows, 2)
	if !got[0].IsContainer {
		t.Fatalf("billing: is_container = false, want true")
	}
	if got[0].Title != "" {
		t.Fatalf("billing: title = %q, want empty", got[0].Title)
	}
	if got[0].Children[0].IsContainer {
		t.Fatalf("invoicing: is_container = true, want false")
	}
}

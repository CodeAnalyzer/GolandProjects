package indexer

import (
	"testing"

	"github.com/codebase/internal/model"
)

func TestExtractCapabilityDeps_LinkedDomainsNoColon(t *testing.T) {
	slugToID := map[string]int64{
		"1|consumer-cession":          100,
		"1|consumer-credit":           200,
		"1|api-credit":                300,
		"1|client-ui-consumer":        400,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "consumer-cession",
		Notes:          "Связан с доменами `consumer-credit`, `api-credit`, `client-ui-consumer`",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 3 {
		t.Fatalf("expected 3 relations, got %d", len(relations))
	}
	expected := map[int64]bool{200: false, 300: false, 400: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExtractCapabilityDeps_Subdomains(t *testing.T) {
	slugToID := map[string]int64{
		"1|consumer-cession":                       100,
		"1|consumer-cession/portfolio-management":   101,
		"1|consumer-cession/nominal-calculation":     102,
		"1|consumer-cession/purchase":                103,
	}
	cap := &model.SpecCapability{
		ID:             100,
		SpecConfigID:   1,
		CapabilityName: "consumer-cession",
		Purpose:        "Домен разделён на пять поддоменов, каждый из которых описывает отдельную функциональную область.",
		Notes:          "Подробные спецификации по каждому поддомену: `portfolio-management`, `nominal-calculation`, `purchase`",
	}
	relations := extractCapabilityDeps(cap, slugToID)
	if len(relations) != 3 {
		t.Fatalf("expected 3 relations, got %d", len(relations))
	}
	expected := map[int64]bool{101: false, 102: false, 103: false}
	for _, r := range relations {
		if r.SourceID != 100 || r.RelationType != "depends_on_capability" {
			t.Fatalf("unexpected relation: %+v", r)
		}
		if _, ok := expected[r.TargetID]; !ok {
			t.Fatalf("unexpected targetID %d", r.TargetID)
		}
		expected[r.TargetID] = true
	}
	for id, found := range expected {
		if !found {
			t.Fatalf("missing relation to targetID %d", id)
		}
	}
}

func TestExpandHierarchyDeps_ParentToChildren(t *testing.T) {
	// parent=1 has children 2,3
	childrenOf := map[int64][]int64{1: {2, 3}}
	parentOf := map[int64]int64{2: 1, 3: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 1, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 1 direct + 2 hierarchy (children)
	if len(result) != 3 {
		t.Fatalf("expected 3 relations, got %d: %+v", len(result), result)
	}
	hierarchyCount := 0
	for _, r := range result {
		if r.Confidence == "hierarchy" {
			hierarchyCount++
			if r.TargetID != 2 && r.TargetID != 3 {
				t.Fatalf("unexpected hierarchy target %d", r.TargetID)
			}
		}
	}
	if hierarchyCount != 2 {
		t.Fatalf("expected 2 hierarchy relations, got %d", hierarchyCount)
	}
}

func TestExpandHierarchyDeps_ChildToParent(t *testing.T) {
	// child=2 has parent=1
	childrenOf := map[int64][]int64{1: {2}}
	parentOf := map[int64]int64{2: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 2, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 1 direct + 1 hierarchy (parent)
	if len(result) != 2 {
		t.Fatalf("expected 2 relations, got %d: %+v", len(result), result)
	}
	foundParent := false
	for _, r := range result {
		if r.Confidence == "hierarchy" && r.TargetID == 1 {
			foundParent = true
		}
	}
	if !foundParent {
		t.Fatalf("missing hierarchy relation to parent ID 1")
	}
}

func TestExpandHierarchyDeps_Dedup(t *testing.T) {
	// parent=1 has child=2; direct relation to both 1 and 2
	childrenOf := map[int64][]int64{1: {2}}
	parentOf := map[int64]int64{2: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 1, RelationType: "depends_on_capability", Confidence: "explicit"},
		{SourceType: "spec_capability", SourceID: 10, TargetType: "spec_capability", TargetID: 2, RelationType: "depends_on_capability", Confidence: "explicit"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// 2 direct + 0 hierarchy (child 2 already direct, parent 1 already direct)
	if len(result) != 2 {
		t.Fatalf("expected 2 relations (dedup), got %d: %+v", len(result), result)
	}
	for _, r := range result {
		if r.Confidence == "hierarchy" {
			t.Fatalf("unexpected hierarchy relation: %+v", r)
		}
	}
}

func TestExpandHierarchyDeps_SiblingNoParentCycle(t *testing.T) {
	// parent=1 has children 2,3 (siblings). Child 2 depends on child 3.
	// Hierarchy expansion should NOT add child→parent (2→1) because
	// source (2) and target (3) share the same parent (1).
	childrenOf := map[int64][]int64{1: {2, 3}}
	parentOf := map[int64]int64{2: 1, 3: 1}
	direct := []*model.Relation{
		{SourceType: "spec_capability", SourceID: 2, TargetType: "spec_capability", TargetID: 3, RelationType: "depends_on_capability", Confidence: "notes"},
	}
	result := expandHierarchyDeps(direct, childrenOf, parentOf)
	// Only the direct relation; no hierarchy relation to parent 1
	if len(result) != 1 {
		t.Fatalf("expected 1 relation (no parent cycle), got %d: %+v", len(result), result)
	}
	if result[0].Confidence == "hierarchy" {
		t.Fatalf("should not have hierarchy relation for siblings: %+v", result[0])
	}
}

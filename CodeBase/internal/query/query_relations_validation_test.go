package query

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/codebase/internal/errs"
	"github.com/codebase/internal/model"
)

// Валидация фильтров срабатывает до обращения к БД, поэтому в юнит-тестах
// используется Query без соединения (New(nil)).

func TestSearchRelations_RejectsUnknownRelationType(t *testing.T) {
	q := New(nil)
	_, err := q.SearchRelations(context.Background(), "", "", "", "", "__no_such_relation__", 10)
	if !errors.Is(err, errs.ErrUnknownRelationType) {
		t.Fatalf("err = %v, want ErrUnknownRelationType", err)
	}
	if !strings.Contains(err.Error(), "calls_procedure") {
		t.Fatalf("error must list valid relation types: %v", err)
	}
}

func TestSearchRelations_RejectsUnknownEntityTypes(t *testing.T) {
	q := New(nil)
	if _, err := q.SearchRelations(context.Background(), "__no_such_type__", "", "", "", "", 10); !errors.Is(err, errs.ErrUnknownRelationType) {
		t.Fatalf("unknown source_type: err = %v, want ErrUnknownRelationType", err)
	}
	if _, err := q.SearchRelations(context.Background(), "", "", "__no_such_type__", "", "", 10); !errors.Is(err, errs.ErrUnknownRelationType) {
		t.Fatalf("unknown target_type: err = %v, want ErrUnknownRelationType", err)
	}
}

func TestSearchRelations_AllEmptyFilters(t *testing.T) {
	q := New(nil)
	if _, err := q.SearchRelations(context.Background(), "", "", "", "", "", 10); !errors.Is(err, errs.ErrNoRelationFilters) {
		t.Fatalf("err = %v, want ErrNoRelationFilters", err)
	}
}

func TestValidateRelationFilters_NormalizesKindAliases(t *testing.T) {
	src, tgt, err := validateRelationFilters("service", "event", "")
	if err != nil {
		t.Fatalf("validateRelationFilters(kind aliases) error = %v, want nil", err)
	}
	if src != "api_contract" || tgt != "api_contract" {
		t.Fatalf("normalized = (%q, %q), want (api_contract, api_contract)", src, tgt)
	}
}

func TestValidateRelationFilters_PassthroughCanonicalAndEmpty(t *testing.T) {
	src, tgt, err := validateRelationFilters("sql_procedure", "  ", "")
	if err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
	if src != "sql_procedure" || tgt != "" {
		t.Fatalf("normalized = (%q, %q), want (sql_procedure, )", src, tgt)
	}
}

func TestNormalizeRelationEntityFilter_RejectsUnknown(t *testing.T) {
	if _, err := normalizeRelationEntityFilter("__no_such_type__"); !errors.Is(err, errs.ErrUnknownRelationType) {
		t.Fatalf("err = %v, want ErrUnknownRelationType", err)
	}
	if _, err := normalizeRelationEntityFilter("pas_class"); !errors.Is(err, errs.ErrUnknownRelationType) {
		t.Fatalf("pas_class err = %v, want ErrUnknownRelationType", err)
	}
	if got, err := normalizeRelationEntityFilter(""); err != nil || got != "" {
		t.Fatalf("empty filter = (%q, %v), want (, nil)", got, err)
	}
}

func TestRelationEntityMatchQueryParts_KindAliasAndNewTypes(t *testing.T) {
	cases := []struct {
		entityType string
		wantIn     string
	}{
		{"service", "api_contract"},
		{"api_contract", "api_contract"},
		{"dfm_form", "dfm_forms"},
		{"spec_capability", "spec_capabilities"},
		{"spec_scenario", "spec_scenarios"},
	}
	for _, tc := range cases {
		parts, ok := relationEntityMatchQueryParts(tc.entityType, true)
		if !ok {
			t.Fatalf("relationEntityMatchQueryParts(%q) ok = false, want true", tc.entityType)
		}
		if len(parts) != 1 || !strings.Contains(parts[0], tc.wantIn) {
			t.Fatalf("relationEntityMatchQueryParts(%q) = %v, want single part containing %q", tc.entityType, parts, tc.wantIn)
		}
	}
	if _, ok := relationEntityMatchQueryParts("__no_such_type__", true); ok {
		t.Fatalf("unknown type must return ok = false")
	}
}

func TestRelationEntityMatchQueryParts_EmptyCoversDictionary(t *testing.T) {
	parts, ok := relationEntityMatchQueryParts("", true)
	if !ok {
		t.Fatalf("empty type must resolve all sources, got ok = false")
	}
	if len(parts) != len(model.RelationEntityTypes) {
		t.Fatalf("empty type parts = %d, want %d (dictionary size)", len(parts), len(model.RelationEntityTypes))
	}
}

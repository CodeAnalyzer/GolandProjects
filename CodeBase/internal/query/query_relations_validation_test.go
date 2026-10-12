package query

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/codebase/internal/errs"
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

package store

import (
	"context"
	"testing"
)

// TestNormalizeTableLookupNames: приведение к нижнему регистру, отбрасывание
// пустых имён и дубликатов с сохранением порядка первого вхождения.
func TestNormalizeTableLookupNames(t *testing.T) {
	got := normalizeTableLookupNames([]string{" tRequisitParticipantSB ", "", "TCLIENTSERVICE", "trequisitparticipantsb"})
	want := []string{"trequisitparticipantsb", "tclientservice"}
	if len(got) != len(want) {
		t.Fatalf("normalizeTableLookupNames = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalizeTableLookupNames[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len(normalizeTableLookupNames(nil)) != 0 {
		t.Fatal("normalizeTableLookupNames(nil) must return empty slice")
	}
}

// TestBatchLookupTableExistenceEvidence_NoNames: пустой набор имён не обращается
// к БД и возвращает пустую карту (путь без соединения).
func TestBatchLookupTableExistenceEvidence_NoNames(t *testing.T) {
	var db *DB
	got, err := db.BatchLookupTableExistenceEvidence(context.Background(), []string{"", "  "}, ProductBranchPairs{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("result = %v, want empty map", got)
	}
}

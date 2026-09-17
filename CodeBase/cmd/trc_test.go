package cmd

import (
	"testing"

	"github.com/codebase/internal/trcsvc"
)

func TestFormatTRCEventsHeader(t *testing.T) {
	got := formatTRCEventsHeader(&trcsvc.EventsResult{ReturnedCount: 1, FilteredCount: 2, TotalCount: 3, Limit: 4})
	want := "1 event(s) returned (2 matched, 3 total, limit 4):\n\n"
	if got != want {
		t.Fatalf("header = %q, want %q", got, want)
	}
}

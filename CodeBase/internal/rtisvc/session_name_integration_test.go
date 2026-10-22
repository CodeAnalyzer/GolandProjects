//go:build integration

package rtisvc

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertNamedRTISession(t *testing.T, db *store.DB, path, name string) int64 {
	t.Helper()
	result := &rti.RTIParseResult{Summary: rti.RTISummary{FilePath: path, FileSize: 10}}
	id, err := rti.SaveSession(context.Background(), db, result, path, name)
	if err != nil {
		t.Fatalf("SaveSession(%s): %v", path, err)
	}
	return id
}

func TestParseSessionName_RTI(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := writeTempFile(t, ".rti", rtiSample)

	first, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse default name: %v", err)
	}
	session, err := rti.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if session.SessionName != filepath.Base(p) {
		t.Fatalf("default session_name = %q, want %q", session.SessionName, filepath.Base(p))
	}
	summary, err := ExecuteSummary(ctx, db, SessionSource{SessionID: first.SessionID})
	if err != nil {
		t.Fatalf("ExecuteSummary: %v", err)
	}
	if summary.Session == nil || summary.Session.SessionName != filepath.Base(p) {
		t.Fatalf("summary session = %+v, want session_name %q", summary.Session, filepath.Base(p))
	}

	renamed, err := ExecuteParse(ctx, db, p, ParseOptions{Name: "Повторный анализ"})
	if err != nil {
		t.Fatalf("ExecuteParse explicit name on reuse: %v", err)
	}
	if renamed.SessionID != first.SessionID {
		t.Fatalf("reuse renamed session id = %d, want %d", renamed.SessionID, first.SessionID)
	}
	session, err = rti.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession after rename: %v", err)
	}
	if session.SessionName != "Повторный анализ" {
		t.Fatalf("session_name after rename = %q", session.SessionName)
	}

	withoutName, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse reuse without name: %v", err)
	}
	if withoutName.SessionID != first.SessionID {
		t.Fatalf("reuse without name id = %d, want %d", withoutName.SessionID, first.SessionID)
	}
	session, err = rti.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession after reuse without name: %v", err)
	}
	if session.SessionName != "Повторный анализ" {
		t.Fatalf("reuse without name changed session_name to %q", session.SessionName)
	}
}

func TestDeleteByName_RTI(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	first := insertNamedRTISession(t, db, "a.rti", "trace.rti")
	second := insertNamedRTISession(t, db, "b.rti", "trace.rti")
	other := insertNamedRTISession(t, db, "c.rti", "other.rti")

	result, err := ExecuteDelete(ctx, db, DeleteParams{Name: "trace.rti"})
	if err != nil {
		t.Fatalf("ExecuteDelete by name: %v", err)
	}
	if !result.Deleted || result.DeletedCount != 2 {
		t.Fatalf("DeleteResult = %+v, want deleted_count=2", result)
	}
	if len(result.SessionIDs) != 2 || result.SessionIDs[0] != first || result.SessionIDs[1] != second {
		t.Fatalf("session_ids = %v, want [%d %d]", result.SessionIDs, first, second)
	}
	if _, err := rti.GetSession(ctx, db, other); err != nil {
		t.Fatalf("unrelated session removed: %v", err)
	}

	single := insertNamedRTISession(t, db, "d.rti", "single.rti")
	result, err = ExecuteDelete(ctx, db, DeleteParams{Name: "single.rti"})
	if err != nil {
		t.Fatalf("ExecuteDelete single by name: %v", err)
	}
	if result.DeletedCount != 1 || result.SessionID != single || result.FilePath != "d.rti" {
		t.Fatalf("single DeleteResult = %+v", result)
	}

	if _, err := ExecuteDelete(ctx, db, DeleteParams{SessionID: other, Name: "other.rti"}); err == nil {
		t.Fatal("delete with both selectors must fail")
	}
	if _, err := ExecuteDelete(ctx, db, DeleteParams{}); err == nil {
		t.Fatal("delete without selectors must fail")
	}
	if _, err := ExecuteDelete(ctx, db, DeleteParams{Name: "missing.rti"}); err == nil {
		t.Fatal("delete by missing name must fail")
	}
}

func TestPruneModes_RTI(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	old := insertNamedRTISession(t, db, "old.rti", "old.rti")
	fresh := insertNamedRTISession(t, db, "fresh.rti", "fresh.rti")
	if _, err := db.Exec(`UPDATE rti_sessions SET parsed_at = NOW() - INTERVAL '25 hours' WHERE id = $1`, old); err != nil {
		t.Fatalf("age old session: %v", err)
	}
	if _, err := db.Exec(`UPDATE rti_sessions SET parsed_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, fresh); err != nil {
		t.Fatalf("age fresh session: %v", err)
	}

	hours := 24
	result, err := ExecutePrune(ctx, db, PruneParams{KeepHours: &hours})
	if err != nil {
		t.Fatalf("ExecutePrune keep_hours: %v", err)
	}
	if result.DeletedCount != 1 || result.KeptHours == nil || *result.KeptHours != hours {
		t.Fatalf("PruneResult = %+v, want deleted_count=1 and kept_hours=24", result)
	}
	if result.Cutoff == nil || time.Since(*result.Cutoff) < 23*time.Hour {
		t.Fatalf("cutoff = %v, want about 24h ago", result.Cutoff)
	}
	if _, err := rti.GetSession(ctx, db, old); err == nil {
		t.Fatal("old session was not removed")
	}
	if _, err := rti.GetSession(ctx, db, fresh); err != nil {
		t.Fatalf("fresh session removed: %v", err)
	}

	keepLast := 0
	if _, err := ExecutePrune(ctx, db, PruneParams{KeepLast: &keepLast, KeepHours: &hours}); err == nil {
		t.Fatal("prune with both modes must fail")
	}
	zero := 0
	if _, err := ExecutePrune(ctx, db, PruneParams{KeepHours: &zero}); err == nil {
		t.Fatal("prune with keep_hours=0 must fail")
	}

	result, err = ExecutePrune(ctx, db, PruneParams{})
	if err != nil {
		t.Fatalf("ExecutePrune legacy default: %v", err)
	}
	if result.DeletedCount != 1 || result.KeptLast != 0 {
		t.Fatalf("legacy prune = %+v, want delete all remaining", result)
	}
}

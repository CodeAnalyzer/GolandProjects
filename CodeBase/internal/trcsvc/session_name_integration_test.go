//go:build integration

package trcsvc

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
	"github.com/codebase/internal/trc"
)

func insertNamedTRCSession(t *testing.T, db *store.DB, path, name string) int64 {
	t.Helper()
	result := &trc.TRCParseResult{Header: &trc.TraceHeader{}}
	id, err := trc.SaveSession(context.Background(), db, result, path, name, 10)
	if err != nil {
		t.Fatalf("SaveSession(%s): %v", path, err)
	}
	return id
}

func TestParseSessionName_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	p := trcTestPath(t)

	first, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse default name: %v", err)
	}
	session, err := trc.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if session.SessionName != filepath.Base(p) {
		t.Fatalf("default session_name = %q, want %q", session.SessionName, filepath.Base(p))
	}

	renamed, err := ExecuteParse(ctx, db, p, ParseOptions{Name: "Проблемный SPID"})
	if err != nil {
		t.Fatalf("ExecuteParse explicit name on reuse: %v", err)
	}
	if renamed.SessionID != first.SessionID {
		t.Fatalf("reuse renamed session id = %d, want %d", renamed.SessionID, first.SessionID)
	}
	session, err = trc.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession after rename: %v", err)
	}
	if session.SessionName != "Проблемный SPID" {
		t.Fatalf("session_name after rename = %q", session.SessionName)
	}

	withoutName, err := ExecuteParse(ctx, db, p, ParseOptions{})
	if err != nil {
		t.Fatalf("ExecuteParse reuse without name: %v", err)
	}
	if withoutName.SessionID != first.SessionID {
		t.Fatalf("reuse without name id = %d, want %d", withoutName.SessionID, first.SessionID)
	}
	session, err = trc.GetSession(ctx, db, first.SessionID)
	if err != nil {
		t.Fatalf("GetSession after reuse without name: %v", err)
	}
	if session.SessionName != "Проблемный SPID" {
		t.Fatalf("reuse without name changed session_name to %q", session.SessionName)
	}
}

func TestDeleteByName_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	first := insertNamedTRCSession(t, db, "a.trc", "trace.trc")
	second := insertNamedTRCSession(t, db, "b.trc", "trace.trc")
	other := insertNamedTRCSession(t, db, "c.trc", "other.trc")

	result, err := ExecuteDelete(ctx, db, DeleteParams{Name: "trace.trc"})
	if err != nil {
		t.Fatalf("ExecuteDelete by name: %v", err)
	}
	if !result.Deleted || result.DeletedCount != 2 {
		t.Fatalf("DeleteResult = %+v, want deleted_count=2", result)
	}
	if len(result.SessionIDs) != 2 || result.SessionIDs[0] != first || result.SessionIDs[1] != second {
		t.Fatalf("session_ids = %v, want [%d %d]", result.SessionIDs, first, second)
	}
	if _, err := trc.GetSession(ctx, db, other); err != nil {
		t.Fatalf("unrelated session removed: %v", err)
	}

	single := insertNamedTRCSession(t, db, "d.trc", "single.trc")
	result, err = ExecuteDelete(ctx, db, DeleteParams{Name: "single.trc"})
	if err != nil {
		t.Fatalf("ExecuteDelete single by name: %v", err)
	}
	if result.DeletedCount != 1 || result.SessionID != single || result.FilePath != "d.trc" {
		t.Fatalf("single DeleteResult = %+v", result)
	}

	if _, err := ExecuteDelete(ctx, db, DeleteParams{SessionID: other, Name: "other.trc"}); err == nil {
		t.Fatal("delete with both selectors must fail")
	}
	if _, err := ExecuteDelete(ctx, db, DeleteParams{}); err == nil {
		t.Fatal("delete without selectors must fail")
	}
	if _, err := ExecuteDelete(ctx, db, DeleteParams{Name: "missing.trc"}); err == nil {
		t.Fatal("delete by missing name must fail")
	}
}

func TestPruneModes_TRC(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	old := insertNamedTRCSession(t, db, "old.trc", "old.trc")
	fresh := insertNamedTRCSession(t, db, "fresh.trc", "fresh.trc")
	if _, err := db.Exec(`UPDATE trc_sessions SET parsed_at = NOW() - INTERVAL '25 hours' WHERE id = $1`, old); err != nil {
		t.Fatalf("age old session: %v", err)
	}
	if _, err := db.Exec(`UPDATE trc_sessions SET parsed_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, fresh); err != nil {
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
	if _, err := trc.GetSession(ctx, db, old); err == nil {
		t.Fatal("old session was not removed")
	}
	if _, err := trc.GetSession(ctx, db, fresh); err != nil {
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

//go:build integration

package trc

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

func TestPruneAndDeleteTRCSessions(t *testing.T) {
	db := testutil.Open(t)

	insert := func(path string) int64 {
		t.Helper()
		var id int64
		err := db.QueryRow(
			`INSERT INTO trc_sessions (file_path, file_size, total_events)
			 VALUES ($1, 10, 1) RETURNING id`,
			path,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert session %s: %v", path, err)
		}
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, duration_ms) VALUES ($1, 10, 'RPC:Completed', 5)`, id); err != nil {
			t.Fatalf("insert event: %v", err)
		}
		return id
	}

	id1 := insert("s1.trc")
	id2 := insert("s2.trc")
	id3 := insert("s3.trc")

	deleted, err := PruneSessions(context.Background(), db, 1)
	if err != nil {
		t.Fatalf("PruneSessions(1): %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	sessions, err := ListSessions(context.Background(), db, 10)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != id3 {
		t.Fatalf("remaining = %+v, want %d", sessions, id3)
	}
	var leftover int
	if err := db.QueryRow(`SELECT count(*) FROM trc_events WHERE session_id IN ($1, $2)`, id1, id2).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("orphan events = %d", leftover)
	}

	if err := DeleteSession(context.Background(), db, id3); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	id4 := insert("s4.trc")
	if err := DeleteSession(context.Background(), db, id3); err != nil {
		t.Fatalf("DeleteSession missing: %v", err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM trc_sessions WHERE id=$1)`, id4).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("unrelated session was deleted")
	}

	n, err := PruneSessions(context.Background(), db, 0)
	if err != nil {
		t.Fatalf("PruneSessions(0): %v", err)
	}
	if n != 1 {
		t.Fatalf("truncate count = %d, want 1", n)
	}
	if id := insert("s5.trc"); id == 0 {
		t.Fatal("insert after truncate failed")
	}
}

func TestLoadProceduresAggregated_CompletedOnlyAndZeroDuration(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('agg-test.trc', 10, 4) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)
	for _, e := range []struct {
		name     string
		duration int
	}{
		{"SP:Completed", 0}, {"SP:Completed", 100}, {"SP:StmtCompleted", 100}, {"SQL:StmtCompleted", 100},
	} {
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, procedure, duration_ms) VALUES ($1, 10, $2, 'ProcA', $3)`, sessionID, e.name, e.duration); err != nil {
			t.Fatal(err)
		}
	}
	aggs, err := LoadProceduresAggregated(context.Background(), db, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 1 || aggs[0].Count != 2 || aggs[0].TotalMs != 100 || aggs[0].MinMs != 0 || aggs[0].MaxMs != 100 || aggs[0].AvgMs != 50 {
		t.Fatalf("got %+v", aggs)
	}
}

func TestLoadEventsFilteredAndCount(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('filter-test.trc', 10, 3) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)
	for _, proc := range []string{"ProcA", "ProcA", "ProcB"} {
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, procedure, duration_ms) VALUES ($1, 10, 'SP:Completed', $2, 1)`, sessionID, proc); err != nil {
			t.Fatal(err)
		}
	}
	f := TRCEventFilter{Procedure: "ProcA"}
	events, err := LoadEventsFiltered(context.Background(), db, sessionID, f, 1)
	if err != nil {
		t.Fatal(err)
	}
	count, err := LoadEventCountFiltered(context.Background(), db, sessionID, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || count != 2 {
		t.Fatalf("len=%d count=%d, want 1/2", len(events), count)
	}
}

func TestLoadEventsForTree_DiagnosticFilteredFromRoot(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 4) RETURNING id`,
		"diag-test.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() {
		_ = DeleteSession(context.Background(), db, sessionID)
	}()

	events := []struct {
		name      string
		class     int
		spid      int
		parentID  *int
		procedure string
	}{
		{"SP:Recompile", 37, 76, nil, ""},
		{"SQL:StmtRecompile", 166, 76, nil, ""},
		{"RPC:Starting", 10, 76, nil, "ProcA"},
		{"RPC:Completed", 11, 76, nil, "ProcA"},
	}
	for _, e := range events {
		_, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms)
			 VALUES ($1, $2, $3, $4, $5, $6, 0)`,
			sessionID, e.class, e.name, e.spid, e.parentID, e.procedure,
		)
		if err != nil {
			t.Fatalf("insert event %s: %v", e.name, err)
		}
	}

	treeEvents, err := LoadEventsForTree(context.Background(), db, sessionID, 76, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}
	for _, ev := range treeEvents {
		if ev.EventName != "RPC:Starting" && ev.EventName != "RPC:Completed" {
			t.Errorf("diagnostic event %q leaked into tree", ev.EventName)
		}
	}
	if len(treeEvents) == 0 {
		t.Fatal("expected at least RPC:Starting/Completed events in tree, got 0")
	}
}

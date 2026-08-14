//go:build integration

package rti

import (
	"testing"

	"github.com/codebase/internal/store/testutil"
)

func TestPruneAndDeleteRTISessions(t *testing.T) {
	db := testutil.Open(t)

	insert := func(path string) int64 {
		t.Helper()
		var id int64
		err := db.QueryRow(
			`INSERT INTO rti_sessions (file_path, file_size, total_calls, errors_count, max_nest_level, unparsed_lines)
			 VALUES ($1, 10, 1, 0, 0, 0) RETURNING id`,
			path,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert session %s: %v", path, err)
		}
		if _, err := db.Exec(`INSERT INTO rti_calls (session_id, procedure, enter_line, exit_line, elapsed_ms, nest_level, module_id, tran_count, begin_cnt, spid) VALUES ($1, 'P', 1, 2, 1, 0, 0, 0, 0, 1)`, id); err != nil {
			t.Fatalf("insert call: %v", err)
		}
		return id
	}

	id1 := insert("s1.rti")
	id2 := insert("s2.rti")
	id3 := insert("s3.rti")

	deleted, err := PruneSessions(db, 1)
	if err != nil {
		t.Fatalf("PruneSessions(1): %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}

	sessions, err := ListSessions(db, 10)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != id3 {
		t.Fatalf("remaining sessions = %+v, want only %d", sessions, id3)
	}
	var leftover int
	if err := db.QueryRow(`SELECT count(*) FROM rti_calls WHERE session_id IN ($1, $2)`, id1, id2).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("orphan calls = %d", leftover)
	}

	keepID := id3
	if err := DeleteSession(db, keepID); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	id4 := insert("s4.rti")
	if err := DeleteSession(db, keepID); err != nil {
		t.Fatalf("DeleteSession missing: %v", err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM rti_sessions WHERE id=$1)`, id4).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("unrelated session was deleted")
	}

	n, err := PruneSessions(db, 0)
	if err != nil {
		t.Fatalf("PruneSessions(0): %v", err)
	}
	if n != 1 {
		t.Fatalf("truncate count = %d, want 1", n)
	}
	id5 := insert("s5.rti")
	if id5 == 0 {
		t.Fatal("insert after truncate failed")
	}

	if _, err := PruneSessions(db, -1); err == nil {
		t.Log("PruneSessions(-1) accepted; documented as current behavior")
	}
}

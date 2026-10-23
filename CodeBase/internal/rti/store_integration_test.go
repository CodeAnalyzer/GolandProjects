//go:build integration

package rti

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/store"
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
	if err := DeleteSession(context.Background(), db, keepID); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	id4 := insert("s4.rti")
	if err := DeleteSession(context.Background(), db, keepID); err != nil {
		t.Fatalf("DeleteSession missing: %v", err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM rti_sessions WHERE id=$1)`, id4).Scan(&exists); err != nil {
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
	id5 := insert("s5.rti")
	if id5 == 0 {
		t.Fatal("insert after truncate failed")
	}

	if _, err := PruneSessions(context.Background(), db, -1); err == nil {
		t.Log("PruneSessions(-1) accepted; documented as current behavior")
	}
}

// insertPlainSession создаёт сессию с одним вызовом без клиентских событий.
func insertPlainSession(t *testing.T, db *store.DB, path string) int64 {
	t.Helper()
	var sid int64
	err := db.QueryRow(
		`INSERT INTO rti_sessions (file_path, file_size, total_calls, errors_count, max_nest_level, unparsed_lines)
		 VALUES ($1, 10, 1, 0, 0, 0) RETURNING id`,
		path,
	).Scan(&sid)
	if err != nil {
		t.Fatalf("insert session %s: %v", path, err)
	}
	if _, err := db.Exec(`INSERT INTO rti_calls (session_id, procedure, enter_line, exit_line, elapsed_ms, nest_level, module_id, tran_count, begin_cnt, spid) VALUES ($1, 'P', 1, 2, 1, 0, 0, 0, 0, 1)`, sid); err != nil {
		t.Fatalf("insert call: %v", err)
	}
	return sid
}

// insertCorrelatedSession создаёт сессию с вызовом и клиентским событием,
// ссылающимся на этот вызов через server_call_id, и возвращает id сессии.
func insertCorrelatedSession(t *testing.T, db *store.DB, path string) int64 {
	t.Helper()
	var sid int64
	err := db.QueryRow(
		`INSERT INTO rti_sessions (file_path, file_size, total_calls, errors_count, max_nest_level, unparsed_lines, client_events_count)
		 VALUES ($1, 10, 1, 0, 0, 0, 1) RETURNING id`,
		path,
	).Scan(&sid)
	if err != nil {
		t.Fatalf("insert session %s: %v", path, err)
	}
	var callID int64
	err = db.QueryRow(
		`INSERT INTO rti_calls (session_id, procedure, enter_line, exit_line, elapsed_ms, nest_level, module_id, tran_count, begin_cnt, spid)
		 VALUES ($1, 'P', 1, 2, 1, 0, 0, 0, 0, 1) RETURNING id`,
		sid,
	).Scan(&callID)
	if err != nil {
		t.Fatalf("insert call for %s: %v", path, err)
	}
	if _, err := db.Exec(
		`INSERT INTO rti_client_events (session_id, pid, seq_no, line_no, kind, server_call_id)
		 VALUES ($1, 100, 1, 1, 'sql_block', $2)`,
		sid, callID,
	); err != nil {
		t.Fatalf("insert client event for %s: %v", path, err)
	}
	return sid
}

// degradeToLegacyFK пересоздаёт FK server_call_id без правила удаления
// (NO ACTION) — имитирует БД, созданную до миграции.
func degradeToLegacyFK(t *testing.T, db *store.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE rti_client_events DROP CONSTRAINT rti_client_events_server_call_id_fkey`); err != nil {
		t.Fatalf("drop fk: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE rti_client_events ADD CONSTRAINT rti_client_events_server_call_id_fkey FOREIGN KEY (server_call_id) REFERENCES rti_calls(id)`); err != nil {
		t.Fatalf("add legacy fk: %v", err)
	}
}

// assertSessionGone проверяет, что от сессии не осталось строк ни в одной таблице.
func assertSessionGone(t *testing.T, db *store.DB, sid int64) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM rti_sessions WHERE id = $1`, sid).Scan(&n); err != nil {
		t.Fatalf("count rti_sessions: %v", err)
	}
	if n != 0 {
		t.Fatalf("session %d still present in rti_sessions", sid)
	}
	// rti_params/rti_checkpoints удаляются каскадом вместе с rti_calls,
	// поэтому их покрывает проверка calls = 0
	for _, table := range []string{"rti_calls", "rti_blog_blocks", "rti_blog_tables", "rti_client_events"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM `+table+` WHERE session_id = $1`, sid).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("session %d: %d leftover rows in %s", sid, n, table)
		}
	}
}

// TestDeletePruneWithCorrelatedClientEvents: удаление/prune сессий, у которых
// клиентские события ссылаются на вызовы (server_call_id) —Regression-тест
// FK-ошибки, обрушившей `rti prune --keep-last`.
func TestDeletePruneWithCorrelatedClientEvents(t *testing.T) {
	db := testutil.Open(t)

	id1 := insertCorrelatedSession(t, db, "ce1.rti")
	id2 := insertCorrelatedSession(t, db, "ce2.rti")
	// чистая сессия без клиентских событий — самая свежая, останется после prune
	idClean := insertPlainSession(t, db, "clean.rti")

	// DeleteSession на сессии с коррелированным событием
	if err := DeleteSession(context.Background(), db, id1); err != nil {
		t.Fatalf("DeleteSession with correlated client events: %v", err)
	}
	assertSessionGone(t, db, id1)

	// PruneSessions: удаляет сессию с коррелированным событием, оставляет последнюю
	deleted, err := PruneSessions(context.Background(), db, 1)
	if err != nil {
		t.Fatalf("PruneSessions(1) with correlated client events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	assertSessionGone(t, db, id2)
	sessions, err := ListSessions(context.Background(), db, 10)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != idClean {
		t.Fatalf("remaining sessions = %+v, want only %d", sessions, idClean)
	}
}

// TestDeletePruneWithLegacyClientEventsFK: порядок удаления (client_events до
// calls) работает и без миграции — на старом FK без правила удаления.
func TestDeletePruneWithLegacyClientEventsFK(t *testing.T) {
	db := testutil.Open(t)
	degradeToLegacyFK(t, db)

	id1 := insertCorrelatedSession(t, db, "legacy1.rti")
	id2 := insertCorrelatedSession(t, db, "legacy2.rti")

	if err := DeleteSession(context.Background(), db, id1); err != nil {
		t.Fatalf("DeleteSession with legacy NO ACTION fk: %v", err)
	}
	assertSessionGone(t, db, id1)

	deleted, err := PruneSessions(context.Background(), db, 0)
	if err != nil {
		t.Fatalf("PruneSessions(0) with legacy NO ACTION fk: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	assertSessionGone(t, db, id2)
}

// --- Reuse сессий по (file_hash, parser_version) ---

// TestAcquireReleaseSessionUse: семантика AcquireSessionByHash (атомарный
// find+increment), SetSessionHash (хеш + use_count = 1), ReleaseSession
// (декремент при use_count > 1), legacy-NULL не находится, версия в ключе,
// unique violation на дубликат ключа.
func TestAcquireReleaseSessionUse(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// legacy-сессия без хеша (создана до reuse) и целевая сессия
	legacy := insertPlainSession(t, db, "legacy-null.rti")
	if legacy == 0 {
		t.Fatal("legacy insert failed")
	}
	sid := insertPlainSession(t, db, "hashed.rti")

	hash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	// До простановки хеша — не находится
	if id, cnt, err := AcquireSessionByHash(ctx, db, hash, 1); err != nil || id != 0 || cnt != 0 {
		t.Fatalf("AcquireSessionByHash before set = (%d, %d, %v), want (0, 0, nil)", id, cnt, err)
	}

	// SetSessionHash проставляет хеш и use_count = 1 в одном statement
	if err := SetSessionHash(ctx, db, sid, hash, 1); err != nil {
		t.Fatalf("SetSessionHash: %v", err)
	}
	if s, err := GetSession(ctx, db, sid); err != nil || s.UseCount != 1 {
		t.Fatalf("use_count after SetSessionHash = (%d, %v), want 1", s.UseCount, err)
	}

	// Acquire — find + инкремент, дважды: use_count 2, затем 3
	if id, cnt, err := AcquireSessionByHash(ctx, db, hash, 1); err != nil || id != sid || cnt != 2 {
		t.Fatalf("AcquireSessionByHash #1 = (%d, %d, %v), want (%d, 2, nil)", id, cnt, err, sid)
	}
	if id, cnt, err := AcquireSessionByHash(ctx, db, hash, 1); err != nil || id != sid || cnt != 3 {
		t.Fatalf("AcquireSessionByHash #2 = (%d, %d, %v), want (%d, 3, nil)", id, cnt, err, sid)
	}
	if s, err := GetSession(ctx, db, sid); err != nil || s.UseCount != 3 {
		t.Fatalf("use_count after 2 acquires = (%d, %v), want 3", s.UseCount, err)
	}

	// Другая версия парсера — тот же хеш не находится (версия в ключе)
	if id, _, err := AcquireSessionByHash(ctx, db, hash, 2); err != nil || id != 0 {
		t.Fatalf("AcquireSessionByHash other version = (%d, %v), want (0, nil)", id, err)
	}

	// Дубликат ключа — unique violation (гонка параллельных parse);
	// statement откатывается целиком — use_count не «прилипает».
	loser := insertPlainSession(t, db, "loser.rti")
	err := SetSessionHash(ctx, db, loser, hash, 1)
	if err == nil || !store.IsUniqueViolation(err) {
		t.Fatalf("second SetSessionHash = %v, want unique violation", err)
	}
	if s, gerr := GetSession(ctx, db, sid); gerr != nil || s.UseCount != 3 {
		t.Fatalf("use_count after violation = (%d, %v), want 3", s.UseCount, gerr)
	}

	// Release при use_count > 1: декремент, сессия жива
	if remaining, ok, err := ReleaseSession(ctx, db, sid); err != nil || !ok || remaining != 2 {
		t.Fatalf("ReleaseSession at 3 = (%d, %v, %v), want (2, true, nil)", remaining, ok, err)
	}
	if _, gerr := GetSession(ctx, db, sid); gerr != nil {
		t.Fatalf("session gone after release: %v", gerr)
	}

	// Release при use_count <= 1: false — вызывающая сторона удаляет окончательно.
	if err := DeleteSession(ctx, db, legacy); err != nil { // cleanup legacy
		t.Fatalf("delete legacy: %v", err)
	}
	for i := 0; i < 2; i++ { // 2 → 1 → false
		if _, ok, err := ReleaseSession(ctx, db, sid); err != nil {
			t.Fatalf("ReleaseSession #%d: %v", i, err)
		} else if i == 1 && ok {
			t.Fatal("ReleaseSession at use_count=1 = ok, want false")
		}
	}
	if err := DeleteSession(ctx, db, sid); err != nil {
		t.Fatalf("DeleteSession after last release: %v", err)
	}
	assertSessionGone(t, db, sid)

	// Legacy-сессия (use_count = 0, hash NULL): release-семантика —
	// окончательное удаление (ок > 0 нет, счётчик не трогаем).
	legacy2 := insertPlainSession(t, db, "legacy2.rti")
	if remaining, ok, err := ReleaseSession(ctx, db, legacy2); err != nil || ok || remaining != 0 {
		t.Fatalf("ReleaseSession legacy = (%d, %v, %v), want (0, false, nil)", remaining, ok, err)
	}
	if err := DeleteSession(ctx, db, legacy2); err != nil {
		t.Fatalf("DeleteSession legacy: %v", err)
	}
	assertSessionGone(t, db, legacy2)
}

// --- Имя сессии, delete-by-name, prune-by-hours ---

func TestSaveSessionName(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	result := &RTIParseResult{Summary: RTISummary{FilePath: "named.rti", FileSize: 10}}
	sid, err := SaveSession(ctx, db, result, "named.rti", "Проблемный расчёт")
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	defer DeleteSession(ctx, db, sid)

	session, err := GetSession(ctx, db, sid)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if session.SessionName != "Проблемный расчёт" {
		t.Fatalf("GetSession session_name = %q", session.SessionName)
	}

	sessions, err := ListSessions(ctx, db, 10)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].SessionName != "Проблемный расчёт" {
		t.Fatalf("ListSessions = %+v", sessions)
	}
}

func TestFindSessionIDsByNameAndDelete(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	first := insertCorrelatedSession(t, db, "first.rti")
	second := insertPlainSession(t, db, "second.rti")
	other := insertPlainSession(t, db, "other.rti")
	for _, id := range []int64{first, second} {
		if _, err := db.Exec(`UPDATE rti_sessions SET session_name = 'trace.rti' WHERE id = $1`, id); err != nil {
			t.Fatalf("set name: %v", err)
		}
	}
	if _, err := db.Exec(`UPDATE rti_sessions SET session_name = 'other.rti' WHERE id = $1`, other); err != nil {
		t.Fatalf("set other name: %v", err)
	}

	ids, err := FindSessionIDsByName(ctx, db, "trace.rti")
	if err != nil {
		t.Fatalf("FindSessionIDsByName: %v", err)
	}
	if len(ids) != 2 || ids[0] != first || ids[1] != second {
		t.Fatalf("ids = %v, want [%d %d]", ids, first, second)
	}

	if err := deleteSessionIDs(ctx, db, ids); err != nil {
		t.Fatalf("deleteSessionIDs: %v", err)
	}
	assertSessionGone(t, db, first)
	assertSessionGone(t, db, second)
	if _, err := GetSession(ctx, db, other); err != nil {
		t.Fatalf("unrelated session removed: %v", err)
	}
}

func TestPruneSessionsOlderThan(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	old := insertPlainSession(t, db, "old.rti")
	fresh := insertPlainSession(t, db, "fresh.rti")
	if _, err := db.Exec(`UPDATE rti_sessions SET parsed_at = NOW() - INTERVAL '25 hours' WHERE id = $1`, old); err != nil {
		t.Fatalf("age old session: %v", err)
	}
	if _, err := db.Exec(`UPDATE rti_sessions SET parsed_at = NOW() - INTERVAL '1 hour' WHERE id = $1`, fresh); err != nil {
		t.Fatalf("age fresh session: %v", err)
	}

	deleted, err := PruneSessionsOlderThan(ctx, db, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneSessionsOlderThan: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	assertSessionGone(t, db, old)
	if _, err := GetSession(ctx, db, fresh); err != nil {
		t.Fatalf("fresh session removed: %v", err)
	}
}

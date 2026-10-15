//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// clientEventsFKRule возвращает правило удаления (confdeltype) и oid FK
// rti_client_events.server_call_id: 'a' = NO ACTION (legacy), 'n' = SET NULL.
func clientEventsFKRule(t *testing.T, db *store.DB) (string, uint32) {
	t.Helper()
	var rule string
	var oid uint32
	err := db.QueryRow(
		`SELECT c.confdeltype, c.oid
		 FROM pg_constraint c
		 JOIN pg_class tbl ON tbl.oid = c.conrelid
		 WHERE c.conname = 'rti_client_events_server_call_id_fkey'
		   AND tbl.relname = 'rti_client_events'`,
	).Scan(&rule, &oid)
	if err != nil {
		t.Fatalf("read fk rule: %v", err)
	}
	return rule, oid
}

// degradeClientEventsFK пересоздаёт FK без правила удаления (NO ACTION) —
// имитирует БД, созданную до миграции.
func degradeClientEventsFK(t *testing.T, db *store.DB) {
	t.Helper()
	if _, err := db.Exec(`ALTER TABLE rti_client_events DROP CONSTRAINT rti_client_events_server_call_id_fkey`); err != nil {
		t.Fatalf("drop fk: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE rti_client_events ADD CONSTRAINT rti_client_events_server_call_id_fkey FOREIGN KEY (server_call_id) REFERENCES rti_calls(id)`); err != nil {
		t.Fatalf("add legacy fk: %v", err)
	}
}

// TestClientEventsFK_FreshSchemaIsSetNull: на пустой БД FK создаётся сразу
// с ON DELETE SET NULL.
func TestClientEventsFK_FreshSchemaIsSetNull(t *testing.T) {
	db := testutil.Open(t)
	rule, _ := clientEventsFKRule(t, db)
	if rule != "n" {
		t.Fatalf("confdeltype = %q, want 'n' (SET NULL)", rule)
	}
}

// TestClientEventsFK_MigratesLegacyConstraint: legacy FK (NO ACTION)
// пересоздаётся с SET NULL одним прогоном InitSchemaCtx; повторный прогон —
// no-op (oid констрейнта неизменен).
func TestClientEventsFK_MigratesLegacyConstraint(t *testing.T) {
	db := testutil.Open(t)
	degradeClientEventsFK(t, db)
	rule, _ := clientEventsFKRule(t, db)
	if rule != "a" {
		t.Fatalf("degraded confdeltype = %q, want 'a' (NO ACTION)", rule)
	}

	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchemaCtx after degrade: %v", err)
	}
	rule, migratedOID := clientEventsFKRule(t, db)
	if rule != "n" {
		t.Fatalf("confdeltype after migration = %q, want 'n' (SET NULL)", rule)
	}

	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("second InitSchemaCtx: %v", err)
	}
	rule, oid := clientEventsFKRule(t, db)
	if rule != "n" {
		t.Fatalf("confdeltype after second run = %q, want 'n' (SET NULL)", rule)
	}
	if oid != migratedOID {
		t.Fatalf("constraint oid changed on no-op run: %d -> %d", migratedOID, oid)
	}
}

// TestClientEventsFK_DeleteCallNullsReference: удаление вызова из rti_calls
// обнуляет server_call_id события, а не блокируется и не удаляет событие.
func TestClientEventsFK_DeleteCallNullsReference(t *testing.T) {
	db := testutil.Open(t)
	var sid int64
	if err := db.QueryRow(
		`INSERT INTO rti_sessions (file_path, file_size, total_calls) VALUES ('fk.rti', 10, 1) RETURNING id`,
	).Scan(&sid); err != nil {
		t.Fatalf("insert session: %v", err)
	}
	var callID int64
	if err := db.QueryRow(
		`INSERT INTO rti_calls (session_id, procedure, enter_line, exit_line, elapsed_ms, nest_level, module_id, tran_count, begin_cnt, spid)
		 VALUES ($1, 'P', 1, 2, 1, 0, 0, 0, 0, 1) RETURNING id`, sid,
	).Scan(&callID); err != nil {
		t.Fatalf("insert call: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO rti_client_events (session_id, pid, seq_no, line_no, kind, server_call_id)
		 VALUES ($1, 100, 1, 1, 'sql_block', $2)`, sid, callID,
	); err != nil {
		t.Fatalf("insert client event: %v", err)
	}

	if _, err := db.Exec(`DELETE FROM rti_calls WHERE id = $1`, callID); err != nil {
		t.Fatalf("delete call: %v", err)
	}
	var events int
	var nullRefs int
	if err := db.QueryRow(
		`SELECT count(*), count(*) FILTER (WHERE server_call_id IS NULL) FROM rti_client_events WHERE session_id = $1`, sid,
	).Scan(&events, &nullRefs); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 1 || nullRefs != 1 {
		t.Fatalf("events = %d, nullRefs = %d; want event preserved with NULL server_call_id", events, nullRefs)
	}
}

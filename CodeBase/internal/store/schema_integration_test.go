//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

func TestInitSchema_IdempotentAndHasRequiredObjects(t *testing.T) {
	db := testutil.Open(t)
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}

	requiredTables := []string{
		"scan_runs", "files", "symbols", "sql_procedures", "relations",
		"rti_sessions", "trc_sessions", "api_contracts", "ds_return_codes",
	}
	for _, table := range requiredTables {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=$1)`, table).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("missing table %s", table)
		}
	}

	requiredIndexes := []string{
		"idx_api_contracts_lower_name_kind",
		"idx_symbols_symbol_name_type_lower",
		"idx_rti_calls_session_proc",
		"idx_trc_events_session_proc",
	}
	for _, idx := range requiredIndexes {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname=$1)`, idx).Scan(&exists); err != nil {
			t.Fatalf("check index %s: %v", idx, err)
		}
		if !exists {
			t.Fatalf("missing index %s", idx)
		}
	}

	dropped := []string{
		"idx_relations_relation_type",
		"idx_rti_calls_session_id",
		"idx_trc_events_session_id",
		"idx_api_contracts_name_kind",
		"idx_symbols_symbol_name_lower",
	}
	for _, idx := range dropped {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE indexname=$1)`, idx).Scan(&exists); err != nil {
			t.Fatalf("check dropped index %s: %v", idx, err)
		}
		if exists {
			t.Fatalf("dropped index still exists: %s", idx)
		}
	}
}

func TestInitSchema_AddsMissingColumns(t *testing.T) {
	db := testutil.OpenEmpty(t)
	if _, err := db.Exec(`
		CREATE TABLE rti_sessions (
			id BIGSERIAL PRIMARY KEY,
			file_path TEXT NOT NULL,
			file_size BIGINT NOT NULL DEFAULT 0,
			parsed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			total_calls INTEGER NOT NULL DEFAULT 0,
			errors_count INTEGER NOT NULL DEFAULT 0,
			max_nest_level INTEGER NOT NULL DEFAULT 0,
			unparsed_lines INTEGER NOT NULL DEFAULT 0
		)
	`); err != nil {
		t.Fatalf("create stub rti_sessions: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE trc_sessions (
			id BIGSERIAL PRIMARY KEY,
			file_path TEXT NOT NULL,
			file_size BIGINT NOT NULL DEFAULT 0,
			parsed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			total_events INTEGER NOT NULL DEFAULT 0
		)
	`); err != nil {
		t.Fatalf("create stub trc_sessions: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE trc_events (
			id BIGSERIAL PRIMARY KEY,
			session_id BIGINT NOT NULL REFERENCES trc_sessions(id) ON DELETE CASCADE,
			event_class INTEGER NOT NULL DEFAULT 0,
			event_name TEXT,
			procedure TEXT,
			spid INTEGER,
			duration_ms BIGINT NOT NULL DEFAULT 0,
			error INTEGER,
			parent_id BIGINT
		)
	`); err != nil {
		t.Fatalf("create stub trc_events: %v", err)
	}
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchema after stubs: %v", err)
	}

	var clientEventsExists bool
	if err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.columns
			WHERE table_name='rti_sessions' AND column_name='client_events_count'
		)
	`).Scan(&clientEventsExists); err != nil {
		t.Fatalf("check client_events_count: %v", err)
	}
	if !clientEventsExists {
		t.Fatal("rti_sessions.client_events_count was not added")
	}

	for _, col := range []string{"parent_id", "depth"} {
		var exists bool
		if err := db.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM information_schema.columns
				WHERE table_name='trc_events' AND column_name=$1
			)
		`, col).Scan(&exists); err != nil {
			t.Fatalf("check trc_events.%s: %v", col, err)
		}
		if !exists {
			t.Fatalf("trc_events.%s was not added", col)
		}
	}
}

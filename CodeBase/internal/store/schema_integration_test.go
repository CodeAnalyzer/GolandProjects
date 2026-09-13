//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

func TestInitSchema_IdempotentAndHasRequiredObjects(t *testing.T) {
	db := testutil.Open(t)
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}
	var migrationExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = 'spec_search_vectors_weighted_v1')`).Scan(&migrationExists); err != nil {
		t.Fatalf("check weighted migration: %v", err)
	}
	if !migrationExists {
		t.Fatal("weighted search vector migration is not recorded")
	}

	requiredTables := []string{
		"scan_runs", "files", "symbols", "sql_procedures", "relations",
		"rti_sessions", "trc_sessions", "api_contracts", "ds_return_codes",
		"schema_migrations", "spec_configs", "spec_capabilities", "spec_requirements", "spec_scenarios",
		"spec_usecases", "spec_usecase_steps", "spec_changes", "spec_change_delta",
		"spec_code_mentions", "spec_vocab", "spec_embeddings",
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
		"idx_spec_capabilities_name_lower",
		"idx_spec_capabilities_config_name_unique",
		"idx_spec_capabilities_config",
		"idx_spec_capabilities_parent",
		"idx_spec_requirements_cap",
		"idx_spec_scenarios_req",
		"idx_spec_usecases_config",
		"idx_spec_usecase_steps_file_id",
		"idx_spec_usecase_steps_uc",
		"idx_spec_changes_config_name_unique",
		"idx_spec_changes_config_name",
		"idx_spec_change_delta_change",
		"idx_spec_code_mentions_name_lower",
		"idx_spec_code_mentions_source",
		"idx_spec_vocab_term",
		"idx_spec_embeddings_spec",
		"idx_spec_capabilities_fts",
		"idx_spec_requirements_fts",
		"idx_spec_scenarios_fts",
		"idx_spec_usecases_fts",
		"idx_spec_scenarios_text_trgm",
		"idx_spec_usecases_text_trgm",
		"idx_spec_code_mentions_name_trgm",
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

	for _, idx := range []string{"idx_spec_capabilities_config_name_unique", "idx_spec_changes_config_name_unique"} {
		var unique bool
		if err := db.QueryRow(`
			SELECT i.indisunique
			FROM pg_index i
			JOIN pg_class c ON c.oid = i.indexrelid
			WHERE c.relname = $1
		`, idx).Scan(&unique); err != nil {
			t.Fatalf("check unique index %s: %v", idx, err)
		}
		if !unique {
			t.Fatalf("index is not unique: %s", idx)
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

func TestInitSchema_WeightedMigrationRunsOnce(t *testing.T) {
	db := testutil.Open(t)
	var scanRunID, fileID, configID, capabilityID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/spec.md', 'spec.md', '.md', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'product') RETURNING id`, fileID).Scan(&configID); err != nil {
		t.Fatalf("insert config: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		VALUES ($1, $2, 'sentinel-capability', 'Sentinel') RETURNING id`, fileID, configID).Scan(&capabilityID); err != nil {
		t.Fatalf("insert capability: %v", err)
	}
	if _, err := db.Exec(`UPDATE spec_capabilities SET search_vector = to_tsvector('simple', 'sentinel') WHERE id = $1`, capabilityID); err != nil {
		t.Fatalf("set sentinel vector: %v", err)
	}
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}
	var vectorText string
	if err := db.QueryRow(`SELECT search_vector::text FROM spec_capabilities WHERE id = $1`, capabilityID).Scan(&vectorText); err != nil {
		t.Fatalf("read sentinel vector: %v", err)
	}
	if !strings.Contains(vectorText, "sentinel") {
		t.Fatalf("sentinel vector was overwritten: %q", vectorText)
	}
}

func TestWeightedSpecRequirementRankPrefersName(t *testing.T) {
	db := testutil.Open(t)
	var scanRunID, fileID, configID, capabilityID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/spec.md', 'spec.md', '.md', 'hash', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'product') RETURNING id`, fileID).Scan(&configID); err != nil {
		t.Fatalf("insert config: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		VALUES ($1, $2, 'weighted-capability', 'Weighted') RETURNING id`, fileID, configID).Scan(&capabilityID); err != nil {
		t.Fatalf("insert capability: %v", err)
	}
	var nameID, bodyID int64
	if err := db.QueryRow(`
		INSERT INTO spec_requirements (file_id, capability_id, requirement_name, body_text)
		VALUES ($1, $2, 'арест', 'обычный текст') RETURNING id`, fileID, capabilityID).Scan(&nameID); err != nil {
		t.Fatalf("insert name requirement: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO spec_requirements (file_id, capability_id, requirement_name, body_text)
		VALUES ($1, $2, 'обычное требование', 'арест') RETURNING id`, fileID, capabilityID).Scan(&bodyID); err != nil {
		t.Fatalf("insert body requirement: %v", err)
	}
	if err := db.EnsureSpecSearchVectors(context.Background(), fileID); err != nil {
		t.Fatalf("EnsureSpecSearchVectors: %v", err)
	}
	var nameRank, bodyRank float64
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'арест'))
		FROM spec_requirements WHERE id = $1`, nameID).Scan(&nameRank); err != nil {
		t.Fatalf("name rank: %v", err)
	}
	if err := db.QueryRow(`
		SELECT ts_rank(search_vector, plainto_tsquery('russian', 'арест'))
		FROM spec_requirements WHERE id = $1`, bodyID).Scan(&bodyRank); err != nil {
		t.Fatalf("body rank: %v", err)
	}
	if nameRank <= bodyRank {
		t.Fatalf("name rank = %v, body rank = %v, want name > body", nameRank, bodyRank)
	}
}

func TestInitSchema_LSAGenerationSchema(t *testing.T) {
	db := testutil.Open(t)
	for _, tc := range []struct {
		table string
	}{
		{table: "spec_vocab"},
		{table: "spec_embeddings"},
	} {
		var columnDefault, isNullable string
		if err := db.QueryRow(`
			SELECT column_default, is_nullable
			FROM information_schema.columns
			WHERE table_name = $1 AND column_name = 'generation'`, tc.table).Scan(&columnDefault, &isNullable); err != nil {
			t.Fatalf("generation column %s: %v", tc.table, err)
		}
		if !strings.Contains(columnDefault, "legacy") || isNullable != "NO" {
			t.Fatalf("generation column %s: default=%q nullable=%q", tc.table, columnDefault, isNullable)
		}
	}

	for _, tc := range []struct {
		name string
		want string
	}{
		{name: "idx_spec_vocab_term", want: "(generation, term)"},
		{name: "idx_spec_embeddings_spec", want: "(generation, spec_id, embed_level)"},
	} {
		var definition string
		if err := db.QueryRow(`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, tc.name).Scan(&definition); err != nil {
			t.Fatalf("index %s: %v", tc.name, err)
		}
		if !strings.Contains(definition, tc.want) {
			t.Fatalf("index %s definition = %q, want %q", tc.name, definition, tc.want)
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

func TestInitSchema_BackfillsSpecUsecaseStepFileID(t *testing.T) {
	db := testutil.Open(t)
	var scanRunID, fileID, configID, usecaseID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/openspec/usecases/a.md', 'openspec/usecases/a.md', '.md', 'hash', NOW())
		RETURNING id
	`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'product') RETURNING id`, fileID).Scan(&configID); err != nil {
		t.Fatalf("insert spec config: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO spec_usecases (file_id, spec_config_id, usecase_name)
		VALUES ($1, $2, 'uc') RETURNING id
	`, fileID, configID).Scan(&usecaseID); err != nil {
		t.Fatalf("insert usecase: %v", err)
	}
	if _, err := db.Exec(`ALTER TABLE spec_usecase_steps DROP COLUMN file_id CASCADE`); err != nil {
		t.Fatalf("drop legacy-missing file_id: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO spec_usecase_steps (usecase_id, step_text) VALUES ($1, 'step')`, usecaseID); err != nil {
		t.Fatalf("insert legacy step: %v", err)
	}

	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchema migration: %v", err)
	}
	var gotFileID int64
	if err := db.QueryRow(`SELECT file_id FROM spec_usecase_steps WHERE usecase_id = $1`, usecaseID).Scan(&gotFileID); err != nil {
		t.Fatalf("select backfilled file_id: %v", err)
	}
	if gotFileID != fileID {
		t.Fatalf("backfilled file_id = %d, want %d", gotFileID, fileID)
	}
	var notNull, cascade bool
	if err := db.QueryRow(`
		SELECT a.attnotnull
		FROM pg_attribute a
		WHERE a.attrelid = 'spec_usecase_steps'::regclass AND a.attname = 'file_id'
	`).Scan(&notNull); err != nil {
		t.Fatalf("check file_id not null: %v", err)
	}
	if err := db.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM pg_constraint
			WHERE conrelid = 'spec_usecase_steps'::regclass
			  AND conname = 'spec_usecase_steps_file_id_fkey'
			  AND confdeltype = 'c'
		)
	`).Scan(&cascade); err != nil {
		t.Fatalf("check file_id cascade FK: %v", err)
	}
	if !notNull || !cascade {
		t.Fatalf("invalid file_id migration: notNull=%v cascade=%v", notNull, cascade)
	}
}

func TestSpecProfileDirectoriesAreScopedToConfigRoot(t *testing.T) {
	db := testutil.Open(t)
	var scanRunID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	insertFile := func(relPath string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
			VALUES ($1, $2, $2, '.md', $2, NOW()) RETURNING id
		`, scanRunID, relPath).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", relPath, err)
		}
		return id
	}
	configAFileID := insertFile("products/a/openspec/config.yaml")
	configBFileID := insertFile("products/b/openspec/config.yaml")
	insertFile("products/a/openspec/audit/report.md")
	insertFile("products/a/openspec/docs/adr/0001.md")
	var configAID, configBID int64
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'a') RETURNING id`, configAFileID).Scan(&configAID); err != nil {
		t.Fatalf("insert config a: %v", err)
	}
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'b') RETURNING id`, configBFileID).Scan(&configBID); err != nil {
		t.Fatalf("insert config b: %v", err)
	}

	for _, tc := range []struct {
		name     string
		configID int64
		check    func(context.Context, int64) (bool, error)
		want     bool
	}{
		{name: "audit in root a", configID: configAID, check: db.HasSpecAudit, want: true},
		{name: "adr in root a", configID: configAID, check: db.HasSpecADR, want: true},
		{name: "audit absent in root b", configID: configBID, check: db.HasSpecAudit, want: false},
		{name: "adr absent in root b", configID: configBID, check: db.HasSpecADR, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.check(context.Background(), tc.configID)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

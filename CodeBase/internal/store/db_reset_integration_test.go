//go:build integration

package store_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// resetTables — эталонный список усекаемых таблиц (52), синхронизирован с
// store-списком усечения; тест-сторож сверяет его с фактической схемой БД.
var resetTables = []string{
	"files", "symbols", "ds_products", "stats_snapshot",
	"sql_procedures", "sql_tables", "sql_columns", "sql_column_definitions",
	"sql_index_definitions", "sql_index_definition_fields",
	"pas_units", "pas_classes", "pas_methods", "pas_fields", "h_files_defines",
	"js_functions", "js_constants", "smf_instruments", "dfm_forms", "dfm_components",
	"report_forms", "report_fields", "report_params", "vb_functions",
	"query_fragments", "include_directives",
	"api_business_objects", "api_contracts", "api_contract_params",
	"api_contract_tables", "api_contract_table_fields", "api_business_object_params",
	"api_business_object_tables", "api_business_object_table_fields",
	"api_business_object_table_indexes", "api_business_object_table_index_fields",
	"api_contract_return_values", "api_contract_contexts", "api_macro_invocations",
	"relations", "ds_return_codes",
	"spec_configs", "spec_capabilities", "spec_requirements", "spec_scenarios",
	"spec_usecases", "spec_usecase_steps", "spec_changes", "spec_change_delta",
	"spec_code_mentions", "spec_vocab", "spec_embeddings",
}

// keptTables — таблицы, сохраняемые при пересборке: анализаторы (RTI/TRC)
// и служебные (schema_migrations, scan_runs — история прогонов).
var keptTables = []string{
	"scan_runs", "schema_migrations",
	"rti_sessions", "rti_calls", "rti_params", "rti_checkpoints",
	"rti_blog_blocks", "rti_blog_tables", "rti_client_events",
	"trc_sessions", "trc_events",
}

// seedResetFixture заполняет representative-таблицы из каждой группы усечения
// и по одной строке в каждой сохраняемой таблице.
func seedResetFixture(t *testing.T, db *store.DB) {
	t.Helper()
	insert := func(q string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed: %v (%s)", err, q)
		}
	}
	var scanRunID, fileID, configID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo-old', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("seed scan_run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/a.sql', 'a.sql', '.sql', 'hash1', NOW()) RETURNING id`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	insert(`INSERT INTO ds_products (product_name) VALUES ('prod')`)
	insert(`INSERT INTO stats_snapshot (payload) VALUES ('{"files":1}'::jsonb)`)
	insert(`INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id)
		VALUES ($1, 'CallerA', 'procedure', 'sql_procedure', 1)`, fileID)
	insert(`INSERT INTO sql_procedures (file_id, proc_name) VALUES ($1, 'CallerA')`, fileID)
	insert(`INSERT INTO sql_tables (file_id, table_name, context) VALUES ($1, 'M_LOG', 'select')`, fileID)
	insert(`INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type)
		VALUES ('sql_procedure', 1, 'sql_procedure', 2, 'calls_procedure')`)
	insert(`INSERT INTO ds_return_codes (file_id, ret_code, message) VALUES ($1, 10001, 'msg')`, fileID)
	if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'product') RETURNING id`, fileID).Scan(&configID); err != nil {
		t.Fatalf("seed spec_config: %v", err)
	}
	insert(`INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
		VALUES ($1, $2, 'cap', 'Cap')`, fileID, configID)
	insert(`INSERT INTO rti_sessions (file_path, file_size, total_calls) VALUES ('trace.rti', 10, 5)`)
	insert(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('trace.trc', 10, 7)`)
	insert(`INSERT INTO schema_migrations (version) VALUES ('reset-test-marker')`)
}

func countRows(t *testing.T, db *store.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s`, table)).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

// TestResetCodebaseTables_ClearsCodebaseKeepsAnalyzers — после сброса все
// таблицы усечения пусты, RTI/TRC-сессии, история scan_runs и схема живы.
func TestResetCodebaseTables_ClearsCodebaseKeepsAnalyzers(t *testing.T) {
	db := testutil.Open(t)
	seedResetFixture(t, db)

	n, err := db.ResetCodebaseTables(context.Background())
	if err != nil {
		t.Fatalf("ResetCodebaseTables: %v", err)
	}
	if n != len(resetTables) {
		t.Fatalf("reset count = %d, want %d", n, len(resetTables))
	}

	for _, table := range resetTables {
		if got := countRows(t, db, table); got != 0 {
			t.Fatalf("table %s = %d rows after reset, want 0", table, got)
		}
	}
	if got := countRows(t, db, "scan_runs"); got != 1 {
		t.Fatalf("scan_runs history = %d rows, want 1", got)
	}
	var rootPath string
	if err := db.QueryRow(`SELECT root_path FROM scan_runs`).Scan(&rootPath); err != nil {
		t.Fatalf("read scan_runs history: %v", err)
	}
	if rootPath != "repo-old" {
		t.Fatalf("scan_runs history root_path = %q, want repo-old", rootPath)
	}
	// schema_migrations содержит миграции InitSchema — проверяем выживание
	// тестового маркера, а не точное число строк.
	for _, tc := range []struct {
		table string
		want  int
	}{
		{"rti_sessions", 1}, {"trc_sessions", 1},
	} {
		if got := countRows(t, db, tc.table); got != tc.want {
			t.Fatalf("%s = %d rows, want %d", tc.table, got, tc.want)
		}
	}
	var markerExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = 'reset-test-marker')`).Scan(&markerExists); err != nil {
		t.Fatalf("check schema_migrations marker: %v", err)
	}
	if !markerExists {
		t.Fatal("schema_migrations marker did not survive reset")
	}
}

// TestResetCodebaseTables_GuardSchemaCoverage — сторож: список усечения
// (52 таблицы) плюс сохраняемые (11) покрывают все таблицы схемы без пропусков
// и лишних; у сохраняемых таблиц нет FK на усекаемые (CASCADE зацепил бы сессии).
func TestResetCodebaseTables_GuardSchemaCoverage(t *testing.T) {
	db := testutil.Open(t)

	rows, err := db.Query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		actual = append(actual, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("tables rows: %v", err)
	}

	expected := append(slices.Clone(resetTables), keptTables...)
	slices.Sort(expected)
	slices.Sort(actual)
	if !slices.Equal(actual, expected) {
		var missing, extra []string
		for _, e := range expected {
			if !slices.Contains(actual, e) {
				missing = append(missing, e)
			}
		}
		for _, a := range actual {
			if !slices.Contains(expected, a) {
				extra = append(extra, a)
			}
		}
		t.Fatalf("schema mismatch: missing=%v extra=%v — новая таблица схемы требует решения: усечение или сохранение", missing, extra)
	}

	// FK из сохраняемых таблиц на усекаемые: TRUNCATE ... CASCADE их зацепил бы.
	fkRows, err := db.Query(`
		SELECT conrelid::regclass::text, confrelid::regclass::text
		FROM pg_constraint
		WHERE contype = 'f'
	`)
	if err != nil {
		t.Fatalf("list FKs: %v", err)
	}
	defer fkRows.Close()
	for fkRows.Next() {
		var fromTable, toTable string
		if err := fkRows.Scan(&fromTable, &toTable); err != nil {
			t.Fatalf("scan FK: %v", err)
		}
		if slices.Contains(keptTables, fromTable) && slices.Contains(resetTables, toTable) {
			t.Fatalf("kept table %s has FK to reset table %s — пересборка удалит данные анализатора", fromTable, toTable)
		}
	}
	if err := fkRows.Err(); err != nil {
		t.Fatalf("fk rows: %v", err)
	}
}

// TestResetCodebaseTables_IdempotentOnEmptyDB — сброс безопасен на пустой БД
// и при повторном вызове.
func TestResetCodebaseTables_IdempotentOnEmptyDB(t *testing.T) {
	db := testutil.Open(t)
	for i := 0; i < 2; i++ {
		if _, err := db.ResetCodebaseTables(context.Background()); err != nil {
			t.Fatalf("ResetCodebaseTables #%d: %v", i+1, err)
		}
	}
}

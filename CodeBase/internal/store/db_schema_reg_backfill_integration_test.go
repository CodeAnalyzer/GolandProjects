//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// TestInitSchema_RegTableNameV9 — миграция v8→v9: на БД со сброшенным
// маркером версии повторный InitSchemaCtx создаёт reg_table_name с
// индексами (LOWER(table_name), file_id), а удаление файла каскадно
// удаляет его регистрации (change table-existence-source-fixes).
func TestInitSchema_RegTableNameV9(t *testing.T) {
	db := testutil.Open(t)

	// Легаси-БД версии v8: маркер текущей версии отсутствует.
	testutil.ResetSchemaVersion(t, db)

	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchema migration %s: %v", store.CurrentSchemaVersion, err)
	}

	var tableExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='reg_table_name')`).Scan(&tableExists); err != nil {
		t.Fatalf("check reg_table_name: %v", err)
	}
	if !tableExists {
		t.Fatal("reg_table_name was not created by v9 migration")
	}

	for _, idx := range []string{"idx_reg_table_name_lower", "idx_reg_table_name_file_id"} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=$1)`, idx).Scan(&exists); err != nil {
			t.Fatalf("check index %s: %v", idx, err)
		}
		if !exists {
			t.Fatalf("index %s was not created by v9 migration", idx)
		}
	}

	var indexDef string
	if err := db.QueryRow(`SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='idx_reg_table_name_lower'`).Scan(&indexDef); err != nil {
		t.Fatalf("read indexdef: %v", err)
	}
	// pg_indexes приводит выражение индекса к нижнему регистру: lower(table_name).
	if !strings.Contains(strings.ToLower(indexDef), "lower(table_name)") {
		t.Fatalf("indexdef = %q, want expression index on LOWER(table_name)", indexDef)
	}

	// Каскад: удаление файла удаляет его регистрации реестра.
	var scanRunID, fileID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, 'repo/ProductTable.sql', 'ProductTable.sql', 'sql', 'hash', NOW())
		RETURNING id
	`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO reg_table_name (file_id, table_name, line_number) VALUES ($1, 'tKey', 7)`, fileID); err != nil {
		t.Fatalf("insert reg_table_name: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM files WHERE id = $1`, fileID); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM reg_table_name`).Scan(&remaining); err != nil {
		t.Fatalf("count reg_table_name: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("reg_table_name rows survived file deletion: %d", remaining)
	}
}

// TestTableExistenceSourceBackfills — backfill-миграции
// reg_table_name_backfill_v1 / sql_table_creators_backfill_v1 и очистка
// sql_tables_prefix_garbage_cleanup_v1 на легаси-данных (change
// table-existence-source-fixes, design D5): создатели и реестр
// восстанавливаются из query_fragments (с допуском скобок и вырезанием
// -- строк), plain-create не дублируется (NOT EXISTS), мусор
// префиксной экстракции dbo/MIDDLE удаляется из всех таблиц, легитимные
// define MIDDLE, реальные alter_add и рёбра на реальные таблицы остаются,
// повторный прогон после маркеров — no-op. Свежая БД (testutil.Open уже
// применил схему к пустой базе) получает маркеры без легаси-строк —
// INSERT'ы миграций выбирают из пустых query_fragments.
func TestTableExistenceSourceBackfills(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// --- Сид легаси-состояния (БД до миграций) ---
	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	newFile := func(path, ext string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
			VALUES ($1, $2, $2, $3, 'h', NOW()) RETURNING id
		`, scanID, path, ext).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	eisFile := newFile("/repo/DB/eis_5nt.sql", "sql")
	ptFile := newFile("/repo/DB/ProductTable.sql", "sql")
	procFile := newFile("/repo/SERVER/Sample.sql", "sql")

	newFragment := func(fileID int64, text string, line int) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, query_hash, context, line_number)
			VALUES ($1, 'sql_script', 0, 'sql_script', 'sql_script', $2, 'h', '', $3) RETURNING id
		`, fileID, text, line).Scan(&id); err != nil {
			t.Fatalf("insert fragment (line %d): %v", line, err)
		}
		return id
	}
	fragCreateDbo := newFragment(eisFile, "create table dbo.tReplTables (Tbl varchar(30) NOT NULL)", 10)
	newFragment(eisFile, "CREATE UNLOGGED TABLE pAPI_ExpProperty_Type (spid DSSPID)", 20)
	newFragment(eisFile, "create table MIDDLE..dcSubcontoConnect (ID DSIDENTIFIER)", 25)
	fragCreatePlain := newFragment(procFile, "create table tPlain (A int NULL)", 30)
	regText := strings.Join([]string{
		"#include <macros.h>",
		"__MASS_DELETE_TABLE_NAME__",
		"__REG_TABLE_NAME__(hDocument)",
		"__REG_TABLE_NAME__(tKey)",
		"__REG_TABLE_NAME__([tPoolAccountLimitTable])",
		"-- __REG_TABLE_NAME__(tCommented)",
	}, "\n")
	newFragment(ptFile, regText, 1)

	newTable := func(fileID int64, name, context string, line int) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_tables (file_id, table_name, context, is_temporary, line_number, column_number)
			VALUES ($1, $2, $3, false, $4, 0) RETURNING id
		`, fileID, name, context, line).Scan(&id); err != nil {
			t.Fatalf("insert sql_tables %s: %v", name, err)
		}
		return id
	}
	dboCreateRow := newTable(eisFile, "dbo", "create", 10)       // мусор префиксной экстракции
	newTable(eisFile, "tReplTables", "create", 10)               // корректный создатель до миграции (NOT EXISTS)
	middleSelectRow := newTable(eisFile, "MIDDLE", "select", 15) // мусор
	newTable(eisFile, "dbo", "select", 16)                       // мусор
	newTable(eisFile, "MIDDLE", "create", 5)                     // мусор без фрагмента
	legitTableRow := newTable(procFile, "tLegacy", "select", 40) // легитимная строка — остаётся

	var dboProcID int64
	if err := db.QueryRow(`
		INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end)
		VALUES ($1, 'dbo', 1, 2) RETURNING id
	`, procFile).Scan(&dboProcID); err != nil {
		t.Fatalf("insert sql_procedures dbo: %v", err)
	}

	newSymbol := func(fileID int64, name, symbolType, entityType string, entityID int64) {
		if _, err := db.Exec(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
			VALUES ($1, $2, $3, $4, $5, 1)
		`, fileID, name, symbolType, entityType, entityID); err != nil {
			t.Fatalf("insert symbol %s: %v", name, err)
		}
	}
	newSymbol(eisFile, "dbo", "table", "sql", dboCreateRow)
	newSymbol(eisFile, "MIDDLE", "table", "sql", middleSelectRow)
	newSymbol(procFile, "dbo", "procedure", "sql", dboProcID)
	newSymbol(procFile, "MIDDLE", "define", "sql", 0) // легитимный define — остаётся

	newRelation := func(sourceID int64, targetType string, targetID int64, relationType string) {
		if _, err := db.Exec(`
			INSERT INTO relations (source_type, source_id, target_type, target_id, relation_type, line_number)
			VALUES ('query_fragment', $1, $2, $3, $4, 0)
		`, sourceID, targetType, targetID, relationType); err != nil {
			t.Fatalf("insert relation %s: %v", relationType, err)
		}
	}
	newRelation(fragCreateDbo, "sql_table", dboCreateRow, "references_table")    // удаляется
	newRelation(fragCreateDbo, "sql_table", middleSelectRow, "references_table") // удаляется
	newRelation(fragCreatePlain, "sql_table", legitTableRow, "references_table") // остаётся
	newRelation(fragCreateDbo, "sql_procedure", dboProcID, "calls_procedure")    // удаляется

	if _, err := db.Exec(`INSERT INTO sql_columns (file_id, table_name, column_name, line_number) VALUES ($1, 'dbo', 'A', 11)`, eisFile); err != nil {
		t.Fatalf("insert sql_columns dbo: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sql_columns (file_id, table_name, column_name, line_number) VALUES ($1, 'MIDDLE', 'B', 12)`, eisFile); err != nil {
		t.Fatalf("insert sql_columns MIDDLE: %v", err)
	}

	newColumnDefinition := func(name, kind string) {
		if _, err := db.Exec(`
			INSERT INTO sql_column_definitions (file_id, table_name, column_name, data_type, definition_kind, line_number, column_order)
			VALUES ($1, $2, $3, $4, $5, 6, 1)
		`, eisFile, name, name, "int", kind); err != nil {
			t.Fatalf("insert sql_column_definitions %s: %v", name, err)
		}
	}
	// Мусор create_table с table_name dbo/MIDDLE и кривые alter_add семьи.
	if _, err := db.Exec(`
		INSERT INTO sql_column_definitions (file_id, table_name, column_name, data_type, definition_kind, line_number, column_order)
		VALUES ($1, 'dbo', 'Tbl', 'varchar(30)', 'create_table', 10, 1)
	`, eisFile); err != nil {
		t.Fatalf("insert sql_column_definitions dbo: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO sql_column_definitions (file_id, table_name, column_name, data_type, definition_kind, line_number, column_order)
		VALUES ($1, 'MIDDLE', 'C', 'int', 'create_table', 5, 1)
	`, eisFile); err != nil {
		t.Fatalf("insert sql_column_definitions MIDDLE: %v", err)
	}
	newColumnDefinition("CONSTRAINT", "alter_add")
	newColumnDefinition("PRIMARY", "alter_add")
	newColumnDefinition("NewCol", "alter_add") // реальная колонка — остаётся

	var idxID int64
	if err := db.QueryRow(`
		INSERT INTO sql_index_definitions (file_id, table_name, index_name, index_fields, index_type, is_unique, definition_kind, line_number)
		VALUES ($1, 'MIDDLE', 'IX_Test', 'F1', '', false, 'create_index', 6) RETURNING id
	`, eisFile).Scan(&idxID); err != nil {
		t.Fatalf("insert sql_index_definitions MIDDLE: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sql_index_definition_fields (table_index_id, field_name, field_order, line_number) VALUES ($1, 'F1', 1, 6)`, idxID); err != nil {
		t.Fatalf("insert sql_index_definition_fields: %v", err)
	}

	// --- Эмуляция БД до миграций: сброс маркера версии и маркеров backfill'ов ---
	testutil.ResetSchemaVersion(t, db)
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version IN ('reg_table_name_backfill_v1', 'sql_table_creators_backfill_v1', 'sql_tables_prefix_garbage_cleanup_v1')`); err != nil {
		t.Fatalf("reset backfill markers: %v", err)
	}
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (backfills): %v", err)
	}

	count := func(query string, args ...interface{}) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
			t.Fatalf("count query %q: %v", query, err)
		}
		return n
	}

	// Backfill создателей: префикс, двойная точка, UNLOGGED; plain-create
	// не матчится формами; дубль корректной строки не создаётся (NOT EXISTS).
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE table_name = 'tReplTables' AND context = 'create'`); n != 1 {
		t.Fatalf("tReplTables create rows = %d, want 1 (NOT EXISTS guard)", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE table_name = 'pAPI_ExpProperty_Type' AND context = 'create' AND line_number = 20`); n != 1 {
		t.Fatalf("pAPI_ExpProperty_Type create rows = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE table_name = 'dcSubcontoConnect' AND context = 'create' AND line_number = 25`); n != 1 {
		t.Fatalf("dcSubcontoConnect create rows = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE table_name = 'tPlain'`); n != 0 {
		t.Fatalf("tPlain rows = %d, want 0 (plain-create не матчится backfill-формами)", n)
	}

	// symbols для вставленных создателей зеркалят индексатор: entity_id
	// указывает на строку sql_tables (tReplTables не в списке: его создатель
	// существовал до миграции, NOT EXISTS корректно пропустил вставку —
	// и строку, и symbol).
	for _, name := range []string{"pAPI_ExpProperty_Type", "dcSubcontoConnect"} {
		var entityID int64
		if err := db.QueryRowContext(ctx, `
			SELECT s.entity_id FROM symbols s WHERE s.symbol_name = $1 AND s.symbol_type = 'table' AND s.entity_type = 'sql'
		`, name).Scan(&entityID); err != nil {
			t.Fatalf("symbol for %s: %v", name, err)
		}
		if n := count(`SELECT COUNT(*) FROM sql_tables WHERE id = $1 AND table_name = $2`, entityID, name); n != 1 {
			t.Fatalf("symbol %s entity_id %d does not point to its sql_tables row", name, entityID)
		}
	}

	// Backfill реестра: 3 регистрации, скобочное имя без скобок, закомментированная не вставляется.
	if n := count(`SELECT COUNT(*) FROM reg_table_name WHERE file_id = $1`, ptFile); n != 3 {
		t.Fatalf("reg_table_name rows = %d, want 3", n)
	}
	for _, name := range []string{"hDocument", "tKey", "tPoolAccountLimitTable"} {
		if n := count(`SELECT COUNT(*) FROM reg_table_name WHERE file_id = $1 AND table_name = $2`, ptFile, name); n != 1 {
			t.Fatalf("reg_table_name %s rows = %d, want 1", name, n)
		}
	}
	if n := count(`SELECT COUNT(*) FROM reg_table_name WHERE table_name = 'tCommented'`); n != 0 {
		t.Fatalf("commented registration inserted: %d", n)
	}

	// Очистка мусора: dbo/MIDDLE во всех таблицах, рёбра на фиктивные строки.
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE LOWER(table_name) IN ('dbo','middle')`); n != 0 {
		t.Fatalf("sql_tables garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_column_definitions WHERE LOWER(table_name) IN ('dbo','middle')`); n != 0 {
		t.Fatalf("sql_column_definitions garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_columns WHERE LOWER(table_name) IN ('dbo','middle')`); n != 0 {
		t.Fatalf("sql_columns garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_index_definitions WHERE LOWER(table_name) IN ('dbo','middle')`); n != 0 {
		t.Fatalf("sql_index_definitions garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_index_definition_fields`); n != 0 {
		t.Fatalf("sql_index_definition_fields rows = %d, want 0 (каскад по FK)", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_procedures WHERE LOWER(proc_name) = 'dbo'`); n != 0 {
		t.Fatalf("sql_procedures garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM symbols WHERE symbol_type IN ('table','procedure') AND LOWER(symbol_name) IN ('dbo','middle')`); n != 0 {
		t.Fatalf("symbols garbage rows = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM relations WHERE target_type = 'sql_table'`); n != 1 {
		t.Fatalf("relations to sql_tables = %d, want 1 (только легитимное ребро)", n)
	}
	if n := count(`SELECT COUNT(*) FROM relations WHERE target_type = 'sql_procedure'`); n != 0 {
		t.Fatalf("relations to sql_procedures = %d, want 0", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_column_definitions WHERE definition_kind = 'alter_add' AND UPPER(column_name) IN ('CONSTRAINT','PRIMARY','FOREIGN','UNIQUE','CHECK','DEFAULT','COLUMN')`); n != 0 {
		t.Fatalf("crooked alter_add rows = %d, want 0", n)
	}

	// Легитимные данные остаются.
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE table_name = 'tLegacy'`); n != 1 {
		t.Fatalf("legit sql_tables tLegacy rows = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM symbols WHERE symbol_type = 'define' AND LOWER(symbol_name) = 'middle'`); n != 1 {
		t.Fatalf("legit define MIDDLE rows = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_column_definitions WHERE definition_kind = 'alter_add' AND column_name = 'NewCol'`); n != 1 {
		t.Fatalf("legit alter_add NewCol rows = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM relations r JOIN sql_tables t ON t.id = r.target_id WHERE r.target_type = 'sql_table' AND t.table_name = 'tLegacy'`); n != 1 {
		t.Fatalf("legit relation to tLegacy = %d, want 1", n)
	}

	// Маркеры миграций зафиксированы.
	for _, marker := range []string{"reg_table_name_backfill_v1", "sql_table_creators_backfill_v1", "sql_tables_prefix_garbage_cleanup_v1", store.CurrentSchemaVersion} {
		if n := count(`SELECT COUNT(*) FROM schema_migrations WHERE version = $1`, marker); n != 1 {
			t.Fatalf("migration marker %s = %d, want 1", marker, n)
		}
	}

	// No-op на уровне маркеров миграций: сбрасываем только маркер версии —
	// проход доходит до applyMigrationTx, видит маркеры backfill'ов и не
	// выполняет ни вставки, ни удаления (ручная мутация не затирается).
	if _, err := db.Exec(`UPDATE reg_table_name SET table_name = 'MANUAL' WHERE table_name = 'tKey'`); err != nil {
		t.Fatalf("manual mutation: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO sql_tables (file_id, table_name, context, is_temporary, line_number, column_number) VALUES ($1, 'dbo', 'select', false, 99, 0)`, eisFile); err != nil {
		t.Fatalf("manual garbage insert: %v", err)
	}
	testutil.ResetSchemaVersion(t, db)
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (no-op): %v", err)
	}
	if n := count(`SELECT COUNT(*) FROM reg_table_name WHERE table_name = 'MANUAL'`); n != 1 {
		t.Fatalf("no-op reg backfill re-ran: MANUAL = %d, want 1", n)
	}
	if n := count(`SELECT COUNT(*) FROM sql_tables WHERE LOWER(table_name) = 'dbo'`); n != 1 {
		t.Fatalf("no-op cleanup re-ran: manual dbo garbage = %d, want 1 (не удаляется повторно)", n)
	}
}

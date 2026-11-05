//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// TestSymbolsOwnerName_BackfillIdempotent — колонка symbols.owner_name
// создаётся схемой (v5), backfill-миграция symbols_owner_name_backfill_v1
// однократно заполняет легаси-строки (резолвнутые классы/формы — именем,
// нерезолвнутые — пустой строкой), строки прочих типов не переписываются и
// повторный прогон после маркера не выполняет UPDATE (change
// symbols-owner-key-materialization).
func TestSymbolsOwnerName_BackfillIdempotent(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// Колонка существует на свежей БД (CREATE TABLE + идемпотентный ALTER).
	var hasColumn bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		 WHERE table_name = 'symbols' AND column_name = 'owner_name')
	`).Scan(&hasColumn); err != nil {
		t.Fatalf("column check: %v", err)
	}
	if !hasColumn {
		t.Fatalf("symbols.owner_name missing on fresh schema")
	}

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
	pasFile := newFile("/repo/uMain.pas", "pas")
	dfmFile := newFile("/repo/AimCnt.dfm", "dfm")

	// Легаси-данные: класс TFormA (резолвнут), формы AimCnt (резолвнута);
	// методы unresolvedZero (class_id = 0) и unresolvedDangling (class_id
	// указывает на несуществующий класс).
	var unitID int64
	if err := db.QueryRow(`INSERT INTO pas_units (file_id, unit_name, line_start, line_end) VALUES ($1, 'uMain', 1, 2) RETURNING id`, pasFile).Scan(&unitID); err != nil {
		t.Fatalf("insert unit: %v", err)
	}
	var classID int64
	if err := db.QueryRow(`INSERT INTO pas_classes (file_id, unit_id, class_name, line_start, line_end) VALUES ($1, $2, 'TFormA', 1, 2) RETURNING id`, pasFile, unitID).Scan(&classID); err != nil {
		t.Fatalf("insert class: %v", err)
	}
	newMethod := func(classID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO pas_methods (file_id, class_id, unit_id, method_name, line_number) VALUES ($1, $2, $3, $4, 1) RETURNING id`, pasFile, classID, unitID, name).Scan(&id); err != nil {
			t.Fatalf("insert method %s: %v", name, err)
		}
		return id
	}
	resolvedMethod := newMethod(classID, "Execute")
	unresolvedZero := newMethod(0, "Save")
	unresolvedDangling := newMethod(999999, "Load")

	var formID int64
	if err := db.QueryRow(`INSERT INTO dfm_forms (file_id, form_name, form_class, line_start, line_end) VALUES ($1, 'AimCnt', 'TAimCnt', 1, 2) RETURNING id`, dfmFile).Scan(&formID); err != nil {
		t.Fatalf("insert form: %v", err)
	}
	var componentID int64
	if err := db.QueryRow(`INSERT INTO dfm_components (file_id, form_id, component_name, component_type, line_start, line_end) VALUES ($1, $2, 'dlName', 'TLabel', 1, 2) RETURNING id`, dfmFile, formID).Scan(&componentID); err != nil {
		t.Fatalf("insert component: %v", err)
	}

	// Легаси-символы без owner_name (DEFAULT '' — как записи до v5).
	newSymbol := func(entityID int64, name, symbolType, entityType string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
			VALUES ($1, $2, $3, $4, $5, 1) RETURNING id
		`, pasFile, name, symbolType, entityType, entityID).Scan(&id); err != nil {
			t.Fatalf("insert symbol %s: %v", name, err)
		}
		return id
	}
	resolvedSymbol := newSymbol(resolvedMethod, "Execute", "method", "pas")
	zeroSymbol := newSymbol(unresolvedZero, "Save", "method", "pas")
	danglingSymbol := newSymbol(unresolvedDangling, "Load", "method", "pas")
	componentSymbol := newSymbol(componentID, "dlName", "component", "dfm")
	procedureSymbol := newSymbol(0, "pAPI_Old", "procedure", "sql")

	// Эмуляция БД до миграции: сброс маркера версии и маркера backfill —
	// иначе fast-path пропустит проход.
	testutil.ResetSchemaVersion(t, db)
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = 'symbols_owner_name_backfill_v1'`); err != nil {
		t.Fatalf("reset backfill marker: %v", err)
	}
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (backfill): %v", err)
	}

	ownerOf := func(symbolID int64) string {
		var owner string
		if err := db.QueryRowContext(ctx, `SELECT owner_name FROM symbols WHERE id = $1`, symbolID).Scan(&owner); err != nil {
			t.Fatalf("read owner_name of symbol %d: %v", symbolID, err)
		}
		return owner
	}
	if got := ownerOf(resolvedSymbol); got != "TFormA" {
		t.Fatalf("resolved method owner = %q, want TFormA", got)
	}
	if got := ownerOf(zeroSymbol); got != "" {
		t.Fatalf("unresolved (class_id=0) method owner = %q, want \"\"", got)
	}
	if got := ownerOf(danglingSymbol); got != "" {
		t.Fatalf("unresolved (dangling class_id) method owner = %q, want \"\"", got)
	}
	if got := ownerOf(componentSymbol); got != "TAimCnt" {
		t.Fatalf("component owner = %q, want TAimCnt", got)
	}
	if got := ownerOf(procedureSymbol); got != "" {
		t.Fatalf("procedure owner = %q, want \"\" (DEFAULT не переписывается)", got)
	}

	// Идемпотентность на уровне маркера миграции: сбрасываем только маркер
	// версии схемы — проход доходит до applyMigrationTx, видит маркер
	// symbols_owner_name_backfill_v1 и не выполняет UPDATE: ручная мутация
	// не возвращается к вычисленному значению.
	if _, err := db.Exec(`UPDATE symbols SET owner_name = 'MANUAL' WHERE id = $1`, resolvedSymbol); err != nil {
		t.Fatalf("manual mutation: %v", err)
	}
	testutil.ResetSchemaVersion(t, db)
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (marker check): %v", err)
	}
	if got := ownerOf(resolvedSymbol); got != "MANUAL" {
		t.Fatalf("marker check reverted owner to %q, want MANUAL (UPDATE must not re-run while backfill marker present)", got)
	}

	// Апгрейд легаси-БД без колонки: DROP COLUMN + сброс обоих маркеров —
	// ALTER ... ADD COLUMN IF NOT EXISTS пересоздаёт колонку, backfill
	// заполняет её заново (предикат owner_name='' проходит по свежему DEFAULT).
	if _, err := db.Exec(`ALTER TABLE symbols DROP COLUMN owner_name`); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	testutil.ResetSchemaVersion(t, db)
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = 'symbols_owner_name_backfill_v1'`); err != nil {
		t.Fatalf("reset backfill marker (upgrade phase): %v", err)
	}
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (upgrade): %v", err)
	}
	if got := ownerOf(resolvedSymbol); got != "TFormA" {
		t.Fatalf("after upgrade resolved method owner = %q, want TFormA", got)
	}
	if got := ownerOf(componentSymbol); got != "TAimCnt" {
		t.Fatalf("after upgrade component owner = %q, want TAimCnt", got)
	}
	if got := ownerOf(procedureSymbol); got != "" {
		t.Fatalf("after upgrade procedure owner = %q, want \"\"", got)
	}
}

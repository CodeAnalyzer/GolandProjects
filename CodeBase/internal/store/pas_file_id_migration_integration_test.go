//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// TestInitSchema_PASFileIDMigration — денормализация file_id на
// pas_classes/pas_methods/pas_fields: имитация легаси-БД (колонка
// отсутствует), миграция без переиндексации, удаление сирот до backfill,
// идемпотентность повторного запуска и каскадное удаление файла.
func TestInitSchema_PASFileIDMigration(t *testing.T) {
	db := testutil.Open(t)

	// Легаси-состояние v1: колонка file_id отсутствует; CASCADE уводит
	// FK-констрейнты и индексы idx_pas_*_file_id
	for _, table := range []string{"pas_classes", "pas_methods", "pas_fields"} {
		if _, err := db.Exec(`ALTER TABLE ` + table + ` DROP COLUMN file_id CASCADE`); err != nil {
			t.Fatalf("drop legacy %s.file_id: %v", table, err)
		}
	}

	// Легаси-данные: валидная цепочка file → unit → class → method → field
	// плюс сироты — метод с несуществующим unit_id и поле с NULL class_id
	// (накопленные инкрементальными update до миграции)
	var scanRunID, fileID, unitID, classID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/repo', 'done') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, '/repo/unit1.pas', 'unit1.pas', '.pas', 'hash', NOW()) RETURNING id
	`, scanRunID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO pas_units (file_id, unit_name) VALUES ($1, 'Unit1') RETURNING id
	`, fileID).Scan(&unitID); err != nil {
		t.Fatalf("insert unit: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO pas_classes (unit_id, class_name) VALUES ($1, 'TFoo') RETURNING id
	`, unitID).Scan(&classID); err != nil {
		t.Fatalf("insert class: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pas_methods (class_id, unit_id, method_name) VALUES ($1, $2, 'Bar')
	`, classID, unitID); err != nil {
		t.Fatalf("insert method: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pas_fields (class_id, field_name) VALUES ($1, 'Baz')
	`, classID); err != nil {
		t.Fatalf("insert field: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pas_methods (unit_id, method_name) VALUES (424242, 'OrphanMethod')
	`); err != nil {
		t.Fatalf("insert orphan method: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pas_fields (class_id, field_name) VALUES (NULL, 'OrphanField')
	`); err != nil {
		t.Fatalf("insert orphan field: %v", err)
	}

	// Миграция
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchema migration: %v", err)
	}

	// Сироты удалены
	for _, query := range []string{
		`SELECT count(*) FROM pas_methods WHERE method_name = 'OrphanMethod'`,
		`SELECT count(*) FROM pas_fields WHERE field_name = 'OrphanField'`,
	} {
		var n int
		if err := db.QueryRow(query).Scan(&n); err != nil {
			t.Fatalf("check orphan: %v", err)
		}
		if n != 0 {
			t.Fatalf("orphan rows survived migration: %d", n)
		}
	}

	// file_id заполнен backfill-ом у всех выживших строк
	for _, query := range []string{
		`SELECT count(*) FROM pas_classes WHERE file_id = $1`,
		`SELECT count(*) FROM pas_methods WHERE file_id = $1`,
		`SELECT count(*) FROM pas_fields WHERE file_id = $1`,
	} {
		var n int
		if err := db.QueryRow(query, fileID).Scan(&n); err != nil {
			t.Fatalf("check backfill: %v", err)
		}
		if n != 1 {
			t.Fatalf("backfilled rows = %d, want 1", n)
		}
	}

	// NOT NULL + FK ON DELETE CASCADE на всех трёх таблицах
	for _, table := range []string{"pas_classes", "pas_methods", "pas_fields"} {
		var notNull bool
		if err := db.QueryRow(`
			SELECT a.attnotnull
			FROM pg_attribute a
			WHERE a.attrelid = $1::regclass AND a.attname = 'file_id'
		`, table).Scan(&notNull); err != nil {
			t.Fatalf("check %s.file_id not null: %v", table, err)
		}
		var cascade bool
		if err := db.QueryRow(`
			SELECT EXISTS(
				SELECT 1 FROM pg_constraint
				WHERE conrelid = $1::regclass
				  AND conname = $2
				  AND confdeltype = 'c'
			)
		`, table, table+"_file_id_fkey").Scan(&cascade); err != nil {
			t.Fatalf("check %s file_id cascade FK: %v", table, err)
		}
		if !notNull || !cascade {
			t.Fatalf("%s: notNull=%v cascadeFK=%v", table, notNull, cascade)
		}
	}

	// Повторный запуск — no-op: данные не меняются
	if err := db.InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("second InitSchema: %v", err)
	}
	for _, table := range []string{"pas_classes", "pas_methods", "pas_fields"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatalf("count %s after rerun: %v", table, err)
		}
		if n != 1 {
			t.Fatalf("%s rows after rerun = %d, want 1 (no-op)", table, n)
		}
	}

	// Каскадное удаление файла уводит все PAS-строки (сирот не остаётся)
	if _, err := db.Exec(`DELETE FROM files WHERE id = $1`, fileID); err != nil {
		t.Fatalf("delete file: %v", err)
	}
	for _, table := range []string{"pas_units", "pas_classes", "pas_methods", "pas_fields"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
			t.Fatalf("count %s after cascade: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("%s rows after file delete = %d, want 0", table, n)
		}
	}
}

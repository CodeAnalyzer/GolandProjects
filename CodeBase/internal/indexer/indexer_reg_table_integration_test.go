//go:build integration

package indexer

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store/testutil"
)

// Фикстура ProductTable.sql (только ASCII — SQL-файлы читаются в CP866,
// подмножество ASCII совпадает). Регистрации: строки 7, 8, 10, 12
// (скобочная форма без скобок); закомментированная строка 11 и маркер
// __MASS_DELETE_TABLE_NAME__ (строка 5) записей не дают.
const regFixtureFull = `#include <macros.h>

#define CURRENT_MODULEID 514

__MASS_DELETE_TABLE_NAME__
go
__REG_TABLE_NAME__(hDocument)
__REG_TABLE_NAME__(tKey)
go
__REG_TABLE_NAME__(tWebUserCertificate)
-- __REG_TABLE_NAME__(tCommentedOut)
__REG_TABLE_NAME__([tPoolAccountLimitTable])
go
`

// Правка: tKey удалён, добавлен tNovosibNumbers.
const regFixtureReduced = `#include <macros.h>

#define CURRENT_MODULEID 514

__MASS_DELETE_TABLE_NAME__
go
__REG_TABLE_NAME__(hDocument)
go
__REG_TABLE_NAME__(tWebUserCertificate)
-- __REG_TABLE_NAME__(tCommentedOut)
__REG_TABLE_NAME__([tPoolAccountLimitTable])
__REG_TABLE_NAME__(tNovosibNumbers)
go
`

func newRegTestIndexer(t *testing.T) (*Indexer, string, *bytes.Buffer) {
	t.Helper()
	db := testutil.Open(t)
	var errBuf bytes.Buffer
	idx := &Indexer{
		db: db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.sql"},
		}},
		errorLogger: log.New(&errBuf, "", 0),
		shared:      newIndexerSharedState(),
	}
	return idx, t.TempDir(), &errBuf
}

func writeRegFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func regQueryInt(t *testing.T, idx *Indexer, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// TestRegTableIndexing: индексация ProductTable.sql наполняет reg_table_name
// ровно регистрациями файла: скобочная форма нормализуется, закомментированная
// строка и маркер __MASS_DELETE_TABLE_NAME__ записей не дают.
func TestRegTableIndexing(t *testing.T) {
	idx, root, errBuf := newRegTestIndexer(t)
	writeRegFile(t, root, "Payments/SERVER/DB/ProductTable.sql", regFixtureFull)

	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}

	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name`); n != 4 {
		t.Fatalf("reg_table_name rows = %d, want 4", n)
	}

	var name string
	var line int
	for _, want := range []struct {
		table string
		line  int
	}{
		{"hdocument", 7},
		{"tkey", 8},
		{"twebusercertificate", 10},
		{"tpoolaccountlimittable", 12},
	} {
		if err := idx.db.QueryRow(
			`SELECT table_name, line_number FROM reg_table_name WHERE LOWER(table_name) = $1`, want.table,
		).Scan(&name, &line); err != nil {
			t.Fatalf("registration %s: %v", want.table, err)
		}
		if !strings.EqualFold(name, want.table) || line != want.line {
			t.Fatalf("registration %s = (%q, %d), want (%q, %d)", want.table, name, line, want.table, want.line)
		}
	}

	// Закомментированная регистрация и маркер массового удаления записей не дают
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name WHERE LOWER(table_name) IN ('tcommentedout','__mass_delete_table_name__')`); n != 0 {
		t.Fatalf("commented/marker rows = %d, want 0", n)
	}

	// Файл отражён в files, ошибки разбора нет
	if n := regQueryInt(t, idx, `SELECT count(*) FROM files WHERE path LIKE '%ProductTable.sql'`); n != 1 {
		t.Fatalf("files rows = %d, want 1", n)
	}
	if errBuf.Len() != 0 {
		t.Fatalf("errorLogger must be empty, got: %q", errBuf.String())
	}
}

// TestRegTableUpdateIncremental: правка ProductTable.sql пересоздаёт
// регистрации файла по новому содержимому; удаление файла сносит записи
// каскадом вместе со строкой files.
func TestRegTableUpdateIncremental(t *testing.T) {
	idx, root, _ := newRegTestIndexer(t)
	regPath := filepath.Join(root, "Payments", "SERVER", "DB", "ProductTable.sql")
	writeRegFile(t, root, "Payments/SERVER/DB/ProductTable.sql", regFixtureFull)

	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name`); n != 4 {
		t.Fatalf("initial reg_table_name rows = %d, want 4", n)
	}

	// Правка: tKey удалён, добавлен tNovosibNumbers (содержимое и размер изменились)
	writeRegFile(t, root, "Payments/SERVER/DB/ProductTable.sql", regFixtureReduced)
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update modified: %v", err)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name`); n != 4 {
		t.Fatalf("reg_table_name rows after edit = %d, want 4", n)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name WHERE LOWER(table_name) = 'tkey'`); n != 0 {
		t.Fatalf("tkey rows after edit = %d, want 0", n)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name WHERE LOWER(table_name) = 'tnovosibnumbers'`); n != 1 {
		t.Fatalf("tnovosibnumbers rows after edit = %d, want 1", n)
	}
	// Выжившие регистрации не задублированы
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name WHERE LOWER(table_name) = 'hdocument'`); n != 1 {
		t.Fatalf("hdocument rows after edit = %d, want 1 (surviving rows re-created once)", n)
	}

	// Удаление файла: строка files исчезает, регистрации сносятся каскадом
	if err := os.Remove(regPath); err != nil {
		t.Fatalf("remove ProductTable.sql: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update after delete: %v", err)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM reg_table_name`); n != 0 {
		t.Fatalf("reg_table_name rows after delete = %d, want 0 (cascade)", n)
	}
	if n := regQueryInt(t, idx, `SELECT count(*) FROM files WHERE path LIKE '%ProductTable.sql'`); n != 0 {
		t.Fatalf("files rows after delete = %d, want 0", n)
	}
}

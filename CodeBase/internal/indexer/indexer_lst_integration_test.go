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

const lstFixtureFull = `[FA-Administrator]
pAPI_AttrValue;API_Administrator
pAPI_STP_ParamSet;API_STP
[FA-GAAP]
pRuleDoc_QtyStack;GAAP
мусорная строка без точки с запятой
pAnother;GAAP
`

const lstFixtureReduced = `[FA-Administrator]
pAPI_AttrValue;API_Administrator
[FA-GAAP]
pRuleDoc_QtyStack;GAAP
мусорная строка без точки с запятой
pAnother;GAAP
`

const ldAfterApiRegFixture = `FA/fa-cards/Server/API_Cards/DSArchitectData/x.xml
FA/fa-payments/Server/API_Payments/DSArchitectData/y.xml
`

func newLSTTestIndexer(t *testing.T) (*Indexer, string, *bytes.Buffer) {
	t.Helper()
	db := testutil.Open(t)
	var errBuf bytes.Buffer
	idx := &Indexer{
		db: db,
		config: &config.Config{Indexer: config.IndexerConfig{
			Parallel:        1,
			BatchSize:       100,
			IncludePatterns: []string{"*.lst"},
		}},
		errorLogger: log.New(&errBuf, "", 0),
		shared:      newIndexerSharedState(),
	}
	return idx, t.TempDir(), &errBuf
}

func writeLSTFile(t *testing.T, root, rel, content string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatalf("write %s: %v", rel, err)
	}
}

func lstQueryInt(t *testing.T, idx *Indexer, q string, args ...interface{}) int {
	t.Helper()
	var n int
	if err := idx.db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return n
}

// TestLSTIndexing: канонический ModuleTableInMemory.lst порождает записи
// memory_tables (нормализованный продукт, модуль, 1-based строка); ld_*.lst
// индексируется как файл без сущностей; мусорная строка попадает в
// errorLogger/stats.Errors, не прерывая разбор.
func TestLSTIndexing(t *testing.T) {
	idx, root, errBuf := newLSTTestIndexer(t)
	writeLSTFile(t, root, "DBMaintenance_Ext/Server/ModuleTableInMemory.lst", lstFixtureFull)
	writeLSTFile(t, root, "Server/ld_AfterApiReg.lst", ldAfterApiRegFixture)

	stats, err := idx.Init(root, 1)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	// 4 корректные декларации: 2 из FA-Administrator + 2 из FA-GAAP
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables`); n != 4 {
		t.Fatalf("memory_tables rows = %d, want 4", n)
	}

	// Запись с нормализацией продукта, модулем и номером строки (1-based)
	var product, module string
	var line int
	if err := idx.db.QueryRow(
		`SELECT product_name, module_name, line_number FROM memory_tables WHERE LOWER(table_name) = 'papi_attrvalue'`,
	).Scan(&product, &module, &line); err != nil {
		t.Fatalf("pAPI_AttrValue: %v", err)
	}
	if product != "fa-administrator" || module != "API_Administrator" || line != 2 {
		t.Fatalf("pAPI_AttrValue = (%q, %q, %d), want (fa-administrator, API_Administrator, 2)", product, module, line)
	}
	if err := idx.db.QueryRow(
		`SELECT product_name, module_name, line_number FROM memory_tables WHERE LOWER(table_name) = 'pruledoc_qtystack'`,
	).Scan(&product, &module, &line); err != nil {
		t.Fatalf("pRuleDoc_QtyStack: %v", err)
	}
	if product != "fa-gaap" || module != "GAAP" || line != 5 {
		t.Fatalf("pRuleDoc_QtyStack = (%q, %q, %d), want (fa-gaap, GAAP, 5)", product, module, line)
	}

	// Мусорная строка не прервала разбор: pAnother после неё проиндексирован
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables WHERE LOWER(table_name) = 'panother'`); n != 1 {
		t.Fatalf("panother rows = %d, want 1 (malformed line must not stop parsing)", n)
	}

	// ld_AfterApiReg.lst — файл без сущностей: записей с его file_id нет
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables mt
		JOIN files f ON f.id = mt.file_id WHERE f.path LIKE '%ld_AfterApiReg.lst'`); n != 0 {
		t.Fatalf("memory_tables rows from ld_AfterApiReg.lst = %d, want 0", n)
	}

	// Оба .lst отражены в files
	for _, name := range []string{"ModuleTableInMemory.lst", "ld_AfterApiReg.lst"} {
		if n := lstQueryInt(t, idx, `SELECT count(*) FROM files WHERE path LIKE '%`+name+`'`); n != 1 {
			t.Fatalf("files rows for %s = %d, want 1", name, n)
		}
	}

	// Счётчики: оба .lst обработаны, одна мусорная строка в ошибках
	if stats.LSTFiles != 2 {
		t.Fatalf("stats.LSTFiles = %d, want 2", stats.LSTFiles)
	}
	if stats.Errors != 1 {
		t.Fatalf("stats.Errors = %d, want 1 (malformed line)", stats.Errors)
	}
	if !strings.Contains(errBuf.String(), "malformed ModuleTableInMemory.lst line") || !strings.Contains(errBuf.String(), ":6") {
		t.Fatalf("errorLogger must report malformed line 6, got: %q", errBuf.String())
	}
}

// TestLSTUpdateIncremental: правка ModuleTableInMemory.lst пересоздаёт записи
// файла по новому содержимому; удаление файла удаляет строку files и каскадно
// сносит записи признака.
func TestLSTUpdateIncremental(t *testing.T) {
	idx, root, _ := newLSTTestIndexer(t)
	lstPath := filepath.Join(root, "DBMaintenance_Ext", "Server", "ModuleTableInMemory.lst")
	writeLSTFile(t, root, "DBMaintenance_Ext/Server/ModuleTableInMemory.lst", lstFixtureFull)

	if _, err := idx.Init(root, 1); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables`); n != 4 {
		t.Fatalf("initial memory_tables rows = %d, want 4", n)
	}

	// Правка: строка pAPI_STP_ParamSet удалена (размер файла изменился)
	writeLSTFile(t, root, "DBMaintenance_Ext/Server/ModuleTableInMemory.lst", lstFixtureReduced)
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update modified: %v", err)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables`); n != 3 {
		t.Fatalf("memory_tables rows after edit = %d, want 3", n)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables WHERE LOWER(table_name) = 'papi_stp_paramset'`); n != 0 {
		t.Fatalf("papi_stp_paramset rows after edit = %d, want 0", n)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables WHERE LOWER(table_name) = 'papi_attrvalue'`); n != 1 {
		t.Fatalf("papi_attrvalue rows after edit = %d, want 1 (surviving rows re-created once)", n)
	}

	// Удаление файла: строка files исчезает, записи признака сносятся каскадом
	if err := os.Remove(lstPath); err != nil {
		t.Fatalf("remove lst: %v", err)
	}
	if _, err := idx.Update(root, true, 1); err != nil {
		t.Fatalf("Update after delete: %v", err)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM memory_tables`); n != 0 {
		t.Fatalf("memory_tables rows after delete = %d, want 0 (cascade)", n)
	}
	if n := lstQueryInt(t, idx, `SELECT count(*) FROM files WHERE path LIKE '%ModuleTableInMemory.lst'`); n != 0 {
		t.Fatalf("files rows after delete = %d, want 0", n)
	}
}

//go:build integration

package review

import (
	"context"
	"testing"

	sqlparser "github.com/codebase/internal/parser/sql"
	"github.com/codebase/internal/store/testutil"
)

// TestIsViewTable_LazyLoadFromFragments: множество имён view лениво
// выводится из query_fragments (create [or replace] view), включая
// схемный префикс public.; обращение к view в серверном коде даёт
// viewUsageInServerCode, а tableExistsInMetadata на то же имя
// подавляется (атрибуция одному правилу).
func TestIsViewTable_LazyLoadFromFragments(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	var scanRunID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('/review-view-fixture', 'finished') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("insert scan_run: %v", err)
	}
	newFile := func(path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at)
			VALUES ($1, $2, $2, 'sql', 0, 'x', NOW()) RETURNING id`, scanRunID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	viewDefFileID := newFile("/review/view-fixture/view_def.sql")

	newViewFragment := func(fileID int64, text string) {
		if _, err := db.Exec(`
			INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, query_hash, tables_referenced, context, line_number)
			VALUES ($1, 'sql_file', 0, 'sql_script', 'sql_script', $2, '', '[]', 'sql_statement', 1)`, fileID, text); err != nil {
			t.Fatalf("insert view fragment: %v", err)
		}
	}
	newViewFragment(viewDefFileID, "create view pViewFixture\n  select ID from tFixtureSrc")
	newViewFragment(viewDefFileID, "create or replace view public.tViewPgFixture as\n  select ID from tFixtureSrc2")
	// Управляющий фрагмент: SELECT внутри процедуры — не view-определение.
	procFileID := newFile("/review/view-fixture/no_view.sql")
	newViewFragment(procFileID, "select ID from tNotAView")
	// Два определения в одном фрагменте: 'g'-флаг извлекает оба.
	newViewFragment(viewDefFileID, "create view pViewFirst\n  select 1;\ncreate view pViewSecond\n  select 2")

	r := NewRunner(db)
	if !r.isViewTable(ctx, "pViewFixture") {
		t.Fatal("isViewTable(pViewFixture) must be true after lazy load")
	}
	if !r.isViewTable(ctx, "dbo.pViewFixture") {
		t.Fatal("isViewTable(dbo.pViewFixture) must be true (схемный префикс срезается)")
	}
	if !r.isViewTable(ctx, "TVIEWPGFIXTURE") {
		t.Fatal("isViewTable(TVIEWPGFIXTURE) must be true (public. + регистронезависимость)")
	}
	if r.isViewTable(ctx, "tNotAView") {
		t.Fatal("isViewTable(tNotAView) must be false: select-фрагмент не является view-определением")
	}
	if !r.isViewTable(ctx, "pViewFirst") || !r.isViewTable(ctx, "pViewSecond") {
		t.Fatal("оба определения из одного фрагмента должны попасть в кэш ('g'-флаг)")
	}

	// Серверный код: SELECT из view → viewUsageInServerCode;
	// tableExistsInMetadata на то же имя подавлен (нет создателя-таблицы).
	// Ссылка на tViewPgFixture — bare: парсерный tableNameRe срезает
	// qualified-имя до первого токена (dbo./public. в ссылке не резолвится
	// ни одним правилом); префикс public. проверяется на стороне определения.
	content := `create proc UseView
as
begin
  select ID from pViewFixture
  select ID from tViewPgFixture
end
`
	parser := sqlparser.NewParser()
	parsed, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	r.exec = &reviewExecContext{filePath: "/review/view-fixture/use_view.sql", content: []byte(content)}
	file := &indexedFile{Path: "/review/view-fixture/use_view.sql", DsProductID: 42}

	existsFindings, err := r.checkTableExistsInMetadata(ctx, parsed, file)
	if err != nil {
		t.Fatalf("checkTableExistsInMetadata: %v", err)
	}
	for _, f := range existsFindings {
		if f.Object == "pViewFixture" || f.Object == "tViewPgFixture" {
			t.Fatalf("view name must be suppressed in tableExistsInMetadata: %+v", f)
		}
	}

	viewFindings, err := r.checkViewUsageInServerCode(ctx, parsed, file)
	if err != nil {
		t.Fatalf("checkViewUsageInServerCode: %v", err)
	}
	if len(viewFindings) != 2 {
		t.Fatalf("expected 2 viewUsageInServerCode findings, got %d: %+v", len(viewFindings), viewFindings)
	}
	seen := map[string]bool{}
	for _, f := range viewFindings {
		if f.Rule != RuleViewUsageInServerCode || f.Severity != SeverityDeployStopper {
			t.Fatalf("unexpected finding: %+v", f)
		}
		seen[normalizeIdentifier(f.Object)] = true
	}
	if !seen["pviewfixture"] || !seen["tviewpgfixture"] {
		t.Fatalf("findings must cover both views: %+v", viewFindings)
	}
}

// TestIsViewTable_QueryError_FailOpen: реальная ошибка запроса (закрытое
// соединение) — не db == nil: кэш остаётся пустым и помеченным загруженным,
// правило инертно до конца запуска, review не падает.
func TestIsViewTable_QueryError_FailOpen(t *testing.T) {
	db := testutil.Open(t)
	db.Close()

	r := NewRunner(db)
	if r.isViewTable(context.Background(), "pViewFixture") {
		t.Fatal("isViewTable on closed DB must be false")
	}
	if !r.viewNamesLoaded {
		t.Fatal("viewNamesLoaded must be set (inert until end of run, no query storm)")
	}
	if len(r.viewNameCache) != 0 {
		t.Fatalf("cache must stay empty on query error, got %d names", len(r.viewNameCache))
	}
	if r.isViewTable(context.Background(), "pViewFixture") {
		t.Fatal("repeat call must stay false (loaded flag prevents query storm)")
	}
}

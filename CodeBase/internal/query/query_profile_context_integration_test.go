//go:build integration

package query

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedProfileContextFixture — два продукта с копиями одной процедуры,
// таблицы, символа, query-фрагмента и кода возврата:
//   - fa-prof: develop (default) + fa-prof-11-01-7;
//   - fa-other: develop (default).
func seedProfileContextFixture(t *testing.T) (*store.DB, branchIDs) {
	t.Helper()
	db := testutil.Open(t)

	type seededBranch struct {
		productID int64
		branchID  int64
	}
	ids := branchIDs{}

	insertProduct := func(name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
			t.Fatalf("insert product %s: %v", name, err)
		}
		return id
	}
	insertBranch := func(productID int64, name, key string, isDefault bool) int64 {
		var id int64
		path := fmt.Sprintf("/repo/p%d/%s", productID, key)
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			productID, name, key, path, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}

	profID := insertProduct("fa-prof")
	otherID := insertProduct("fa-other")
	ids.ProfProduct = profID
	ids.ProfDevelop = insertBranch(profID, "develop", "develop", true)
	ids.ProfRelease = insertBranch(profID, "FA-Prof-11-01-7", "fa-prof-11-01-7", false)
	ids.OtherProduct = otherID
	ids.OtherDevelop = insertBranch(otherID, "develop", "develop", true)

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	insertFile := func(productID, branchID int64, path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'hash-' || $4, NOW(), 'UTF8', 'SQL')
			RETURNING id`,
			scanID, productID, branchID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	profDevFile := insertFile(profID, ids.ProfDevelop, "/repo/fa-prof/develop/prof.sql")
	profRelFile := insertFile(profID, ids.ProfRelease, "/repo/fa-prof/11017/prof.sql")
	otherFile := insertFile(otherID, ids.OtherDevelop, "/repo/fa-other/develop/prof.sql")

	procs := []*model.SQLProcedure{
		{FileID: profRelFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 10, Description: "release copy"},
		{FileID: profDevFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 12, Description: "develop copy"},
		{FileID: otherFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 8, Description: "other copy"},
		{FileID: profDevFile, ProcName: "PROF_DevOnly", LineStart: 20, LineEnd: 30, Description: "only develop"},
	}
	if err := db.BatchInsertSQLProcedures(context.Background(), procs, 100); err != nil {
		t.Fatalf("insert procedures: %v", err)
	}

	// Таблицы и query-фрагменты одной формы во всех трёх файлах.
	tables := []struct {
		fileID int64
	}{{profDevFile}, {profRelFile}, {otherFile}}
	for i, tbl := range tables {
		if _, err := db.Exec(`
			INSERT INTO sql_tables (file_id, table_name, context, line_number)
			VALUES ($1, 'PROF_TABLE', 'select', $2)`, tbl.fileID, 10+i); err != nil {
			t.Fatalf("insert table: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, context, line_number)
			VALUES ($1, 'sql_procedure', 0, '', 'sql', 'SELECT * FROM PROF_TABLE', 'select', $2)`, tbl.fileID, 11+i); err != nil {
			t.Fatalf("insert fragment: %v", err)
		}
	}

	// ds_return_codes уникален по ret_code (глобальный справочник), поэтому
	// профильная ось проверяется на единственной строке: важен факт выбора
	// копии и заполнения product/branch_used/branch_requested.
	if _, err := db.Exec(`
		INSERT INTO ds_return_codes (file_id, ret_code, message, proc_name, module_id)
		VALUES ($1, 777, 'retcode release', 'PROF_Proc', 1)`, profRelFile); err != nil {
		t.Fatalf("insert retcode: %v", err)
	}

	// Символы процедуры (entity_id — id процедуры своего файла).
	rows, err := db.Query(`SELECT id, file_id FROM sql_procedures WHERE proc_name = 'PROF_Proc'`)
	if err != nil {
		t.Fatalf("select procedures: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var procID, fileID int64
		if err := rows.Scan(&procID, &fileID); err != nil {
			t.Fatalf("scan procedure: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number, signature)
			VALUES ($1, 'PROF_Proc', 'procedure', 'sql_procedure', $2, 1, '')`, fileID, procID); err != nil {
			t.Fatalf("insert symbol: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate procedures: %v", err)
	}

	return db, ids
}

// branchIDs — идентификаторы, нужные тестам профильного контекста.
type branchIDs struct {
	ProfProduct  int64
	ProfDevelop  int64
	ProfRelease  int64
	OtherProduct int64
	OtherDevelop int64
}

// TestProfileContext_Procedure — профиль выбирает линию релиза; запрошенная
// линия отражается в branch_requested.
func TestProfileContext_Procedure(t *testing.T) {
	db, ids := seedProfileContextFixture(t)
	ctx := context.Background()

	plain := New(db)
	base, err := plain.GetProcedureResult(ctx, "PROF_Proc")
	if err != nil {
		t.Fatalf("plain GetProcedureResult: %v", err)
	}
	if base.BranchUsed != "develop" || base.BranchRequested != "" {
		t.Fatalf("plain = %+v", base)
	}

	profiled := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		Branches:  map[int64]int64{ids.ProfProduct: ids.ProfRelease},
		Requested: map[string][]string{"fa-prof": {"fa-prof-11-01-7"}},
	}))
	got, err := profiled.GetProcedureResult(ctx, "PROF_Proc")
	if err != nil {
		t.Fatalf("profiled GetProcedureResult: %v", err)
	}
	if got.BranchUsed != "fa-prof-11-01-7" || got.Description != "release copy" {
		t.Fatalf("profiled = %+v", got)
	}
	if got.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("branch_requested = %q", got.BranchRequested)
	}
}

// TestProfileContext_Fallback — запрошенная линия не проиндексирована:
// выбирается default-ветка, branch_requested сохраняет запрос.
func TestProfileContext_Fallback(t *testing.T) {
	db, ids := seedProfileContextFixture(t)
	ctx := context.Background()

	q := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		// Линия не проиндексирована: ось профиля указывает на default-линию
		// продукта (так её строит dsinfosvc.ResolveAnalysisContext).
		Branches:  map[int64]int64{ids.ProfProduct: ids.ProfDevelop},
		Requested: map[string][]string{"fa-prof": {"fa-prof-11-01-6"}},
	}))
	got, err := q.GetProcedureResult(ctx, "PROF_Proc")
	if err != nil {
		t.Fatalf("GetProcedureResult: %v", err)
	}
	if got.BranchUsed != "develop" || got.Description != "develop copy" || got.Product != "fa-prof" {
		t.Fatalf("fallback = %+v", got)
	}
	if got.BranchRequested != "fa-prof-11-01-6" {
		t.Fatalf("branch_requested = %q, want fa-prof-11-01-6", got.BranchRequested)
	}
}

// TestProfileContext_ProductPriority — продукт профиля выигрывает у копии
// другого продукта даже при default-ветке последнего.
func TestProfileContext_ProductPriority(t *testing.T) {
	db, ids := seedProfileContextFixture(t)
	ctx := context.Background()

	q := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		Branches:  map[int64]int64{ids.OtherProduct: ids.OtherDevelop},
		Requested: map[string][]string{"fa-other": {"fa-other-11-01-7"}},
	}))
	got, err := q.GetProcedureResult(ctx, "PROF_Proc")
	if err != nil {
		t.Fatalf("GetProcedureResult: %v", err)
	}
	if got.Product != "fa-other" || got.BranchUsed != "develop" || got.Description != "other copy" {
		t.Fatalf("product priority = %+v", got)
	}
	if got.BranchRequested != "fa-other-11-01-7" {
		t.Fatalf("branch_requested = %q", got.BranchRequested)
	}
}

// TestProfileContext_EntitySurfaces — ось профиля действует в списковых
// поверхностях: таблицы, символы, фрагменты, коды возврата.
func TestProfileContext_EntitySurfaces(t *testing.T) {
	db, ids := seedProfileContextFixture(t)
	ctx := context.Background()

	q := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		Branches:  map[int64]int64{ids.ProfProduct: ids.ProfRelease},
		Requested: map[string][]string{"fa-prof": {"fa-prof-11-01-7"}},
	}))

	tables, err := q.SearchTable(ctx, "PROF_TABLE", false, 10)
	if err != nil {
		t.Fatalf("SearchTable: %v", err)
	}
	var profTable *TableResult
	for i := range tables {
		if tables[i].Product == "fa-prof" {
			profTable = &tables[i]
		}
	}
	if profTable == nil {
		t.Fatalf("fa-prof table not found: %+v", tables)
	}
	if profTable.BranchUsed != "fa-prof-11-01-7" || profTable.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("table = %+v", profTable)
	}

	symbols, err := q.SearchSymbol(ctx, "PROF_Proc", "procedure", false, 10)
	if err != nil {
		t.Fatalf("SearchSymbol: %v", err)
	}
	var profSymbol *SymbolResult
	for i := range symbols {
		if symbols[i].Product == "fa-prof" {
			profSymbol = &symbols[i]
		}
	}
	if profSymbol == nil {
		t.Fatalf("fa-prof symbol not found: %+v", symbols)
	}
	if profSymbol.BranchUsed != "fa-prof-11-01-7" || profSymbol.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("symbol = %+v", profSymbol)
	}

	fragments, err := q.SearchQueryFragment(ctx, "PROF_TABLE", 10)
	if err != nil {
		t.Fatalf("SearchQueryFragment: %v", err)
	}
	var profFragment *QueryFragmentResult
	for i := range fragments {
		if fragments[i].Product == "fa-prof" {
			profFragment = &fragments[i]
		}
	}
	if profFragment == nil {
		t.Fatalf("fa-prof fragment not found: %+v", fragments)
	}
	if profFragment.BranchUsed != "fa-prof-11-01-7" || profFragment.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("fragment = %+v", profFragment)
	}

	rc, err := q.LookupRetCode(ctx, 777)
	if err != nil {
		t.Fatalf("LookupRetCode: %v", err)
	}
	if rc == nil {
		t.Fatal("retcode not found")
	}
	// Ось профиля выбирает продукт профиля, а внутри него — линию релиза.
	if rc.Product != "fa-prof" || rc.BranchUsed != "fa-prof-11-01-7" {
		t.Fatalf("retcode = %+v", rc)
	}
	if rc.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("retcode branch_requested = %q", rc.BranchRequested)
	}
}

// TestProfileContext_ResponseWarnings — линия профиля проиндексирована, но
// сущности в ней нет: выбирается копия из develop, а предупреждение
// вычисляется по фактической паре branch_requested/branch_used.
func TestProfileContext_ResponseWarnings(t *testing.T) {
	db, ids := seedProfileContextFixture(t)
	ctx := context.Background()

	q := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		Branches:  map[int64]int64{ids.ProfProduct: ids.ProfRelease},
		Requested: map[string][]string{"fa-prof": {"fa-prof-11-01-7"}},
	}))
	got, err := q.GetProcedureResult(ctx, "PROF_DevOnly")
	if err != nil {
		t.Fatalf("GetProcedureResult: %v", err)
	}
	if got.BranchUsed != "develop" || got.Product != "fa-prof" {
		t.Fatalf("mismatch copy = %+v", got)
	}
	if got.BranchRequested != "fa-prof-11-01-7" {
		t.Fatalf("branch_requested = %q", got.BranchRequested)
	}
	warnings := CollectBranchWarnings(got)
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one", warnings)
	}
	if !strings.Contains(warnings[0], "fa-prof-11-01-7") || !strings.Contains(warnings[0], "develop") {
		t.Fatalf("warning text = %q", warnings[0])
	}
}

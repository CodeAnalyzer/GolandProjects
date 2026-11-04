//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedBranchDedupFixture — продукт fa-x с ветками develop (default) и
// fa-x-11-01-7, продукт fa-y (develop); таблицы/символы/методы/фрагменты
// в копиях обеих веток fa-x и в fa-y.
func seedBranchDedupFixture(t *testing.T) (*store.DB, *Query) {
	t.Helper()
	db := testutil.Open(t)

	newProduct := func(name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
			t.Fatalf("insert product %s: %v", name, err)
		}
		return id
	}
	fax := newProduct("fa-x")
	fay := newProduct("fa-y")

	newBranch := func(productID int64, productName, name, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, '/repo/' || $4 || '/' || $3, $5) RETURNING id
		`, productID, name, key, productName, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", name, err)
		}
		return id
	}
	faxDev := newBranch(fax, "fa-x", "develop", "develop", true)
	faxSP := newBranch(fax, "fa-x", "FA-X-11-01-7", "fa-x-11-01-7", false)
	fayDev := newBranch(fay, "fa-y", "develop", "develop", true)

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}

	newFile := func(productID, branchID int64, path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'hash-' || $4, NOW(), 'UTF8', 'SQL')
			RETURNING id
		`, scanID, productID, branchID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	devFile := newFile(fax, faxDev, "/repo/develop/src.sql")
	spFile := newFile(fax, faxSP, "/repo/fa-x-11-01-7/src.sql")
	yFile := newFile(fay, fayDev, "/repo-fa-y/src.sql")

	newTable := func(fileID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_tables (file_id, table_name, context) VALUES ($1, $2, 'create_table') RETURNING id
		`, fileID, name).Scan(&id); err != nil {
			t.Fatalf("insert table %s: %v", name, err)
		}
		return id
	}
	devTable := newTable(devFile, "TA_DUP")
	// Повторное объявление той же таблицы в том же файле: в list-запросе
	// путь файла обязан схлопнуться (DISTINCT в enrichment-лоадере), иначе
	// files/files_ids дублируются (регрессия, пойманная на боевом индексе).
	newTable(devFile, "TA_DUP")
	spTable := newTable(spFile, "TA_DUP")
	yTable := newTable(yFile, "TA_DUP")
	newTable(spFile, "TA_SPONLY")

	// Колонки для проверки изоляции enrichment по продукту/ветке.
	newColumn := func(fileID int64, tableName, columnName string) {
		if _, err := db.Exec(`
			INSERT INTO sql_columns (file_id, table_name, column_name, line_number, column_number)
			VALUES ($1, $2, $3, 1, 1)
		`, fileID, tableName, columnName); err != nil {
			t.Fatalf("insert column %s.%s: %v", tableName, columnName, err)
		}
	}
	newColumn(devFile, "TA_DUP", "C_DEV")
	newColumn(yFile, "TA_DUP", "C_Y")
	_ = devTable
	_ = spTable
	_ = yTable

	newSymbol := func(fileID, entityID int64, name, symbolType, entityType string) {
		if _, err := db.Exec(`
			INSERT INTO symbols (file_id, symbol_name, symbol_type, entity_type, entity_id, line_number)
			VALUES ($1, $2, $3, $4, $5, 1)
		`, fileID, name, symbolType, entityType, entityID); err != nil {
			t.Fatalf("insert symbol %s: %v", name, err)
		}
	}
	newSymbol(devFile, devTable, "TA_DUP", "table", "sql")
	newSymbol(spFile, spTable, "TA_DUP", "table", "sql")
	// Два одноимённых символа в ОДНОЙ ветке: двухфазный дедуп обязан
	// сохранить обе строки (P2.2: схлопываются только копии разных веток).
	newSymbol(devFile, 9001, "SAME_SYM", "procedure", "sql")
	newSymbol(devFile, 9002, "SAME_SYM", "procedure", "sql")

	// PAS-методы: TMyForm.Execute в develop + SP (схлопнутся),
	// TOther.Execute только в develop (останется отдельной строкой).
	newUnit := func(fileID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO pas_units (file_id, unit_name, line_start, line_end) VALUES ($1, $2, 1, 2) RETURNING id`, fileID, name).Scan(&id); err != nil {
			t.Fatalf("insert unit %s: %v", name, err)
		}
		return id
	}
	newClass := func(fileID, unitID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO pas_classes (file_id, unit_id, class_name, line_start, line_end) VALUES ($1, $2, $3, 1, 2) RETURNING id`, fileID, unitID, name).Scan(&id); err != nil {
			t.Fatalf("insert class %s: %v", name, err)
		}
		return id
	}
	newMethod := func(fileID, classID, unitID int64, name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO pas_methods (file_id, class_id, unit_id, method_name, line_number) VALUES ($1, $2, $3, $4, 1) RETURNING id`, fileID, classID, unitID, name).Scan(&id); err != nil {
			t.Fatalf("insert method %s: %v", name, err)
		}
		return id
	}
	newMethodSymbol := func(fileID, methodID int64, name string) {
		newSymbol(fileID, methodID, name, "method", "pas")
	}

	devUnit := newUnit(devFile, "uMain")
	devFormClass := newClass(devFile, devUnit, "TMyForm")
	devOtherClass := newClass(devFile, devUnit, "TOther")
	m1 := newMethod(devFile, devFormClass, devUnit, "Execute")
	m2 := newMethod(devFile, devOtherClass, devUnit, "Execute")
	newMethodSymbol(devFile, m1, "Execute")
	newMethodSymbol(devFile, m2, "Execute")

	spUnit := newUnit(spFile, "uMain")
	spFormClass := newClass(spFile, spUnit, "TMyForm")
	m3 := newMethod(spFile, spFormClass, spUnit, "Execute")
	newMethodSymbol(spFile, m3, "Execute")

	// Фрагменты: побайтово идентичный текст в develop и SP (одинаковый query_hash)
	// и другой текст только в develop.
	newFragment := func(fileID int64, text, hash string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO query_fragments (file_id, parent_type, parent_id, component_name, component_type, query_text, query_hash, context, line_number)
			VALUES ($1, 'pas_method', 1, 'cmp', 'pas', $2, $3, 'select', 1) RETURNING id
		`, fileID, text, hash).Scan(&id); err != nil {
			t.Fatalf("insert fragment %q: %v", text, err)
		}
		return id
	}
	newFragment(devFile, "SELECT * FROM TA_DUP WHERE Rest > 0", "frag-equal")
	newFragment(spFile, "SELECT * FROM TA_DUP WHERE Rest > 0", "frag-equal")
	newFragment(devFile, "SELECT 1", "frag-one")

	return db, New(db)
}

// TestSearchTableBranchDedup — таблица в двух ветках fa-x схлопывается в
// одну строку develop, таблица другого продукта остаётся отдельной строкой.
func TestSearchTableBranchDedup(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	results, err := q.SearchTable(context.Background(), "TA_DUP", false, 10)
	if err != nil {
		t.Fatalf("SearchTable: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("tables = %d, want 2 (fa-x + fa-y): %+v", len(results), results)
	}
	byProduct := map[string]TableResult{}
	for _, r := range results {
		byProduct[r.Product] = r
	}
	fax := byProduct["fa-x"]
	if fax.BranchUsed != "develop" {
		t.Fatalf("fa-x branch_used = %q, want develop", fax.BranchUsed)
	}
	if fax.Usages != 2 {
		// usages считаются в пределах выбранной ветки: SP-копия TA_DUP не
		// надувает счётчик develop-копии (решение P2.6); две декларации
		// внутри develop-файла дают 2 использования.
		t.Fatalf("fa-x usages = %d, want 2 (two develop declarations)", fax.Usages)
	}
	if _, ok := byProduct["fa-y"]; !ok {
		t.Fatalf("fa-y table row missing: %+v", results)
	}
}

// TestSearchTableBranchUsedIsBranchKey — branch_used всегда branch_key
// (нормализованный), а не отображаемое branch_name ветки.
func TestSearchTableBranchUsedIsBranchKey(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	results, err := q.SearchTable(context.Background(), "TA_SPONLY", false, 10)
	if err != nil {
		t.Fatalf("SearchTable: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("tables = %d, want 1: %+v", len(results), results)
	}
	// branch_name в фикстуре — "FA-X-11-01-7", branch_key — "fa-x-11-01-7".
	if results[0].BranchUsed != "fa-x-11-01-7" {
		t.Fatalf("branch_used = %q, want normalized branch_key fa-x-11-01-7", results[0].BranchUsed)
	}
	if results[0].Product != "fa-x" {
		t.Fatalf("product = %q, want fa-x", results[0].Product)
	}
}

// TestSearchTableEnrichmentIsolatedByProduct — файлы/колонки выбранной копии
// не подмешиваются из другой ветки или другого продукта (P1.2).
func TestSearchTableEnrichmentIsolatedByProduct(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	results, err := q.SearchTable(context.Background(), "TA_DUP", false, 10)
	if err != nil {
		t.Fatalf("SearchTable: %v", err)
	}
	byProduct := map[string]TableResult{}
	for _, r := range results {
		byProduct[r.Product] = r
	}
	fax, ok := byProduct["fa-x"]
	if !ok {
		t.Fatalf("fa-x row missing: %+v", results)
	}
	if len(fax.Files) != 1 || fax.Files[0] != "/repo/develop/src.sql" {
		t.Fatalf("fa-x files = %v, want only develop copy", fax.Files)
	}
	if len(fax.Columns) != 1 || fax.Columns[0] != "C_DEV" {
		t.Fatalf("fa-x columns = %v, want only C_DEV", fax.Columns)
	}
	fay, ok := byProduct["fa-y"]
	if !ok {
		t.Fatalf("fa-y row missing: %+v", results)
	}
	if len(fay.Files) != 1 || fay.Files[0] != "/repo-fa-y/src.sql" {
		t.Fatalf("fa-y files = %v, want only fa-y copy", fay.Files)
	}
	if len(fay.Columns) != 1 || fay.Columns[0] != "C_Y" {
		t.Fatalf("fa-y columns = %v, want only C_Y", fay.Columns)
	}
}

// TestSearchSymbolSameNameInOneBranchSurvives — одноимённые символы одной
// ветки не схлопываются (P2.2): двухфазный дедуп сохраняет все строки
// выбранной ветки.
func TestSearchSymbolSameNameInOneBranchSurvives(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	syms, err := q.SearchSymbol(context.Background(), "SAME_SYM", "procedure", false, 10)
	if err != nil {
		t.Fatalf("SearchSymbol: %v", err)
	}
	if len(syms) != 2 {
		t.Fatalf("symbols = %d, want 2 rows from one branch: %+v", len(syms), syms)
	}
	for _, s := range syms {
		if s.BranchUsed != "develop" || s.Product != "fa-x" {
			t.Fatalf("symbol {product=%q branch=%q}: want fa-x/develop", s.Product, s.BranchUsed)
		}
	}
}

// TestSearchSymbolBranchDedup — символ в двух ветках одного продукта даёт
// одну строку (develop); одноимённые методы разных классов не схлопываются.
func TestSearchSymbolBranchDedup(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	ctx := context.Background()

	syms, err := q.SearchSymbol(ctx, "TA_DUP", "table", false, 10)
	if err != nil {
		t.Fatalf("SearchSymbol table: %v", err)
	}
	if len(syms) != 1 {
		t.Fatalf("table symbols = %d, want 1: %+v", len(syms), syms)
	}
	if syms[0].Product != "fa-x" || syms[0].BranchUsed != "develop" {
		t.Fatalf("symbol = {product=%q branch=%q}, want fa-x/develop", syms[0].Product, syms[0].BranchUsed)
	}

	methods, err := q.SearchSymbol(ctx, "Execute", "method", false, 10)
	if err != nil {
		t.Fatalf("SearchSymbol method: %v", err)
	}
	if len(methods) != 2 {
		t.Fatalf("method symbols = %d, want 2 (TMyForm + TOther): %+v", len(methods), methods)
	}
	seenEntity := map[int64]bool{}
	for _, m := range methods {
		if m.BranchUsed != "develop" || m.File != "/repo/develop/src.sql" {
			t.Fatalf("method {entity=%d branch=%q file=%q}: want develop copy of /repo/develop/src.sql", m.EntityID, m.BranchUsed, m.File)
		}
		seenEntity[m.EntityID] = true
	}
	if len(seenEntity) != 2 {
		t.Fatalf("expected 2 distinct method entities (TMyForm.Execute + TOther.Execute), got %v", seenEntity)
	}
}

// TestFindPASMethodsByNameBranchDedup — методы (класс+имя) в двух ветках
// схлопываются; другой класс сохраняется отдельной строкой.
func TestFindPASMethodsByNameBranchDedup(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	results, err := q.FindPASMethodsByName(context.Background(), "Execute", false, 10)
	if err != nil {
		t.Fatalf("FindPASMethodsByName: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("methods = %d, want 2 (TMyForm.Execute + TOther.Execute): %+v", len(results), results)
	}
	for _, m := range results {
		if m.BranchUsed != "develop" {
			t.Fatalf("method %s.%s branch_used = %q, want develop", m.ClassName, m.MethodName, m.BranchUsed)
		}
	}
}

// TestSearchQueryFragmentCollapse — побайтово идентичные копии фрагмента из
// develop и SP схлопываются в одну строку develop.
func TestSearchQueryFragmentCollapse(t *testing.T) {
	_, q := seedBranchDedupFixture(t)
	results, _, err := q.SearchQueryFragment(context.Background(), "FROM TA_DUP WHERE Rest", 10)
	if err != nil {
		t.Fatalf("SearchQueryFragment: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("fragments = %d, want 1: %+v", len(results), results)
	}
	if results[0].BranchUsed != "develop" || results[0].Product != "fa-x" {
		t.Fatalf("fragment = {product=%q branch=%q}, want fa-x/develop", results[0].Product, results[0].BranchUsed)
	}
}

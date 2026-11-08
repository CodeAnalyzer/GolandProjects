//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// seedInMemoryFixture: продукты fa-administrator (develop default + non-default
// ветка prj для второй копии .lst), fa-gaap, fa-cards; таблицы в SQL-коде
// (pRuleDoc_QtyStack, pAPI_AttrValue в чужом продукте, tContract без
// декларации, pOnlyInPrj с декларацией только в non-default ветке) и
// standalone API-таблица pAPI_AttrValue; декларации memory_tables от
// ModuleTableInMemory.lst (default-копия и non-default-копия), плюс
// pMemoryOnly — декларация без вхождения в sql_tables.
func seedInMemoryFixture(t *testing.T) *Query {
	t.Helper()
	db := testutil.Open(t)

	newProduct := func(name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
			t.Fatalf("insert product %s: %v", name, err)
		}
		return id
	}
	faAdmin := newProduct("fa-administrator")
	faGaap := newProduct("fa-gaap")
	faCards := newProduct("fa-cards")

	newBranch := func(productID int64, productName, branchName, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, '/repo/' || $4 || '/' || $3, $5) RETURNING id
		`, productID, branchName, key, productName, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", branchName, err)
		}
		return id
	}
	adminDev := newBranch(faAdmin, "fa-administrator", "develop", "develop", true)
	adminPrj := newBranch(faAdmin, "fa-administrator", "prj/TblInMemory", "prj-tblinmemory", false) // non-default копия .lst
	gaapDev := newBranch(faGaap, "fa-gaap", "develop", "develop", true)
	cardsDev := newBranch(faCards, "fa-cards", "develop", "develop", true)

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}

	newFile := func(productID, branchID int64, path, language string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, $4, 'x', 1, 'hash-' || $4, NOW(), 'UTF8', $5)
			RETURNING id
		`, scanID, productID, branchID, path, language).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}

	gaapSQL := newFile(faGaap, gaapDev, "/repo/fa-gaap/tables.sql", "SQL")
	cardsSQL := newFile(faCards, cardsDev, "/repo/fa-cards/tables.sql", "SQL")
	cardsXML := newFile(faCards, cardsDev, "/repo/fa-cards/bobject.xml", "XML")
	// Две копии ModuleTableInMemory.lst: default-ветка и prj/TblInMemory
	lstDev := newFile(faAdmin, adminDev, "/repo/fa-administrator/DBMaintenance_Ext/Server/ModuleTableInMemory.lst", "LST")
	lstPrj := newFile(faAdmin, adminPrj, "/repo-prj/fa-administrator/DBMaintenance_Ext/Server/ModuleTableInMemory.lst", "LST")

	newTable := func(fileID int64, name string) {
		if _, err := db.Exec(`INSERT INTO sql_tables (file_id, table_name, context) VALUES ($1, $2, 'create_table')`, fileID, name); err != nil {
			t.Fatalf("insert table %s: %v", name, err)
		}
	}
	newTable(gaapSQL, "pRuleDoc_QtyStack") // обычная p-таблица, задекларирована
	newTable(cardsSQL, "pAPI_AttrValue")   // API-таблица, вхождение в чужом продукте
	newTable(gaapSQL, "tContract")         // без декларации
	newTable(gaapSQL, "pOnlyInPrj")        // декларация только в non-default ветке

	if _, err := db.Exec(`
		INSERT INTO api_business_object_tables (file_id, business_object, table_name, description, line_number)
		VALUES ($1, 'ExtraAttrib', 'pAPI_AttrValue', '', 10)
	`, cardsXML); err != nil {
		t.Fatalf("insert api_business_object_tables: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO api_business_object_tables (file_id, business_object, table_name, description, line_number)
		VALUES ($1, 'Contracts', 'pAPI_NotInMemory', '', 20)
	`, cardsXML); err != nil {
		t.Fatalf("insert api_business_object_tables 2: %v", err)
	}

	newMemory := func(fileID int64, product, module, table string, line int) {
		if _, err := db.Exec(`
			INSERT INTO memory_tables (file_id, product_name, module_name, table_name, line_number)
			VALUES ($1, $2, $3, $4, $5)
		`, fileID, product, module, table, line); err != nil {
			t.Fatalf("insert memory_tables %s: %v", table, err)
		}
	}
	// default-копия: pAPI_AttrValue + pRuleDoc_QtyStack + pMemoryOnly (без
	// вхождения в sql_tables); non-default копия: pAPI_AttrValue с ДРУГИМ
	// модулем (проверка приоритета default) и pOnlyInPrj (проверка, что lone
	// non-default декларация даёт признак).
	newMemory(lstDev, "fa-administrator", "API_Administrator", "pAPI_AttrValue", 2)
	newMemory(lstDev, "fa-gaap", "GAAP", "pRuleDoc_QtyStack", 26)
	newMemory(lstDev, "fa-cards", "CARDS", "pMemoryOnly", 4)
	newMemory(lstPrj, "fa-administrator", "API_Administrator_PRJ", "pAPI_AttrValue", 2)
	newMemory(lstPrj, "fa-contracttransfers", "CT", "pOnlyInPrj", 3)

	return &Query{db: db}
}

func findTableResult(t *testing.T, items []TableResult, name string) TableResult {
	t.Helper()
	for _, r := range items {
		if r.TableName == name {
			return r
		}
	}
	t.Fatalf("table %s not found in results", name)
	return TableResult{}
}

// TestSearchTable_InMemoryFlag: признак «объявлена как таблица в памяти»
// join-ится чисто по имени (без product-scope) — вхождение в чужом продукте
// тоже несёт флаг; при двух копиях .lst выигрывает default-ветка; единственная
// декларация в non-default ветке тоже даёт признак; без декларации флаг не
// установлен; декларация без вхождения в sql_tables не порождает строк.
func TestSearchTable_InMemoryFlag(t *testing.T) {
	q := seedInMemoryFixture(t)
	ctx := context.Background()

	// Обычная p-таблица с признаком и источником декларации
	res, err := q.SearchTable(ctx, "pRuleDoc_QtyStack", false, 10)
	if err != nil {
		t.Fatalf("SearchTable pRuleDoc_QtyStack: %v", err)
	}
	r := findTableResult(t, res, "pRuleDoc_QtyStack")
	if !r.InMemory || r.InMemoryProduct != "fa-gaap" || r.InMemoryModule != "GAAP" {
		t.Fatalf("pRuleDoc_QtyStack in-memory = (%v, %q, %q), want (true, fa-gaap, GAAP)", r.InMemory, r.InMemoryProduct, r.InMemoryModule)
	}

	// Вхождение в чужом продукте: pAPI_AttrValue объявлена в fa-administrator,
	// встречается в SQL-коде fa-cards — флаг сопоставлен по имени
	res, err = q.SearchTable(ctx, "pAPI_AttrValue", false, 10)
	if err != nil {
		t.Fatalf("SearchTable pAPI_AttrValue: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("pAPI_AttrValue results = %d, want 1 (no row multiplication from two memory_tables copies)", len(res))
	}
	r = res[0]
	if r.Product != "fa-cards" {
		t.Fatalf("pAPI_AttrValue product = %q, want fa-cards", r.Product)
	}
	if !r.InMemory || r.InMemoryProduct != "fa-administrator" || r.InMemoryModule != "API_Administrator" {
		t.Fatalf("pAPI_AttrValue in-memory = (%v, %q, %q), want (true, fa-administrator, API_Administrator) — default copy must win over prj copy", r.InMemory, r.InMemoryProduct, r.InMemoryModule)
	}

	// Таблица без декларации: признак не установлен
	res, err = q.SearchTable(ctx, "tContract", false, 10)
	if err != nil {
		t.Fatalf("SearchTable tContract: %v", err)
	}
	r = findTableResult(t, res, "tContract")
	if r.InMemory {
		t.Fatalf("tContract in-memory = true, want false")
	}

	// Единственная декларация лежит в non-default ветке — признак всё равно
	// установлен (приоритет реализован СОРТИРОВКОЙ, не фильтром)
	res, err = q.SearchTable(ctx, "pOnlyInPrj", false, 10)
	if err != nil {
		t.Fatalf("SearchTable pOnlyInPrj: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("pOnlyInPrj results = %d, want 1", len(res))
	}
	r = findTableResult(t, res, "pOnlyInPrj")
	if !r.InMemory || r.InMemoryProduct != "fa-contracttransfers" || r.InMemoryModule != "CT" {
		t.Fatalf("pOnlyInPrj in-memory = (%v, %q, %q), want (true, fa-contracttransfers, CT) — lone non-default declaration must yield the flag", r.InMemory, r.InMemoryProduct, r.InMemoryModule)
	}

	// Декларация без вхождения в sql_tables не порождает фантомных строк:
	// память-декларация сама по себе не создаёт строку таблицы
	res, err = q.SearchTable(ctx, "pMemoryOnly", false, 10)
	if err != nil {
		t.Fatalf("SearchTable pMemoryOnly: %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("pMemoryOnly results = %d, want 0 (memory_tables declaration alone must not create table rows)", len(res))
	}
}

// TestSearchAPITable_InMemoryFlag: standalone API-таблица несёт признак по
// имени таблицы; без декларации — не установлен.
func TestSearchAPITable_InMemoryFlag(t *testing.T) {
	q := seedInMemoryFixture(t)
	ctx := context.Background()

	res, err := q.SearchAPITable(ctx, "pAPI_AttrValue", false, 10)
	if err != nil {
		t.Fatalf("SearchAPITable pAPI_AttrValue: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("pAPI_AttrValue api results = %d, want 1", len(res))
	}
	r := res[0]
	if !r.InMemory || r.InMemoryProduct != "fa-administrator" || r.InMemoryModule != "API_Administrator" {
		t.Fatalf("pAPI_AttrValue in-memory = (%v, %q, %q), want (true, fa-administrator, API_Administrator)", r.InMemory, r.InMemoryProduct, r.InMemoryModule)
	}

	res, err = q.SearchAPITable(ctx, "pAPI_NotInMemory", false, 10)
	if err != nil {
		t.Fatalf("SearchAPITable pAPI_NotInMemory: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("pAPI_NotInMemory api results = %d, want 1", len(res))
	}
	if res[0].InMemory {
		t.Fatalf("pAPI_NotInMemory in-memory = true, want false")
	}
}

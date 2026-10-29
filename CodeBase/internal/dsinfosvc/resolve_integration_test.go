//go:build integration

package dsinfosvc

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// matchFixture — манифест для проверки матчинга: 5 продуктов, покрывающих
// все статусы (точное совпадение, fallback по версии, продукт без линии,
// отсутствующий продукт).
const matchFixture = `
Информация
Parameter;Info;
Алиас SQL-сервера;sql2019_testv;
Имя базы данных;testdb2;

Продукты Digital Q
FA-Administrator-11-01-7;
FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA\FA-Contracts-Ext-9-01-553-PKG-IFRS9_TOYOTA-901553;
FA-AdminToolKit-11-01-7\FA-AdminToolKit-11-01-7-HF001;
FA-Custody-11-01-7;
FA-Payments-11-01-9;

Модули
FileName;FileDescription;ModuleName;ProductVersion;ProductRelease;ProductIssue;ProductBuild;PacketName;PacketVersion;PacketRelease;PacketBuild;AdditionName;AdditionVersion;AdditionRelease;AdditionBuild;HotfixName;Comments;Hash;
TYPEDBF.EXE;;;0;0;0;0;;;;;;;;;;;6CBF9680;

Процедуры

Статистика
Статус;Количество
ОК;0
Всего;0
`

// insertProductBranch создаёт (или переиспользует) продукт и его линию.
// Возвращает product_id и branch_id.
func insertProductBranch(t *testing.T, db *store.DB, product, branchName, branchKey, path string, isDefault bool) (int64, int64) {
	t.Helper()
	var productID int64
	err := db.QueryRow(`SELECT id FROM ds_products WHERE product_name = $1 ORDER BY id LIMIT 1`, product).Scan(&productID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("select product %s: %v", product, err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, product).Scan(&productID); err != nil {
			t.Fatalf("insert product %s: %v", product, err)
		}
	}
	var branchID int64
	err = db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
		VALUES ($1, $2, $3, $4, $5) RETURNING id`,
		productID, branchName, branchKey, path, isDefault).Scan(&branchID)
	if err != nil {
		t.Fatalf("insert branch %s/%s: %v", product, branchKey, err)
	}
	return productID, branchID
}

// TestResolveProfile_MatchingStatuses — точное совпадение, fallback по
// числовым токенам, branch_not_indexed и product_not_indexed; fallback
// обновляется после индексации линии без повторного parse.
func TestResolveProfile_MatchingStatuses(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// fa-administrator: exact-линия вставлена раньше sup-линии (меньший id),
	// чтобы проверить приоритет точного совпадения над порядком строк.
	insertProductBranch(t, db, "fa-administrator", "develop", "develop", "/repo/fa-administrator/develop", true)
	insertProductBranch(t, db, "fa-administrator", "FA-Administrator-11-01-7", "fa-administrator-11-01-7", "/repo/fa-administrator/11017", false)
	insertProductBranch(t, db, "fa-administrator", "sup/11-01-7", "sup/11-01-7", "/repo/fa-administrator/sup", false)

	insertProductBranch(t, db, "fa-contracts-ext", "develop", "develop", "/repo/fa-contracts-ext/develop", true)
	insertProductBranch(t, db, "fa-contracts-ext", "FA-Contracts-Ext-9-01-553", "fa-contracts-ext-9-01-553", "/repo/fa-contracts-ext/901553", false)

	insertProductBranch(t, db, "fa-admintoolkit", "develop", "develop", "/repo/fa-admintoolkit/develop", true)

	// fa-custody: только нестандартная линия sup/11-01-7 — fallback.
	insertProductBranch(t, db, "fa-custody", "develop", "develop", "/repo/fa-custody/develop", true)
	insertProductBranch(t, db, "fa-custody", "sup/11-01-7", "sup/11-01-7", "/repo/fa-custody/sup", false)

	// fa-payments не индексирован.

	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", matchFixture)
	parsed, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse: %v", err)
	}
	if parsed.Matching == nil {
		t.Fatal("parse result: matching is nil")
	}
	if parsed.Matching.Summary != "matched 3/5 products; 1 branches missing" {
		t.Fatalf("parse summary = %q", parsed.Matching.Summary)
	}

	byProduct := make(map[string]store.DsInfoProductMatch)
	for _, item := range parsed.Matching.Items {
		byProduct[item.ProductName] = item
	}
	cases := []struct {
		product   string
		status    string
		requested string
		matched   string
	}{
		{"fa-administrator", store.DsInfoMatchOK, "fa-administrator-11-01-7", "fa-administrator-11-01-7"},
		{"fa-contracts-ext", store.DsInfoMatchOK, "fa-contracts-ext-9-01-553", "fa-contracts-ext-9-01-553"},
		{"fa-custody", store.DsInfoMatchOK, "fa-custody-11-01-7", "sup/11-01-7"},
		{"fa-admintoolkit", store.DsInfoMatchBranchNotIndexed, "fa-admintoolkit-11-01-7", ""},
		{"fa-payments", store.DsInfoMatchProductNotIndexed, "fa-payments-11-01-9", ""},
	}
	for _, tc := range cases {
		item, ok := byProduct[tc.product]
		if !ok {
			t.Fatalf("match for %s not found", tc.product)
		}
		if item.Status != tc.status {
			t.Errorf("%s status = %q, want %q", tc.product, item.Status, tc.status)
		}
		if item.RequestedBranchKey != tc.requested {
			t.Errorf("%s requested = %q, want %q", tc.product, item.RequestedBranchKey, tc.requested)
		}
		if item.MatchedBranchKey != tc.matched {
			t.Errorf("%s matched = %q, want %q", tc.product, item.MatchedBranchKey, tc.matched)
		}
	}

	// Индексация недостающей линии обновляет статус без повторного parse.
	insertProductBranch(t, db, "fa-admintoolkit", "FA-AdminToolKit-11-01-7", "fa-admintoolkit-11-01-7", "/repo/fa-admintoolkit/11017", false)
	after, err := ResolveProfile(ctx, db, parsed.ProfileID)
	if err != nil {
		t.Fatalf("ResolveProfile after indexing: %v", err)
	}
	if after.Summary != "matched 4/5 products; 0 branches missing" {
		t.Fatalf("summary after indexing = %q", after.Summary)
	}
	for _, item := range after.Items {
		if item.ProductName == "fa-admintoolkit" {
			if item.Status != store.DsInfoMatchOK || item.MatchedBranchKey != "fa-admintoolkit-11-01-7" {
				t.Fatalf("fa-admintoolkit after indexing = %+v", item)
			}
		}
	}

	// Show возвращает то же сопоставление.
	shown, err := ExecuteShow(ctx, db, ShowParams{ProfileID: parsed.ProfileID})
	if err != nil {
		t.Fatalf("ExecuteShow: %v", err)
	}
	if shown.Matching == nil || shown.Matching.ProductsMatched != 4 {
		t.Fatalf("show matching = %+v", shown.Matching)
	}
}

// TestExecuteParse_MatchingOnEmptyIndex — на пустом индексе все строки
// получают product_not_indexed (parse не падает).
func TestExecuteParse_MatchingOnEmptyIndex(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", matchFixture)
	parsed, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse: %v", err)
	}
	if parsed.Matching == nil {
		t.Fatal("matching is nil")
	}
	if parsed.Matching.ProductsMatched != 0 || parsed.Matching.ProductsTotal != 5 || parsed.Matching.BranchesMissing != 0 {
		t.Fatalf("matching on empty index = %+v", parsed.Matching)
	}
	for _, item := range parsed.Matching.Items {
		if item.Status != store.DsInfoMatchProductNotIndexed {
			t.Fatalf("%s status = %q", item.ProductName, item.Status)
		}
	}
}

// TestResolveAnalysisContext_MultiLineProduct — продукт с несколькими
// строками манифеста: ok-линия не перекрывается fallback'ом, а при двух
// проиндексированных линиях выбирается default, затем старшая версия.
func TestResolveAnalysisContext_MultiLineProduct(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	// fa-ext-a: строка 7-02-2 проиндексирована, 9-01-553 — нет.
	aID, _ := insertProductBranch(t, db, "fa-ext-a", "develop", "develop", "/repo/fa-ext-a/develop", true)
	insertProductBranch(t, db, "fa-ext-a", "FA-Ext-A-7-02-2", "fa-ext-a-7-02-2", "/repo/fa-ext-a/7022", false)

	// fa-ext-b: обе линии проиндексированы, default среди них нет.
	bID, _ := insertProductBranch(t, db, "fa-ext-b", "develop", "develop", "/repo/fa-ext-b/develop", true)
	insertProductBranch(t, db, "fa-ext-b", "FA-Ext-B-7-02-2", "fa-ext-b-7-02-2", "/repo/fa-ext-b/7022", false)
	insertProductBranch(t, db, "fa-ext-b", "FA-Ext-B-9-01-553", "fa-ext-b-9-01-553", "/repo/fa-ext-b/901553", false)

	const manifest = `
Информация
Parameter;Info;
Алиас SQL-сервера;sql2019_testv;
Имя базы данных;testdb2;

Продукты Digital Q
FA-Ext-A-7-02-2;
FA-Ext-A-9-01-553;
FA-Ext-B-7-02-2;
FA-Ext-B-9-01-553;

Модули
FileName;FileDescription;ModuleName;ProductVersion;ProductRelease;ProductIssue;ProductBuild;PacketName;PacketVersion;PacketRelease;PacketBuild;AdditionName;AdditionVersion;AdditionRelease;AdditionBuild;HotfixName;Comments;Hash;
TYPEDBF.EXE;;;0;0;0;0;;;;;;;;;;;6CBF9680;

Процедуры

Статистика
Статус;Количество
ОК;0
Всего;0
`
	path := writeDsInfoFile(t, "DiasoftSystemInfo.csv", manifest)
	parsed, err := ExecuteParse(ctx, db, ParseParams{FilePath: path})
	if err != nil {
		t.Fatalf("ExecuteParse: %v", err)
	}
	if parsed.Matching == nil {
		t.Fatal("matching is nil")
	}

	ac, err := ResolveAnalysisContext(ctx, db, parsed.ProfileID)
	if err != nil {
		t.Fatalf("ResolveAnalysisContext: %v", err)
	}
	if got := ac.Branches[aID]; got == 0 {
		t.Fatalf("fa-ext-a branch not selected: %v", ac.Branches)
	}
	if len(ac.Requested[aID]) != 2 {
		t.Fatalf("fa-ext-a requested = %v, want two lines", ac.Requested[aID])
	}
	// ok-линия 7-02-2 не должна быть перекрыта fallback'ом отсутствующей 9-01-553.
	var aBranchKey string
	if err := db.QueryRow(`SELECT branch_key FROM ds_products_branch WHERE id = $1`, ac.Branches[aID]).Scan(&aBranchKey); err != nil {
		t.Fatalf("select fa-ext-a branch: %v", err)
	}
	if aBranchKey != "fa-ext-a-7-02-2" {
		t.Fatalf("fa-ext-a branch = %q, want fa-ext-a-7-02-2", aBranchKey)
	}

	var bBranchKey string
	if err := db.QueryRow(`SELECT branch_key FROM ds_products_branch WHERE id = $1`, ac.Branches[bID]).Scan(&bBranchKey); err != nil {
		t.Fatalf("select fa-ext-b branch: %v", err)
	}
	if bBranchKey != "fa-ext-b-9-01-553" {
		t.Fatalf("fa-ext-b branch = %q, want fa-ext-b-9-01-553 (higher version)", bBranchKey)
	}
}

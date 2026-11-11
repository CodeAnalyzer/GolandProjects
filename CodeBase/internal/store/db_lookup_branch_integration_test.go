//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// branchScopeFixture — фикстура веточного скоупинга: продукт fa-area
// (develop default + sup-линия), fa-area-other (develop) и EXT-продукт
// fa-area-ext (develop + prj/bank-a + prj/bank-b).
type branchScopeFixture struct {
	scanID      int64
	productID   int64
	otherID     int64
	extID       int64
	devBranch   int64 // fa-area/develop
	spBranch    int64 // fa-area/sup-11-01-7
	otherBranch int64 // fa-area-other/develop
	extDev      int64 // fa-area-ext/develop
	extPrjA     int64 // fa-area-ext/prj/bank-a
	extPrjB     int64 // fa-area-ext/prj/bank-b
	devFileA    int64 // fa-area/develop
	spFileA     int64 // fa-area/sup
	otherFile   int64 // fa-area-other/develop
	extDevFile  int64 // fa-area-ext/develop
	extPrjAFile int64 // fa-area-ext/prj/bank-a
	extPrjBFile int64 // fa-area-ext/prj/bank-b
}

func seedBranchScopeFixture(t *testing.T, db *store.DB) *branchScopeFixture {
	t.Helper()
	ctx := context.Background()
	f := &branchScopeFixture{}

	scanID, err := db.CreateScanRun(ctx, "/repo-branch-scope")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	f.scanID = scanID
	for _, p := range []struct {
		name string
		id   *int64
	}{
		{"fa-area", &f.productID},
		{"fa-area-other", &f.otherID},
		{"fa-area-ext", &f.extID},
	} {
		if *p.id, err = db.GetOrCreateDSProductIDByName(ctx, p.name); err != nil {
			t.Fatalf("GetOrCreateDSProductIDByName %s: %v", p.name, err)
		}
	}
	insertBranch := func(product int64, productName, name, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			product, name, key, "/repo-branch-scope/"+productName+"/"+key, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}
	f.devBranch = insertBranch(f.productID, "fa-area", "develop", "develop", true)
	f.spBranch = insertBranch(f.productID, "fa-area", "FA-Area-11-01-7", "fa-area-11-01-7", false)
	f.otherBranch = insertBranch(f.otherID, "fa-area-other", "develop", "develop", true)
	f.extDev = insertBranch(f.extID, "fa-area-ext", "develop", "develop", true)
	f.extPrjA = insertBranch(f.extID, "fa-area-ext", "prj bank a", "prj/bank-a", false)
	f.extPrjB = insertBranch(f.extID, "fa-area-ext", "prj bank b", "prj/bank-b", false)

	f.devFileA = insertBranchFile(t, db, scanID, f.productID, f.devBranch, "/repo-branch-scope/fa-area/develop/Dev.sql")
	f.spFileA = insertBranchFile(t, db, scanID, f.productID, f.spBranch, "/repo-branch-scope/fa-area-11017/Sp.sql")
	f.otherFile = insertBranchFile(t, db, scanID, f.otherID, f.otherBranch, "/repo-branch-scope/fa-area-other/develop/Other.sql")
	f.extDevFile = insertBranchFile(t, db, scanID, f.extID, f.extDev, "/repo-branch-scope/fa-area-ext/develop/Ext.sql")
	f.extPrjAFile = insertBranchFile(t, db, scanID, f.extID, f.extPrjA, "/repo-branch-scope/fa-area-ext/prj-bank-a/ExtA.sql")
	f.extPrjBFile = insertBranchFile(t, db, scanID, f.extID, f.extPrjB, "/repo-branch-scope/fa-area-ext/prj-bank-b/ExtB.sql")

	insertProcParams := func(fileID int64, procName, paramsJSON string) {
		if _, err := db.Exec(
			`INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end, parameters) VALUES ($1, $2, 1, 2, $3::jsonb)`,
			fileID, procName, paramsJSON,
		); err != nil {
			t.Fatalf("insert procedure %s: %v", procName, err)
		}
	}
	// Порядок вставки: develop-копии раньше, SP-копии позже (больший id).
	insertProcParams(f.devFileA, "AREA_MyProc", `[{"name":"@P1","dataType":"char","length":10,"direction":"in"}]`)
	insertProcParams(f.spFileA, "AREA_MyProc", `[{"name":"@P2","dataType":"int","direction":"in"}]`)
	insertProcParams(f.devFileA, "AREA_DevOnly", `[{"name":"@D","dataType":"int","direction":"in"}]`)
	insertProcParams(f.spFileA, "AREA_SPOnly", `[{"name":"@S","dataType":"int","direction":"in"}]`)
	insertProcParams(f.otherFile, "AREA_Util", `[{"name":"@U","dataType":"int","direction":"in"}]`)
	insertProcParams(f.extDevFile, "EXT_Proc", `[{"name":"@E","dataType":"int","direction":"in"}]`)

	// Колонки: pAreaList.QtyType — develop DSINT_KEY (раньше), SP DSIDENTIFIER.
	insertColumnDef(t, db, f.devFileA, "pAreaList", "QtyType", "DSINT_KEY")
	insertColumnDef(t, db, f.spFileA, "pAreaList", "QtyType", "DSIDENTIFIER")
	insertColumnDef(t, db, f.extDevFile, "pExtTbl", "ExtCol", "DSVARCHAR")
	insertColumnDef(t, db, f.extPrjAFile, "pExtTbl", "ExtCol", "DSDECIMAL")

	// Создатели таблиц: tAreaDevOnly — только develop, tAreaSpOnly — только SP,
	// tExtDevOnly — develop EXT, tExtPrjBOnly — только prj/bank-b.
	insertTableCreate := func(fileID int64, table string) {
		if _, err := db.Exec(`INSERT INTO sql_tables (file_id, table_name, context, line_number) VALUES ($1, $2, 'create', 1)`, fileID, table); err != nil {
			t.Fatalf("insert sql_tables %s: %v", table, err)
		}
	}
	insertTableCreate(f.devFileA, "tAreaDevOnly")
	insertTableCreate(f.spFileA, "tAreaSpOnly")
	insertTableCreate(f.extDevFile, "tExtDevOnly")
	insertTableCreate(f.extPrjBFile, "tExtPrjBOnly")

	// API-колонки: pApiArea.ApiCol — develop DSVARCHAR, SP DSINT.
	insertAPIColumn := func(fileID int64, dataType string) {
		var contractID int64
		if err := db.QueryRow(`
			INSERT INTO api_contracts (file_id, contract_name, contract_kind) VALUES ($1, 'IArea', 'service') RETURNING id`,
			fileID).Scan(&contractID); err != nil {
			t.Fatalf("insert api_contracts: %v", err)
		}
		var tableID int64
		if err := db.QueryRow(`
			INSERT INTO api_contract_tables (contract_id, direction, table_name) VALUES ($1, 'out', 'pApiArea') RETURNING id`,
			contractID).Scan(&tableID); err != nil {
			t.Fatalf("insert api_contract_tables: %v", err)
		}
		if _, err := db.Exec(`
			INSERT INTO api_contract_table_fields (contract_table_id, field_name, type_name) VALUES ($1, 'ApiCol', $2)`,
			tableID, dataType); err != nil {
			t.Fatalf("insert api_contract_table_fields: %v", err)
		}
	}
	insertAPIColumn(f.devFileA, "DSVARCHAR")
	insertAPIColumn(f.spFileA, "DSINT")

	return f
}

func areasPairs(areas map[int64][]int64) store.ProductBranchPairs {
	return store.NewProductBranchPairs(areas)
}

// TestBatchLookupProcedureParams_BranchArea: параметры процедуры выбираются
// из разрешённой области (value-режим); обе ветки в области — при равенстве
// тиров выигрывает default-ветка; пустые pairs — прежнее поведение.
func TestBatchLookupProcedureParams_BranchArea(t *testing.T) {
	db := testutil.Open(t)
	f := seedBranchScopeFixture(t, db)
	ctx := context.Background()

	// Scope SP-файла: область {sp} — параметры SP-копии.
	got, err := db.BatchLookupProcedureParams(ctx, []string{"AREA_MyProc"},
		store.LookupScope{ProductID: f.productID, BranchID: f.spBranch},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams (SP area): %v", err)
	}
	if params := got["area_myproc"]; len(params) != 1 || params[0].Name != "@P2" {
		t.Fatalf("SP area: area_myproc params = %+v, want @P2", params)
	}

	// Scope develop-файла: область {develop} — параметры develop-копии.
	got, err = db.BatchLookupProcedureParams(ctx, []string{"AREA_MyProc"},
		store.LookupScope{ProductID: f.productID, BranchID: f.devBranch},
		areasPairs(map[int64][]int64{f.productID: {f.devBranch}}))
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams (develop area): %v", err)
	}
	if params := got["area_myproc"]; len(params) != 1 || params[0].Name != "@P1" {
		t.Fatalf("develop area: area_myproc params = %+v, want @P1", params)
	}

	// Product-scope {0}: обе копии в области, тир продукта совпадает —
	// решает default-ветка (develop, @P1), несмотря на больший id SP-копии.
	got, err = db.BatchLookupProcedureParams(ctx, []string{"AREA_MyProc"},
		store.LookupScope{ProductID: f.productID},
		areasPairs(map[int64][]int64{f.productID: {0}}))
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams (product scope): %v", err)
	}
	if params := got["area_myproc"]; len(params) != 1 || params[0].Name != "@P1" {
		t.Fatalf("product scope: area_myproc params = %+v, want @P1 (default-ветка)", params)
	}

	// Имя только в чужой ветке своего продукта (AREA_SPOnly при develop-scope)
	// резолвится fallback-тирами (продукт/default/id): существует, но ниже
	// областных кандидатов — параметры возвращаются.
	got, err = db.BatchLookupProcedureParams(ctx, []string{"AREA_SPOnly"},
		store.LookupScope{ProductID: f.productID, BranchID: f.devBranch},
		areasPairs(map[int64][]int64{f.productID: {f.devBranch}}))
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams (SPOnly): %v", err)
	}
	if params := got["area_sponly"]; len(params) != 1 || params[0].Name != "@S" {
		t.Fatalf("develop area: area_sponly params = %+v, want @S (value-fallback)", params)
	}

	// Чужой продукт: область {develop} fa-area-other.
	got, err = db.BatchLookupProcedureParams(ctx, []string{"AREA_Util"},
		store.LookupScope{ProductID: f.productID, BranchID: f.spBranch},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}, f.otherID: {f.otherBranch}}))
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams (foreign): %v", err)
	}
	if params := got["area_util"]; len(params) != 1 || params[0].Name != "@U" {
		t.Fatalf("foreign area: area_util params = %+v, want @U", params)
	}
}

// TestBatchLookupProcedureProductIDs_BranchArea: владелец имени — свой продукт
// раньше чужого (identity); ExistsInArea отражает копию в области владельца.
func TestBatchLookupProcedureProductIDs_BranchArea(t *testing.T) {
	db := testutil.Open(t)
	f := seedBranchScopeFixture(t, db)
	ctx := context.Background()

	spScope := store.LookupScope{ProductID: f.productID, BranchID: f.spBranch}
	spAreas := areasPairs(map[int64][]int64{
		f.productID: {f.spBranch},
		f.otherID:   {f.otherBranch},
		f.extID:     {f.extDev},
	})

	// AREA_MyProc: владелец — свой продукт (любая ветка), копия в SP-области есть.
	got, err := db.BatchLookupProcedureProductIDs(ctx, []string{"AREA_MyProc"}, spScope, spAreas)
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs: %v", err)
	}
	if info := got["area_myproc"]; info.ProductID != f.productID || !info.ExistsInArea {
		t.Fatalf("area_myproc = %+v, want {ProductID: own, ExistsInArea: true}", info)
	}

	// AREA_SPOnly при develop-scope: владелец свой, но в области {develop} копии
	// нет — ExistsInArea = false (execNotExistsProc сформирует finding).
	devAreas := areasPairs(map[int64][]int64{
		f.productID: {f.devBranch},
		f.otherID:   {f.otherBranch},
		f.extID:     {f.extDev},
	})
	got, err = db.BatchLookupProcedureProductIDs(ctx, []string{"AREA_SPOnly"},
		store.LookupScope{ProductID: f.productID, BranchID: f.devBranch}, devAreas)
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs (SPOnly): %v", err)
	}
	if info := got["area_sponly"]; info.ProductID != f.productID || info.ExistsInArea {
		t.Fatalf("area_sponly = %+v, want {ProductID: own, ExistsInArea: false}", info)
	}

	// AREA_Util: владелец — чужой продукт (своих копий нет), копия в его
	// разрешённой области есть.
	got, err = db.BatchLookupProcedureProductIDs(ctx, []string{"AREA_Util"}, spScope, spAreas)
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs (Util): %v", err)
	}
	if info := got["area_util"]; info.ProductID != f.otherID || !info.ExistsInArea {
		t.Fatalf("area_util = %+v, want {ProductID: other, ExistsInArea: true}", info)
	}

	// EXT_Proc: владелец EXT, копия только в develop — при области чужого EXT
	// {develop} существует; при профильной prj-области {prjA, develop} — тоже
	// (union поставки банка).
	got, err = db.BatchLookupProcedureProductIDs(ctx, []string{"EXT_Proc"}, spScope, spAreas)
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs (ExtProc): %v", err)
	}
	if info := got["ext_proc"]; info.ProductID != f.extID || !info.ExistsInArea {
		t.Fatalf("ext_proc = %+v, want {ProductID: ext, ExistsInArea: true}", info)
	}

	// Пустые pairs: ExistsInArea = false для всех («существование неизвестно»).
	got, err = db.BatchLookupProcedureProductIDs(ctx, []string{"AREA_MyProc"}, spScope, store.ProductBranchPairs{})
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs (empty pairs): %v", err)
	}
	if info := got["area_myproc"]; info.ProductID != f.productID || info.ExistsInArea {
		t.Fatalf("empty pairs: area_myproc = %+v, want ExistsInArea = false", info)
	}
}

// TestBatchLookupTableCreators_BranchArea: создатель таблицы только в develop
// не подтверждает существование при scope SP-ветки; union EXT и product-scope
// расширяют область; пустые pairs — прежнее глобальное поведение.
func TestBatchLookupTableCreators_BranchArea(t *testing.T) {
	db := testutil.Open(t)
	f := seedBranchScopeFixture(t, db)
	ctx := context.Background()

	// Scope SP: создатель в develop не виден.
	creators, err := db.BatchLookupTableCreators(ctx, []string{"tAreaDevOnly", "tAreaSpOnly"},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("BatchLookupTableCreators (SP): %v", err)
	}
	if creators["tareadevonly"] {
		t.Fatal("tareadevonly: создатель в develop не должен подтверждать существование при SP scope")
	}
	if !creators["tareasponly"] {
		t.Fatal("tareasponly: создатель в SP должен подтверждать существование")
	}

	// EXT prj-файл: область {prjA, develop} — создатель в develop виден (union).
	creators, err = db.BatchLookupTableCreators(ctx, []string{"tExtDevOnly", "tExtPrjBOnly"},
		areasPairs(map[int64][]int64{f.extID: {f.extPrjA, f.extDev}}))
	if err != nil {
		t.Fatalf("BatchLookupTableCreators (EXT): %v", err)
	}
	if !creators["textdevonly"] {
		t.Fatal("textdevonly: создатель в develop должен входить в область prj-файла (union)")
	}
	if creators["textprjbonly"] {
		t.Fatal("textprjbonly: prj-ветка другого банка вне области")
	}

	// Product-scope {0}: любая ветка продукта.
	creators, err = db.BatchLookupTableCreators(ctx, []string{"tAreaDevOnly"},
		areasPairs(map[int64][]int64{f.productID: {0}}))
	if err != nil {
		t.Fatalf("BatchLookupTableCreators (product scope): %v", err)
	}
	if !creators["tareadevonly"] {
		t.Fatal("product scope: создатель в любой ветке должен подтверждать существование")
	}

	// Пустые pairs — прежнее глобальное поведение.
	creators, err = db.BatchLookupTableCreators(ctx, []string{"tAreaDevOnly"}, store.ProductBranchPairs{})
	if err != nil {
		t.Fatalf("BatchLookupTableCreators (legacy): %v", err)
	}
	if !creators["tareadevonly"] {
		t.Fatal("legacy: без пар областей создатель виден глобально")
	}
}

// TestFindLatestSQLColumnDefinitionType_BranchArea: тип колонки — value-режим:
// тир файла → область → продукт → default → id; SP-тип выигрывает при SP-области
// даже с меньшим id; batch-путь даёт тот же результат (эквивалентность D3/D4).
func TestFindLatestSQLColumnDefinitionType_BranchArea(t *testing.T) {
	db := testutil.Open(t)
	f := seedBranchScopeFixture(t, db)
	ctx := context.Background()

	// SP-область: DSIDENTIFIER (SP-копия вставлена позже, но выигрывает область).
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pAreaList", "QtyType", 0,
		store.LookupScope{ProductID: f.productID, BranchID: f.spBranch},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType (SP): %v", err)
	}
	if got != "DSIDENTIFIER" {
		t.Fatalf("SP area type = %q, want DSIDENTIFIER", got)
	}

	// develop-область: DSINT_KEY.
	got, err = db.FindLatestSQLColumnDefinitionType(ctx, "pAreaList", "QtyType", 0,
		store.LookupScope{ProductID: f.productID, BranchID: f.devBranch},
		areasPairs(map[int64][]int64{f.productID: {f.devBranch}}))
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType (develop): %v", err)
	}
	if got != "DSINT_KEY" {
		t.Fatalf("develop area type = %q, want DSINT_KEY", got)
	}

	// Тир файла сильнее области: файл develop при SP-области → DSINT_KEY.
	got, err = db.FindLatestSQLColumnDefinitionType(ctx, "pAreaList", "QtyType", f.devFileA,
		store.LookupScope{ProductID: f.productID, BranchID: f.spBranch},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType (file tier): %v", err)
	}
	if got != "DSINT_KEY" {
		t.Fatalf("file tier type = %q, want DSINT_KEY (тир файла сильнее области)", got)
	}

	// EXT prj-область {prjA, develop}: prj-определение ближе к файлу ветки.
	got, err = db.FindLatestSQLColumnDefinitionType(ctx, "pExtTbl", "ExtCol", 0,
		store.LookupScope{ProductID: f.extID, BranchID: f.extPrjA},
		areasPairs(map[int64][]int64{f.extID: {f.extPrjA, f.extDev}}))
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType (EXT): %v", err)
	}
	if got != "DSDECIMAL" {
		t.Fatalf("EXT prj area type = %q, want DSDECIMAL (prj-копия вставлена позже)", got)
	}

	// Batch-эквивалентность: тот же результат одним запросом.
	batch, err := db.BatchFindColumnDefinitionTypes(ctx, []string{"pAreaList"}, 0,
		store.LookupScope{ProductID: f.productID, BranchID: f.spBranch},
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("BatchFindColumnDefinitionTypes (SP): %v", err)
	}
	if batch["parealist|qtytype"] != "DSIDENTIFIER" {
		t.Fatalf("batch SP type = %q, want DSIDENTIFIER", batch["parealist|qtytype"])
	}
}

// TestFindAPIColumnDefinitionType_BranchArea: API-fallback тоже скоупится —
// тип берётся из разрешённой области, а не самый свежий глобально.
func TestFindAPIColumnDefinitionType_BranchArea(t *testing.T) {
	db := testutil.Open(t)
	f := seedBranchScopeFixture(t, db)
	ctx := context.Background()

	got, err := db.FindAPIColumnDefinitionType(ctx, "pApiArea", "ApiCol", f.productID,
		areasPairs(map[int64][]int64{f.productID: {f.spBranch}}))
	if err != nil {
		t.Fatalf("FindAPIColumnDefinitionType (SP): %v", err)
	}
	if got != "DSINT" {
		t.Fatalf("SP area API type = %q, want DSINT", got)
	}

	got, err = db.FindAPIColumnDefinitionType(ctx, "pApiArea", "ApiCol", f.productID,
		areasPairs(map[int64][]int64{f.productID: {f.devBranch}}))
	if err != nil {
		t.Fatalf("FindAPIColumnDefinitionType (develop): %v", err)
	}
	if got != "DSVARCHAR" {
		t.Fatalf("develop area API type = %q, want DSVARCHAR", got)
	}

	// Пустые pairs: областного тира нет — тир default-ветки (develop,
	// DSVARCHAR) сильнее более свежей SP-копии; синхронизировано с ленивым
	// FindLatestSQLColumnDefinitionType.
	got, err = db.FindAPIColumnDefinitionType(ctx, "pApiArea", "ApiCol", f.productID, store.ProductBranchPairs{})
	if err != nil {
		t.Fatalf("FindAPIColumnDefinitionType (legacy): %v", err)
	}
	if got != "DSVARCHAR" {
		t.Fatalf("legacy API type = %q, want DSVARCHAR", got)
	}
}

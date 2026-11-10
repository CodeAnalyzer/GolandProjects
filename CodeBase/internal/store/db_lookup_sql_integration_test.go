//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// insertProductFile вставляет файл, привязанный к продукту productID.
func insertProductFile(t *testing.T, db *store.DB, scanID int64, productID int64, path string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, NULLIF($2, 0), $3, $3, 'sql', 1, $4, $5, 'CP866', 'SQL')
		RETURNING id
	`, scanID, productID, path, path, time.Now()).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

func insertColumnDef(t *testing.T, db *store.DB, fileID int64, table, column, dataType string) {
	t.Helper()
	if err := db.BatchInsertSQLColumnDefinitions(context.Background(), []*model.SQLColumnDefinition{
		{FileID: fileID, TableName: table, ColumnName: column, DataType: dataType, DefinitionKind: "create_table"},
	}, 100); err != nil {
		t.Fatalf("insert column def %s.%s: %v", table, column, err)
	}
}

// seedCrossProductQtyType создаёт воспроизведение бага BUG-datatype-ptable-cross-product-type-lookup:
// pConsQtyListRight.QtyType объявлена DSIDENTIFIER в fa-contracts (раньше) и
// DSINT_KEY в fa-reports ADP-копии (позже, больший id). Возвращает контекстные id.
func seedCrossProductQtyType(t *testing.T, db *store.DB) (contractsProductID, reportsProductID, contractsDDLFileID, reviewFileID int64) {
	t.Helper()
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err = db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-contracts: %v", err)
	}
	reportsProductID, err = db.GetOrCreateDSProductIDByName(ctx, "fa-reports")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-reports: %v", err)
	}
	contractsDDLFileID = insertProductFile(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Consumer/pConsQtyListRight_TmpTbl.sql")
	reportsDDLFileID := insertProductFile(t, db, scanID, reportsProductID, "/repo/fa-reports/ADP_ConsumerToReports/Server/ContractCredit/api_rpt_ContractCredit_tmptbl.sql")
	// Анализируемый файл MassProcess — fa-contracts, своих определений pConsQtyListRight не содержит
	reviewFileID = insertProductFile(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Accrual/CreditTurnoverLink_MassProcess.sql")

	// Порядок вставки задаёт порядок id: fa-contracts раньше, fa-reports позже
	insertColumnDef(t, db, contractsDDLFileID, "pConsQtyListRight", "QtyType", "DSIDENTIFIER")
	insertColumnDef(t, db, reportsDDLFileID, "pConsQtyListRight", "QtyType", "DSINT_KEY")
	return
}

func TestFindLatestSQLColumnDefinitionType_ProductScope(t *testing.T) {
	db := testutil.Open(t)
	contractsProductID, _, _, reviewFileID := seedCrossProductQtyType(t, db)
	ctx := context.Background()

	// С контекстом fa-contracts тип разрешается из своего продукта, а не из
	// более свежей ADP-копии fa-reports
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", reviewFileID, store.LookupScope{ProductID: contractsProductID})
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSIDENTIFIER" {
		t.Fatalf("scoped type = %q, want DSIDENTIFIER", got)
	}
}

func TestFindLatestSQLColumnDefinitionType_SameFileTier(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName: %v", err)
	}
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	reviewFileID := insertProductFile(t, db, scanID, contractsProductID, "/repo/fa-contracts/reviewed.sql")
	otherFileID := insertProductFile(t, db, scanID, contractsProductID, "/repo/fa-contracts/other.sql")

	// Определение в анализируемом файле раньше, одноимённое — в другом файле
	// того же продукта позже (больший id)
	insertColumnDef(t, db, reviewFileID, "#Rest", "RestCol", "DSBIGMONEY")
	insertColumnDef(t, db, otherFileID, "#Rest", "RestCol", "DSMONEY")

	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "#Rest", "RestCol", reviewFileID, store.LookupScope{ProductID: contractsProductID})
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSBIGMONEY" {
		t.Fatalf("same-file type = %q, want DSBIGMONEY", got)
	}

	// Без файлового яруса (productID only) побеждает более свежее same-product
	got, err = db.FindLatestSQLColumnDefinitionType(ctx, "#Rest", "RestCol", 0, store.LookupScope{ProductID: contractsProductID})
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSMONEY" {
		t.Fatalf("same-product type = %q, want DSMONEY", got)
	}
}

func TestFindLatestSQLColumnDefinitionType_GlobalFallback(t *testing.T) {
	db := testutil.Open(t)
	_, _, _, _ = seedCrossProductQtyType(t, db)
	ctx := context.Background()

	// Файл из другого продукта (fa-payments) ссылается на таблицу, объявленную
	// только в fa-contracts — работает глобальный latest-wins
	paymentsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-payments")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-payments: %v", err)
	}
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", 0, store.LookupScope{ProductID: paymentsProductID})
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSINT_KEY" {
		t.Fatalf("fallback type = %q, want DSINT_KEY (глобальный latest)", got)
	}
}

func TestFindLatestSQLColumnDefinitionType_ZeroContextLegacy(t *testing.T) {
	db := testutil.Open(t)
	_, _, _, _ = seedCrossProductQtyType(t, db)
	ctx := context.Background()

	// Нулевой контекст воспроизводит прежнее поведение: глобальный latest-wins
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", 0, store.LookupScope{})
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSINT_KEY" {
		t.Fatalf("legacy type = %q, want DSINT_KEY", got)
	}
}

func TestBatchFindColumnDefinitionTypes_ProductScope(t *testing.T) {
	db := testutil.Open(t)
	contractsProductID, _, _, reviewFileID := seedCrossProductQtyType(t, db)
	ctx := context.Background()

	scoped, err := db.BatchFindColumnDefinitionTypes(ctx, []string{"pConsQtyListRight"}, reviewFileID, contractsProductID)
	if err != nil {
		t.Fatalf("BatchFindColumnDefinitionTypes scoped: %v", err)
	}
	if got := scoped["pconsqtylistright|qtytype"]; got != "DSIDENTIFIER" {
		t.Fatalf("batch scoped type = %q, want DSIDENTIFIER", got)
	}

	legacy, err := db.BatchFindColumnDefinitionTypes(ctx, []string{"pConsQtyListRight"}, 0, 0)
	if err != nil {
		t.Fatalf("BatchFindColumnDefinitionTypes legacy: %v", err)
	}
	if got := legacy["pconsqtylistright|qtytype"]; got != "DSINT_KEY" {
		t.Fatalf("batch legacy type = %q, want DSINT_KEY", got)
	}
}

// insertBranchFile вставляет файл, привязанный к продукту и ветке.
func insertBranchFile(t *testing.T, db *store.DB, scanID int64, productID, branchID int64, path string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, NULLIF($2, 0), NULLIF($3, 0), $4, $4, 'sql', 1, $5, $6, 'CP866', 'SQL')
		RETURNING id
	`, scanID, productID, branchID, path, path, time.Now()).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

// insertProcDirect вставляет процедуру напрямую, возвращает id.
func insertProcDirect(t *testing.T, db *store.DB, fileID int64, procName string) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(
		`INSERT INTO sql_procedures (file_id, proc_name, line_start, line_end) VALUES ($1, $2, 1, 2) RETURNING id`,
		fileID, procName,
	).Scan(&id); err != nil {
		t.Fatalf("insert procedure %s: %v", procName, err)
	}
	return id
}

// seedTierLookupFixture создаёт продукт fa-tier (develop default + SP-линия) и
// fa-tier-other; процедуры: MyProc в develop и SP (SP вставлена позже —
// больший id), OnlyInSP — только в SP, UTIL_X — только в другом продукте.
func seedTierLookupFixture(t *testing.T, db *store.DB) (productID, otherProductID, devBranchID, spBranchID, devMyProcID, spMyProcID, spOnlyID, otherUtilID int64) {
	t.Helper()
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	productID, err = db.GetOrCreateDSProductIDByName(ctx, "fa-tier")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-tier: %v", err)
	}
	otherProductID, err = db.GetOrCreateDSProductIDByName(ctx, "fa-tier-other")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName fa-tier-other: %v", err)
	}

	insertBranch := func(product int64, name, key, path string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			product, name, key, path, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}
	devBranchID = insertBranch(productID, "develop", "develop", "/repo/fa-tier/develop", true)
	spBranchID = insertBranch(productID, "FA-Tier-11-01-7-SP", "fa-tier-11-01-7", "/repo/fa-tier-11017", false)
	otherBranchID := insertBranch(otherProductID, "develop", "develop", "/repo/fa-tier-other/develop", true)

	// Порядок вставки: develop-копии раньше, SP-копии позже (больший id).
	devFile := insertBranchFile(t, db, scanID, productID, devBranchID, "/repo/fa-tier/develop/MyProcDev.sql")
	spFile := insertBranchFile(t, db, scanID, productID, spBranchID, "/repo/fa-tier-11017/MyProcSP.sql")
	otherFile := insertBranchFile(t, db, scanID, otherProductID, otherBranchID, "/repo/fa-tier-other/develop/UtilX.sql")

	devMyProcID = insertProcDirect(t, db, devFile, "TIER_MyProc")
	spOnlyID = insertProcDirect(t, db, spFile, "TIER_OnlyInSP")
	spMyProcID = insertProcDirect(t, db, spFile, "TIER_MyProc")
	otherUtilID = insertProcDirect(t, db, otherFile, "TIER_UTIL_X")
	return
}

// TestFindLatestSQLProcedureIDsByNames_TierScope проверяет трёхъярусное
// ограничение кандидатов (D4 среза branch-aware-lookup): ветка источника →
// default-ветка продукта → другие продукты; чужие ветки того же продукта
// исключаются.
func TestFindLatestSQLProcedureIDsByNames_TierScope(t *testing.T) {
	db := testutil.Open(t)
	productID, otherProductID, devBranchID, spBranchID, devMyProcID, spMyProcID, spOnlyID, otherUtilID := seedTierLookupFixture(t, db)
	ctx := context.Background()

	// Scope SP-ветки: своя копия MyProc побеждает develop (branch_match > id).
	ids, err := db.FindLatestSQLProcedureIDsByNames(ctx, []string{"TIER_MyProc"}, store.LookupScope{ProductID: productID, BranchID: spBranchID})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames (SP scope): %v", err)
	}
	if ids["tier_myproc"] != spMyProcID {
		t.Fatalf("SP scope: tier_myproc = %d, want SP copy %d", ids["tier_myproc"], spMyProcID)
	}

	// Scope develop-ветки: develop-копия; OnlyInSP не резолвится (чужая ветка
	// того же продукта исключена из кандидатов).
	ids, err = db.FindLatestSQLProcedureIDsByNames(ctx, []string{"TIER_MyProc", "TIER_OnlyInSP"}, store.LookupScope{ProductID: productID, BranchID: devBranchID})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames (develop scope): %v", err)
	}
	if ids["tier_myproc"] != devMyProcID {
		t.Fatalf("develop scope: tier_myproc = %d, want develop copy %d", ids["tier_myproc"], devMyProcID)
	}
	if ids["tier_onlyinsp"] != 0 {
		t.Fatalf("develop scope: tier_onlyinsp = %d, want 0 (чужая ветка исключена)", ids["tier_onlyinsp"])
	}

	// Scope без ветки (productID only): default-ветка + другие продукты.
	ids, err = db.FindLatestSQLProcedureIDsByNames(ctx, []string{"TIER_MyProc"}, store.LookupScope{ProductID: productID})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames (product scope): %v", err)
	}
	if ids["tier_myproc"] != devMyProcID {
		t.Fatalf("product scope: tier_myproc = %d, want develop copy %d", ids["tier_myproc"], devMyProcID)
	}

	// Третий ярус: общая утилита другого продукта резолвится.
	ids, err = db.FindLatestSQLProcedureIDsByNames(ctx, []string{"TIER_UTIL_X"}, store.LookupScope{ProductID: productID, BranchID: devBranchID})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames (tier3): %v", err)
	}
	if ids["tier_util_x"] != otherUtilID {
		t.Fatalf("tier3: tier_util_x = %d, want other product copy %d", ids["tier_util_x"], otherUtilID)
	}

	// Нулевой scope: глобальное поведение без продуктово-веточных
	// предпочтений, но с приоритетом default-ветки (ось D2/D3) — develop-копия.
	ids, err = db.FindLatestSQLProcedureIDsByNames(ctx, []string{"TIER_MyProc", "TIER_OnlyInSP"}, store.LookupScope{})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames (zero scope): %v", err)
	}
	if ids["tier_myproc"] != devMyProcID {
		t.Fatalf("zero scope: tier_myproc = %d, want default-branch copy %d", ids["tier_myproc"], devMyProcID)
	}
	if ids["tier_onlyinsp"] != spOnlyID {
		t.Fatalf("zero scope: tier_onlyinsp = %d, want %d", ids["tier_onlyinsp"], spOnlyID)
	}
	_ = otherProductID
}

// TestBatchLookupTableCreators: единый источник «таблица создана» — все четыре
// ветви (sql_tables create, sql_column_definitions select_into,
// api_contract_tables, api_business_object_tables) дают true, отсутствующая
// таблица в результате отсутствует.
func TestBatchLookupTableCreators(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo-creators")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	newFile := func(path string) int64 {
		return insertProductFile(t, db, scanID, 0, path)
	}
	ddlFileID := newFile("/repo/tCreators_TmpTbl.sql")
	apiFileID := newFile("/repo/tCreators_Api.xml")

	// sql_tables: DDL-источник (create) и шумовой контекст select (не создатель)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO sql_tables (file_id, table_name, context, line_number)
		VALUES ($1, 'tCreatedByDDL', 'create', 1), ($1, 'tOnlySelected', 'select', 2)
	`, ddlFileID); err != nil {
		t.Fatalf("insert sql_tables: %v", err)
	}

	// sql_column_definitions: создание через SELECT ... INTO
	if err := db.BatchInsertSQLColumnDefinitions(ctx, []*model.SQLColumnDefinition{
		{FileID: ddlFileID, TableName: "tCreatedBySelectInto", ColumnName: "KeyCol", DataType: "DSINT_KEY", DefinitionKind: "select_into"},
	}, 100); err != nil {
		t.Fatalf("insert select_into column def: %v", err)
	}

	// api_contract_tables: объявление таблицы в API-контракте
	var contractID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO api_contracts (file_id, contract_name, contract_kind)
		VALUES ($1, 'CreatorsContract', 'service')
		RETURNING id
	`, apiFileID).Scan(&contractID); err != nil {
		t.Fatalf("insert api_contracts: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO api_contract_tables (contract_id, direction, table_name)
		VALUES ($1, 'input', 'pApiContractTable')
	`, contractID); err != nil {
		t.Fatalf("insert api_contract_tables: %v", err)
	}

	// api_business_object_tables: объявление таблицы бизнес-объекта
	if _, err := db.ExecContext(ctx, `
		INSERT INTO api_business_object_tables (file_id, business_object, table_name)
		VALUES ($1, 'CreatorsObject', 'pApiBusinessTable')
	`, apiFileID); err != nil {
		t.Fatalf("insert api_business_object_tables: %v", err)
	}

	creators, err := db.BatchLookupTableCreators(ctx, []string{
		"TCreatedByDDL", "tcreatedbyselectinto", "PAPICONTRACTTABLE", "pApiBusinessTable",
		"tOnlySelected", "pNotDeclaredAnywhere",
	})
	if err != nil {
		t.Fatalf("BatchLookupTableCreators: %v", err)
	}

	for _, name := range []string{"tcreatedbyddl", "tcreatedbyselectinto", "papicontracttable", "papibusinesstable"} {
		if !creators[name] {
			t.Fatalf("creators[%s] = false, want true", name)
		}
	}
	for _, name := range []string{"tonlyselected", "pnotdeclaredanywhere"} {
		if creators[name] {
			t.Fatalf("creators[%s] = true, want false (select-контекст не создаёт таблицу)", name)
		}
	}
}

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
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", reviewFileID, contractsProductID)
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

	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "#Rest", "RestCol", reviewFileID, contractsProductID)
	if err != nil {
		t.Fatalf("FindLatestSQLColumnDefinitionType: %v", err)
	}
	if got != "DSBIGMONEY" {
		t.Fatalf("same-file type = %q, want DSBIGMONEY", got)
	}

	// Без файлового яруса (productID only) побеждает более свежее same-product
	got, err = db.FindLatestSQLColumnDefinitionType(ctx, "#Rest", "RestCol", 0, contractsProductID)
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
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", 0, paymentsProductID)
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
	got, err := db.FindLatestSQLColumnDefinitionType(ctx, "pConsQtyListRight", "QtyType", 0, 0)
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

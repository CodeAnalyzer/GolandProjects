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

// insertFileGen вставляет файл с явным признаком is_generated.
func insertFileGen(t *testing.T, db *store.DB, scanID, productID int64, path string, isGenerated bool) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language, is_generated)
		VALUES ($1, NULLIF($2, 0), $3, $3, 'sql', 1, $4, $5, 'CP866', 'SQL', $6)
		RETURNING id
	`, scanID, productID, path, path, time.Now(), isGenerated).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

func insertProcParams(t *testing.T, db *store.DB, fileID int64, name string, params []model.SQLParam) {
	t.Helper()
	if err := db.BatchInsertSQLProcedures(context.Background(), []*model.SQLProcedure{
		{FileID: fileID, ProcName: name, Params: params, LineStart: 1, LineEnd: 2},
	}, 100); err != nil {
		t.Fatalf("insert procedure %s: %v", name, err)
	}
}

func insertContractWithTable(t *testing.T, db *store.DB, fileID int64, contractName, tableName string) int64 {
	t.Helper()
	ctx := context.Background()
	var contractID int64
	if err := db.QueryRowContext(ctx, `
		INSERT INTO api_contracts (file_id, contract_name, contract_kind)
		VALUES ($1, $2, 'service')
		RETURNING id
	`, fileID, contractName).Scan(&contractID); err != nil {
		t.Fatalf("insert contract %s: %v", contractName, err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO api_contract_tables (contract_id, direction, table_name)
		VALUES ($1, 'input', $2)
	`, contractID, tableName); err != nil {
		t.Fatalf("insert contract table %s: %v", tableName, err)
	}
	return contractID
}

func TestFindLatestSQLProcedureIDsByNames_CanonicalBeatsUploadCopy(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("product: %v", err)
	}
	canonicalFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql", false)
	copyFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql", true)
	insertProcParams(t, db, canonicalFile, "BaseAlg_ConsMinRest", nil)
	insertProcParams(t, db, copyFile, "BaseAlg_ConsMinRest", nil)

	var canonicalID, copyID int64
	if err := db.QueryRow(`SELECT id FROM sql_procedures WHERE file_id = $1`, canonicalFile).Scan(&canonicalID); err != nil {
		t.Fatalf("canonical id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM sql_procedures WHERE file_id = $1`, copyFile).Scan(&copyID); err != nil {
		t.Fatalf("copy id: %v", err)
	}
	if copyID <= canonicalID {
		t.Fatalf("seed order broken: copy id %d must exceed canonical %d", copyID, canonicalID)
	}

	// С продуктовым контекстом и без — побеждает канонический источник
	for _, productID := range []int64{contractsProductID, 0} {
		ids, err := db.FindLatestSQLProcedureIDsByNames(ctx, []string{"BaseAlg_ConsMinRest"}, productID)
		if err != nil {
			t.Fatalf("lookup (product=%d): %v", productID, err)
		}
		if got := ids["basealg_consminrest"]; got != canonicalID {
			t.Fatalf("lookup (product=%d) = %d, want canonical %d", productID, got, canonicalID)
		}
	}
}

func TestFindLatestSQLProcedureIDsByNames_SplitByProduct(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("product contracts: %v", err)
	}
	otherProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-other")
	if err != nil {
		t.Fatalf("product other: %v", err)
	}
	contractsFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Accrual/BaseAlgAmrtCostSinglePmnt.sql", false)
	otherFile := insertFileGen(t, db, scanID, otherProductID, "/repo/fa-other/Server/UPLOAD/BaseAlgAmrtCostSinglePmnt.sql", false)
	insertProcParams(t, db, contractsFile, "BaseAlgAmrtCostSinglePmnt", nil)
	insertProcParams(t, db, otherFile, "BaseAlgAmrtCostSinglePmnt", nil)

	var contractsProcID, otherProcID int64
	if err := db.QueryRow(`SELECT id FROM sql_procedures WHERE file_id = $1`, contractsFile).Scan(&contractsProcID); err != nil {
		t.Fatalf("contracts proc id: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM sql_procedures WHERE file_id = $1`, otherFile).Scan(&otherProcID); err != nil {
		t.Fatalf("other proc id: %v", err)
	}

	ids, err := db.FindLatestSQLProcedureIDsByNames(ctx, []string{"BaseAlgAmrtCostSinglePmnt"}, contractsProductID)
	if err != nil {
		t.Fatalf("lookup contracts: %v", err)
	}
	if got := ids["basealgamrtcostsinglepmnt"]; got != contractsProcID {
		t.Fatalf("contracts lookup = %d, want own-product %d", got, contractsProcID)
	}

	ids, err = db.FindLatestSQLProcedureIDsByNames(ctx, []string{"BaseAlgAmrtCostSinglePmnt"}, otherProductID)
	if err != nil {
		t.Fatalf("lookup other: %v", err)
	}
	if got := ids["basealgamrtcostsinglepmnt"]; got != otherProcID {
		t.Fatalf("other lookup = %d, want own-product %d", got, otherProcID)
	}
}

func TestBatchLookupProcedureProductIDs_CanonicalProductWins(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("product contracts: %v", err)
	}
	otherProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-other")
	if err != nil {
		t.Fatalf("product other: %v", err)
	}
	// Канон в fa-contracts (раньше), копия в fa-other (позже, больший id)
	canonicalFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Accrual/BaseAlgCalcCreditPortfolio.sql", false)
	copyFile := insertFileGen(t, db, scanID, otherProductID, "/repo/fa-other/Server/UPLOAD/BaseAlgCalcCreditPortfolio.sql", true)
	insertProcParams(t, db, canonicalFile, "BaseAlgCalcCreditPortfolio", nil)
	insertProcParams(t, db, copyFile, "BaseAlgCalcCreditPortfolio", nil)

	products, err := db.BatchLookupProcedureProductIDs(ctx, []string{"BaseAlgCalcCreditPortfolio"})
	if err != nil {
		t.Fatalf("BatchLookupProcedureProductIDs: %v", err)
	}
	if got := products["basealgcalccreditportfolio"]; got != contractsProductID {
		t.Fatalf("product = %d, want canonical product %d (не продукт копии %d)", got, contractsProductID, otherProductID)
	}
}

func TestBatchLookupProcedureParams_PrefersCanonical(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("product: %v", err)
	}
	canonicalFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/Consumer/SERVER/Accrual/BaseAlgSumRegistryOfOuterAmount.sql", false)
	copyFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/LoanBureau/Server/UPLOAD/BaseAlgSumRegistryOfOuterAmount.sql", true)
	insertProcParams(t, db, canonicalFile, "BaseAlgSumRegistryOfOuterAmount", []model.SQLParam{{Name: "Sum", Type: "DSBIGMONEY", Direction: "in"}})
	insertProcParams(t, db, copyFile, "BaseAlgSumRegistryOfOuterAmount", []model.SQLParam{{Name: "Sum", Type: "DSMONEY", Direction: "in"}})

	params, err := db.BatchLookupProcedureParams(ctx, []string{"BaseAlgSumRegistryOfOuterAmount"})
	if err != nil {
		t.Fatalf("BatchLookupProcedureParams: %v", err)
	}
	got := params["basealgsumregistryofouteramount"]
	if len(got) != 1 || got[0].Type != "DSBIGMONEY" {
		t.Fatalf("params = %+v, want canonical DSBIGMONEY", got)
	}
}

func TestFindAPIContractIDsByTableNames_FiltersGenerated(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	contractsProductID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-contracts")
	if err != nil {
		t.Fatalf("product: %v", err)
	}
	canonicalFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/DSArchitectData/CON_Accrual_Process.xml", false)
	copyFile := insertFileGen(t, db, scanID, contractsProductID, "/repo/fa-contracts/Server/UPLOAD/CON_Accrual_Process.xml", true)

	// Таблица T у канона и у копии; таблица U — только у копии
	canonicalID := insertContractWithTable(t, db, canonicalFile, "CON_Accrual_Process", "pAPI_Accrual_ObjDate")
	copyID := insertContractWithTable(t, db, copyFile, "CON_Accrual_Process_Copy", "pAPI_Accrual_ObjDate")
	onlyCopyID := insertContractWithTable(t, db, copyFile, "CON_Only_Copy", "pAPI_Only_In_Copy")

	owners, err := db.FindAPIContractIDsByTableNames(ctx, []string{"pAPI_Accrual_ObjDate", "pAPI_Only_In_Copy"})
	if err != nil {
		t.Fatalf("FindAPIContractIDsByTableNames: %v", err)
	}
	gotT := owners["papi_accrual_objdate"]
	if len(gotT) != 1 || gotT[0] != canonicalID {
		t.Fatalf("shared table owners = %v, want only canonical %d (копия %d отфильтрована)", gotT, canonicalID, copyID)
	}
	gotU := owners["papi_only_in_copy"]
	if len(gotU) != 1 || gotU[0] != onlyCopyID {
		t.Fatalf("copy-only table owners = %v, want copy %d (копия — единственный владелец)", gotU, onlyCopyID)
	}
}

func TestFilesIsGenerated_BackfillIdempotent(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	// Файл вставлен со значением по умолчанию (false), как записи до миграции backfill;
	// для .t01 расширение задаётся явно (insertProductFile жёстко пишет 'sql')
	copyFile := insertProductFile(t, db, scanID, 0, "/repo/fa-contracts/LoanBureau/Server/UPLOAD/OldCopy.sql")
	var t01File int64
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, '/repo/fa-contracts/Consumer/SERVER/OldPreproc.t01', '/repo/fa-contracts/Consumer/SERVER/OldPreproc.t01', 't01', 'h', NOW())
		RETURNING id
	`, scanID).Scan(&t01File); err != nil {
		t.Fatalf("insert t01 file: %v", err)
	}
	canonicalFile := insertProductFile(t, db, scanID, 0, "/repo/fa-contracts/Consumer/SERVER/Accrual/OldCanonical.sql")

	// Повторная инициализация схемы применяет идемпотентный backfill
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (backfill): %v", err)
	}

	var uploadFlag, t01Flag, canonicalFlag bool
	if err := db.QueryRowContext(ctx, `SELECT is_generated FROM files WHERE id = $1`, copyFile).Scan(&uploadFlag); err != nil {
		t.Fatalf("read upload flag: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT is_generated FROM files WHERE id = $1`, t01File).Scan(&t01Flag); err != nil {
		t.Fatalf("read t01 flag: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT is_generated FROM files WHERE id = $1`, canonicalFile).Scan(&canonicalFlag); err != nil {
		t.Fatalf("read canonical flag: %v", err)
	}
	if !uploadFlag || !t01Flag {
		t.Fatalf("backfill failed: upload=%v t01=%v, want true/true", uploadFlag, t01Flag)
	}
	if canonicalFlag {
		t.Fatalf("canonical file must not be marked generated")
	}

	// Повторный запуск не меняет результат и не ломается
	if err := db.InitSchema(); err != nil {
		t.Fatalf("InitSchema (repeat): %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT is_generated FROM files WHERE id = $1`, copyFile).Scan(&uploadFlag); err != nil {
		t.Fatalf("re-read upload flag: %v", err)
	}
	if !uploadFlag {
		t.Fatalf("repeat backfill lost the flag")
	}
}

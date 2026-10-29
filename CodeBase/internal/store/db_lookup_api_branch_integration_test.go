//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertAPIContractForTest(t *testing.T, db *store.DB, fileID int64, name, kind, ownerModule string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO api_contracts (file_id, contract_name, contract_kind, owner_module)
		VALUES ($1, $2, $3, NULLIF($4, ''))
		RETURNING id
	`, fileID, name, kind, ownerModule).Scan(&id)
	if err != nil {
		t.Fatalf("insert api contract %s/%s: %v", name, kind, err)
	}
	return id
}

// TestFindLatestEventContractIDsByNames_PriorityBeforeModule — оси приоритета
// (default-ветка) идут ПЕРЕД module_key: fallback ByName выбирает копию
// default-ветки даже если у копии другой ветки алфавитно меньший модуль, а
// ByNameAndModule сохраняет каждую пару (name, module).
func TestFindLatestEventContractIDsByNames_PriorityBeforeModule(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	productID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-evt")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName: %v", err)
	}
	devBranchID, err := db.UpsertDSProductBranch(ctx, productID, "develop", "develop", "/repo/fa-evt/develop", "", time.Time{})
	if err != nil {
		t.Fatalf("UpsertDSProductBranch develop: %v", err)
	}
	if err := db.SetDefaultBranch(ctx, productID, "develop"); err != nil {
		t.Fatalf("SetDefaultBranch: %v", err)
	}
	spBranchID, err := db.UpsertDSProductBranch(ctx, productID, "FA-EVT-11-01-7-SP", "fa-evt-11-01-7", "/repo/fa-evt/sp", "", time.Time{})
	if err != nil {
		t.Fatalf("UpsertDSProductBranch sp: %v", err)
	}

	devFileID := insertBranchFile(t, db, scanID, productID, devBranchID, "/repo/fa-evt/develop/events.xml")
	spFileID := insertBranchFile(t, db, scanID, productID, spBranchID, "/repo/fa-evt/sp/events.xml")
	// Модуль SP-копии ("aaa") алфавитно раньше модуля develop ("zzz"):
	// порядок module_key до осей выбрал бы SP-копию — регрессия P1.3.
	devID := insertAPIContractForTest(t, db, devFileID, "EVT_Dual", "event", "zzz")
	spID := insertAPIContractForTest(t, db, spFileID, "EVT_Dual", "event", "aaa")

	lookup, err := db.FindLatestEventContractIDsByNames(ctx, []string{"EVT_Dual"}, store.LookupScope{})
	if err != nil {
		t.Fatalf("FindLatestEventContractIDsByNames: %v", err)
	}
	if got := lookup.ByName["evt_dual"]; got != devID {
		t.Fatalf("ByName = %d, want develop copy %d (default branch before module_key)", got, devID)
	}
	if got := lookup.ByNameAndModule["evt_dual|zzz"]; got != devID {
		t.Fatalf("ByNameAndModule[zzz] = %d, want %d", got, devID)
	}
	if got := lookup.ByNameAndModule["evt_dual|aaa"]; got != spID {
		t.Fatalf("ByNameAndModule[aaa] = %d, want %d", got, spID)
	}

	// Scope ветки источника: tier1 (ветка SP) резолвится в SP-копию.
	scoped, err := db.FindLatestEventContractIDsByNames(ctx, []string{"EVT_Dual"}, store.LookupScope{ProductID: productID, BranchID: spBranchID})
	if err != nil {
		t.Fatalf("FindLatestEventContractIDsByNames scoped: %v", err)
	}
	if got := scoped.ByName["evt_dual"]; got != spID {
		t.Fatalf("scoped ByName = %d, want SP copy %d", got, spID)
	}
}

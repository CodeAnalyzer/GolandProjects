//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedAPIBranchFixture — контракт и реализующая его процедура в двух ветках
// одного продукта: копии сущностей и рёбер в develop (default) и SP-ветке.
func seedAPIBranchFixture(t *testing.T) (*store.DB, *Query) {
	t.Helper()
	db := testutil.Open(t)

	var fax int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-x') RETURNING id`).Scan(&fax); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	newBranch := func(name, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, '/repo/fa-x/' || $3, $4) RETURNING id
		`, fax, name, key, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", name, err)
		}
		return id
	}
	devBranch := newBranch("develop", "develop", true)
	spBranch := newBranch("FA-X-11-01-7", "fa-x-11-01-7", false)

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	newFile := func(branchID int64, path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'hash-' || $4, NOW(), 'UTF8', 'SQL')
			RETURNING id
		`, scanID, fax, branchID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	devFile := newFile(devBranch, "/repo/fa-x/develop/src.sql")
	spFile := newFile(spBranch, "/repo/fa-x/fa-x-11-01-7/src.sql")

	devContract := insertRelationsAPIContract(t, db, devFile, "API_BranchSvc", "service")
	spContract := insertRelationsAPIContract(t, db, spFile, "API_BranchSvc", "service")

	devProc := insertRelationsProc(t, db, devFile, "BrImplProc")
	spProc := insertRelationsProc(t, db, spFile, "BrImplProc")

	// Рёбра каждой ветки указывают на свою копию контракта.
	insertRelationsEdge(t, db, "sql_procedure", devProc, "api_contract", devContract, "implements_contract")
	insertRelationsEdge(t, db, "sql_procedure", spProc, "api_contract", spContract, "implements_contract")

	return db, New(db)
}

// TestSearchAPIContractBranchDedup — контракт в двух ветках даёт одну строку
// из default-ветки с product/branch_used.
func TestSearchAPIContractBranchDedup(t *testing.T) {
	_, q := seedAPIBranchFixture(t)
	contracts, err := q.SearchAPIContract(context.Background(), "API_BranchSvc", false, 10)
	if err != nil {
		t.Fatalf("SearchAPIContract: %v", err)
	}
	if len(contracts) != 1 {
		t.Fatalf("contracts = %d, want 1: %+v", len(contracts), contracts)
	}
	if contracts[0].BranchUsed != "develop" || contracts[0].Product != "fa-x" {
		t.Fatalf("contract = {product=%q branch=%q}, want fa-x/develop", contracts[0].Product, contracts[0].BranchUsed)
	}
}

// TestSearchAPIImplementationsNoDuplicateEdges — реализации строятся только
// в пределах выбранной (default) копии контракта: рёбра второй ветки
// не дублируют ответ.
func TestSearchAPIImplementationsNoDuplicateEdges(t *testing.T) {
	_, q := seedAPIBranchFixture(t)
	impls, err := q.SearchAPIImplementations(context.Background(), "API_BranchSvc", 10)
	if err != nil {
		t.Fatalf("SearchAPIImplementations: %v", err)
	}
	if len(impls) != 1 {
		t.Fatalf("implementations = %d, want 1: %+v", len(impls), impls)
	}
	if impls[0].ProcedureName != "BrImplProc" || impls[0].BranchUsed != "develop" {
		t.Fatalf("impl = {proc=%q branch=%q}, want BrImplProc/develop", impls[0].ProcedureName, impls[0].BranchUsed)
	}
	if impls[0].File != "/repo/fa-x/develop/src.sql" {
		t.Fatalf("impl file = %q, want develop copy", impls[0].File)
	}
}

// TestAPIContractsKindHomonyms — контракты с одинаковым именем, но разным
// видом (service + event) в одном продукте не схлопываются (P2.4): и список
// контрактов, и публикаторы сохраняют event-копию.
func TestAPIContractsKindHomonyms(t *testing.T) {
	db := testutil.Open(t)

	var productID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-kind') RETURNING id`).Scan(&productID); err != nil {
		t.Fatalf("insert product: %v", err)
	}
	var branchID int64
	if err := db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
		VALUES ($1, 'develop', 'develop', '/repo/fa-kind/develop', TRUE) RETURNING id
	`, productID).Scan(&branchID); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	var fileID int64
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, '/repo/fa-kind/develop/src.sql', '/repo/fa-kind/develop/src.sql', 'sql', 1, 'hash-kind', NOW(), 'UTF8', 'SQL')
		RETURNING id
	`, scanID, productID, branchID).Scan(&fileID); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	insertRelationsAPIContract(t, db, fileID, "API_Dual", "service")
	eventID := insertRelationsAPIContract(t, db, fileID, "API_Dual", "event")
	pubProc := insertRelationsProc(t, db, fileID, "EvtPub")
	insertRelationsEdge(t, db, "sql_procedure", pubProc, "api_contract", eventID, "publishes_event")

	q := New(db)
	contracts, err := q.SearchAPIContract(context.Background(), "API_Dual", false, 10)
	if err != nil {
		t.Fatalf("SearchAPIContract: %v", err)
	}
	if len(contracts) != 2 {
		t.Fatalf("contracts = %d, want 2 (service + event): %+v", len(contracts), contracts)
	}

	publishers, err := q.SearchAPIPublishers(context.Background(), "API_Dual", 10)
	if err != nil {
		t.Fatalf("SearchAPIPublishers: %v", err)
	}
	if len(publishers) != 1 {
		t.Fatalf("publishers = %d, want 1: %+v", len(publishers), publishers)
	}
	if publishers[0].ContractID != eventID {
		t.Fatalf("publisher contract = %d, want event copy %d", publishers[0].ContractID, eventID)
	}
	if publishers[0].ProcedureName != "EvtPub" {
		t.Fatalf("publisher proc = %q, want EvtPub", publishers[0].ProcedureName)
	}
}

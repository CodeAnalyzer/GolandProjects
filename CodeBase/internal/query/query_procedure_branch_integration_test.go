//go:build integration

package query

import (
	"context"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedProcedureBranchFixture создаёт продукт с двумя ветками (develop —
// default, fa-x-11-01-7 — релизная) и копии процедуры BrDupProc в обеих
// ветках (с разными телами/описаниями).
func seedProcedureBranchFixture(t *testing.T) (*store.DB, *Query) {
	t.Helper()
	db := testutil.Open(t)

	var productID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-x') RETURNING id`).Scan(&productID); err != nil {
		t.Fatalf("insert product: %v", err)
	}

	insertBranch := func(name, key string, isDefault bool) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, '/repo/' || $3, $4)
			RETURNING id
		`, productID, name, key, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", name, err)
		}
		return id
	}
	developID := insertBranch("develop", "develop", true)
	releaseID := insertBranch("FA-X-11-01-7", "fa-x-11-01-7", false)

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}

	insertFile := func(branchID int64, path string) int64 {
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
	developFile := insertFile(developID, "/repo/develop/br_dup_proc.sql")
	releaseFile := insertFile(releaseID, "/repo/fa-x-11-01-7/br_dup_proc.sql")

	procs := []*model.SQLProcedure{
		{FileID: releaseFile, ProcName: "BrDupProc", LineStart: 1, LineEnd: 10, Description: "release copy"},
		{FileID: developFile, ProcName: "BrDupProc", LineStart: 1, LineEnd: 12, Description: "develop copy"},
	}
	if err := db.BatchInsertSQLProcedures(context.Background(), procs, 100); err != nil {
		t.Fatalf("insert procedures: %v", err)
	}

	return db, New(db)
}

// TestGetProcedureResultBranchDeterminism — при копиях процедуры в двух
// ветках query procedure стабильно (повторные вызовы) возвращает копию
// default-ветки с branch_used = develop.
func TestGetProcedureResultBranchDeterminism(t *testing.T) {
	_, q := seedProcedureBranchFixture(t)
	ctx := context.Background()

	first, err := q.GetProcedureResult(ctx, "BrDupProc")
	if err != nil {
		t.Fatalf("GetProcedureResult: %v", err)
	}
	if first.BranchUsed != "develop" {
		t.Fatalf("branch_used = %q, want develop", first.BranchUsed)
	}
	if first.Product != "fa-x" {
		t.Fatalf("product = %q, want fa-x", first.Product)
	}
	if first.Description != "develop copy" {
		t.Fatalf("description = %q, want develop copy (default branch priority)", first.Description)
	}

	second, err := q.GetProcedureResult(ctx, "BrDupProc")
	if err != nil {
		t.Fatalf("GetProcedureResult (repeat): %v", err)
	}
	if second.ID != first.ID || second.BranchUsed != first.BranchUsed || second.File != first.File {
		t.Fatalf("unstable selection: first={id=%d branch=%q file=%q}, second={id=%d branch=%q file=%q}",
			first.ID, first.BranchUsed, first.File, second.ID, second.BranchUsed, second.File)
	}
}

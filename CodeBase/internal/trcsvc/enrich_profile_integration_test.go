//go:build integration

package trcsvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
	"github.com/codebase/internal/trc"
)

// seedTRCEnrichFixture — fa-trprof (develop default + fa-trprof-11-01-7) и
// fa-trother (develop) с копиями процедуры PROF_Proc; профиль указывает на
// линию fa-trprof-11-01-7.
func seedTRCEnrichFixture(t *testing.T) (*store.DB, int64) {
	t.Helper()
	db := testutil.Open(t)
	ctx := context.Background()

	insertProduct := func(name string) int64 {
		var id int64
		if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
			t.Fatalf("insert product %s: %v", name, err)
		}
		return id
	}
	insertBranch := func(productID int64, name, key string, isDefault bool) int64 {
		var id int64
		path := fmt.Sprintf("/repo/p%d/%s", productID, key)
		if err := db.QueryRow(`
			INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
			VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			productID, name, key, path, isDefault).Scan(&id); err != nil {
			t.Fatalf("insert branch %s: %v", key, err)
		}
		return id
	}

	var scanID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('repo', 'done') RETURNING id`).Scan(&scanID); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	insertFile := func(productID, branchID int64, path string) int64 {
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'hash-' || $4, NOW(), 'UTF8', 'SQL')
			RETURNING id`,
			scanID, productID, branchID, path).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}

	profID := insertProduct("fa-trprof")
	otherID := insertProduct("fa-trother")
	profDev := insertBranch(profID, "develop", "develop", true)
	profRel := insertBranch(profID, "FA-TrProf-11-01-7", "fa-trprof-11-01-7", false)
	otherDev := insertBranch(otherID, "develop", "develop", true)

	devFile := insertFile(profID, profDev, "/repo/fa-trprof/develop/prof.sql")
	relFile := insertFile(profID, profRel, "/repo/fa-trprof/11017/prof.sql")
	otherFile := insertFile(otherID, otherDev, "/repo/fa-trother/develop/prof.sql")

	procs := []*model.SQLProcedure{
		{FileID: relFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 10},
		{FileID: devFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 12},
		{FileID: otherFile, ProcName: "PROF_Proc", LineStart: 1, LineEnd: 8},
	}
	if err := db.BatchInsertSQLProcedures(ctx, procs, 100); err != nil {
		t.Fatalf("insert procedures: %v", err)
	}

	var profileID int64
	if err := db.QueryRow(`
		INSERT INTO dsinfo (file_path, file_hash, parser_version, products_count)
		VALUES ('/tmp/trc-enrich.csv', 'hash-trc-enrich', 1, 1) RETURNING id`).Scan(&profileID); err != nil {
		t.Fatalf("insert dsinfo profile: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO dsinfo_products (dsinfo_id, line_no, raw_name, product_name, branch_key)
		VALUES ($1, 1, 'FA-TrProf-11-01-7', 'fa-trprof', 'fa-trprof-11-01-7')`, profileID); err != nil {
		t.Fatalf("insert dsinfo product: %v", err)
	}
	return db, profileID
}

// TestTRCEnrichAggregates_ProfileLine — агрегации процедур обогащаются из
// линии профиля и сообщают product/branch_used.
func TestTRCEnrichAggregates_ProfileLine(t *testing.T) {
	db, profileID := seedTRCEnrichFixture(t)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")

	q, err := queryWithProfile(ctx, db, profileID)
	if err != nil {
		t.Fatalf("queryWithProfile: %v", err)
	}
	aggs := []trc.TRCProcAgg{{Procedure: "PROF_Proc", Count: 1, TotalMs: 5}}
	enrichProcedureAggregates(ctx, q, aggs)
	if aggs[0].SourceFile != "/repo/fa-trprof/11017/prof.sql" {
		t.Fatalf("source = %q", aggs[0].SourceFile)
	}
	if aggs[0].Product != "fa-trprof" || aggs[0].BranchUsed != "fa-trprof-11-01-7" {
		t.Fatalf("aggregate = %+v", aggs[0])
	}
}

// TestTRCEnrichAggregates_NoProfile — без профиля действует default-ветка.
func TestTRCEnrichAggregates_NoProfile(t *testing.T) {
	db, _ := seedTRCEnrichFixture(t)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")

	q, err := queryWithProfile(ctx, db, 0)
	if err != nil {
		t.Fatalf("queryWithProfile: %v", err)
	}
	aggs := []trc.TRCProcAgg{{Procedure: "PROF_Proc", Count: 1}}
	enrichProcedureAggregates(ctx, q, aggs)
	if aggs[0].BranchUsed != "develop" {
		t.Fatalf("branch_used = %q, want develop", aggs[0].BranchUsed)
	}
}

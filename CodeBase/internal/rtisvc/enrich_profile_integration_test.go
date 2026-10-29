//go:build integration

package rtisvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/query"
	"github.com/codebase/internal/rti"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedEnrichProfileFixture — fa-prof (develop default + fa-prof-11-01-7) и
// fa-other (develop) с копиями процедуры PROF_Proc; профиль указывает на
// линию fa-prof-11-01-7.
func seedEnrichProfileFixture(t *testing.T) (*store.DB, int64) {
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

	profID := insertProduct("fa-prof")
	otherID := insertProduct("fa-other")
	profDev := insertBranch(profID, "develop", "develop", true)
	profRel := insertBranch(profID, "FA-Prof-11-01-7", "fa-prof-11-01-7", false)
	otherDev := insertBranch(otherID, "develop", "develop", true)

	devFile := insertFile(profID, profDev, "/repo/fa-prof/develop/prof.sql")
	relFile := insertFile(profID, profRel, "/repo/fa-prof/11017/prof.sql")
	otherFile := insertFile(otherID, otherDev, "/repo/fa-other/develop/prof.sql")

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
		VALUES ('/tmp/enrich.csv', 'hash-enrich', 1, 1) RETURNING id`).Scan(&profileID); err != nil {
		t.Fatalf("insert dsinfo profile: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO dsinfo_products (dsinfo_id, line_no, raw_name, product_name, branch_key)
		VALUES ($1, 1, 'FA-Prof-11-01-7', 'fa-prof', 'fa-prof-11-01-7')`, profileID); err != nil {
		t.Fatalf("insert dsinfo product: %v", err)
	}
	return db, profileID
}

// TestEnrichProfile_PicksProfileLine — enrichment резолвит копию в линии
// профиля и сообщает product/branch_used.
func TestEnrichProfile_PicksProfileLine(t *testing.T) {
	db, profileID := seedEnrichProfileFixture(t)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")

	q, err := queryWithProfile(ctx, db, profileID)
	if err != nil {
		t.Fatalf("queryWithProfile: %v", err)
	}
	enrich, err := rti.EnrichProcedure(ctx, q, "PROF_Proc")
	if err != nil {
		t.Fatalf("EnrichProcedure: %v", err)
	}
	if enrich.Product != "fa-prof" || enrich.BranchUsed != "fa-prof-11-01-7" {
		t.Fatalf("enrichment = %+v", enrich)
	}
	if enrich.SourceFile != "/repo/fa-prof/11017/prof.sql" {
		t.Fatalf("source = %q", enrich.SourceFile)
	}
}

// TestEnrichProfile_NoProfileUsesDefault — без профиля действует срез B:
// выбирается default-ветка.
func TestEnrichProfile_NoProfileUsesDefault(t *testing.T) {
	db, _ := seedEnrichProfileFixture(t)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")

	q, err := queryWithProfile(ctx, db, 0)
	if err != nil {
		t.Fatalf("queryWithProfile: %v", err)
	}
	enrich, err := rti.EnrichProcedure(ctx, q, "PROF_Proc")
	if err != nil {
		t.Fatalf("EnrichProcedure: %v", err)
	}
	if enrich.BranchUsed != "develop" {
		t.Fatalf("branch_used = %q, want develop", enrich.BranchUsed)
	}
}

// TestEnrichProfile_FallbackWarns — линия профиля не проиндексирована:
// выбирается default-ветка продукта профиля, ответ несёт предупреждение.
func TestEnrichProfile_FallbackWarns(t *testing.T) {
	db, profileID := seedEnrichProfileFixture(t)
	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")
	if _, err := db.Exec(`
		UPDATE dsinfo_products SET raw_name = 'FA-Prof-11-01-6', branch_key = 'fa-prof-11-01-6'
		WHERE dsinfo_id = $1`, profileID); err != nil {
		t.Fatalf("update dsinfo product: %v", err)
	}

	q, err := queryWithProfile(ctx, db, profileID)
	if err != nil {
		t.Fatalf("queryWithProfile: %v", err)
	}
	enrich, err := rti.EnrichProcedure(ctx, q, "PROF_Proc")
	if err != nil {
		t.Fatalf("EnrichProcedure: %v", err)
	}
	if enrich.Product != "fa-prof" || enrich.BranchUsed != "develop" {
		t.Fatalf("fallback enrichment = %+v", enrich)
	}
	if enrich.BranchRequested != "fa-prof-11-01-6" {
		t.Fatalf("branch_requested = %q", enrich.BranchRequested)
	}
	warnings := query.CollectBranchWarnings(enrich)
	if len(warnings) != 1 {
		t.Fatalf("warnings = %v, want one fallback warning", warnings)
	}
}

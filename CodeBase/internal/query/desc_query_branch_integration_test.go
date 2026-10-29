//go:build integration

package query

import (
	"context"
	"strings"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedDescBranchFixture — процедура с одинаковым описанием в develop (default)
// и SP-ветке одного продукта.
func seedDescBranchFixture(t *testing.T) (*store.DB, *Query) {
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
	devFile := newFile(devBranch, "/repo/fa-x/develop/proc.sql")
	spFile := newFile(spBranch, "/repo/fa-x/fa-x-11-01-7/proc.sql")

	newProc := func(fileID int64) {
		if _, err := db.Exec(`
			INSERT INTO sql_procedures (file_id, proc_name, description, line_start, line_end)
			VALUES ($1, 'BrDescProc', 'Расчёт лимита кредита по договору', 1, 2)
		`, fileID); err != nil {
			t.Fatalf("insert proc in file %d: %v", fileID, err)
		}
	}
	newProc(devFile)
	newProc(spFile)

	// Векторы: штатный per-file ensure (вес A имя / вес B описание)
	for _, fid := range []int64{devFile, spFile} {
		if err := db.EnsureDescriptionSearchVectors(context.Background(), fid); err != nil {
			t.Fatalf("ensure vectors for file %d: %v", fid, err)
		}
	}

	return db, New(db)
}

// TestSearchDescriptionsBranchDedup — описание сущности в двух ветках даёт
// одну exact-строку из default-ветки с product/branch_used.
func TestSearchDescriptionsBranchDedup(t *testing.T) {
	_, q := seedDescBranchFixture(t)
	out, err := q.SearchDescriptions(context.Background(), "договор", nil, 10)
	if err != nil {
		t.Fatalf("SearchDescriptions: %v", err)
	}
	var exact []DescriptionSearchResult
	for _, item := range out.Items {
		if item.Source == "exact" && item.Name == "BrDescProc" {
			exact = append(exact, item)
		}
	}
	if len(exact) != 1 {
		t.Fatalf("exact hits for BrDescProc = %d, want 1: %+v", len(exact), exact)
	}
	if exact[0].Product != "fa-x" || exact[0].BranchUsed != "develop" {
		t.Fatalf("exact hit = {product=%q branch=%q}, want fa-x/develop", exact[0].Product, exact[0].BranchUsed)
	}
	if exact[0].File != "/repo/fa-x/develop/proc.sql" {
		t.Fatalf("exact hit file = %q, want develop copy", exact[0].File)
	}
}

// TestSearchDescriptionsProfileProductPreference — при совпадении (kind, name)
// в двух продуктах предпочитается копия продукта профиля, даже если она
// ранжирована ниже.
func TestSearchDescriptionsProfileProductPreference(t *testing.T) {
	db, _ := seedDescBranchFixture(t)
	ctx := context.Background()

	var faxID, firstScanID int64
	if err := db.QueryRow(`SELECT id FROM ds_products WHERE product_name = 'fa-x'`).Scan(&faxID); err != nil {
		t.Fatalf("select fa-x: %v", err)
	}
	if err := db.QueryRow(`SELECT id FROM scan_runs LIMIT 1`).Scan(&firstScanID); err != nil {
		t.Fatalf("select scan run: %v", err)
	}
	var spBranchID int64
	if err := db.QueryRow(`SELECT id FROM ds_products_branch WHERE ds_product_id = $1 AND branch_key = 'fa-x-11-01-7'`, faxID).Scan(&spBranchID); err != nil {
		t.Fatalf("select sp branch: %v", err)
	}

	// fa-y: та же процедура с более релевантным описанием (rank выше).
	var fayID int64
	if err := db.QueryRow(`INSERT INTO ds_products (product_name) VALUES ('fa-y') RETURNING id`).Scan(&fayID); err != nil {
		t.Fatalf("insert fa-y: %v", err)
	}
	var fayBranchID, fayFileID int64
	if err := db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
		VALUES ($1, 'develop', 'develop', '/repo/fa-y/develop', TRUE) RETURNING id`, fayID).Scan(&fayBranchID); err != nil {
		t.Fatalf("insert fa-y branch: %v", err)
	}
	if err := db.QueryRow(`
		INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, '/repo/fa-y/develop/proc.sql', '/repo/fa-y/develop/proc.sql', 'sql', 1, 'hash-fa-y', NOW(), 'UTF8', 'SQL')
		RETURNING id`, firstScanID, fayID, fayBranchID).Scan(&fayFileID); err != nil {
		t.Fatalf("insert fa-y file: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO sql_procedures (file_id, proc_name, description, line_start, line_end)
		VALUES ($1, 'BrDescProc', $2, 1, 2)`,
		fayFileID, strings.Repeat("договор ", 20)); err != nil {
		t.Fatalf("insert fa-y proc: %v", err)
	}
	if err := db.EnsureDescriptionSearchVectors(ctx, fayFileID); err != nil {
		t.Fatalf("ensure vectors: %v", err)
	}

	plain, err := New(db).SearchDescriptions(ctx, "договор", nil, 10)
	if err != nil {
		t.Fatalf("plain SearchDescriptions: %v", err)
	}
	if got := exactProcProduct(plain.Items, "BrDescProc"); got != "fa-y" {
		t.Fatalf("plain exact product = %q, want fa-y", got)
	}

	profiled, err := New(db, WithProfile(&ProfileContext{
		ProfileID: 1,
		Branches:  map[int64]int64{faxID: spBranchID},
		Requested: map[string][]string{"fa-x": {"fa-x-11-01-7"}},
	})).SearchDescriptions(ctx, "договор", nil, 10)
	if err != nil {
		t.Fatalf("profiled SearchDescriptions: %v", err)
	}
	if got := exactProcProduct(profiled.Items, "BrDescProc"); got != "fa-x" {
		t.Fatalf("profiled exact product = %q, want fa-x", got)
	}
	for _, item := range profiled.Items {
		if item.Source == "exact" && item.Name == "BrDescProc" && item.BranchUsed != "fa-x-11-01-7" {
			t.Fatalf("profiled branch_used = %q, want fa-x-11-01-7", item.BranchUsed)
		}
	}
}

func exactProcProduct(items []DescriptionSearchResult, name string) string {
	for _, item := range items {
		if item.Source == "exact" && item.Name == name {
			return item.Product
		}
	}
	return ""
}

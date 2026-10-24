//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// lsaBranchFixture — продукт с двумя линиями (develop is_default + SP),
// файлы в каждой линии; возвращает id линий.
func lsaBranchFixture(t *testing.T, db *store.DB) (developBranchID, spBranchID int64, fileIn func(branchID int64, name string) int64) {
	t.Helper()
	ctx := context.Background()
	productID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-lsa")
	if err != nil {
		t.Fatalf("product: %v", err)
	}
	developBranchID, err = db.UpsertDSProductBranch(ctx, productID, "develop", "develop", `D:\FA\fa-lsa`, "sha", time.Time{})
	if err != nil {
		t.Fatalf("develop branch: %v", err)
	}
	if err := db.SetDefaultBranch(ctx, productID, "develop"); err != nil {
		t.Fatalf("set default: %v", err)
	}
	spBranchID, err = db.UpsertDSProductBranch(ctx, productID, "FA-Lsa-11-01-7-SP", "fa-lsa-11-01-7", `D:\FA\fa-lsa-11017`, "sha", time.Time{})
	if err != nil {
		t.Fatalf("sp branch: %v", err)
	}

	var scanRunID int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('lsa-branch-test', 'completed') RETURNING id`).Scan(&scanRunID); err != nil {
		t.Fatalf("scan run: %v", err)
	}
	fileIn = func(branchID int64, name string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_branch_id, path, rel_path, extension, hash_sha256, modified_at, encoding, language)
			VALUES ($1, $2, $3, $4, '.sql', 'h', NOW(), '', '') RETURNING id
		`, scanRunID, branchID, `D:/FA/`+name, name).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", name, err)
		}
		return id
	}
	return
}

// TestLoadSpecCorpus_MultiBranchDedup — capability в develop и SP даёт один
// документ из default-ветки; capability только в SP включается.
func TestLoadSpecCorpus_MultiBranchDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	developBranchID, spBranchID, fileIn := lsaBranchFixture(t, db)

	newConfig := func(fileID int64) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`INSERT INTO spec_configs (file_id, product_name) VALUES ($1, 'fa-lsa') RETURNING id`, fileID).Scan(&id); err != nil {
			t.Fatalf("config: %v", err)
		}
		return id
	}
	newCapability := func(configID, fileID int64, slug, title string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO spec_capabilities (file_id, spec_config_id, capability_name, title)
			VALUES ($1, $2, $3, $4) RETURNING id
		`, fileID, configID, slug, title).Scan(&id); err != nil {
			t.Fatalf("capability %s: %v", slug, err)
		}
		return id
	}

	// Общая capability в двух ветках с разными заголовками.
	devConfig := newConfig(fileIn(developBranchID, "develop/openspec/config.yaml"))
	spConfig := newConfig(fileIn(spBranchID, "sp/openspec/config.yaml"))
	newCapability(devConfig, fileIn(developBranchID, "develop/openspec/specs/a/spec.md"), "shared/cap", "Develop Title")
	spSharedID := newCapability(spConfig, fileIn(spBranchID, "sp/openspec/specs/a/spec.md"), "shared/cap", "SP Title")
	// Capability только в SP-ветке.
	spOnlyID := newCapability(spConfig, fileIn(spBranchID, "sp/openspec/specs/b/spec.md"), "only-sp/cap", "Only SP")

	caps, err := db.LoadAllSpecCapabilitiesWithReqsForLSA(ctx)
	if err != nil {
		t.Fatalf("load corpus: %v", err)
	}
	var developShared, spOnly bool
	for _, c := range caps {
		switch c.ID {
		case spSharedID:
			t.Fatalf("SP copy of shared capability leaked into corpus: %+v", c)
		case spOnlyID:
			spOnly = true
			if c.Title != "Only SP" {
				t.Fatalf("sp-only capability title = %q", c.Title)
			}
		default:
			if c.Title == "Develop Title" {
				developShared = true
			}
		}
	}
	if !developShared {
		t.Fatal("develop copy of shared capability missing from corpus")
	}
	if !spOnly {
		t.Fatal("sp-only capability missing from corpus")
	}
}

// TestLoadDescriptionsForLSA_MultiBranchDedup — процедура в двух ветках даёт
// один документ из default-ветки; процедура только в SP включается;
// повторная загрузка идентична (стабильный fingerprint).
func TestLoadDescriptionsForLSA_MultiBranchDedup(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	developBranchID, spBranchID, fileIn := lsaBranchFixture(t, db)

	insertProc := func(fileID int64, name, description string) int64 {
		t.Helper()
		var id int64
		if err := db.QueryRow(`
			INSERT INTO sql_procedures (file_id, proc_name, description) VALUES ($1, $2, $3) RETURNING id
		`, fileID, name, description).Scan(&id); err != nil {
			t.Fatalf("insert proc %s: %v", name, err)
		}
		return id
	}

	// Общая процедура: develop-копия «каноническая».
	devShared := insertProc(fileIn(developBranchID, "develop/SharedProc.sql"), "SharedProc", "develop description")
	spShared := insertProc(fileIn(spBranchID, "sp/SharedProc.sql"), "SharedProc", "sp description")
	// Процедура только в SP-ветке.
	spOnly := insertProc(fileIn(spBranchID, "sp/OnlySp.sql"), "OnlySpProc", "only sp description")

	docs, err := db.LoadDescriptionsForLSA(ctx)
	if err != nil {
		t.Fatalf("load descriptions: %v", err)
	}
	var seenShared, seenOnly, seenSpShared bool
	for _, d := range docs {
		switch d.ID {
		case devShared:
			seenShared = true
			if d.Description != "develop description" {
				t.Fatalf("shared doc description = %q, want develop copy", d.Description)
			}
		case spShared:
			seenSpShared = true
		case spOnly:
			seenOnly = true
		}
	}
	if !seenShared {
		t.Fatal("develop copy of shared procedure missing from corpus")
	}
	if seenSpShared {
		t.Fatal("SP copy of shared procedure leaked into corpus")
	}
	if !seenOnly {
		t.Fatal("SP-only procedure missing from corpus")
	}

	// Детерминированность: повторная загрузка идентична.
	again, err := db.LoadDescriptionsForLSA(ctx)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(again) != len(docs) {
		t.Fatalf("corpus size changed: %d vs %d", len(again), len(docs))
	}
	for i := range docs {
		if docs[i] != again[i] {
			t.Fatalf("corpus row %d differs between loads: %+v vs %+v", i, docs[i], again[i])
		}
	}
}

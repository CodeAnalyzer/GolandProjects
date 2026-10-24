//go:build integration

package store_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// noCutoff — нулевой cutoff: upsert не освобождает чужие строки (защита
// свежих строк текущего скана). Тесты переноса каталога используют
// будущий cutoff явно.
var noCutoff = time.Time{}

func branchTestScanRun(t *testing.T, db *store.DB) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow(`INSERT INTO scan_runs (root_path, status) VALUES ('branch-test', 'completed') RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("insert scan run: %v", err)
	}
	return id
}

func branchTestProduct(t *testing.T, db *store.DB, name string) int64 {
	t.Helper()
	id, err := db.GetOrCreateDSProductIDByName(context.Background(), name)
	if err != nil {
		t.Fatalf("get or create product %s: %v", name, err)
	}
	return id
}

func branchRow(t *testing.T, db *store.DB, id int64) (branchKey, path string, isDefault bool, commit string) {
	t.Helper()
	if err := db.QueryRow(`
		SELECT branch_key, path, is_default, commit_sha FROM ds_products_branch WHERE id = $1
	`, id).Scan(&branchKey, &path, &isDefault, &commit); err != nil {
		t.Fatalf("read branch %d: %v", id, err)
	}
	return
}

func TestUpsertDSProductBranch(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	productID := branchTestProduct(t, db, "fa-contracts")

	id1, err := db.UpsertDSProductBranch(ctx, productID, "develop", "develop", `D:\FA\fa-contracts`, "sha1", noCutoff)
	if err != nil {
		t.Fatalf("upsert #1: %v", err)
	}

	// Повторный upsert того же каталога — та же строка, commit обновляется.
	id2, err := db.UpsertDSProductBranch(ctx, productID, "develop", "develop", `D:\FA\fa-contracts`, "sha2", noCutoff)
	if err != nil {
		t.Fatalf("upsert #2: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("upsert #2 created new row: %d vs %d", id2, id1)
	}
	if _, path, _, commit := branchRow(t, db, id1); commit != "sha2" || path != `D:\FA\fa-contracts` {
		t.Fatalf("upsert #2 did not update in place: path=%q commit=%q", path, commit)
	}

	// Смена ветки в каталоге — та же строка (unique path), ключ обновляется.
	id3, err := db.UpsertDSProductBranch(ctx, productID, "FA-Contracts-11-01-7-SP", "fa-contracts-11-01-7", `D:\FA\fa-contracts`, "sha3", noCutoff)
	if err != nil {
		t.Fatalf("upsert switch: %v", err)
	}
	if id3 != id1 {
		t.Fatalf("switch created new row: %d vs %d", id3, id1)
	}
	if key, _, isDefault, _ := branchRow(t, db, id1); key != "fa-contracts-11-01-7" || isDefault {
		t.Fatalf("switch result: key=%q is_default=%v", key, isDefault)
	}
}

func TestUpsertDSProductBranch_RejectsTwoLiveDirsPerLine(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	productID := branchTestProduct(t, db, "fa-payments")

	idA, err := db.UpsertDSProductBranch(ctx, productID, "L1-SP", "l1", `D:\FA\fa-payments-a`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert A: %v", err)
	}
	// Файл делает строку A «живой» — устаревшей она считаться не может.
	scanRunID := branchTestScanRun(t, db)
	if _, err := db.Exec(`
		INSERT INTO files (scan_run_id, ds_product_branch_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, $2, 'D:\FA\fa-payments-a\X.sql', 'X.sql', '.sql', 'h', NOW())
	`, scanRunID, idA); err != nil {
		t.Fatalf("attach file to A: %v", err)
	}

	// Тот же (продукт, ключ) в другом каталоге при живой строке A — отказ.
	if _, err := db.UpsertDSProductBranch(ctx, productID, "L1-SP", "l1", `D:\FA\fa-payments-b`, "sha", noCutoff); err == nil {
		t.Fatal("upsert of duplicate line in second live dir must fail on unique constraint")
	}
}

func TestUpsertDSProductBranch_FreesStaleLineWithoutFiles(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	productID := branchTestProduct(t, db, "fa-agreement")

	idOld, err := db.UpsertDSProductBranch(ctx, productID, "L2-SP", "l2", `D:\FA\old-dir`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert old: %v", err)
	}
	// Каталог перенесён (строка old существовала до cutoff): тот же ключ в
	// новом каталоге, старая строка без файлов удаляется upsert'ом.
	idNew, err := db.UpsertDSProductBranch(ctx, productID, "L2-SP", "l2", `D:\FA\new-dir`, "sha2", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("upsert new: %v", err)
	}
	if idNew == idOld {
		t.Fatalf("move must create new row: %d == %d", idNew, idOld)
	}
	var oldExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM ds_products_branch WHERE id = $1)`, idOld).Scan(&oldExists); err != nil {
		t.Fatalf("check old row: %v", err)
	}
	if oldExists {
		t.Fatal("stale line row without files must be removed")
	}
}

func TestSetDefaultBranch(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	productID := branchTestProduct(t, db, "fa-cards")

	idDevelop, err := db.UpsertDSProductBranch(ctx, productID, "develop", "develop", `D:\FA\fa-cards-dev`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert develop: %v", err)
	}
	idSP, err := db.UpsertDSProductBranch(ctx, productID, "FA-Cards-11-01-7-SP", "fa-cards-11-01-7", `D:\FA\fa-cards-11017`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert sp: %v", err)
	}

	if err := db.SetDefaultBranch(ctx, productID, "develop"); err != nil {
		t.Fatalf("set default: %v", err)
	}
	assertDefault := func(id int64, want bool) {
		t.Helper()
		var got bool
		if err := db.QueryRow(`SELECT is_default FROM ds_products_branch WHERE id = $1`, id).Scan(&got); err != nil {
			t.Fatalf("read is_default %d: %v", id, err)
		}
		if got != want {
			t.Fatalf("is_default(%d) = %v, want %v", id, got, want)
		}
	}
	assertDefault(idDevelop, true)
	assertDefault(idSP, false)

	// Смена default-ветки перекладывает флаг.
	if err := db.SetDefaultBranch(ctx, productID, "FA-Cards-11-01-7-SP"); err != nil {
		t.Fatalf("flip default: %v", err)
	}
	assertDefault(idDevelop, false)
	assertDefault(idSP, true)

	// Отсутствующее имя — не ошибка, флаг снимается со всех линий.
	if err := db.SetDefaultBranch(ctx, productID, "missing-branch"); err != nil {
		t.Fatalf("missing default name must not error: %v", err)
	}
	assertDefault(idSP, false)
}

func TestDeleteStaleDSProductBranches(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()
	productID := branchTestProduct(t, db, "fa-common")

	idOld, err := db.UpsertDSProductBranch(ctx, productID, "old-sp", "old", `D:\FA\old`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert old: %v", err)
	}
	idFresh, err := db.UpsertDSProductBranch(ctx, productID, "new-sp", "new", `D:\FA\new`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert fresh: %v", err)
	}
	idWithFiles, err := db.UpsertDSProductBranch(ctx, productID, "kept-sp", "kept", `D:\FA\kept`, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert with files: %v", err)
	}
	scanRunID := branchTestScanRun(t, db)
	if _, err := db.Exec(`
		INSERT INTO files (scan_run_id, ds_product_branch_id, path, rel_path, extension, hash_sha256, modified_at)
		VALUES ($1, $2, 'D:\FA\kept\X.sql', 'X.sql', '.sql', 'h', NOW())
	`, scanRunID, idWithFiles); err != nil {
		t.Fatalf("attach file: %v", err)
	}
	// Обе «старые» строки искусственно состариваем до discovery-cutoff.
	if _, err := db.Exec(`UPDATE ds_products_branch SET indexed_at = NOW() - interval '1 hour' WHERE id IN ($1, $2)`, idOld, idWithFiles); err != nil {
		t.Fatalf("age rows: %v", err)
	}

	deleted, err := db.DeleteStaleDSProductBranches(ctx, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("delete stale: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	for _, id := range []int64{idOld} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM ds_products_branch WHERE id = $1)`, id).Scan(&exists); err != nil {
			t.Fatalf("check %d: %v", id, err)
		}
		if exists {
			t.Fatalf("row %d must be deleted", id)
		}
	}
	for _, id := range []int64{idFresh, idWithFiles} {
		var exists bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM ds_products_branch WHERE id = $1)`, id).Scan(&exists); err != nil {
			t.Fatalf("check %d: %v", id, err)
		}
		if !exists {
			t.Fatalf("row %d must survive", id)
		}
	}
}

func TestBackfillFilesFromSources(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	root := t.TempDir()
	srcDir := filepath.Join(root, "fa-contracts-11017")
	plainDir := filepath.Join(root, "plain")
	neighborDir := filepath.Join(root, "fa-contracts-11017extra")

	productID := branchTestProduct(t, db, "fa-contracts")
	legacyProductID := branchTestProduct(t, db, "fa-contracts-11017") // псевдопродукт старой эвристики
	branchID, err := db.UpsertDSProductBranch(ctx, productID, "FA-Contracts-11-01-7-SP", "fa-contracts-11-01-7", srcDir, "sha", noCutoff)
	if err != nil {
		t.Fatalf("upsert branch: %v", err)
	}

	scanRunID := branchTestScanRun(t, db)
	insertFile := func(path string, product interface{}) int64 {
		t.Helper()
		var id int64
		// Путь — ToSlash, как сохраняет walker.
		if err := db.QueryRow(`
			INSERT INTO files (scan_run_id, ds_product_id, path, rel_path, extension, hash_sha256, modified_at)
			VALUES ($1, $2, $3, $4, '.sql', 'h', NOW()) RETURNING id
		`, scanRunID, product, filepath.ToSlash(path), filepath.Base(path)).Scan(&id); err != nil {
			t.Fatalf("insert file %s: %v", path, err)
		}
		return id
	}
	srcFile := insertFile(filepath.Join(srcDir, "SOURCE", "A.sql"), legacyProductID)
	plainFile := insertFile(filepath.Join(plainDir, "B.sql"), nil)
	neighborFile := insertFile(filepath.Join(neighborDir, "C.sql"), nil)

	updated, err := db.BackfillFilesFromSources(ctx, []store.FileSourceBinding{
		{Path: filepath.ToSlash(srcDir), ProductID: productID, BranchID: branchID},
	})
	if err != nil {
		t.Fatalf("backfill: %v", err)
	}
	if updated != 1 {
		t.Fatalf("backfill updated %d rows, want 1", updated)
	}

	assertBound := func(id int64, wantProduct, wantBranch int64) {
		t.Helper()
		var gotProduct, gotBranch int64
		if err := db.QueryRow(`SELECT COALESCE(ds_product_id, 0), COALESCE(ds_product_branch_id, 0) FROM files WHERE id = $1`, id).Scan(&gotProduct, &gotBranch); err != nil {
			t.Fatalf("read file %d: %v", id, err)
		}
		if gotProduct != wantProduct || gotBranch != wantBranch {
			t.Fatalf("file %d bound to (%d, %d), want (%d, %d)", id, gotProduct, gotBranch, wantProduct, wantBranch)
		}
	}
	assertBound(srcFile, productID, branchID)
	assertBound(plainFile, 0, 0)
	assertBound(neighborFile, 0, 0) // граница префикса — разделитель, «...11017extra» не матчится

	// Повторный запуск идемпотентен (0 изменений).
	if n, err := db.BackfillFilesFromSources(ctx, []store.FileSourceBinding{{Path: filepath.ToSlash(srcDir), ProductID: productID, BranchID: branchID}}); err != nil || n != 0 {
		t.Fatalf("second backfill: n=%d err=%v, want 0 nil", n, err)
	}
}

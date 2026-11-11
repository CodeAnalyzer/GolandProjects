//go:build integration

package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/review"
	"github.com/codebase/internal/reviewsvc"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

// seedReviewDsinfoFile — минимальный индекс для review: продукт с develop-веткой
// и один проиндексированный SQL-файл по фактическому пути на диске.
func seedReviewDsinfoFile(t *testing.T, db *store.DB, path string) {
	t.Helper()
	ctx := context.Background()
	productID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-cli-rv")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName: %v", err)
	}
	scanID, err := db.CreateScanRun(ctx, "/repo-cli-review")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var branchID int64
	if err := db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
		VALUES ($1, 'develop', 'develop', '/repo-cli-review/fa-cli-rv/develop', TRUE) RETURNING id`,
		productID).Scan(&branchID); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'rv', NOW(), 'CP866', 'SQL')`,
		scanID, productID, branchID, path); err != nil {
		t.Fatalf("insert file: %v", err)
	}
}

// TestReviewDsinfoFlagBehavior: --dsinfo (Options.DsinfoID) доходит до
// построения области — несуществующий профиль валит review без fallback,
// нулевой id (env-цепочка) работает.
func TestReviewDsinfoFlagBehavior(t *testing.T) {
	db := testutil.Open(t)
	useDBConfig(t, db)

	content := "create proc Cons_Rv_Check\nas\n  return\n"
	path := filepath.Join(t.TempDir(), "RvCheck.sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}
	seedReviewDsinfoFile(t, db, path)
	t.Setenv("CODEBASE_DSINFO_ID", "")

	opts := review.Options{Rules: []review.RuleID{review.RuleExecNotExistsProc}}

	// Несуществующий профиль — ошибка без fallback.
	_, err := reviewsvc.ExecuteCtx(context.Background(), path, review.Options{Rules: opts.Rules, DsinfoID: 999}, nil)
	if err == nil || !strings.Contains(err.Error(), "dsinfo profile 999 not found") {
		t.Fatalf("ExecuteCtx (dsinfo 999) err = %v, want dsinfo profile 999 not found", err)
	}

	// Нулевой id — env/active-цепочка, профиль не требуется.
	result, err := reviewsvc.ExecuteCtx(context.Background(), path, opts, nil)
	if err != nil {
		t.Fatalf("ExecuteCtx (dsinfo 0): %v", err)
	}
	if result == nil || result.Summary.Total < 0 {
		t.Fatalf("ExecuteCtx (dsinfo 0) result = %+v, want result", result)
	}
}

//go:build integration

package mcp

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codebase/internal/store/testutil"
)

// TestReviewToolDsinfoIDBehavior: параметр dsinfo_id инструмента
// codebase_review_sql доходит до построения области — несуществующий профиль
// валит вызов без fallback, вызов без dsinfo_id работает.
func TestReviewToolDsinfoIDBehavior(t *testing.T) {
	db := testutil.Open(t)

	content := "create proc Cons_Rv_Check\nas\n  return\n"
	path := filepath.Join(t.TempDir(), "RvCheck.sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp sql: %v", err)
	}

	ctx := context.Background()
	t.Setenv("CODEBASE_DSINFO_ID", "")
	productID, err := db.GetOrCreateDSProductIDByName(ctx, "fa-mcp-rv")
	if err != nil {
		t.Fatalf("GetOrCreateDSProductIDByName: %v", err)
	}
	scanID, err := db.CreateScanRun(ctx, "/repo-mcp-review")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	var branchID int64
	if err := db.QueryRow(`
		INSERT INTO ds_products_branch (ds_product_id, branch_name, branch_key, path, is_default)
		VALUES ($1, 'develop', 'develop', '/repo-mcp-review/fa-mcp-rv/develop', TRUE) RETURNING id`,
		productID).Scan(&branchID); err != nil {
		t.Fatalf("insert branch: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO files (scan_run_id, ds_product_id, ds_product_branch_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, $4, $4, 'sql', 1, 'rv', NOW(), 'CP866', 'SQL')`,
		scanID, productID, branchID, path); err != nil {
		t.Fatalf("insert file: %v", err)
	}

	registry := buildToolRegistry(db)
	tool, ok := registry["codebase_review_sql"]
	if !ok {
		t.Fatal("codebase_review_sql не зарегистрирован")
	}

	// Несуществующий профиль — ошибка без fallback.
	_, err = tool.Handler(ctx, map[string]interface{}{
		"file_path": path,
		"dsinfo_id": float64(999),
	})
	if err == nil || !strings.Contains(err.Error(), "dsinfo profile 999 not found") {
		t.Fatalf("handler (dsinfo_id 999) err = %v, want dsinfo profile 999 not found", err)
	}

	// Вызов без dsinfo_id — env/active-цепочка, работает без профиля.
	if _, err = tool.Handler(ctx, map[string]interface{}{"file_path": path}); err != nil {
		t.Fatalf("handler (без dsinfo_id): %v", err)
	}
}

// TestReviewToolRegistryNilDB — toolRegistry без БД (package-level) всё равно
// содержит схему с dsinfo_id (handler уйдёт в file-only Execute).
func TestReviewToolRegistryNilDB(t *testing.T) {
	if _, ok := toolRegistry["codebase_review_sql"]; !ok {
		t.Fatal("codebase_review_sql отсутствует в toolRegistry")
	}
}

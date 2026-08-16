//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func insertNamedFile(t *testing.T, db *store.DB, scanID int64, path, hash string) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, 'sql', 1, $4, $5, 'CP866', 'SQL')
		RETURNING id
	`, scanID, path, path, hash, time.Now()).Scan(&id)
	if err != nil {
		t.Fatalf("insert file %s: %v", path, err)
	}
	return id
}

func TestDeleteFilesByPathsExcept_KeepsNewIDAndCascades(t *testing.T) {
	db := testutil.Open(t)
	scanID, err := db.CreateScanRun(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}

	oldID := insertNamedFile(t, db, scanID, "/repo/a.sql", "old")
	newID := insertNamedFile(t, db, scanID, "/repo/a.sql", "new")
	otherID := insertNamedFile(t, db, scanID, "/repo/b.sql", "b")

	if err := db.BatchInsertSQLProcedures(context.Background(), []*model.SQLProcedure{
		{FileID: oldID, ProcName: "OldProc", LineStart: 1, LineEnd: 2},
		{FileID: newID, ProcName: "NewProc", LineStart: 1, LineEnd: 2},
	}, 100); err != nil {
		t.Fatalf("insert procs: %v", err)
	}

	if err := db.DeleteFilesByPathsExcept(context.Background(), []string{"/repo/a.sql", "/repo/missing.sql"}, map[string]int64{"/repo/a.sql": newID}); err != nil {
		t.Fatalf("DeleteFilesByPathsExcept: %v", err)
	}

	var oldExists, newExists, otherExists bool
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM files WHERE id=$1)`, oldID).Scan(&oldExists)
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM files WHERE id=$1)`, newID).Scan(&newExists)
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM files WHERE id=$1)`, otherID).Scan(&otherExists)
	if oldExists || !newExists || !otherExists {
		t.Fatalf("old=%v new=%v other=%v", oldExists, newExists, otherExists)
	}

	var oldProcExists bool
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM sql_procedures WHERE file_id=$1)`, oldID).Scan(&oldProcExists)
	if oldProcExists {
		t.Fatal("old procedure must cascade-delete")
	}

	if err := db.DeleteFilesByPaths(context.Background(), nil); err != nil {
		t.Fatalf("empty DeleteFilesByPaths: %v", err)
	}
	if err := db.DeleteFilesByPaths(context.Background(), []string{"/repo/b.sql"}); err != nil {
		t.Fatalf("DeleteFilesByPaths: %v", err)
	}
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM files WHERE id=$1)`, otherID).Scan(&otherExists)
	if otherExists {
		t.Fatal("b.sql must be deleted")
	}
}

func TestGetLatestFilesByRootPath_PicksLatestID(t *testing.T) {
	db := testutil.Open(t)
	scanID, err := db.CreateScanRun(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	_ = insertNamedFile(t, db, scanID, "/repo/a.sql", "old")
	newID := insertNamedFile(t, db, scanID, "/repo/a.sql", "new")

	files, err := db.GetLatestFilesByRootPath(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("GetLatestFilesByRootPath: %v", err)
	}
	got := files["/repo/a.sql"]
	if got == nil || got.ID != newID || got.HashSHA256 != "new" {
		t.Fatalf("latest file = %+v, want id=%d", got, newID)
	}
}

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

func insertScanAndFile(t *testing.T, db *store.DB) (scanID, fileID int64) {
	t.Helper()
	var err error
	scanID, err = db.CreateScanRun(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	err = db.QueryRow(`
		INSERT INTO files (scan_run_id, path, rel_path, extension, size_bytes, hash_sha256, modified_at, encoding, language)
		VALUES ($1, $2, $3, 'sql', 10, 'abc', $4, 'CP866', 'SQL')
		RETURNING id
	`, scanID, t.Name()+"/a.sql", "a.sql", time.Now()).Scan(&fileID)
	if err != nil {
		t.Fatalf("insert file: %v", err)
	}
	return scanID, fileID
}

func TestBatchInsertSQLProceduresAndLookup(t *testing.T) {
	db := testutil.Open(t)
	_, fileID := insertScanAndFile(t, db)

	if err := db.BatchInsertSQLProcedures(context.Background(), nil, 100); err != nil {
		t.Fatalf("empty batch: %v", err)
	}

	invalidName := "Callee" + string([]byte{0xff}) + "B"
	procs := []*model.SQLProcedure{
		{FileID: fileID, ProcName: "CallerA", LineStart: 1, LineEnd: 10, BodyHash: "h1"},
		{FileID: fileID, ProcName: invalidName, LineStart: 11, LineEnd: 20, BodyHash: "h2"},
	}
	if err := db.BatchInsertSQLProcedures(context.Background(), procs, 100); err != nil {
		t.Fatalf("BatchInsertSQLProcedures: %v", err)
	}

	ids, err := db.FindLatestSQLProcedureIDsByNames(context.Background(), []string{"callera", "CalleeB"})
	if err != nil {
		t.Fatalf("FindLatestSQLProcedureIDsByNames: %v", err)
	}
	if ids["callera"] == 0 || ids["calleeb"] == 0 {
		t.Fatalf("procedure ids = %#v", ids)
	}
}

func TestWithBatchTx_SeesUncommittedInsert(t *testing.T) {
	db := testutil.Open(t)
	_, fileID := insertScanAndFile(t, db)

	var seenID int64
	err := db.WithBatchTx(func(txdb *store.DB) error {
		if err := txdb.BatchInsertSQLProcedures(context.Background(), []*model.SQLProcedure{
			{FileID: fileID, ProcName: "InTxProc", LineStart: 1, LineEnd: 2},
		}, 100); err != nil {
			return err
		}
		ids, err := txdb.FindLatestSQLProcedureIDsByNames(context.Background(), []string{"InTxProc"})
		if err != nil {
			return err
		}
		seenID = ids["intxproc"]
		return nil
	})
	if err != nil {
		t.Fatalf("WithBatchTx: %v", err)
	}
	if seenID == 0 {
		t.Fatal("SELECT in same tx did not see COPY insert")
	}
}

func TestBatchInsertRelationsAndScanRun(t *testing.T) {
	db := testutil.Open(t)
	if err := db.BatchInsertRelations(context.Background(), nil, 100); err != nil {
		t.Fatalf("empty relations: %v", err)
	}

	rels := []*model.Relation{
		{SourceType: "sql_procedure", SourceID: 1, TargetType: "sql_procedure", TargetID: 2, RelationType: "calls_procedure", Confidence: "ast", LineNumber: 3},
		{SourceType: "sql_procedure", SourceID: 1, TargetType: "sql_procedure", TargetID: 2, RelationType: "calls_procedure", Confidence: "ast", LineNumber: 3},
	}
	if err := db.BatchInsertRelations(context.Background(), rels, 100); err != nil {
		t.Fatalf("BatchInsertRelations: %v", err)
	}
	if err := db.BatchInsertRelations(context.Background(), rels, 100); err != nil {
		t.Fatalf("duplicate BatchInsertRelations: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT count(*) FROM relations WHERE relation_type='calls_procedure'`).Scan(&count); err != nil {
		t.Fatalf("count relations: %v", err)
	}
	if count != 4 {
		t.Fatalf("relations count = %d, want 4 (no unique constraint)", count)
	}

	scanID, err := db.CreateScanRun(context.Background(), "/tmp/root")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}
	ok, err := db.HasCompletedInit(context.Background())
	if err != nil {
		t.Fatalf("HasCompletedInit: %v", err)
	}
	if ok {
		t.Fatal("running scan must not count as completed init")
	}
	if err := db.UpdateScanRun(context.Background(), scanID, 1, 1, 0, "completed"); err != nil {
		t.Fatalf("UpdateScanRun: %v", err)
	}
	ok, err = db.HasCompletedInit(context.Background())
	if err != nil {
		t.Fatalf("HasCompletedInit after update: %v", err)
	}
	if !ok {
		t.Fatal("expected completed init")
	}
}

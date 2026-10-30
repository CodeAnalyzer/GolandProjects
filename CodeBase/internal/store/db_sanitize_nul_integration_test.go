//go:build integration

package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
	"github.com/codebase/internal/store/testutil"
)

// TestBatchInsertNULTerms_Sanitized — safety-net NUL-strip: текстовые значения
// с байтом 0x00 (недекодированный UTF-16) вставляются в PostgreSQL без ошибки
// и сохраняются без NUL (query_fragments, описания процедур, symbols).
func TestBatchInsertNULTerms_Sanitized(t *testing.T) {
	db := testutil.Open(t)
	_, fileID := insertScanAndFile(t, db)

	fragments := make([]*model.QueryFragment, 0, 128)
	for i := 0; i < 128; i++ {
		fragments = append(fragments, &model.QueryFragment{
			FileID:        fileID,
			ParentType:    "sql_procedure",
			ParentID:      int64(i + 1),
			ComponentName: "Cursor\x00Select" + string(rune('A'+i%26)),
			ComponentType: "cursor",
			QueryText:     "select \x00Name from \x00pTest_" + string(rune('a'+i%26)),
			Context:       "body\x00/main",
			LineNumber:    i + 1,
		})
	}
	if err := db.BatchInsertQueryFragments(context.Background(), fragments, 100); err != nil {
		t.Fatalf("BatchInsertQueryFragments with NUL: %v", err)
	}

	var fragCount int
	if err := db.QueryRow(`SELECT count(*) FROM query_fragments WHERE file_id=$1`, fileID).Scan(&fragCount); err != nil {
		t.Fatalf("count fragments: %v", err)
	}
	if fragCount != 128 {
		t.Fatalf("fragments = %d, want 128", fragCount)
	}
	rows, err := db.Query(`SELECT query_text FROM query_fragments WHERE file_id=$1`, fileID)
	if err != nil {
		t.Fatalf("select fragments: %v", err)
	}
	defer rows.Close()
	nulCount := 0
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			t.Fatalf("scan fragment: %v", err)
		}
		if strings.IndexByte(text, 0) >= 0 {
			nulCount++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate fragments: %v", err)
	}
	if nulCount != 0 {
		t.Fatalf("%d fragments still contain NUL in query_text", nulCount)
	}
	var text string
	if err := db.QueryRow(`SELECT query_text FROM query_fragments WHERE file_id=$1 AND line_number=1`, fileID).Scan(&text); err != nil {
		t.Fatalf("select fragment: %v", err)
	}
	if !strings.Contains(text, "select Name from pTest_a") {
		t.Fatalf("query_text lost content after NUL-strip: %q", text)
	}

	procs := []*model.SQLProcedure{
		{FileID: fileID, ProcName: "Proc\x00NUL", LineStart: 1, LineEnd: 5, Description: "Начисление\x00 процентов"},
	}
	if err := db.BatchInsertSQLProcedures(context.Background(), procs, 100); err != nil {
		t.Fatalf("BatchInsertSQLProcedures with NUL: %v", err)
	}
	var description string
	if err := db.QueryRow(`SELECT description FROM sql_procedures WHERE proc_name='ProcNUL'`).Scan(&description); err != nil {
		t.Fatalf("select procedure: %v", err)
	}
	if description != "Начисление процентов" {
		t.Fatalf("description = %q, want NUL stripped", description)
	}

	symbols := []*model.Symbol{
		{FileID: fileID, SymbolName: "Sym\x00bol", SymbolType: "table", EntityType: "sql", LineNumber: 1, Signature: "sig\x00nature"},
	}
	if err := db.BatchInsertSymbols(context.Background(), symbols, 100); err != nil {
		t.Fatalf("BatchInsertSymbols with NUL: %v", err)
	}
	var signature string
	if err := db.QueryRow(`SELECT signature FROM symbols WHERE symbol_name='Symbol'`).Scan(&signature); err != nil {
		t.Fatalf("select symbol: %v", err)
	}
	if signature != "signature" {
		t.Fatalf("signature = %q, want NUL stripped", signature)
	}
}

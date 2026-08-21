//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func TestExecContext_NoBoundTx_ExecutesDirectly(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}

	// ExecContext без boundTx должен работать как обычный Exec (автокоммит)
	_, err = db.ExecContext(ctx, `DELETE FROM scan_runs WHERE id = $1`, scanID)
	if err != nil {
		t.Fatalf("ExecContext without boundTx: %v", err)
	}

	var exists bool
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM scan_runs WHERE id=$1)`, scanID).Scan(&exists)
	if exists {
		t.Fatalf("row should have been deleted")
	}
}

func TestExecContext_WithBoundTx_RollsBackOnError(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}

	// WithBatchTxCtx: если fn возвращает ошибку, tx откатывается
	err = db.WithBatchTxCtx(ctx, func(txdb *store.DB) error {
		_, err := txdb.ExecContext(ctx, `DELETE FROM scan_runs WHERE id = $1`, scanID)
		if err != nil {
			return err
		}
		// Возвращаем ошибку → rollback
		return context.DeadlineExceeded
	})
	if err == nil {
		t.Fatalf("expected error from WithBatchTxCtx")
	}

	// После rollback строка должна остаться
	var exists bool
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM scan_runs WHERE id=$1)`, scanID).Scan(&exists)
	if !exists {
		t.Fatalf("row should survive rollback")
	}

	// Cleanup
	_, _ = db.ExecContext(ctx, `DELETE FROM scan_runs WHERE id = $1`, scanID)
}

func TestExecContext_WithBoundTx_CommitsOnSuccess(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	scanID, err := db.CreateScanRun(ctx, "/repo")
	if err != nil {
		t.Fatalf("CreateScanRun: %v", err)
	}

	// WithBatchTxCtx: если fn возвращает nil, tx коммитится
	err = db.WithBatchTxCtx(ctx, func(txdb *store.DB) error {
		_, err := txdb.ExecContext(ctx, `DELETE FROM scan_runs WHERE id = $1`, scanID)
		return err
	})
	if err != nil {
		t.Fatalf("WithBatchTxCtx: %v", err)
	}

	// После commit строка должна быть удалена
	var exists bool
	_ = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM scan_runs WHERE id=$1)`, scanID).Scan(&exists)
	if exists {
		t.Fatalf("row should have been deleted within committed tx")
	}
}

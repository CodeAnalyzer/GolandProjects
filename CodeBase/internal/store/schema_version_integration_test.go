//go:build integration

package store_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func TestInitSchema_ConcurrentIndependentHandles(t *testing.T) {
	handles := testutil.OpenEmptyHandles(t, 4)
	start := make(chan struct{})
	errs := make(chan error, len(handles))
	var wg sync.WaitGroup
	for _, db := range handles {
		wg.Add(1)
		go func(db *store.DB) {
			defer wg.Done()
			<-start
			errs <- db.InitSchemaCtx(context.Background())
		}(db)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent InitSchemaCtx: %v", err)
		}
	}

	var exists bool
	if err := handles[0].QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='files')`).Scan(&exists); err != nil {
		t.Fatalf("check required table: %v", err)
	}
	if !exists {
		t.Fatal("files table was not created")
	}
	if err := handles[0].QueryRow(`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, store.CurrentSchemaVersion).Scan(&exists); err != nil {
		t.Fatalf("check current marker: %v", err)
	}
	if !exists {
		t.Fatal("current schema marker was not recorded")
	}
}

func TestInitSchema_ContextCancellationWhileWaitingForLock(t *testing.T) {
	handles := testutil.OpenEmptyHandles(t, 2)
	holder, err := handles[0].BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	if _, err := holder.Exec(`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "codebase/schema-init"); err != nil {
		_ = holder.Rollback()
		t.Fatalf("hold schema init lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waitErr := handles[1].InitSchemaCtx(ctx)
	if waitErr == nil || ctx.Err() == nil {
		t.Fatalf("waiter error = %v, context error = %v, want context cancellation", waitErr, ctx.Err())
	}
	var markerExists bool
	if err := handles[1].QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='schema_migrations')`).Scan(&markerExists); err != nil {
		t.Fatalf("check canceled schema: %v", err)
	}
	if markerExists {
		t.Fatal("canceled initialization created schema_migrations")
	}
	if err := holder.Rollback(); err != nil {
		t.Fatalf("release schema init lock: %v", err)
	}
	if err := handles[1].InitSchemaCtx(context.Background()); err != nil {
		t.Fatalf("InitSchemaCtx after releasing lock: %v", err)
	}
}

func TestCheckSchemaVersion_EmptyDatabaseIsReadOnly(t *testing.T) {
	db := testutil.OpenEmpty(t)
	err := db.CheckSchemaVersion(context.Background())
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
	if !strings.Contains(err.Error(), store.CurrentSchemaVersion) {
		t.Fatalf("error = %v, want expected marker", err)
	}
	var tableExists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='schema_migrations')`).Scan(&tableExists); err != nil {
		t.Fatalf("check schema_migrations: %v", err)
	}
	if tableExists {
		t.Fatal("checker created schema_migrations")
	}
}

func TestCheckSchemaVersion_OutdatedDatabaseIsReadOnly(t *testing.T) {
	db := testutil.OpenEmpty(t)
	if _, err := db.Exec(`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations(version) VALUES ('codebase_schema_legacy')`); err != nil {
		t.Fatalf("insert legacy marker: %v", err)
	}
	var before int
	if err := db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&before); err != nil {
		t.Fatalf("count rows before check: %v", err)
	}
	err := db.CheckSchemaVersion(context.Background())
	if !errors.Is(err, store.ErrSchemaUpdateRequired) {
		t.Fatalf("error = %v, want ErrSchemaUpdateRequired", err)
	}
	if !strings.Contains(err.Error(), "codebase_schema_legacy") || !strings.Contains(err.Error(), store.CurrentSchemaVersion) {
		t.Fatalf("error = %v, want expected and found markers", err)
	}
	var after int
	if err := db.QueryRow(`SELECT count(*) FROM schema_migrations`).Scan(&after); err != nil {
		t.Fatalf("count rows after check: %v", err)
	}
	if after != before {
		t.Fatalf("schema_migrations rows changed: before=%d after=%d", before, after)
	}
}

func TestCheckSchemaVersion_CurrentDatabase(t *testing.T) {
	db := testutil.Open(t)
	if err := db.CheckSchemaVersion(context.Background()); err != nil {
		t.Fatalf("CheckSchemaVersion: %v", err)
	}
}

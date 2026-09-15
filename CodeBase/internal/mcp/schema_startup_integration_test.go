//go:build integration

package mcp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func TestPrepareMCPDatabase_CurrentSchemaIsReadOnly(t *testing.T) {
	seed := testutil.Open(t)
	cfg := testutil.ConfigFor(t, seed)
	var before int
	if err := seed.QueryRow(`SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace`).Scan(&before); err != nil {
		t.Fatalf("catalog count before startup: %v", err)
	}
	db, err := prepareMCPDatabase(context.Background(), cfg)
	if err != nil {
		t.Fatalf("prepareMCPDatabase: %v", err)
	}
	defer db.Close()
	var after int
	if err := seed.QueryRow(`SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace`).Scan(&after); err != nil {
		t.Fatalf("catalog count after startup: %v", err)
	}
	if after != before {
		t.Fatalf("startup changed catalog: before=%d after=%d", before, after)
	}
}

func TestPrepareMCPDatabase_MissingSchemaRecommendsInit(t *testing.T) {
	seed := testutil.OpenEmpty(t)
	cfg := testutil.ConfigFor(t, seed)
	_, err := prepareMCPDatabase(context.Background(), cfg)
	if !errors.Is(err, store.ErrSchemaNotInitialized) {
		t.Fatalf("error = %v, want ErrSchemaNotInitialized", err)
	}
	if !strings.Contains(err.Error(), "codebase init --config <path>") {
		t.Fatalf("error = %v, want init recommendation", err)
	}
	var exists bool
	if err := seed.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='schema_migrations')`).Scan(&exists); err != nil {
		t.Fatalf("check schema_migrations: %v", err)
	}
	if exists {
		t.Fatal("MCP startup created schema_migrations")
	}
}

func TestPrepareMCPDatabase_OutdatedSchemaRecommendsUpdate(t *testing.T) {
	seed := testutil.OpenEmpty(t)
	if _, err := seed.Exec(`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`); err != nil {
		t.Fatalf("create schema_migrations: %v", err)
	}
	if _, err := seed.Exec(`INSERT INTO schema_migrations(version) VALUES ('codebase_schema_legacy')`); err != nil {
		t.Fatalf("insert legacy marker: %v", err)
	}
	cfg := testutil.ConfigFor(t, seed)
	_, err := prepareMCPDatabase(context.Background(), cfg)
	if !errors.Is(err, store.ErrSchemaUpdateRequired) {
		t.Fatalf("error = %v, want ErrSchemaUpdateRequired", err)
	}
	if !strings.Contains(err.Error(), "codebase update --config <path>") {
		t.Fatalf("error = %v, want update recommendation", err)
	}
	if strings.Contains(err.Error(), cfg.Password) && cfg.Password != "" {
		t.Fatalf("error contains database password: %v", err)
	}
}

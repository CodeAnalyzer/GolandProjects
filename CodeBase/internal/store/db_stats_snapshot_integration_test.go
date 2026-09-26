//go:build integration

package store_test

import (
	"context"
	"testing"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
)

func TestStatsSnapshot_TableCreatedAndRoundTrip(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	var tables int
	if err := db.QueryRow(`
		SELECT count(*) FROM information_schema.tables WHERE table_name = 'stats_snapshot'
	`).Scan(&tables); err != nil {
		t.Fatalf("check stats_snapshot table: %v", err)
	}
	if tables != 1 {
		t.Fatalf("stats_snapshot table count = %d, want 1", tables)
	}

	if _, _, ok, err := db.LoadStatsSnapshot(ctx); err != nil || ok {
		t.Fatalf("expected no snapshot initially, ok=%v err=%v", ok, err)
	}

	if err := db.SaveStatsSnapshot(ctx, &store.Stats{TotalFiles: 42, Procedures: 7}, ""); err != nil {
		t.Fatalf("SaveStatsSnapshot: %v", err)
	}
	loaded, _, ok, err := db.LoadStatsSnapshot(ctx)
	if err != nil || !ok {
		t.Fatalf("LoadStatsSnapshot: ok=%v err=%v", ok, err)
	}
	if loaded.TotalFiles != 42 || loaded.Procedures != 7 {
		t.Fatalf("loaded = %+v, want TotalFiles=42 Procedures=7", loaded)
	}

	if err := db.SaveStatsSnapshot(ctx, &store.Stats{TotalFiles: 100}, ""); err != nil {
		t.Fatalf("SaveStatsSnapshot overwrite: %v", err)
	}
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM stats_snapshot`).Scan(&rows); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if rows != 1 {
		t.Fatalf("snapshot rows = %d, want 1", rows)
	}
}

func TestGetStats_UsesSnapshotAndFallbackPersists(t *testing.T) {
	db := testutil.Open(t)
	ctx := context.Background()

	if _, _, ok, err := db.LoadStatsSnapshot(ctx); err != nil || ok {
		t.Fatalf("expected no snapshot initially, ok=%v err=%v", ok, err)
	}

	stats, err := db.GetStats(ctx, "")
	if err != nil {
		t.Fatalf("GetStats fallback: %v", err)
	}
	if stats == nil {
		t.Fatal("GetStats returned nil stats")
	}
	if _, _, ok, err := db.LoadStatsSnapshot(ctx); err != nil || !ok {
		t.Fatalf("fallback must persist snapshot: ok=%v err=%v", ok, err)
	}

	if err := db.SaveStatsSnapshot(ctx, &store.Stats{TotalFiles: 999}, ""); err != nil {
		t.Fatalf("SaveStatsSnapshot: %v", err)
	}
	got, err := db.GetStats(ctx, "")
	if err != nil {
		t.Fatalf("GetStats snapshot: %v", err)
	}
	if got.TotalFiles != 999 {
		t.Fatalf("GetStats did not read snapshot: TotalFiles=%d, want 999", got.TotalFiles)
	}
}

package cmd

import (
	"errors"
	"testing"
	"time"

	"github.com/codebase/internal/store"
)

func TestBuildStatsResponse(t *testing.T) {
	started := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	finished := started.Add(time.Minute)
	stats := &store.Stats{
		TotalFiles:         10,
		SQLFiles:           2,
		HFiles:             1,
		PASFiles:           3,
		Procedures:         4,
		Tables:             5,
		APIContracts:       6,
		APIContractParams:  7,
		APIContractTables:  8,
		APIContractFields:  9,
		APIBusinessObjects: 10,
		Relations:          11,
		Errors:             12,
		LastScanID:         13,
		LastScanStatus:     "completed",
		LastScanStarted:    started,
		LastScanFinished:   finished,
	}

	response := buildStatsResponse(stats)
	if !response.Success || response.FormatVersion != "1.0" || response.Command != "stats" {
		t.Fatalf("unexpected response header: %+v", response)
	}
	if response.Files.Total != 10 || response.Files.SQL != 2 || response.Files.H != 1 || response.Files.PAS != 3 {
		t.Fatalf("unexpected files summary: %+v", response.Files)
	}
	if response.Entities.Procedures != 4 || response.Entities.Tables != 5 || response.Entities.APIContracts != 6 || response.Entities.Relations != 11 {
		t.Fatalf("unexpected entities summary: %+v", response.Entities)
	}
	if response.LastScan.RunID != 13 || response.LastScan.Status != "completed" || response.LastScan.Errors != 12 {
		t.Fatalf("unexpected last scan summary: %+v", response.LastScan)
	}
	if response.LastScan.Started == "" || response.LastScan.Finished == "" {
		t.Fatalf("expected formatted timestamps: %+v", response.LastScan)
	}
}

func TestClassifyStatsError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: errors.New("config not loaded"), want: "config_error"},
		{err: errors.New("failed to connect to database: connection refused"), want: "database_unavailable"},
		{err: errors.New("failed to init schema"), want: "schema_init_failed"},
		{err: errors.New("failed to get stats"), want: "stats_query_failed"},
		{err: errors.New("other"), want: "internal_error"},
	}
	for _, tt := range tests {
		if got := classifyStatsError(tt.err); got != tt.want {
			t.Fatalf("classifyStatsError(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

func TestClassifyHealthError(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: errors.New("config not loaded"), want: "config_error"},
		{err: errors.New("failed to ping database: connection refused"), want: "database_unavailable"},
		{err: errors.New("failed to init schema"), want: "schema_init_failed"},
		{err: errors.New("failed to inspect index readiness"), want: "health_check_failed"},
		{err: errors.New("other"), want: "internal_error"},
	}
	for _, tt := range tests {
		if got := classifyHealthError(tt.err); got != tt.want {
			t.Fatalf("classifyHealthError(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

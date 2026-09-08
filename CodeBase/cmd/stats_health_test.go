package cmd

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/codebase/internal/errs"
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
		MDFiles:            14,
		YAMLFiles:          15,
		Procedures:         4,
		Tables:             5,
		APIContracts:       6,
		APIContractParams:  7,
		APIContractTables:  8,
		APIContractFields:  9,
		APIBusinessObjects: 10,
		Relations:          11,
		SpecConfigs:        16,
		SpecCapabilities:   17,
		SpecRequirements:   18,
		SpecScenarios:      19,
		SpecUsecases:       20,
		SpecChanges:        21,
		SpecChangeDeltas:   22,
		SpecCodeMentions:   23,
		SpecVocabTerms:     24,
		SpecEmbeddings:     25,
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
	if response.Files.Total != 10 || response.Files.SQL != 2 || response.Files.H != 1 || response.Files.PAS != 3 || response.Files.MD != 14 || response.Files.YAML != 15 {
		t.Fatalf("unexpected files summary: %+v", response.Files)
	}
	if response.Entities.Procedures != 4 || response.Entities.Tables != 5 || response.Entities.APIContracts != 6 || response.Entities.Relations != 11 {
		t.Fatalf("unexpected entities summary: %+v", response.Entities)
	}
	if response.Entities.SpecConfigs != 16 || response.Entities.SpecCapabilities != 17 || response.Entities.SpecRequirements != 18 ||
		response.Entities.SpecScenarios != 19 || response.Entities.SpecUsecases != 20 || response.Entities.SpecChanges != 21 ||
		response.Entities.SpecChangeDeltas != 22 || response.Entities.SpecCodeMentions != 23 || response.Entities.SpecVocabTerms != 24 ||
		response.Entities.SpecEmbeddings != 25 {
		t.Fatalf("unexpected spec entities summary: %+v", response.Entities)
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
		{err: errs.ErrConfigNotLoaded, want: "config_error"},
		{err: fmt.Errorf("%w: %w", errs.ErrDBConnect, errors.New("connection refused")), want: "database_unavailable"},
		{err: fmt.Errorf("%w: %w", errs.ErrSchemaInit, errors.New("boom")), want: "schema_init_failed"},
		{err: fmt.Errorf("%w: %w", errs.ErrStatsFailed, errors.New("db error")), want: "stats_query_failed"},
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
		{err: errs.ErrConfigNotLoaded, want: "config_error"},
		{err: errors.New("failed to ping database: connection refused"), want: "database_unavailable"},
		{err: fmt.Errorf("%w: %w", errs.ErrSchemaInit, errors.New("boom")), want: "schema_init_failed"},
		{err: fmt.Errorf("%w: %w", errs.ErrHealthCheckFailed, errors.New("boom")), want: "health_check_failed"},
		{err: errors.New("other"), want: "internal_error"},
	}
	for _, tt := range tests {
		if got := classifyHealthError(tt.err); got != tt.want {
			t.Fatalf("classifyHealthError(%q) = %q, want %q", tt.err, got, tt.want)
		}
	}
}

package trc

import (
	"fmt"
	"testing"

	"github.com/codebase/internal/query"
)

// mockLookup — тестовая реализация ProcedureLookup.
type mockLookup struct {
	procs map[string]*query.SQLProcedureResult
}

func (m *mockLookup) GetProcedureResult(name string) (*query.SQLProcedureResult, error) {
	if proc, ok := m.procs[name]; ok {
		return proc, nil
	}
	return nil, fmt.Errorf("not found: %s", name)
}

// TestEnrichProcedure_Found — процедура найдена, enrichment заполнен.
func TestEnrichProcedure_Found(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"MyProc": {
				ProcName:  "MyProc",
				File:      "/path/to/MyProc.sql",
				LineStart: 10,
				LineEnd:   50,
			},
		},
	}
	enrich, err := EnrichProcedure(mock, "MyProc")
	if err != nil {
		t.Fatalf("EnrichProcedure: %v", err)
	}
	if !enrich.Found {
		t.Error("expected Found=true")
	}
	if enrich.SourceFile != "/path/to/MyProc.sql" {
		t.Errorf("SourceFile = %q, want /path/to/MyProc.sql", enrich.SourceFile)
	}
	if enrich.LineStart != 10 || enrich.LineEnd != 50 {
		t.Errorf("Lines = %d-%d, want 10-50", enrich.LineStart, enrich.LineEnd)
	}
}

// TestEnrichProcedure_NotFound — процедура не найдена, возвращается ошибка.
func TestEnrichProcedure_NotFound(t *testing.T) {
	mock := &mockLookup{procs: map[string]*query.SQLProcedureResult{}}
	_, err := EnrichProcedure(mock, "UnknownProc")
	if err == nil {
		t.Fatal("expected error for unknown procedure")
	}
}

// TestEnrichProcedure_HashSuffix — суффикс #... отбрасывается перед поиском.
func TestEnrichProcedure_HashSuffix(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"BaseProc": {ProcName: "BaseProc", File: "/path/BaseProc.sql"},
		},
	}
	enrich, err := EnrichProcedure(mock, "BaseProc#123")
	if err != nil {
		t.Fatalf("EnrichProcedure: %v", err)
	}
	if enrich.SourceFile != "/path/BaseProc.sql" {
		t.Errorf("SourceFile = %q, want /path/BaseProc.sql", enrich.SourceFile)
	}
	// В enrichment сохраняется исходное имя с суффиксом
	if enrich.Procedure != "BaseProc#123" {
		t.Errorf("Procedure = %q, want BaseProc#123", enrich.Procedure)
	}
}

// TestEnrichEvents_Dedup — одинаковые процедуры обогащаются один раз.
func TestEnrichEvents_Dedup(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"ProcA": {ProcName: "ProcA", File: "/a.sql"},
		},
	}
	events := []TRCEvent{
		{Procedure: "ProcA"},
		{Procedure: "ProcA"},
		{Procedure: "ProcA"},
		{Procedure: ""},
	}
	enrichMap := EnrichEvents(mock, events)
	if len(enrichMap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(enrichMap))
	}
	if e, ok := enrichMap["ProcA"]; !ok || !e.Found {
		t.Errorf("ProcA not enriched properly: %+v", e)
	}
}

// TestTrimHashSuffix — проверка отбрасывания суффикса #...
func TestTrimHashSuffix(t *testing.T) {
	tests := []struct {
		input, want string
	}{
		{"ProcName", "ProcName"},
		{"ProcName#123", "ProcName"},
		{"ProcName#abc#def", "ProcName"},
		{"#hash", ""},
		{"", ""},
	}
	for _, tt := range tests {
		got := trimHashSuffix(tt.input)
		if got != tt.want {
			t.Errorf("trimHashSuffix(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

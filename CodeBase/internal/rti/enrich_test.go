package rti

import (
	"context"
	"fmt"
	"testing"

	"github.com/codebase/internal/query"
)

// mockLookup реализует ProcedureLookup для тестов.
type mockLookup struct {
	procs map[string]*query.SQLProcedureResult
}

func (m *mockLookup) GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error) {
	if p, ok := m.procs[name]; ok {
		return p, nil
	}
	return nil, fmt.Errorf("sql: no rows in result set")
}

func TestEnrichProcedure_Found(t *testing.T) {
	q := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"Cons_Get_ProtocolNumber": {
				ProcName:  "Cons_Get_ProtocolNumber",
				File:      "Consumer/SQL/Cons_Get_ProtocolNumber.sql",
				LineStart: 1,
				LineEnd:   45,
				Params: []query.SQLParamResult{
					{Name: "InterfaceObjectID", Type: "DSIDENTIFIER", Direction: "IN"},
				},
			},
		},
	}

	enrich, err := EnrichProcedure(context.Background(), q, "Cons_Get_ProtocolNumber")
	if err != nil {
		t.Fatalf("EnrichProcedure returned error: %v", err)
	}
	if !enrich.Found {
		t.Errorf("Found = false, want true")
	}
	if enrich.Procedure != "Cons_Get_ProtocolNumber" {
		t.Errorf("Procedure = %q, want %q", enrich.Procedure, "Cons_Get_ProtocolNumber")
	}
	if enrich.SourceFile != "Consumer/SQL/Cons_Get_ProtocolNumber.sql" {
		t.Errorf("SourceFile = %q, want %q", enrich.SourceFile, "Consumer/SQL/Cons_Get_ProtocolNumber.sql")
	}
	if enrich.LineStart != 1 || enrich.LineEnd != 45 {
		t.Errorf("LineStart=%d LineEnd=%d, want 1/45", enrich.LineStart, enrich.LineEnd)
	}
	if len(enrich.Params) != 1 {
		t.Fatalf("Params len = %d, want 1", len(enrich.Params))
	}
	if enrich.Params[0].Name != "InterfaceObjectID" {
		t.Errorf("Param[0].Name = %q, want %q", enrich.Params[0].Name, "InterfaceObjectID")
	}
}

func TestEnrichProcedure_NotFound(t *testing.T) {
	q := &mockLookup{procs: map[string]*query.SQLProcedureResult{}}

	enrich, err := EnrichProcedure(context.Background(), q, "NonExistentProc")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if enrich != nil {
		t.Errorf("enrich = %+v, want nil", enrich)
	}
}

func TestEnrichCalls_FoundInIndex(t *testing.T) {
	q := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"ProcA": {
				ProcName:  "ProcA",
				File:      "some/path/ProcA.sql",
				LineStart: 10,
				LineEnd:   50,
			},
		},
	}

	calls := []*RTICall{
		{ID: 1, Procedure: "ProcA", NestLevel: 1},
	}

	enrichMap := EnrichCalls(context.Background(), q, calls)
	if len(enrichMap) != 1 {
		t.Fatalf("enrichMap len = %d, want 1", len(enrichMap))
	}
	e, ok := enrichMap["ProcA"]
	if !ok {
		t.Fatal("ProcA not in enrichMap")
	}
	if !e.Found {
		t.Errorf("Found = false, want true")
	}
	if e.SourceFile != "some/path/ProcA.sql" {
		t.Errorf("SourceFile = %q", e.SourceFile)
	}
}

func TestEnrichCalls_NotFound(t *testing.T) {
	q := &mockLookup{procs: map[string]*query.SQLProcedureResult{}}

	calls := []*RTICall{
		{ID: 1, Procedure: "UnknownProc", NestLevel: 1},
	}

	enrichMap := EnrichCalls(context.Background(), q, calls)
	e, ok := enrichMap["UnknownProc"]
	if !ok {
		t.Fatal("UnknownProc not in enrichMap")
	}
	if e.Found {
		t.Errorf("Found = true, want false")
	}
	if e.SourceFile != "(not found)" {
		t.Errorf("SourceFile = %q, want %q", e.SourceFile, "(not found)")
	}
}

// countingLookup оборачивает mockLookup и считает число обращений по имени процедуры.
type countingLookup struct {
	inner  *mockLookup
	counts map[string]int
}

func (c *countingLookup) GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error) {
	c.counts[name]++
	return c.inner.GetProcedureResult(ctx, name)
}

func TestEnrichCalls_Deduplication(t *testing.T) {
	q := &countingLookup{
		inner: &mockLookup{
			procs: map[string]*query.SQLProcedureResult{
				"ProcA": {ProcName: "ProcA", File: "ProcA.sql"},
			},
		},
		counts: make(map[string]int),
	}

	// Три вызова ProcA + один ProcB — lookup ProcA должен быть ровно один раз
	calls := []*RTICall{
		{ID: 1, Procedure: "ProcA", NestLevel: 1},
		{ID: 2, Procedure: "ProcA", NestLevel: 2},
		{ID: 3, Procedure: "ProcA", NestLevel: 3},
		{ID: 4, Procedure: "ProcB", NestLevel: 1},
	}

	enrichMap := EnrichCalls(context.Background(), q, calls)

	if len(enrichMap) != 2 {
		t.Fatalf("enrichMap len = %d, want 2", len(enrichMap))
	}
	if !enrichMap["ProcA"].Found {
		t.Error("ProcA: Found = false, want true")
	}
	if enrichMap["ProcB"].Found {
		t.Error("ProcB: Found = true, want false")
	}
	if q.counts["ProcA"] != 1 {
		t.Errorf("GetProcedureResult(ProcA) called %d times, want 1", q.counts["ProcA"])
	}
	if q.counts["ProcB"] != 1 {
		t.Errorf("GetProcedureResult(ProcB) called %d times, want 1", q.counts["ProcB"])
	}
}

func TestEnrichCalls_WithParams(t *testing.T) {
	q := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"MyProc": {
				ProcName:  "MyProc",
				File:      "MyProc.sql",
				LineStart: 1,
				LineEnd:   100,
				Params: []query.SQLParamResult{
					{Name: "Param1", Type: "INT", Direction: "IN"},
					{Name: "Param2", Type: "VARCHAR", Direction: "OUT"},
				},
			},
		},
	}

	enrich, err := EnrichProcedure(context.Background(), q, "MyProc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(enrich.Params) != 2 {
		t.Fatalf("Params len = %d, want 2", len(enrich.Params))
	}
	if enrich.Params[0].Direction != "IN" {
		t.Errorf("Params[0].Direction = %q, want IN", enrich.Params[0].Direction)
	}
	if enrich.Params[1].Direction != "OUT" {
		t.Errorf("Params[1].Direction = %q, want OUT", enrich.Params[1].Direction)
	}
}

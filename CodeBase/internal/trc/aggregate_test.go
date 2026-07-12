package trc

import "testing"

// TestAggregateByProcedure_Basic — проверка count/min/max/avg/total.
func TestAggregateByProcedure_Basic(t *testing.T) {
	events := []TRCEvent{
		{Procedure: "ProcA", DurationMs: 100},
		{Procedure: "ProcA", DurationMs: 200},
		{Procedure: "ProcA", DurationMs: 300},
		{Procedure: "ProcB", DurationMs: 50},
		{Procedure: "ProcB", DurationMs: 150},
		{Procedure: "NoProc", DurationMs: 999}, // empty Procedure — ignored
	}
	// Last event has no Procedure
	events[5].Procedure = ""

	aggs := AggregateByProcedure(events)
	if len(aggs) != 2 {
		t.Fatalf("expected 2 procedures, got %d", len(aggs))
	}

	// Sorted by TotalMs desc: ProcA (600) > ProcB (200)
	if aggs[0].Procedure != "ProcA" {
		t.Errorf("first = %q, want ProcA", aggs[0].Procedure)
	}
	if aggs[0].Count != 3 {
		t.Errorf("ProcA count = %d, want 3", aggs[0].Count)
	}
	if aggs[0].TotalMs != 600 {
		t.Errorf("ProcA total = %d, want 600", aggs[0].TotalMs)
	}
	if aggs[0].MinMs != 100 {
		t.Errorf("ProcA min = %d, want 100", aggs[0].MinMs)
	}
	if aggs[0].MaxMs != 300 {
		t.Errorf("ProcA max = %d, want 300", aggs[0].MaxMs)
	}
	if aggs[0].AvgMs != 200 {
		t.Errorf("ProcA avg = %.1f, want 200", aggs[0].AvgMs)
	}

	if aggs[1].Procedure != "ProcB" {
		t.Errorf("second = %q, want ProcB", aggs[1].Procedure)
	}
	if aggs[1].Count != 2 || aggs[1].TotalMs != 200 {
		t.Errorf("ProcB: count=%d total=%d, want 2/200", aggs[1].Count, aggs[1].TotalMs)
	}
}

// TestAggregateByProcedure_NoDuration — события без Duration дают count, но
// min/max/avg/total остаются нулевыми.
func TestAggregateByProcedure_NoDuration(t *testing.T) {
	events := []TRCEvent{
		{Procedure: "ProcX"},
		{Procedure: "ProcX"},
	}
	aggs := AggregateByProcedure(events)
	if len(aggs) != 1 {
		t.Fatalf("expected 1 procedure, got %d", len(aggs))
	}
	if aggs[0].Count != 2 {
		t.Errorf("count = %d, want 2", aggs[0].Count)
	}
	if aggs[0].TotalMs != 0 || aggs[0].MinMs != 0 || aggs[0].MaxMs != 0 {
		t.Errorf("expected zero durations, got total=%d min=%d max=%d", aggs[0].TotalMs, aggs[0].MinMs, aggs[0].MaxMs)
	}
}

// TestAggregateByProcedure_Empty — пустой вход → пустой результат.
func TestAggregateByProcedure_Empty(t *testing.T) {
	aggs := AggregateByProcedure(nil)
	if len(aggs) != 0 {
		t.Fatalf("expected 0, got %d", len(aggs))
	}
}

// TestEnrichAggregates — проверка заполнения SourceFile из enrichMap.
func TestEnrichAggregates(t *testing.T) {
	aggs := []TRCProcAgg{
		{Procedure: "ProcA"},
		{Procedure: "ProcB"},
		{Procedure: "ProcC"},
	}
	enrichMap := map[string]*ProcedureEnrichment{
		"ProcA": {Found: true, SourceFile: "/path/to/procA.sql"},
		"ProcB": {Found: false, SourceFile: "(not found)"},
		// ProcC — нет в карте
	}
	EnrichAggregates(aggs, enrichMap)
	if aggs[0].SourceFile != "/path/to/procA.sql" {
		t.Errorf("ProcA source = %q, want /path/to/procA.sql", aggs[0].SourceFile)
	}
	if aggs[1].SourceFile != "" {
		t.Errorf("ProcB source = %q, want empty (not found)", aggs[1].SourceFile)
	}
	if aggs[2].SourceFile != "" {
		t.Errorf("ProcC source = %q, want empty (not in map)", aggs[2].SourceFile)
	}
}

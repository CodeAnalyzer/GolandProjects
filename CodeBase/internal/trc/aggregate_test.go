package trc

import (
	"reflect"
	"testing"
)

func opt() AggregateOptions { return AggregateOptions{} }

// TestAggregateByProcedure_Basic — проверка count/min/max/avg/total.
func TestAggregateByProcedure_Basic(t *testing.T) {
	events := []TRCEvent{
		{EventName: "SP:Completed", Procedure: "ProcA", DurationMs: 100},
		{EventName: "SP:Completed", Procedure: "ProcA", DurationMs: 200},
		{EventName: "SP:Completed", Procedure: "ProcA", DurationMs: 300},
		{EventName: "SP:Completed", Procedure: "ProcB", DurationMs: 50},
		{EventName: "SP:Completed", Procedure: "ProcB", DurationMs: 150},
		{EventName: "SP:Completed", Procedure: "NoProc", DurationMs: 999}, // empty Procedure — ignored
	}
	// Last event has no Procedure
	events[5].Procedure = ""

	aggs := AggregateByProcedure(events, opt())
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
		{EventName: "SP:Completed", Procedure: "ProcX"},
		{EventName: "SP:Completed", Procedure: "ProcX"},
	}
	aggs := AggregateByProcedure(events, opt())
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

func TestAggregateByProcedure_CompletedOnly(t *testing.T) {
	events := []TRCEvent{
		{EventName: "SP:Completed", Procedure: "Proc", DurationMs: 100},
		{EventName: "SP:StmtCompleted", Procedure: "Proc", DurationMs: 100},
		{EventName: "SQL:StmtCompleted", Procedure: "Proc", DurationMs: 100},
	}
	aggs := AggregateByProcedure(events, opt())
	if len(aggs) != 1 || aggs[0].Count != 1 || aggs[0].TotalMs != 100 {
		t.Fatalf("got %+v, want one completed event with total 100", aggs)
	}
}

func TestAggregateByProcedure_IncludesZeroDuration(t *testing.T) {
	aggs := AggregateByProcedure([]TRCEvent{
		{EventName: "SP:Completed", Procedure: "Proc", DurationMs: 0},
		{EventName: "SP:Completed", Procedure: "Proc", DurationMs: 100},
	}, opt())
	if len(aggs) != 1 || aggs[0].Count != 2 || aggs[0].TotalMs != 100 || aggs[0].MinMs != 0 || aggs[0].MaxMs != 100 || aggs[0].AvgMs != 50 {
		t.Fatalf("got %+v, want count=2 total=100 min=0 max=100 avg=50", aggs)
	}
}

// TestAggregateByProcedure_Empty — пустой вход → пустой результат.
func TestAggregateByProcedure_Empty(t *testing.T) {
	aggs := AggregateByProcedure(nil, opt())
	if len(aggs) != 0 {
		t.Fatalf("expected 0, got %d", len(aggs))
	}
}

func spidEvent(name, proc string, duration int64, spid int32) TRCEvent {
	return TRCEvent{
		EventName:  name,
		Procedure:  proc,
		DurationMs: duration,
		Columns:    map[int]any{12: spid},
	}
}

// TestAggregateByProcedure_EventNames — явный набор событий вместо default
// SP:Completed.
func TestAggregateByProcedure_EventNames(t *testing.T) {
	events := []TRCEvent{
		spidEvent("SP:Completed", "Proc", 100, 7),
		spidEvent("RPC:Completed", "Proc", 40, 7),
		spidEvent("SP:StmtCompleted", "Proc", 10, 7),
	}
	aggs := AggregateByProcedure(events, AggregateOptions{EventNames: []string{"RPC:Completed"}})
	if len(aggs) != 1 || aggs[0].Count != 1 || aggs[0].TotalMs != 40 {
		t.Fatalf("got %+v, want single RPC:Completed with total 40", aggs)
	}

	both := AggregateByProcedure(events, AggregateOptions{EventNames: []string{"SP:Completed", "RPC:Completed"}})
	if len(both) != 1 || both[0].Count != 2 || both[0].TotalMs != 140 {
		t.Fatalf("got %+v, want count=2 total=140", both)
	}
}

// TestAggregateByProcedure_SPIDsFilter — агрегация только по указанным SPID;
// события без SPID не проходят фильтр.
func TestAggregateByProcedure_SPIDsFilter(t *testing.T) {
	events := []TRCEvent{
		spidEvent("SP:Completed", "Proc", 100, 728),
		spidEvent("SP:Completed", "Proc", 10, 700),
		{EventName: "SP:Completed", Procedure: "Proc", DurationMs: 5}, // без SPID
	}
	aggs := AggregateByProcedure(events, AggregateOptions{SPIDs: []int{728}})
	if len(aggs) != 1 || aggs[0].Count != 1 || aggs[0].TotalMs != 100 {
		t.Fatalf("got %+v, want only SPID 728 event", aggs)
	}

	both := AggregateByProcedure(events, AggregateOptions{SPIDs: []int{728, 700}})
	if len(both) != 1 || both[0].Count != 2 || both[0].TotalMs != 110 {
		t.Fatalf("got %+v, want events of both SPIDs", both)
	}
}

// TestAggregateByProcedure_GroupBySPID — группы (spid, procedure) с полем
// SPID; события без SPID исключаются при группировке, но участвуют без неё.
func TestAggregateByProcedure_GroupBySPID(t *testing.T) {
	noSPID := TRCEvent{EventName: "SP:Completed", Procedure: "Proc", DurationMs: 5}
	events := []TRCEvent{
		spidEvent("SP:Completed", "Proc", 100, 728),
		spidEvent("SP:Completed", "Proc", 200, 728),
		spidEvent("SP:Completed", "Proc", 10, 700),
		noSPID,
	}

	aggs := AggregateByProcedure(events, AggregateOptions{GroupBySPID: true})
	if len(aggs) != 2 {
		t.Fatalf("expected 2 (spid, procedure) groups, got %d: %+v", len(aggs), aggs)
	}
	// total_ms desc: (728, Proc)=300 > (700, Proc)=10
	if aggs[0].SPID != 728 || aggs[0].Count != 2 || aggs[0].TotalMs != 300 {
		t.Errorf("first group = %+v, want SPID 728 count 2 total 300", aggs[0])
	}
	if aggs[1].SPID != 700 || aggs[1].Count != 1 || aggs[1].TotalMs != 10 {
		t.Errorf("second group = %+v, want SPID 700 count 1 total 10", aggs[1])
	}

	// без группировки событие без SPID участвует, поле SPID не заполнено
	flat := AggregateByProcedure(events, opt())
	if len(flat) != 1 || flat[0].Count != 4 || flat[0].TotalMs != 315 || flat[0].SPID != 0 {
		t.Fatalf("got %+v, want single group count=4 total=315 without SPID", flat)
	}
}

// TestAggregateByProcedure_TopAndSort — top после агрегации и метрики sort_by
// с детерминированными tie-breakers (procedure, затем spid).
func TestAggregateByProcedure_TopAndSort(t *testing.T) {
	events := []TRCEvent{
		spidEvent("SP:Completed", "ProcA", 100, 1),
		spidEvent("SP:Completed", "ProcA", 300, 1),
		spidEvent("SP:Completed", "ProcB", 200, 1),
		spidEvent("SP:Completed", "ProcC", 150, 1),
		spidEvent("SP:Completed", "ProcD", 100, 1),
	}

	// top=2 по total_ms: ProcA(400), ProcB(200)
	top2 := AggregateByProcedure(events, AggregateOptions{Top: 2})
	if len(top2) != 2 || top2[0].Procedure != "ProcA" || top2[1].Procedure != "ProcB" {
		t.Fatalf("got %+v, want ProcA then ProcB", top2)
	}

	// top=0 (default) — все процедуры
	all := AggregateByProcedure(events, opt())
	if len(all) != 4 {
		t.Fatalf("expected all 4 procedures, got %d", len(all))
	}

	// sort_by=avg_ms: ProcA(200), ProcB(200) — tie по метрике, break по имени
	avg := AggregateByProcedure(events, AggregateOptions{SortBy: "avg_ms"})
	if avg[0].Procedure != "ProcA" || avg[1].Procedure != "ProcB" {
		t.Fatalf("avg_ms sort = %+v, want ProcA, ProcB (tie by name)", avg)
	}

	// sort_by=count
	cnt := AggregateByProcedure(events, AggregateOptions{SortBy: "count"})
	if cnt[0].Procedure != "ProcA" || cnt[0].Count != 2 {
		t.Fatalf("count sort = %+v, want ProcA first with count 2", cnt)
	}
}

// TestAggregateByProcedure_TieBreakersGroupedBySPID — при равных метрике и
// имени порядок определяется spid по возрастанию.
func TestAggregateByProcedure_TieBreakersGroupedBySPID(t *testing.T) {
	events := []TRCEvent{
		spidEvent("SP:Completed", "Proc", 100, 900),
		spidEvent("SP:Completed", "Proc", 100, 100),
	}
	aggs := AggregateByProcedure(events, AggregateOptions{GroupBySPID: true})
	if len(aggs) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(aggs))
	}
	if aggs[0].SPID != 100 || aggs[1].SPID != 900 {
		t.Fatalf("got spids [%d, %d], want [100, 900] (spid asc tie-break)", aggs[0].SPID, aggs[1].SPID)
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

// TestAggregateEventNames — default и явный набор имён событий.
func TestAggregateEventNames(t *testing.T) {
	if got := (AggregateOptions{}).AggregateEventNames(); !reflect.DeepEqual(got, []string{"SP:Completed"}) {
		t.Errorf("default = %v, want [SP:Completed]", got)
	}
	if got := (AggregateOptions{EventNames: []string{"RPC:Completed"}}).AggregateEventNames(); !reflect.DeepEqual(got, []string{"RPC:Completed"}) {
		t.Errorf("explicit = %v, want [RPC:Completed]", got)
	}
}

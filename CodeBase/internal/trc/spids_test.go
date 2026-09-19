package trc

import (
	"testing"
	"time"
)

func spidsEvent(spid int32, name string, dur int64, cols map[int]any) TRCEvent {
	base := map[int]any{12: spid}
	for k, v := range cols {
		base[k] = v
	}
	return TRCEvent{EventName: name, DurationMs: dur, Columns: base}
}

func sysTime(t *testing.T, ts time.Time) SystemTime {
	t.Helper()
	return SystemTimeFromLocalParts(ts)
}

func TestSummarizeSPIDs_Counters(t *testing.T) {
	base := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	events := []TRCEvent{
		spidsEvent(728, "SP:Completed", 100, map[int]any{14: sysTime(t, base), 31: int32(0)}),
		spidsEvent(728, "SP:Completed", 0, map[int]any{14: sysTime(t, base.Add(time.Minute))}),
		spidsEvent(728, "RPC:Completed", 50, map[int]any{14: sysTime(t, base.Add(2*time.Minute)), 31: int32(50000)}),
		spidsEvent(728, "SQL:BatchCompleted", 10, nil),
		spidsEvent(728, "SP:StmtCompleted", 70, nil),
		spidsEvent(700, "SP:Completed", 5, nil),
	}
	res := SummarizeSPIDs(events, SpidsOptions{})
	if len(res) != 2 {
		t.Fatalf("spids = %d, want 2", len(res))
	}
	s := res[0]
	if s.SPID != 728 || s.EventCount != 5 {
		t.Fatalf("summary = %+v, want SPID 728 with 5 events", s)
	}
	if s.SPCompletedCount != 2 || s.RPCCompletedCount != 1 || s.BatchCompletedCount != 1 {
		t.Fatalf("completed counters = %d/%d/%d, want 2/1/1", s.SPCompletedCount, s.RPCCompletedCount, s.BatchCompletedCount)
	}
	if s.ErrorCount != 1 {
		t.Fatalf("error_count = %d, want 1", s.ErrorCount)
	}
	if s.MaxDurationMs != 100 {
		t.Fatalf("max_duration_ms = %d, want 100", s.MaxDurationMs)
	}
	if s.FirstTime == nil || !s.FirstTime.Equal(base) || s.LastTime == nil || !s.LastTime.Equal(base.Add(2*time.Minute)) {
		t.Fatalf("first/last = %v/%v", s.FirstTime, s.LastTime)
	}
}

// TestSummarizeSPIDs_IdentityModeTieBreak — равная частота решается самым
// ранним событием.
func TestSummarizeSPIDs_IdentityModeTieBreak(t *testing.T) {
	events := []TRCEvent{
		spidsEvent(728, "SP:Completed", 1, map[int]any{10: "AppA"}),
		spidsEvent(728, "SP:Completed", 1, map[int]any{10: "AppB"}),
		spidsEvent(728, "SP:Completed", 1, map[int]any{10: "AppB"}),
		spidsEvent(728, "SP:Completed", 1, map[int]any{10: "AppA"}),
		spidsEvent(728, "SP:Completed", 1, nil), // пустое — не считается
	}
	res := SummarizeSPIDs(events, SpidsOptions{})
	if res[0].ApplicationName != "AppA" {
		t.Fatalf("application_name = %q, want AppA (tie by first occurrence)", res[0].ApplicationName)
	}
	if res[0].LoginName != "" || res[0].HostName != "" {
		t.Fatalf("empty identity must be omitted: %+v", res[0])
	}
}

func TestSummarizeSPIDs_NullTime(t *testing.T) {
	events := []TRCEvent{
		spidsEvent(728, "SP:Completed", 1, nil),
		spidsEvent(728, "RPC:Completed", 2, nil),
	}
	res := SummarizeSPIDs(events, SpidsOptions{})
	if res[0].FirstTime != nil || res[0].LastTime != nil {
		t.Fatalf("first/last = %v/%v, want nil", res[0].FirstTime, res[0].LastTime)
	}
	if res[0].EventCount != 2 || res[0].RPCCompletedCount != 1 {
		t.Fatalf("counters = %+v", res[0])
	}
}

// TestSummarizeSPIDs_TimeFilter — отфильтрованная сводка исключает события
// без start_time и вне полуинтервала.
func TestSummarizeSPIDs_TimeFilter(t *testing.T) {
	base := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	from := base.Add(30 * time.Minute)
	to := base.Add(90 * time.Minute)
	events := []TRCEvent{
		spidsEvent(728, "SP:Completed", 1, map[int]any{14: sysTime(t, base)}),                   // до from
		spidsEvent(728, "SP:Completed", 2, map[int]any{14: sysTime(t, base.Add(time.Hour))}),    // внутри
		spidsEvent(728, "SP:Completed", 4, map[int]any{14: sysTime(t, base.Add(2*time.Hour))}),  // после to
		spidsEvent(728, "SP:Completed", 8, nil),                                            // без времени
	}
	res := SummarizeSPIDs(events, SpidsOptions{TimeFrom: &from, TimeTo: &to})
	if len(res) != 1 || res[0].EventCount != 1 || res[0].MaxDurationMs != 2 {
		t.Fatalf("filtered summary = %+v, want single event dur=2", res)
	}
}

func TestSummarizeSPIDs_SortAndFilter(t *testing.T) {
	events := []TRCEvent{
		spidsEvent(700, "SP:Completed", 1, nil),
		spidsEvent(728, "SP:Completed", 1, map[int]any{31: int32(1)}),
		spidsEvent(728, "SP:Completed", 1, map[int]any{31: int32(2)}),
		spidsEvent(741, "SP:Completed", 1, nil),
	}
	// sort_by=error_count: 728 (2 ошибки) первым
	res := SummarizeSPIDs(events, SpidsOptions{SortBy: "error_count"})
	if res[0].SPID != 728 || res[0].ErrorCount != 2 {
		t.Fatalf("error_count sort = %+v", res[0])
	}
	// фильтр подмножества
	res = SummarizeSPIDs(events, SpidsOptions{SPIDs: []int{700, 741}})
	if len(res) != 2 || res[0].SPID == 728 {
		t.Fatalf("spids filter = %+v", res)
	}
	// default sort = event_count desc, tie по spid asc
	if res[0].SPID != 700 || res[1].SPID != 741 {
		t.Fatalf("tie-break = %d %d, want 700 741", res[0].SPID, res[1].SPID)
	}
}

func TestMaxDurationMsOfEvents(t *testing.T) {
	if got := MaxDurationMsOfEvents(nil); got != 0 {
		t.Fatalf("nil = %d, want 0", got)
	}
	if got := MaxDurationMsOfEvents([]TRCEvent{{DurationMs: 0}, {DurationMs: 0}}); got != 0 {
		t.Fatalf("zeros = %d, want 0", got)
	}
	if got := MaxDurationMsOfEvents([]TRCEvent{{DurationMs: 5}, {DurationMs: 50}, {DurationMs: 7}}); got != 50 {
		t.Fatalf("max = %d, want 50", got)
	}
}

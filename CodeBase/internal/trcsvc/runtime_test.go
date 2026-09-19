package trcsvc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/query"
	"github.com/codebase/internal/trc"
)

type fakeProcedureLookup struct {
	result *query.SQLProcedureResult
	calls  int
}

func (f *fakeProcedureLookup) GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error) {
	f.calls++
	if f.result == nil {
		return nil, os.ErrNotExist
	}
	return f.result, nil
}

type statelessProcedureLookup struct{}

func (statelessProcedureLookup) GetProcedureResult(ctx context.Context, name string) (*query.SQLProcedureResult, error) {
	return &query.SQLProcedureResult{File: "/" + name + ".sql"}, nil
}

// modificationsDir — папка с тестовыми файлами (.trc/.xml), относительно
// пакета internal/trcsvc.
func modificationsDir(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "Modifications")
}

// skipIfMissing проверяет существование файла и пропускает тест, если файл
// отсутствует.
func skipIfMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Skipf("skip: file not found: %s", path)
	}
}

// trcTestPath возвращает путь к DIAPR-391.trc или пропускает тест.
func trcTestPath(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	p := filepath.Join(modificationsDir(t), "DIAPR-391.trc")
	skipIfMissing(t, p)
	return p
}

func TestEnrichProcedureAggregates(t *testing.T) {
	lookup := &fakeProcedureLookup{result: &query.SQLProcedureResult{File: "/proc.sql"}}
	aggs := []trc.TRCProcAgg{{Procedure: "ProcAfterSample"}}
	enrichProcedureAggregates(context.Background(), lookup, aggs)
	if lookup.calls != 1 || aggs[0].SourceFile != "/proc.sql" {
		t.Fatalf("calls=%d aggs=%+v", lookup.calls, aggs)
	}
}

func TestEnrichProcedureAggregates_AllAggregates(t *testing.T) {
	aggs := make([]trc.TRCProcAgg, 1001)
	for i := range aggs {
		aggs[i].Procedure = fmt.Sprintf("Proc%04d", i)
	}
	enrichProcedureAggregates(context.Background(), statelessProcedureLookup{}, aggs)
	if aggs[0].SourceFile != "/Proc0000.sql" {
		t.Errorf("first SourceFile = %q, want /Proc0000.sql", aggs[0].SourceFile)
	}
	if aggs[1000].SourceFile != "/Proc1000.sql" {
		t.Errorf("last SourceFile = %q, want /Proc1000.sql", aggs[1000].SourceFile)
	}
}

func TestExecuteParse_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteParse(ctx, nil, p)
	if err != nil {
		t.Fatalf("ExecuteParse error: %v", err)
	}
	if result.SessionID != 0 {
		t.Errorf("SessionID = %d, want 0 (no DB)", result.SessionID)
	}
	if result.TotalEvents <= 0 {
		t.Errorf("TotalEvents = %d, want > 0", result.TotalEvents)
	}
	if result.Warning == "" {
		t.Errorf("Warning should be set when db is nil")
	}
}

func TestExecuteParse_EmptyPath(t *testing.T) {
	_, err := ExecuteParse(context.Background(), nil, "")
	if err == nil {
		t.Fatal("expected error for empty path, got nil")
	}
}

func TestExecuteSummary_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteSummary(ctx, nil, SessionSource{FilePath: p})
	if err != nil {
		t.Fatalf("ExecuteSummary error: %v", err)
	}
	if result.TotalEvents <= 0 {
		t.Errorf("TotalEvents = %d, want > 0", result.TotalEvents)
	}
	if result.Header == nil {
		t.Error("expected non-nil Header")
	}
}

func TestExecuteSummary_NoSource(t *testing.T) {
	_, err := ExecuteSummary(context.Background(), nil, SessionSource{})
	if err == nil {
		t.Fatal("expected error for empty source, got nil")
	}
}

func TestExecuteEvents_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteEvents(ctx, nil, EventsParams{
		Source: SessionSource{FilePath: p},
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("ExecuteEvents error: %v", err)
	}
	if result.Limit != 10 {
		t.Errorf("Limit = %d, want 10", result.Limit)
	}
	if result.TotalCount == nil || *result.TotalCount <= 0 {
		t.Errorf("TotalCount = %v, want > 0", result.TotalCount)
	}
	if result.ReturnedCount != 10 {
		t.Errorf("ReturnedCount = %d, want 10", result.ReturnedCount)
	}
	if result.ReturnedCount != len(result.Events) {
		t.Errorf("ReturnedCount = %d, want %d", result.ReturnedCount, len(result.Events))
	}
	if result.FilteredCount != *result.TotalCount {
		t.Errorf("FilteredCount = %d, want TotalCount %d", result.FilteredCount, *result.TotalCount)
	}
	if result.FilteredCount <= result.ReturnedCount {
		t.Errorf("FilteredCount = %d, want > ReturnedCount %d", result.FilteredCount, result.ReturnedCount)
	}
	// события страницы несут логический id (EventIndex+1)
	for i, ev := range result.Events {
		if ev.StoreID <= 0 {
			t.Fatalf("event %d StoreID = %d, want > 0", i, ev.StoreID)
		}
	}
	if !result.HasMore {
		t.Error("HasMore = false, want true (filtered > limit)")
	}
	if result.NextAfterID != result.Events[len(result.Events)-1].StoreID {
		t.Errorf("NextAfterID = %d, want StoreID of last event %d", result.NextAfterID, result.Events[len(result.Events)-1].StoreID)
	}
}

func TestExecuteEvents_FileMode_WithEventNameFilter(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteEvents(ctx, nil, EventsParams{
		Source:     SessionSource{FilePath: p},
		EventNames: []string{"RPC:Completed"},
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("ExecuteEvents error: %v", err)
	}
	for _, ev := range result.Events {
		if ev.EventName != "RPC:Completed" {
			t.Errorf("EventName = %q, want RPC:Completed", ev.EventName)
		}
	}
	if result.ReturnedCount != len(result.Events) {
		t.Errorf("ReturnedCount = %d, want %d", result.ReturnedCount, len(result.Events))
	}
	if result.FilteredCount < result.ReturnedCount {
		t.Errorf("FilteredCount = %d, want >= ReturnedCount %d", result.FilteredCount, result.ReturnedCount)
	}
	if result.ReturnedCount < result.Limit && result.FilteredCount != result.ReturnedCount {
		t.Errorf("FilteredCount = %d, want ReturnedCount %d when limit not reached", result.FilteredCount, result.ReturnedCount)
	}
}

func TestExecuteEvents_FileMode_EmptyResult(t *testing.T) {
	p := trcTestPath(t)
	result, err := ExecuteEvents(context.Background(), nil, EventsParams{
		Source:     SessionSource{FilePath: p},
		EventNames: []string{"EventNameThatDoesNotExistInGolden"},
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("ExecuteEvents error: %v", err)
	}
	if result.TotalCount == nil || *result.TotalCount <= 0 {
		t.Fatalf("TotalCount = %v, want > 0", result.TotalCount)
	}
	if result.FilteredCount != 0 || result.ReturnedCount != 0 || len(result.Events) != 0 {
		t.Fatalf("got filtered=%d returned=%d len=%d, want zeroes", result.FilteredCount, result.ReturnedCount, len(result.Events))
	}
	if result.HasMore || result.NextAfterID != 0 {
		t.Fatalf("got has_more=%v next=%d, want false/0", result.HasMore, result.NextAfterID)
	}
}

// TestExecuteEvents_FileMode_Pagination — последовательное чтение по курсору
// EventIndex+1: страницы не пересекаются, не теряют события, filtered_count
// стабилен, total_count отсутствует на страницах продолжения.
func TestExecuteEvents_FileMode_Pagination(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()

	first, err := ExecuteEvents(ctx, nil, EventsParams{Source: SessionSource{FilePath: p}, Limit: 5})
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if first.TotalCount == nil {
		t.Fatal("first page: TotalCount must be set")
	}
	if !first.HasMore {
		t.Fatal("first page: HasMore expected")
	}

	seen := make(map[int64]bool)
	for _, ev := range first.Events {
		if seen[ev.StoreID] {
			t.Fatalf("duplicate id %d on first page", ev.StoreID)
		}
		seen[ev.StoreID] = true
	}

	pages := 1
	cursor := first.NextAfterID
	filtered := first.FilteredCount
	for {
		page, err := ExecuteEvents(ctx, nil, EventsParams{
			Source:  SessionSource{FilePath: p},
			AfterID: &cursor,
			Limit:   5,
		})
		if err != nil {
			t.Fatalf("page %d: %v", pages+1, err)
		}
		pages++
		if page.TotalCount != nil {
			t.Fatal("continuation page must not contain total_count")
		}
		if page.FilteredCount != filtered {
			t.Fatalf("filtered_count changed between pages: %d != %d", page.FilteredCount, filtered)
		}
		for _, ev := range page.Events {
			if seen[ev.StoreID] {
				t.Fatalf("duplicate id %d across pages", ev.StoreID)
			}
			seen[ev.StoreID] = true
		}
		if !page.HasMore {
			if page.NextAfterID != 0 {
				t.Fatalf("last page next_after_id = %d, want 0", page.NextAfterID)
			}
			break
		}
		cursor = page.NextAfterID
		if pages > 1000 {
			t.Fatal("pagination does not terminate")
		}
	}
	if len(seen) != filtered {
		t.Fatalf("collected %d unique events, want filtered_count %d", len(seen), filtered)
	}
}

// TestExecuteEvents_FileMode_ShortFormat — short-формат возвращает те же ID,
// порядок и число событий, что и full, но без params/columns.
func TestExecuteEvents_FileMode_ShortFormat(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	full, err := ExecuteEvents(ctx, nil, EventsParams{Source: SessionSource{FilePath: p}, Limit: 7})
	if err != nil {
		t.Fatalf("full: %v", err)
	}
	short, err := ExecuteEvents(ctx, nil, EventsParams{Source: SessionSource{FilePath: p}, Limit: 7, Format: "short"})
	if err != nil {
		t.Fatalf("short: %v", err)
	}
	if len(short.Views) != len(full.Events) {
		t.Fatalf("short len = %d, want full len %d", len(short.Views), len(full.Events))
	}
	if !short.Short || short.Events != nil {
		t.Fatal("short result must be marked Short without full events")
	}
	for i := range full.Events {
		if short.Views[i].ID != full.Events[i].StoreID {
			t.Errorf("view %d ID = %d, want %d", i, short.Views[i].ID, full.Events[i].StoreID)
		}
		if short.Views[i].EventName != full.Events[i].EventName {
			t.Errorf("view %d EventName = %q, want %q", i, short.Views[i].EventName, full.Events[i].EventName)
		}
		if short.Views[i].DurationMs != full.Events[i].DurationMs {
			t.Errorf("view %d DurationMs = %d, want %d", i, short.Views[i].DurationMs, full.Events[i].DurationMs)
		}
	}
}

// TestExecuteEvents_FileMode_Filters — множественные SPID, имена событий,
// временной полуинтервал и минимальная длительность в file-mode.
func TestExecuteEvents_FileMode_Filters(t *testing.T) {
	// Синтетический набор: фильтры применяются функцией eventMatchesFilter,
	// используемой ExecuteEvents для file-mode.
	base := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	mk := func(minute int) (start, end time.Time) {
		start = base.Add(time.Duration(minute) * time.Minute)
		end = start.Add(30 * time.Second)
		return
	}
	s1, e1 := mk(0)
	s2, e2 := mk(15)
	s3, e3 := mk(45)

	events := []trc.TRCEvent{
		{EventName: "SP:Completed", Procedure: "ProcA", DurationMs: 100, Columns: map[int]any{12: int32(728), 14: toSystemTime(t, s1), 15: toSystemTime(t, e1)}},
		{EventName: "RPC:Completed", Procedure: "ProcB", DurationMs: 10, Columns: map[int]any{12: int32(700), 14: toSystemTime(t, s2), 15: toSystemTime(t, e2)}},
		{EventName: "SP:Completed", Procedure: "ProcC", DurationMs: 900, Columns: map[int]any{12: int32(728), 14: toSystemTime(t, s3), 15: toSystemTime(t, e3)}},
		{EventName: "SP:Completed", Procedure: "ProcD", DurationMs: 5}, // без SPID и времени
	}

	from := base.Add(10 * time.Minute)
	to := base.Add(60 * time.Minute)
	minDur := int64(50)

	f := trc.TRCEventFilter{
		SPIDs:         []int{728},
		EventNames:    []string{"SP:Completed", "RPC:Completed"},
		TimeFrom:      &from,
		TimeTo:        &to,
		MinDurationMs: &minDur,
	}
	var matched []trc.TRCEvent
	for _, ev := range events {
		if eventMatchesFilter(ev, f) {
			matched = append(matched, ev)
		}
	}
	// Проходит только третье событие: SPID 728, SP:Completed, start в [from;to), 900 >= 50
	if len(matched) != 1 || matched[0].Procedure != "ProcC" {
		t.Fatalf("got %+v, want only ProcC", matched)
	}

	// NULL start_time не попадает в заданный временной диапазон
	timeOnly := trc.TRCEventFilter{TimeFrom: &from, TimeTo: &to}
	if eventMatchesFilter(events[3], timeOnly) {
		t.Fatal("event without start_time must not match time range")
	}
	if eventMatchesFilter(events[3], trc.TRCEventFilter{SPIDs: []int{728}}) {
		t.Fatal("event without spid must not match spids filter")
	}
	// без фильтров — все события
	all := trc.TRCEventFilter{}
	count := 0
	for _, ev := range events {
		if eventMatchesFilter(ev, all) {
			count++
		}
	}
	if count != len(events) {
		t.Fatalf("unfiltered matched %d, want %d", count, len(events))
	}
}

func toSystemTime(t *testing.T, ts time.Time) trc.SystemTime {
	t.Helper()
	return trc.SystemTimeFromLocalParts(ts)
}

// TestExecuteEvents_Validation — некорректные параметры возвращают ошибку
// с именем параметра до обращения к источнику.
func TestExecuteEvents_Validation(t *testing.T) {
	negative := int64(-1)
	from := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	to := from.Add(-time.Minute)

	cases := []EventsParams{
		{Source: SessionSource{FilePath: "x.trc"}, SPIDs: []int{728, 0}},
		{Source: SessionSource{FilePath: "x.trc"}, SPIDs: []int{-1}},
		{Source: SessionSource{FilePath: "x.trc"}, EventNames: []string{"SP:Completed", ""}},
		{Source: SessionSource{FilePath: "x.trc"}, TimeFrom: &from, TimeTo: &to},
		{Source: SessionSource{FilePath: "x.trc"}, TimeFrom: &from, TimeTo: &from},
		{Source: SessionSource{FilePath: "x.trc"}, MinDurationMs: &negative},
		{Source: SessionSource{FilePath: "x.trc"}, AfterID: &negative},
		{Source: SessionSource{FilePath: "x.trc"}, Format: "compact"},
	}
	substrs := []string{"spids[1]", "spids[0]", "event_names[1]", "time_from", "time_from", "min_duration_ms", "after_id", "format"}
	for i, p := range cases {
		_, err := ExecuteEvents(context.Background(), nil, p)
		if err == nil {
			t.Errorf("case %d: expected error for %+v", i, p)
		} else if !strings.Contains(err.Error(), substrs[i]) {
			t.Errorf("case %d: error %q must mention %q", i, err, substrs[i])
		}
	}
}

// TestExecuteEvents_Dedup — дубликаты SPID и имён событий удаляются с
// сохранением порядка первого появления (фильтр эквивалентен дедуплицированному).
func TestExecuteEvents_Dedup(t *testing.T) {
	spids, err := normalizeSPIDs([]int{700, 728, 700})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spids, []int{700, 728}) {
		t.Fatalf("spids = %v, want [700 728]", spids)
	}
	names, err := normalizeEventNames([]string{"SP:Completed", "RPC:Completed", "SP:Completed"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"SP:Completed", "RPC:Completed"}) {
		t.Fatalf("names = %v, want [SP:Completed RPC:Completed]", names)
	}
}

func TestExecuteProcedures_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}})
	if err != nil {
		t.Fatalf("ExecuteProcedures error: %v", err)
	}
	if result.Count <= 0 {
		t.Errorf("Count = %d, want > 0", result.Count)
	}
}

// TestExecuteProcedures_FileMode_Params — top/sort/group_by_spid/event_names
// в file-mode.
func TestExecuteProcedures_FileMode_Params(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	all, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}})
	if err != nil {
		t.Fatalf("ExecuteProcedures error: %v", err)
	}
	if all.Count < 2 {
		t.Skip("test file has fewer than 2 procedures")
	}

	top, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}, Top: 1})
	if err != nil {
		t.Fatalf("ExecuteProcedures top: %v", err)
	}
	if top.Count != 1 || top.Procedures[0].Procedure != all.Procedures[0].Procedure {
		t.Fatalf("top=1 = %+v, want first of all: %+v", top.Procedures, all.Procedures[0])
	}

	avg, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}, SortBy: "avg_ms"})
	if err != nil {
		t.Fatalf("ExecuteProcedures sort: %v", err)
	}
	for i := 1; i < len(avg.Procedures); i++ {
		if avg.Procedures[i-1].AvgMs < avg.Procedures[i].AvgMs {
			t.Fatalf("avg_ms not descending: %v then %v", avg.Procedures[i-1].AvgMs, avg.Procedures[i].AvgMs)
		}
	}

	grouped, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}, GroupBySPID: true})
	if err != nil {
		t.Fatalf("ExecuteProcedures group: %v", err)
	}
	if grouped.Count > 0 && grouped.Procedures[0].SPID == 0 {
		t.Fatal("grouped aggregates must carry positive SPID")
	}
}

// TestExecuteProcedures_Validation — top за пределами 0..1000 и неизвестный
// sort_by возвращают ошибку с именем параметра.
func TestExecuteProcedures_Validation(t *testing.T) {
	cases := []ProceduresParams{
		{Source: SessionSource{FilePath: "x.trc"}, Top: 1001},
		{Source: SessionSource{FilePath: "x.trc"}, Top: -1},
		{Source: SessionSource{FilePath: "x.trc"}, SortBy: "duration"},
		{Source: SessionSource{FilePath: "x.trc"}, EventNames: []string{""}},
	}
	substrs := []string{"top", "top", "sort_by", "event_names[0]"}
	for i, p := range cases {
		_, err := ExecuteProcedures(context.Background(), nil, p)
		if err == nil {
			t.Errorf("case %d: expected error for %+v", i, p)
		} else if !strings.Contains(err.Error(), substrs[i]) {
			t.Errorf("case %d: error %q must mention %q", i, err, substrs[i])
		}
	}
}

func TestExecuteTree_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:   SessionSource{FilePath: p},
		MaxDepth: 5,
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}
	if len(result.Trees) == 0 {
		t.Error("expected non-empty trees")
	}
}

func TestExecuteTree_Procedure(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()

	// First get procedures to find a real one.
	procResult, err := ExecuteProcedures(ctx, nil, ProceduresParams{Source: SessionSource{FilePath: p}})
	if err != nil {
		t.Fatalf("ExecuteProcedures error: %v", err)
	}
	if procResult.Count == 0 {
		t.Skip("no procedures in test file")
	}
	procName := procResult.Procedures[0].Procedure
	if procName == "" {
		t.Skip("first procedure has empty name")
	}

	// Now build tree filtered by that procedure.
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:    SessionSource{FilePath: p},
		Procedure: procName,
		MaxDepth:  5,
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}

	// All root nodes in the filtered tree should have the matching procedure.
	for _, roots := range result.Trees {
		for _, root := range roots {
			if root.Start.Procedure != procName {
				t.Errorf("root procedure = %q, want %q", root.Start.Procedure, procName)
			}
		}
	}
}

func TestExecuteTree_ProcedureNotFound(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "NonExistentProcXYZ",
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}
	if len(result.Trees) != 0 {
		t.Errorf("expected empty trees for non-existent procedure, got %d SPIDs", len(result.Trees))
	}
}

func TestExecuteErrors_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteErrors(ctx, nil, ErrorsParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteErrors error: %v", err)
	}
	if result.Limit != 100 {
		t.Errorf("Limit = %d, want 100", result.Limit)
	}
}

func TestExecuteSlow_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteSlow(ctx, nil, SlowParams{
		Source:      SessionSource{FilePath: p},
		ThresholdMs: 1,
		Limit:       50,
	})
	if err != nil {
		t.Fatalf("ExecuteSlow error: %v", err)
	}
	if result.Threshold != 1 {
		t.Errorf("Threshold = %d, want 1", result.Threshold)
	}
	if result.Limit != 50 {
		t.Errorf("Limit = %d, want 50", result.Limit)
	}
	// All events should have DurationMs >= 1
	for _, ev := range result.Events {
		if ev.DurationMs < 1 {
			t.Errorf("Event DurationMs = %d, want >= 1", ev.DurationMs)
		}
	}
}

func TestExecuteSlow_FileMode_DefaultThreshold(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteSlow(ctx, nil, SlowParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteSlow error: %v", err)
	}
	if result.Threshold <= 0 {
		t.Errorf("Threshold = %d, want > 0 (default)", result.Threshold)
	}
}

func TestNormalizeLimit(t *testing.T) {
	tests := []struct {
		input, expected int
	}{
		{0, 100},
		{-1, 100},
		{50, 50},
		{1000, 1000},
		{2000, 1000},
	}
	for _, tc := range tests {
		got := normalizeLimit(tc.input)
		if got != tc.expected {
			t.Errorf("normalizeLimit(%d) = %d, want %d", tc.input, got, tc.expected)
		}
	}
}

// TestExecuteTree_CompletedOnlyFileMode — проверяет, что для SPID с только
// Completed-событиями дерево имеет вложенную структуру (не плоский список).
// Тест парсит DIAPR-391.trc, находит SPID без Starting-событий, и проверяет
// что ExecuteTree строит дерево с глубиной > 0 для этого SPID.
func TestExecuteTree_CompletedOnlyFileMode(t *testing.T) {
	p := trcTestPath(t)

	// Парсим файл, чтобы найти Completed-only SPID.
	parseResult, err := trc.ParseFile(p)
	if err != nil {
		t.Fatalf("ParseFile error: %v", err)
	}

	// Группируем события по SPID и ищем SPID без Starting.
	spidHasStarting := make(map[int]bool)
	spidEventCount := make(map[int]int)
	for _, ev := range parseResult.Events {
		spidVal, ok := ev.Columns[12].(int32)
		if !ok {
			continue
		}
		s := int(spidVal)
		spidEventCount[s]++
		if strings.HasSuffix(ev.EventName, "Starting") {
			spidHasStarting[s] = true
		}
	}

	var completedOnlySPID int
	for s, cnt := range spidEventCount {
		if !spidHasStarting[s] && cnt >= 2 {
			completedOnlySPID = s
			break
		}
	}
	if completedOnlySPID == 0 {
		t.Skip("no Completed-only SPID with >= 2 events found in test file")
	}

	// Вызываем ExecuteTree для найденного SPID.
	ctx := context.Background()
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:   SessionSource{FilePath: p},
		SPID:     completedOnlySPID,
		MaxDepth: 10,
		Limit:    1000,
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}

	roots, ok := result.Trees[completedOnlySPID]
	if !ok || len(roots) == 0 {
		t.Fatalf("expected non-empty trees for SPID %d, got %v", completedOnlySPID, result.Trees)
	}

	// Проверяем, что хотя бы один корень имеет детей (вложенность).
	hasNesting := false
	var checkNode func(n *trc.TRCTreeNode)
	checkNode = func(n *trc.TRCTreeNode) {
		if len(n.Children) > 0 {
			hasNesting = true
		}
		for _, c := range n.Children {
			checkNode(c)
		}
	}
	for _, root := range roots {
		checkNode(root)
	}

	if !hasNesting {
		t.Errorf("expected nested tree structure for Completed-only SPID %d, but all roots are flat", completedOnlySPID)
	}
}

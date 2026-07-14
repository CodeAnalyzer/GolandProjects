package trc

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/codebase/internal/query"
)

// makeEventsWithProc создаёт n событий с чередующимися именами процедур
// и TextData, содержащими exec-вызовы с параметрами.
func makeEventsWithProc(n int) []TRCEvent {
	procs := []string{"ProcA", "ProcB", "ProcC", "ProcD"}
	events := make([]TRCEvent, n)
	for i := 0; i < n; i++ {
		proc := procs[i%len(procs)]
		events[i] = TRCEvent{
			EventClass: 10,
			EventName:  "RPC:Completed",
			Columns: map[int]any{
				1: fmt.Sprintf("exec %s @Param1=value%d, @Param2=value%d", proc, i, i+1),
				13: int64(i * 1000),
			},
		}
	}
	return events
}

// TestEnrichEventsParallel_Deterministic — параллельный enrich даёт те же
// результаты, что и последовательный.
func TestEnrichEventsParallel_Deterministic(t *testing.T) {
	events := makeEventsWithProc(500)

	// Эталон — последовательный enrich копии
	reference := make([]TRCEvent, len(events))
	copy(reference, events)
	for i := range reference {
		enrichEvent(&reference[i])
	}

	// Параллельный enrich
	enrichEventsParallel(events)

	for i := range events {
		if events[i].Procedure != reference[i].Procedure {
			t.Errorf("event %d: Procedure = %q, want %q", i, events[i].Procedure, reference[i].Procedure)
		}
		if events[i].DurationMs != reference[i].DurationMs {
			t.Errorf("event %d: DurationMs = %d, want %d", i, events[i].DurationMs, reference[i].DurationMs)
		}
		if len(events[i].Params) != len(reference[i].Params) {
			t.Errorf("event %d: Params len = %d, want %d", i, len(events[i].Params), len(reference[i].Params))
		}
		for j := range events[i].Params {
			if events[i].Params[j].Name != reference[i].Params[j].Name {
				t.Errorf("event %d param %d: Name = %q, want %q", i, j, events[i].Params[j].Name, reference[i].Params[j].Name)
			}
			if events[i].Params[j].Value != reference[i].Params[j].Value {
				t.Errorf("event %d param %d: Value = %q, want %q", i, j, events[i].Params[j].Value, reference[i].Params[j].Value)
			}
		}
	}
}

// TestEnrichEventsParallel_SmallDataset_Sequential — < minEventsForParallel
// событий: проверка корректности (последовательный путь).
func TestEnrichEventsParallel_SmallDataset_Sequential(t *testing.T) {
	events := makeEventsWithProc(10)
	enrichEventsParallel(events)

	for i, ev := range events {
		if ev.Procedure == "" {
			t.Errorf("event %d: Procedure is empty, expected non-empty", i)
		}
		if ev.DurationMs != int64(i) {
			t.Errorf("event %d: DurationMs = %d, want %d", i, ev.DurationMs, i)
		}
	}
}

// TestSerializeParallel_MatchesSequential — параллельная сериализация даёт
// те же результаты, что и последовательная.
func TestSerializeParallel_MatchesSequential(t *testing.T) {
	events := makeEventsWithProc(300)

	// Последовательная сериализация (эталон)
	refColumns := make([][]byte, len(events))
	refParams := make([]interface{}, len(events))
	if err := serializeSequential(events, refColumns, refParams); err != nil {
		t.Fatalf("serializeSequential: %v", err)
	}

	// Параллельная сериализация
	parColumns := make([][]byte, len(events))
	parParams := make([]interface{}, len(events))
	if err := serializeParallel(events, parColumns, parParams); err != nil {
		t.Fatalf("serializeParallel: %v", err)
	}

	for i := range events {
		if string(parColumns[i]) != string(refColumns[i]) {
			t.Errorf("event %d: columnsJSON mismatch", i)
		}
		// paramsJSON: оба должны быть либо nil, либо одинаковая строка
		refStr, refOK := refParams[i].(string)
		parStr, parOK := parParams[i].(string)
		if refOK != parOK {
			t.Errorf("event %d: paramsJSON type mismatch: ref=%v par=%v", i, refOK, parOK)
		}
		if refOK && parOK && refStr != parStr {
			t.Errorf("event %d: paramsJSON mismatch", i)
		}
	}
}

// TestSerializeParallel_SmallDataset_Sequential — < minEventsForParallel
// событий: проверка корректности (последовательный путь).
func TestSerializeParallel_SmallDataset_Sequential(t *testing.T) {
	events := makeEventsWithProc(10)
	columnsJSONs := make([][]byte, len(events))
	paramsJSONs := make([]interface{}, len(events))

	if err := serializeParallel(events, columnsJSONs, paramsJSONs); err != nil {
		t.Fatalf("serializeParallel: %v", err)
	}

	for i, ev := range events {
		cols, err := unmarshalColumns(columnsJSONs[i])
		if err != nil {
			t.Errorf("event %d: unmarshalColumns: %v", i, err)
		}
		if strVal(cols[1]) != strVal(ev.Columns[1]) {
			t.Errorf("event %d: TextData mismatch", i)
		}
	}
}

// TestSerializeParallel_ErrorPropagation — ошибка сериализации возвращается.
func TestSerializeParallel_ErrorPropagation(t *testing.T) {
	// Создаём событие с невалидируемым типом в Columns (func не marshalable)
	events := make([]TRCEvent, 300)
	for i := 0; i < 300; i++ {
		events[i] = TRCEvent{
			EventClass: 10,
			Columns:    map[int]any{1: "exec ProcA @p=v"},
		}
	}
	// Подмешиваем невалидный тип в одно событие
	events[150].Columns[99] = func() {}

	columnsJSONs := make([][]byte, len(events))
	paramsJSONs := make([]interface{}, len(events))

	err := serializeParallel(events, columnsJSONs, paramsJSONs)
	if err == nil {
		t.Fatal("expected error from serializeParallel with unmarshalable type, got nil")
	}
}

// TestEnrichEvents_ConcurrentSameResult — параллельный EnrichEvents даёт
// тот же результат, что и последовательный (с mockLookup).
func TestEnrichEvents_ConcurrentSameResult(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{},
	}
	// 20 уникальных процедур — выше порога minProcsForParallelEnrich
	procNames := make([]string, 20)
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("Proc%02d", i)
		procNames[i] = name
		mock.procs[name] = &query.SQLProcedureResult{
			ProcName:  name,
			File:      fmt.Sprintf("/path/%s.sql", name),
			LineStart: i * 10,
			LineEnd:   i*10 + 50,
		}
	}

	// События с 20 уникальными процедурами, каждая повторяется несколько раз
	events := make([]TRCEvent, 100)
	for i := range events {
		events[i].Procedure = procNames[i%20]
	}

	result := EnrichEvents(mock, events)

	if len(result) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(result))
	}
	for _, name := range procNames {
		e, ok := result[name]
		if !ok {
			t.Errorf("procedure %q missing from result", name)
			continue
		}
		if !e.Found {
			t.Errorf("procedure %q: expected Found=true", name)
		}
		if e.SourceFile != fmt.Sprintf("/path/%s.sql", name) {
			t.Errorf("procedure %q: SourceFile = %q", name, e.SourceFile)
		}
	}
}

// TestEnrichEvents_SmallDataset_Sequential — < minProcsForParallelEnrich
// уникальных процедур: проверка корректности (последовательный путь).
func TestEnrichEvents_SmallDataset_Sequential(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{
			"ProcA": {ProcName: "ProcA", File: "/a.sql"},
			"ProcB": {ProcName: "ProcB", File: "/b.sql"},
		},
	}
	events := []TRCEvent{
		{Procedure: "ProcA"},
		{Procedure: "ProcB"},
		{Procedure: "ProcA"},
		{Procedure: ""},
	}

	result := EnrichEvents(mock, events)
	if len(result) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(result))
	}
	if e, ok := result["ProcA"]; !ok || !e.Found || e.SourceFile != "/a.sql" {
		t.Errorf("ProcA not enriched properly: %+v", e)
	}
	if e, ok := result["ProcB"]; !ok || !e.Found || e.SourceFile != "/b.sql" {
		t.Errorf("ProcB not enriched properly: %+v", e)
	}
}

// TestEnrichEvents_NotFoundConcurrent — процедуры, не найденные в БД,
// получают Found=false даже в параллельном режиме.
func TestEnrichEvents_NotFoundConcurrent(t *testing.T) {
	mock := &mockLookup{
		procs: map[string]*query.SQLProcedureResult{},
	}
	// 20 уникальных процедур, ни одна не найдена
	events := make([]TRCEvent, 40)
	for i := range events {
		events[i].Procedure = fmt.Sprintf("UnknownProc%02d", i%20)
	}

	result := EnrichEvents(mock, events)
	if len(result) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(result))
	}
	for name, e := range result {
		if e.Found {
			t.Errorf("procedure %q: expected Found=false", name)
		}
		if e.SourceFile != "(not found)" {
			t.Errorf("procedure %q: SourceFile = %q, want (not found)", name, e.SourceFile)
		}
	}
}

// countingLookup — mockLookup с счётчиком вызовов для проверки
// конкурентного доступа.
type countingLookup struct {
	mu    sync.Mutex
	calls int
	procs map[string]*query.SQLProcedureResult
}

func (c *countingLookup) GetProcedureResult(name string) (*query.SQLProcedureResult, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if proc, ok := c.procs[name]; ok {
		return proc, nil
	}
	return nil, fmt.Errorf("not found: %s", name)
}

// TestEnrichEvents_ConcurrentSafety — проверка, что EnrichEvents корректно
// работает при конкурентном доступе к mockLookup (race detector).
func TestEnrichEvents_ConcurrentSafety(t *testing.T) {
	mock := &countingLookup{
		procs: map[string]*query.SQLProcedureResult{},
	}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("Proc%02d", i)
		mock.procs[name] = &query.SQLProcedureResult{
			ProcName:  name,
			File:      fmt.Sprintf("/path/%s.sql", name),
			LineStart: i * 10,
			LineEnd:   i*10 + 50,
		}
	}

	events := make([]TRCEvent, 100)
	for i := range events {
		events[i].Procedure = fmt.Sprintf("Proc%02d", i%20)
	}

	result := EnrichEvents(mock, events)
	if len(result) != 20 {
		t.Fatalf("expected 20 entries, got %d", len(result))
	}
	if mock.calls != 20 {
		t.Errorf("expected 20 GetProcedureResult calls, got %d", mock.calls)
	}
}

// TestSerializeParallel_RoundTrip — проверка round-trip:
// marshalColumns → unmarshalColumns восстанавливает исходные типы.
func TestSerializeParallel_RoundTrip(t *testing.T) {
	events := make([]TRCEvent, 300)
	for i := 0; i < 300; i++ {
		events[i] = TRCEvent{
			EventClass: 10,
			Columns: map[int]any{
				1:  fmt.Sprintf("exec ProcA @p=%d", i),
				12: int32(i),
				13: int64(i * 1000),
			},
			Params: []TRCParam{{Name: "p", Value: fmt.Sprintf("%d", i)}},
		}
	}

	columnsJSONs := make([][]byte, len(events))
	paramsJSONs := make([]interface{}, len(events))
	if err := serializeParallel(events, columnsJSONs, paramsJSONs); err != nil {
		t.Fatalf("serializeParallel: %v", err)
	}

	for i := range events {
		cols, err := unmarshalColumns(columnsJSONs[i])
		if err != nil {
			t.Errorf("event %d: unmarshalColumns: %v", i, err)
			continue
		}
		if strVal(cols[1]) != fmt.Sprintf("exec ProcA @p=%d", i) {
			t.Errorf("event %d: TextData mismatch", i)
		}
		if v, ok := cols[12].(int32); !ok || v != int32(i) {
			t.Errorf("event %d: col 12 = %v, want int32(%d)", i, cols[12], i)
		}
		if v, ok := cols[13].(int64); !ok || v != int64(i*1000) {
			t.Errorf("event %d: col 13 = %v, want int64(%d)", i, cols[13], i*1000)
		}

		// Проверка params
		pj, ok := paramsJSONs[i].(string)
		if !ok {
			t.Errorf("event %d: paramsJSON is not string", i)
			continue
		}
		var params []TRCParam
		if err := json.Unmarshal([]byte(pj), &params); err != nil {
			t.Errorf("event %d: unmarshal params: %v", i, err)
			continue
		}
		if len(params) != 1 || params[0].Name != "p" || params[0].Value != fmt.Sprintf("%d", i) {
			t.Errorf("event %d: params mismatch: %+v", i, params)
		}
	}
}

// TestEnrichEventsParallel_LargeTextData — проверка enrich на событиях с
// длинными TextData (многострочные exec-вызовы с комментариями).
func TestEnrichEventsParallel_LargeTextData(t *testing.T) {
	textData := "exec " + strings.Repeat("/* comment */ ", 10) + "MyProc @Param1=value1, @Param2='long, value, with, commas'"
	events := make([]TRCEvent, 300)
	for i := range events {
		events[i] = TRCEvent{
			EventClass: 10,
			Columns:    map[int]any{1: textData, 13: int64(5000)},
		}
	}

	enrichEventsParallel(events)

	for i, ev := range events {
		if ev.Procedure != "MyProc" {
			t.Errorf("event %d: Procedure = %q, want MyProc", i, ev.Procedure)
		}
		if len(ev.Params) != 2 {
			t.Errorf("event %d: expected 2 params, got %d", i, len(ev.Params))
		}
		if ev.DurationMs != 5 {
			t.Errorf("event %d: DurationMs = %d, want 5", i, ev.DurationMs)
		}
	}
}

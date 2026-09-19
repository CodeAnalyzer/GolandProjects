//go:build integration

package trc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/codebase/internal/store"
	"github.com/codebase/internal/store/testutil"
	"github.com/lib/pq"
)

func TestPruneAndDeleteTRCSessions(t *testing.T) {
	db := testutil.Open(t)

	insert := func(path string) int64 {
		t.Helper()
		var id int64
		err := db.QueryRow(
			`INSERT INTO trc_sessions (file_path, file_size, total_events)
			 VALUES ($1, 10, 1) RETURNING id`,
			path,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert session %s: %v", path, err)
		}
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, duration_ms) VALUES ($1, 10, 'RPC:Completed', 5)`, id); err != nil {
			t.Fatalf("insert event: %v", err)
		}
		return id
	}

	id1 := insert("s1.trc")
	id2 := insert("s2.trc")
	id3 := insert("s3.trc")

	deleted, err := PruneSessions(context.Background(), db, 1)
	if err != nil {
		t.Fatalf("PruneSessions(1): %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	sessions, err := ListSessions(context.Background(), db, 10)
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 || sessions[0].ID != id3 {
		t.Fatalf("remaining = %+v, want %d", sessions, id3)
	}
	var leftover int
	if err := db.QueryRow(`SELECT count(*) FROM trc_events WHERE session_id IN ($1, $2)`, id1, id2).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("orphan events = %d", leftover)
	}

	if err := DeleteSession(context.Background(), db, id3); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	id4 := insert("s4.trc")
	if err := DeleteSession(context.Background(), db, id3); err != nil {
		t.Fatalf("DeleteSession missing: %v", err)
	}
	var exists bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM trc_sessions WHERE id=$1)`, id4).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("unrelated session was deleted")
	}

	n, err := PruneSessions(context.Background(), db, 0)
	if err != nil {
		t.Fatalf("PruneSessions(0): %v", err)
	}
	if n != 1 {
		t.Fatalf("truncate count = %d, want 1", n)
	}
	if id := insert("s5.trc"); id == 0 {
		t.Fatal("insert after truncate failed")
	}
}

func TestLoadProceduresAggregated_CompletedOnlyAndZeroDuration(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('agg-test.trc', 10, 4) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)
	for _, e := range []struct {
		name     string
		duration int
	}{
		{"SP:Completed", 0}, {"SP:Completed", 100}, {"SP:StmtCompleted", 100}, {"SQL:StmtCompleted", 100},
	} {
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, procedure, duration_ms) VALUES ($1, 10, $2, 'ProcA', $3)`, sessionID, e.name, e.duration); err != nil {
			t.Fatal(err)
		}
	}
	aggs, err := LoadProceduresAggregated(context.Background(), db, sessionID, AggregateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 1 || aggs[0].Count != 2 || aggs[0].TotalMs != 100 || aggs[0].MinMs != 0 || aggs[0].MaxMs != 100 || aggs[0].AvgMs != 50 {
		t.Fatalf("got %+v", aggs)
	}
}

// TestLoadProceduresAggregated_Options — spids/event_names/group_by_spid/
// sort_by/top серверно; результат совпадает с контрольным GROUP BY.
func TestLoadProceduresAggregated_Options(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('agg-opts.trc', 10, 8) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	insert := func(name string, spid interface{}, proc string, dur int64) {
		t.Helper()
		if _, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms) VALUES ($1, 10, $2, $3, $4, $5)`,
			sessionID, name, spid, proc, dur,
		); err != nil {
			t.Fatal(err)
		}
	}
	insert("SP:Completed", 728, "ProcA", 100)
	insert("SP:Completed", 728, "ProcA", 300)
	insert("SP:Completed", 700, "ProcA", 10)
	insert("SP:Completed", nil, "ProcA", 5) // NULL spid
	insert("RPC:Completed", 700, "ProcB", 40)
	insert("SP:Completed", 700, "", 999) // пустая процедура — исключается

	ctx := context.Background()

	// group_by_spid: NULL-spid исключён; группы (spid, procedure)
	grouped, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{GroupBySPID: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(grouped) != 2 {
		t.Fatalf("grouped = %+v, want 2 groups", grouped)
	}
	// total_ms desc: (728, ProcA)=400 > (700, ProcA)=10
	if grouped[0].SPID != 728 || grouped[0].Count != 2 || grouped[0].TotalMs != 400 {
		t.Errorf("first group = %+v, want SPID 728 count 2 total 400", grouped[0])
	}
	if grouped[1].SPID != 700 || grouped[1].Count != 1 || grouped[1].TotalMs != 10 {
		t.Errorf("second group = %+v, want SPID 700 count 1 total 10", grouped[1])
	}

	// контрольный GROUP BY: совпадение агрегатов
	var ctrlCount int
	var ctrlTotal int64
	if err := db.QueryRow(
		`SELECT count(*), COALESCE(sum(duration_ms),0) FROM trc_events
		 WHERE session_id=$1 AND event_name = ANY($2) AND procedure IS NOT NULL AND procedure <> '' AND spid IS NOT NULL
		 GROUP BY spid, procedure HAVING spid = 728 AND procedure = 'ProcA'`,
		sessionID, pq.Array([]string{"SP:Completed"}),
	).Scan(&ctrlCount, &ctrlTotal); err != nil {
		t.Fatal(err)
	}
	if grouped[0].Count != ctrlCount || grouped[0].TotalMs != ctrlTotal {
		t.Fatalf("grouped %+v != control count=%d total=%d", grouped[0], ctrlCount, ctrlTotal)
	}

	// без группировки: NULL-spid участвует (текущее поведение)
	flat, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(flat) != 1 || flat[0].Procedure != "ProcA" || flat[0].Count != 4 || flat[0].TotalMs != 415 || flat[0].SPID != 0 {
		t.Fatalf("flat = %+v, want ProcA count 4 total 415 without spid", flat)
	}

	// spids-фильтр
	sp728, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{SPIDs: []int{728}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sp728) != 1 || sp728[0].Count != 2 || sp728[0].TotalMs != 400 {
		t.Fatalf("spids=[728] = %+v", sp728)
	}

	// event_names
	rpc, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{EventNames: []string{"RPC:Completed"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rpc) != 1 || rpc[0].Procedure != "ProcB" || rpc[0].TotalMs != 40 {
		t.Fatalf("rpc = %+v", rpc)
	}

	// top + sort_by=avg_ms: единственная процедура ProcA avg=103.75
	top1, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{Top: 1, SortBy: "avg_ms"})
	if err != nil {
		t.Fatal(err)
	}
	if len(top1) != 1 || top1[0].Procedure != "ProcA" {
		t.Fatalf("top1 avg = %+v", top1)
	}

	// сортировка по count
	byCount, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{GroupBySPID: true, SortBy: "count"})
	if err != nil {
		t.Fatal(err)
	}
	if byCount[0].Count < byCount[len(byCount)-1].Count {
		t.Fatalf("count sort not descending: %+v", byCount)
	}
}

func TestLoadEventsFilteredAndCount(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('filter-test.trc', 10, 3) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)
	for _, proc := range []string{"ProcA", "ProcA", "ProcB"} {
		if _, err := db.Exec(`INSERT INTO trc_events (session_id, event_class, event_name, procedure, duration_ms) VALUES ($1, 10, 'SP:Completed', $2, 1)`, sessionID, proc); err != nil {
			t.Fatal(err)
		}
	}
	f := TRCEventFilter{Procedure: "ProcA"}
	page, err := LoadEventsFiltered(context.Background(), db, sessionID, f, 1)
	if err != nil {
		t.Fatal(err)
	}
	count, err := LoadEventCountFiltered(context.Background(), db, sessionID, f)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || count != 2 {
		t.Fatalf("len=%d count=%d, want 1/2", len(page.Events), count)
	}
	if !page.HasMore || page.NextAfterID == 0 {
		t.Fatalf("page has_more=%v next=%d, want true/non-zero", page.HasMore, page.NextAfterID)
	}
	// вторая страница по курсору возвращает хвост без пересечений
	next, err := LoadEventsFiltered(context.Background(), db, sessionID, TRCEventFilter{Procedure: "ProcA", AfterID: &page.NextAfterID}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Events) != 1 || next.HasMore {
		t.Fatalf("second page len=%d has_more=%v, want 1/false", len(next.Events), next.HasMore)
	}
	if next.Events[0].StoreID == page.Events[0].StoreID {
		t.Fatal("pages overlap on same event id")
	}
}

// TestLoadEventsFiltered_Filters — spids/event_names/time/min_duration в SQL.
func TestLoadEventsFiltered_Filters(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('filter2-test.trc', 10, 6) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	insert := func(name string, spid interface{}, dur int64, start interface{}) {
		t.Helper()
		if _, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms, start_time)
			 VALUES ($1, 10, $2, $3, 'Proc', $4, $5)`,
			sessionID, name, spid, dur, start,
		); err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	insert("SP:Completed", 728, 100, t0)
	insert("SP:Completed", 700, 10, t0.Add(20*time.Minute))
	insert("RPC:Completed", 728, 40, t0.Add(40*time.Minute))
	insert("SP:Completed", nil, 5, nil) // без spid и start_time

	ctx := context.Background()

	from := t0.Add(10 * time.Minute)
	to := t0.Add(50 * time.Minute)
	minDur := int64(50)

	f := TRCEventFilter{SPIDs: []int{728}, EventNames: []string{"SP:Completed", "RPC:Completed"}, TimeFrom: &from, TimeTo: &to, MinDurationMs: &minDur}
	page, err := LoadEventsFiltered(ctx, db, sessionID, f, 100)
	if err != nil {
		t.Fatal(err)
	}
	// только RPC:Completed SPID 728 @40min с 40мс не проходит min_duration=50;
	// SP:Completed 728 @0 вне временного диапазона → пусто
	if len(page.Events) != 0 {
		t.Fatalf("got %+v, want empty", page.Events)
	}

	// без min_duration — RPC проходит
	f.MinDurationMs = nil
	page, err = LoadEventsFiltered(ctx, db, sessionID, f, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].EventName != "RPC:Completed" || page.Events[0].StoreID == 0 {
		t.Fatalf("got %+v, want single RPC:Completed with id", page.Events)
	}

	// только spids без времени: SPID 728 обе строки (NULL start_time вне диапазона не важен)
	timeFree := TRCEventFilter{SPIDs: []int{728}}
	page, err = LoadEventsFiltered(ctx, db, sessionID, timeFree, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 {
		t.Fatalf("spids only: got %d events, want 2", len(page.Events))
	}
}

// TestLoadEventsFiltered_KeysetPagination — страницы не пересекаются, не
// теряют события, сохраняют порядок; filtered_count не зависит от курсора;
// последняя страница has_more=false без next_after_id.
func TestLoadEventsFiltered_KeysetPagination(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('page-test.trc', 10, 25) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	for i := 0; i < 25; i++ {
		spid := 728
		if i%3 == 0 {
			spid = 700
		}
		if _, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms) VALUES ($1, 10, 'SP:Completed', $2, $3, 1)`,
			sessionID, spid, fmt.Sprintf("Proc%02d", i),
		); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	const limit = 10
	f := TRCEventFilter{} // без пользовательских фильтров

	total, err := LoadEventCountFiltered(ctx, db, sessionID, f)
	if err != nil {
		t.Fatal(err)
	}
	if total != 25 {
		t.Fatalf("filtered = %d, want 25", total)
	}

	var seen []int64
	var after *int64
	pages := 0
	for {
		pf := f
		pf.AfterID = after
		page, err := LoadEventsFiltered(ctx, db, sessionID, pf, limit)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, ev := range page.Events {
			seen = append(seen, ev.StoreID)
		}
		if !page.HasMore {
			if page.NextAfterID != 0 {
				t.Fatalf("last page next_after_id = %d, want 0", page.NextAfterID)
			}
			break
		}
		if page.NextAfterID == 0 {
			t.Fatal("has_more=true but next_after_id is 0")
		}
		after = &page.NextAfterID
		if pages > 10 {
			t.Fatal("pagination does not terminate")
		}
	}
	if pages != 3 {
		t.Fatalf("pages = %d, want 3", pages)
	}
	if len(seen) != 25 {
		t.Fatalf("collected %d events, want 25", len(seen))
	}
	for i := 1; i < len(seen); i++ {
		if seen[i] <= seen[i-1] {
			t.Fatalf("ids not strictly increasing: %v", seen)
		}
	}

	// filtered_count не зависит от курсора
	pf := f
	cursor := seen[len(seen)-1]
	pf.AfterID = &cursor
	tail, err := LoadEventCountFiltered(ctx, db, sessionID, pf)
	if err != nil {
		t.Fatal(err)
	}
	if tail != 25 {
		t.Fatalf("filtered_count with cursor = %d, want 25 (cursor not counted)", tail)
	}
}

// TestLoadEventRowsFiltered_Short — short-строки читают только выделенные
// колонки (spid, start_time) и возвращают те же id/порядок, что full.
func TestLoadEventRowsFiltered_Short(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('short-test.trc', 10, 3) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	t0 := time.Date(2026, 9, 14, 13, 56, 50, 0, time.UTC)
	rows := []struct {
		name  string
		spid  interface{}
		start interface{}
		dur   int64
	}{
		{"SP:Completed", 728, t0, 100},
		{"RPC:Completed", 700, t0.Add(time.Minute), 10},
		{"SP:Completed", nil, nil, 5},
	}
	for _, r := range rows {
		if _, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms, start_time)
			 VALUES ($1, 10, $2, $3, 'Proc', $4, $5)`,
			sessionID, r.name, r.spid, r.dur, r.start,
		); err != nil {
			t.Fatal(err)
		}
	}

	ctx := context.Background()
	full, err := LoadEventsFiltered(ctx, db, sessionID, TRCEventFilter{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	short, err := LoadEventRowsFiltered(ctx, db, sessionID, TRCEventFilter{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(short.Rows) != len(full.Events) {
		t.Fatalf("short len = %d, want full len %d", len(short.Rows), len(full.Events))
	}
	for i := range full.Events {
		s := short.Rows[i]
		f := full.Events[i]
		if s.ID != f.StoreID {
			t.Errorf("row %d: id = %d, want %d", i, s.ID, f.StoreID)
		}
		if s.EventName != f.EventName || s.DurationMs != f.DurationMs {
			t.Errorf("row %d: %+v mismatches full event", i, s)
		}
	}
	// start_time читается из выделенной колонки; NULL остаётся nil
	if short.Rows[0].StartTime == nil || !short.Rows[0].StartTime.Equal(t0) {
		t.Errorf("row 0: start_time = %v, want %v", short.Rows[0].StartTime, t0)
	}
	if short.Rows[1].StartTime == nil || !short.Rows[1].StartTime.Equal(t0.Add(time.Minute)) {
		t.Errorf("row 1: start_time = %v", short.Rows[1].StartTime)
	}
	if short.Rows[2].StartTime != nil {
		t.Errorf("row 2: start_time = %v, want nil", short.Rows[2].StartTime)
	}
	if short.Rows[0].SPID != 728 || short.Rows[1].SPID != 700 || short.Rows[2].SPID != 0 {
		t.Fatalf("spids = %d,%d,%d want 728,700,0", short.Rows[0].SPID, short.Rows[1].SPID, short.Rows[2].SPID)
	}

	// фильтр по spid для short-строк
	page, err := LoadEventRowsFiltered(ctx, db, sessionID, TRCEventFilter{SPIDs: []int{728}}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || page.Rows[0].SPID != 728 {
		t.Fatalf("short spid filter = %+v", page.Rows)
	}
}

func TestLoadEventsForTree_DiagnosticFilteredFromRoot(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 4) RETURNING id`,
		"diag-test.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() {
		_ = DeleteSession(context.Background(), db, sessionID)
	}()

	events := []struct {
		name      string
		class     int
		spid      int
		parentID  *int
		procedure string
	}{
		{"SP:Recompile", 37, 76, nil, ""},
		{"SQL:StmtRecompile", 166, 76, nil, ""},
		{"RPC:Starting", 10, 76, nil, "ProcA"},
		{"RPC:Completed", 11, 76, nil, "ProcA"},
	}
	for _, e := range events {
		_, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms)
			 VALUES ($1, $2, $3, $4, $5, $6, 0)`,
			sessionID, e.class, e.name, e.spid, e.parentID, e.procedure,
		)
		if err != nil {
			t.Fatalf("insert event %s: %v", e.name, err)
		}
	}

	treeEvents, err := LoadEventsForTree(context.Background(), db, sessionID, 76, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}
	for _, ev := range treeEvents {
		if ev.EventName != "RPC:Starting" && ev.EventName != "RPC:Completed" {
			t.Errorf("diagnostic event %q leaked into tree", ev.EventName)
		}
	}
	if len(treeEvents) == 0 {
		t.Fatal("expected at least RPC:Starting/Completed events in tree, got 0")
	}
}

// compareRow описывает синтетическое событие для parity-тестов compare/spids:
// одна спецификация порождает и SQL-строку, и эквивалентное TRCEvent.
type compareRow struct {
	spid      int32
	name      string
	proc      string
	dur       int64
	start     *time.Time
	errCode   *int32
	app       string
}

func (r compareRow) toEvent() TRCEvent {
	cols := map[int]any{}
	if r.spid != 0 {
		cols[12] = r.spid
	}
	if r.start != nil {
		cols[14] = SystemTimeFromLocalParts(*r.start)
	}
	if r.errCode != nil {
		cols[31] = *r.errCode
	}
	if r.app != "" {
		cols[10] = r.app
	}
	return TRCEvent{EventName: r.name, Procedure: r.proc, DurationMs: r.dur, Columns: cols}
}

// insertCompareRows вставляет события в БД в порядке спецификации (id растут).
func insertCompareRows(t *testing.T, db *store.DB, sessionID int64, rows []compareRow) {
	t.Helper()
	for _, r := range rows {
		var start, errCode, app, proc interface{}
		if r.spid != 0 {
			// spid передаётся как есть; 0 → NULL ниже
		}
		var spid interface{}
		if r.spid != 0 {
			spid = r.spid
		}
		if r.start != nil {
			start = *r.start
		}
		if r.errCode != nil {
			errCode = *r.errCode
		}
		if r.app != "" {
			app = r.app
		}
		if r.proc != "" {
			proc = r.proc
		}
		if _, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms, start_time, error, application_name)
			 VALUES ($1, 10, $2, $3, $4, $5, $6, $7, $8)`,
			sessionID, r.name, spid, proc, r.dur, start, errCode, app,
		); err != nil {
			t.Fatalf("insert compare row: %v", err)
		}
	}
}

func compareTestRows() []compareRow {
	base := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	min := base
	errCode := int32(50000)
	rows := make([]compareRow, 0, 12)
	// focus 728: A×3, B×1
	rows = append(rows,
		compareRow{spid: 728, name: "SP:Completed", proc: "A", dur: 300, start: &min},
		compareRow{spid: 728, name: "SP:Completed", proc: "A", dur: 100, errCode: &errCode, app: "AppA"},
		compareRow{spid: 728, name: "SP:StmtCompleted", proc: "A", dur: 50}, // не входит
		compareRow{spid: 728, name: "SP:Completed", proc: "B", dur: 40, app: "AppB"},
	)
	// peer 700: A×2, C (только у peer)
	rows = append(rows,
		compareRow{spid: 700, name: "SP:Combined", proc: "A", dur: 1}, // не входит (имя класса)
		compareRow{spid: 700, name: "SP:Completed", proc: "A", dur: 200, start: &min},
		compareRow{spid: 700, name: "SP:Completed", proc: "A", dur: 100},
		compareRow{spid: 700, name: "SP:Completed", proc: "C", dur: 9999},
	)
	// peer 179: пустой для A
	rows = append(rows, compareRow{spid: 179, name: "SP:Completed", proc: "X", dur: 5})
	return rows
}

// TestCompareProcedures_ServerParity — серверная сборка совпадает с file-mode
// и с контрольным GROUP BY.
func TestCompareProcedures_ServerParity(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('cmp-parity.trc', 10, 0) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	rows := compareTestRows()
	insertCompareRows(t, db, sessionID, rows)

	opts := CompareOptions{FocusSPID: 728, CompareSPIDs: []int{700, 179}, Top: 20, SortBy: "total_ms"}

	// серверно: LoadProceduresAggregated (GroupBySPID) + сборка
	aggRows, err := LoadProceduresAggregated(context.Background(), db, sessionID, AggregateOptions{
		EventNames:  []string{"SP:Completed"},
		SPIDs:       []int{728, 700, 179},
		GroupBySPID: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverRes := CompareProceduresRows(aggRows, opts)

	// file-mode
	events := make([]TRCEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, r.toEvent())
	}
	fileRes := CompareProcedures(events, opts)

	if len(serverRes.Procedures) != len(fileRes.Procedures) {
		t.Fatalf("server %d vs file %d procedures", len(serverRes.Procedures), len(fileRes.Procedures))
	}
	for i := range serverRes.Procedures {
		s, f := serverRes.Procedures[i], fileRes.Procedures[i]
		if s.Rank != f.Rank || s.Procedure != f.Procedure {
			t.Fatalf("row %d: server %d/%q vs file %d/%q", i, s.Rank, s.Procedure, f.Rank, f.Procedure)
		}
		if s.Focus != f.Focus {
			t.Fatalf("%s focus: server %+v vs file %+v", s.Procedure, s.Focus, f.Focus)
		}
		if len(s.Peers) != len(f.Peers) {
			t.Fatalf("%s peers len", s.Procedure)
		}
		for j := range s.Peers {
			if !reflect.DeepEqual(s.Peers[j], f.Peers[j]) {
				t.Fatalf("%s peer %d: server %+v vs file %+v", s.Procedure, j, s.Peers[j], f.Peers[j])
			}
		}
	}

	// контрольный GROUP BY для focus A
	var cnt int
	var total int64
	if err := db.QueryRow(
		`SELECT count(*), COALESCE(sum(duration_ms),0) FROM trc_events
		 WHERE session_id=$1 AND event_name='SP:Completed' AND procedure='A' AND spid=728`,
		sessionID,
	).Scan(&cnt, &total); err != nil {
		t.Fatal(err)
	}
	a := serverRes.Procedures[0]
	if a.Procedure != "A" || a.Focus.Count != cnt || a.Focus.TotalMs != total {
		t.Fatalf("A = %+v, want count=%d total=%d (control GROUP BY)", a, cnt, total)
	}
	// peer 179 для A — nullable-пустота
	if a.Peers[1].SPID != 179 || a.Peers[1].Count != 0 || a.Peers[1].AvgMs != nil {
		t.Fatalf("peer 179 = %+v, want nullable zeros", a.Peers[1])
	}
	// C отсутствует в ответе (Top-N только focus)
	for _, p := range serverRes.Procedures {
		if p.Procedure == "C" {
			t.Fatal("peer-only procedure C leaked into compare result")
		}
	}
}

// TestLoadSPIDSummaries_ParityWithFileMode — серверная сводка совпадает с
// file-mode: счётчики, границы, ошибка, мода identity с tie-break по id.
func TestLoadSPIDSummaries_ParityWithFileMode(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('spids-parity.trc', 10, 0) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	base := time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC)
	errCode := int32(50000)
	rows := []compareRow{
		{spid: 728, name: "SP:Completed", dur: 100, start: &base, app: "AppA"},
		{spid: 728, name: "SP:Completed", dur: 0, start: &base, app: "AppB"},
		{spid: 728, name: "RPC:Completed", dur: 50, start: &base, errCode: &errCode, app: "AppB"},
		{spid: 728, name: "SQL:BatchCompleted", dur: 10}, // без времени и app
		{spid: 700, name: "SP:Completed", dur: 5, app: "PeerApp"},
		{spid: 0, name: "SP:Completed", dur: 77}, // без SPID — исключается с обеих сторон
	}
	insertCompareRows(t, db, sessionID, rows)

	opts := SpidsOptions{SortBy: "event_count"}
	serverRes, err := LoadSPIDSummaries(context.Background(), db, sessionID, opts)
	if err != nil {
		t.Fatal(err)
	}

	events := make([]TRCEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, r.toEvent())
	}
	fileRes := SummarizeSPIDs(events, opts)

	if len(serverRes) != 2 || len(fileRes) != 2 {
		t.Fatalf("server %d vs file %d spids, want 2 (events without spid excluded)", len(serverRes), len(fileRes))
	}
	for i := range serverRes {
		s, f := serverRes[i], fileRes[i]
		if s.SPID != f.SPID || s.EventCount != f.EventCount {
			t.Fatalf("row %d: server %+v vs file %+v", i, s, f)
		}
		if s.SPCompletedCount != f.SPCompletedCount || s.RPCCompletedCount != f.RPCCompletedCount ||
			s.BatchCompletedCount != f.BatchCompletedCount || s.ErrorCount != f.ErrorCount ||
			s.MaxDurationMs != f.MaxDurationMs {
			t.Fatalf("counters %d: server %+v vs file %+v", i, s, f)
		}
		if (s.FirstTime == nil) != (f.FirstTime == nil) || (s.LastTime == nil) != (f.LastTime == nil) {
			t.Fatalf("time nullness %d: server %+v vs file %+v", i, s, f)
		}
		if s.FirstTime != nil && !s.FirstTime.Equal(*f.FirstTime) {
			t.Fatalf("first_time %d: %v vs %v", i, s.FirstTime, f.FirstTime)
		}
		if s.ApplicationName != f.ApplicationName {
			t.Fatalf("app %d: server %q vs file %q", i, s.ApplicationName, f.ApplicationName)
		}
	}
	// мода AppA/AppB по 2 вхождения... AppA: 1 (первая строка), AppB: 2 → AppB
	if serverRes[0].ApplicationName != "AppB" {
		t.Fatalf("app mode = %q, want AppB (2 occurrences)", serverRes[0].ApplicationName)
	}

	// временной фильтр: только событие с start_time внутри — NULL-время исключается
	from := base.Add(-time.Minute)
	filtered, err := LoadSPIDSummaries(context.Background(), db, sessionID, SpidsOptions{TimeFrom: &from})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range filtered {
		if s.SPID == 728 && s.EventCount != 3 {
			t.Fatalf("filtered event_count = %d, want 3 (start_time only)", s.EventCount)
		}
	}

	// LoadMaxDurationMs
	maxDur, err := LoadMaxDurationMs(context.Background(), db, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if maxDur != 100 {
		t.Fatalf("max duration = %d, want 100", maxDur)
	}
}

// TestSpidsAndCompareQueryPlans — планы и тайминги сводки SPID и compare-
// агрегации на объёмной сессии: без seq scan (индексы пригодны) и в разумное
// время.
func TestSpidsAndCompareQueryPlans(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('plans-big.trc', 10, 0) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)

	// 300K событий по 4 SPID, процедуры A..E
	if _, err := db.Exec(`
		INSERT INTO trc_events (session_id, event_class, event_name, spid, procedure, duration_ms, error, application_name)
		SELECT $1, 10,
		       CASE WHEN g % 10 = 0 THEN 'RPC:Completed' ELSE 'SP:Completed' END,
		       700 + (g % 4),
		       'Proc' || (g % 5),
		       (g * 7) % 10000,
		       CASE WHEN g % 1000 = 0 THEN 50000 ELSE NULL END,
		       'App' || (g % 3)
		FROM generate_series(1, 300000) AS g
	`, sessionID); err != nil {
		t.Fatalf("bulk insert: %v", err)
	}
	if _, err := db.Exec(`ANALYZE trc_events`); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// тайминги реальных функций
	start := time.Now()
	summaries, err := LoadSPIDSummaries(ctx, db, sessionID, SpidsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	spidsElapsed := time.Since(start)
	if len(summaries) != 4 {
		t.Fatalf("spids = %d, want 4", len(summaries))
	}

	start = time.Now()
	aggRows, err := LoadProceduresAggregated(ctx, db, sessionID, AggregateOptions{
		SPIDs:       []int{700, 701, 702},
		GroupBySPID: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	compareElapsed := time.Since(start)
	if len(aggRows) == 0 {
		t.Fatal("expected non-empty compare rows")
	}
	t.Logf("LoadSPIDSummaries: %s; LoadProceduresAggregated(GroupBySPID): %s", spidsElapsed, compareElapsed)
	if spidsElapsed > 10*time.Second || compareElapsed > 10*time.Second {
		t.Fatalf("too slow: spids=%s compare=%s", spidsElapsed, compareElapsed)
	}

	// планы без seq scan: запросы агрегаций используют индексы trc_events
	if _, err := db.Exec(`SET enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.Exec(`SET enable_seqscan = on`) }()

	spidsQuery := `SELECT spid, count(*), min(start_time), max(start_time),
	       count(*) FILTER (WHERE event_name = 'SP:Completed'),
	       count(*) FILTER (WHERE error IS NOT NULL AND error <> 0),
	       max(duration_ms)
	FROM trc_events WHERE session_id = 1 AND spid IS NOT NULL GROUP BY spid`
	compareQuery := `SELECT spid, procedure, count(*), sum(duration_ms), min(duration_ms), max(duration_ms)
	FROM trc_events WHERE session_id = 1 AND event_name = ANY(ARRAY['SP:Completed'])
	  AND procedure IS NOT NULL AND procedure <> '' AND spid = ANY(ARRAY[700,701,702]) AND spid IS NOT NULL
	GROUP BY spid, procedure`
	for _, q := range []string{spidsQuery, compareQuery} {
		rows, err := db.Query(`EXPLAIN (FORMAT TEXT) ` + q)
		if err != nil {
			t.Fatalf("explain: %v", err)
		}
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		rows.Close()
		if strings.Contains(plan.String(), "Seq Scan") {
			t.Fatalf("plan uses Seq Scan:\n%s", plan.String())
		}
		if !strings.Contains(plan.String(), "idx_trc_events_") && !strings.Contains(plan.String(), "trc_events_pkey") {
			t.Fatalf("plan uses no trc_events index:\n%s", plan.String())
		}
	}
}

// TestLoadMaxDurationMs_ZeroWhenNoDurations — все нули → 0 (недоступность).
func TestLoadMaxDurationMs_ZeroWhenNoDurations(t *testing.T) {
	db := testutil.Open(t)
	var sessionID int64
	if err := db.QueryRow(`INSERT INTO trc_sessions (file_path, file_size, total_events) VALUES ('nodur.trc', 10, 0) RETURNING id`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	defer DeleteSession(context.Background(), db, sessionID)
	insertCompareRows(t, db, sessionID, []compareRow{
		{spid: 728, name: "SP:Completed", dur: 0},
		{spid: 700, name: "SP:Completed", dur: 0},
	})
	maxDur, err := LoadMaxDurationMs(context.Background(), db, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if maxDur != 0 {
		t.Fatalf("max duration = %d, want 0", maxDur)
	}
}

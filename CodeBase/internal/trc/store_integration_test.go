//go:build integration

package trc

import (
	"context"
	"fmt"
	"testing"
	"time"

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

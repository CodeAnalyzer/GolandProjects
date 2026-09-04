//go:build integration

package trc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/codebase/internal/store/testutil"
)

// TestInsertTRCEvents_ParentIDMapping проверяет, что после SaveSession
// (insertTRCEvents с явным id) parent_id в БД содержит реальный id
// родительской строки, без post-insert маппинга.
func TestInsertTRCEvents_ParentIDMapping(t *testing.T) {
	db := testutil.Open(t)

	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Procedure: "ProcA", ParentID: -1, Depth: 0,
			Columns: map[int]any{12: int32(55)}},
		{EventClass: 43, EventName: "SP:Starting", Procedure: "ProcB", ParentID: 0, Depth: 1,
			Columns: map[int]any{12: int32(55)}},
		{EventClass: 43, EventName: "SP:Completed", Procedure: "ProcB", ParentID: 0, Depth: 1,
			Columns: map[int]any{12: int32(55)}},
		{EventClass: 11, EventName: "RPC:Completed", Procedure: "ProcA", ParentID: -1, Depth: 0,
			Columns: map[int]any{12: int32(55)}},
	}
	ComputeParentIDs(events)

	result := &TRCParseResult{
		Header:       &TraceHeader{},
		Events:       events,
		SourceFormat: "trc_binary",
	}

	sessionID, err := SaveSession(context.Background(), db, result, "test-mapping.trc", 100)
	if err != nil {
		t.Fatalf("SaveSession: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// Проверяем, что parent_id в БД = реальный id родителя.
	rows, err := db.Query(
		`SELECT id, parent_id, event_name FROM trc_events
		 WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()

	type dbRow struct {
		id       int64
		parentID *int64
		name     string
	}
	var dbRows []dbRow
	for rows.Next() {
		var r dbRow
		if err := rows.Scan(&r.id, &r.parentID, &r.name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		dbRows = append(dbRows, r)
	}
	if len(dbRows) != 4 {
		t.Fatalf("expected 4 events, got %d", len(dbRows))
	}

	// Events after ComputeParentIDs (proper Starting/Completed nesting):
	// 0: RPC:Starting (root, ParentID=-1)
	// 1: SP:Starting (child of 0, ParentID=0)
	// 2: SP:Completed (child of 0, ParentID=0 — pops SP, attaches to RPC)
	// 3: RPC:Completed (root, ParentID=-1 — pops RPC, stack empty)
	if dbRows[0].parentID != nil {
		t.Errorf("event 0 (%s): parent_id = %v, want NULL", dbRows[0].name, dbRows[0].parentID)
	}
	if dbRows[1].parentID == nil {
		t.Errorf("event 1 (%s): parent_id is NULL, want %d", dbRows[1].name, dbRows[0].id)
	} else if *dbRows[1].parentID != dbRows[0].id {
		t.Errorf("event 1 (%s): parent_id = %d, want %d", dbRows[1].name, *dbRows[1].parentID, dbRows[0].id)
	}
	if dbRows[2].parentID == nil {
		t.Errorf("event 2 (%s): parent_id is NULL, want %d", dbRows[2].name, dbRows[0].id)
	} else if *dbRows[2].parentID != dbRows[0].id {
		t.Errorf("event 2 (%s): parent_id = %d, want %d", dbRows[2].name, *dbRows[2].parentID, dbRows[0].id)
	}
	if dbRows[3].parentID != nil {
		t.Errorf("event 3 (%s): parent_id = %v, want NULL (root)", dbRows[3].name, dbRows[3].parentID)
	}
}

// TestInsertTRCEvents_ExplicitID_MultiBatch проверяет, что insertTRCEvents
// с явным id корректно вставляет parent_id при стриминг-режиме: 2 батча
// insertTRCEvents с одним baseID. Parent может находиться в другом батче.
func TestInsertTRCEvents_ExplicitID_MultiBatch(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 0) RETURNING id`,
		"test-streaming-multibatch.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// 3 события: root (ParentID=-1), child of root (ParentID=0), child of child (ParentID=1).
	// ComputeParentIDs вычисляет ParentID как 0-based индекс и EventIndex.
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Procedure: "ProcA", ParentID: -1, Depth: 0,
			Columns: map[int]any{12: int32(55)}},
		{EventClass: 43, EventName: "SP:Starting", Procedure: "ProcB", ParentID: 0, Depth: 1,
			Columns: map[int]any{12: int32(55)}},
		{EventClass: 45, EventName: "SP:StmtStarting", Procedure: "ProcC", ParentID: 1, Depth: 2,
			Columns: map[int]any{12: int32(55)}},
	}
	ComputeParentIDs(events)

	// Получаем baseID для явных id.
	ctx := context.Background()
	baseID, err := getBaseID(ctx, db)
	if err != nil {
		t.Fatalf("getBaseID: %v", err)
	}

	// Разбиваем на 2 батча: [0,1] и [2].
	// event[2] (ParentID=1) ссылается на event[1] из первого батча.
	batch1 := events[:2]
	batch2 := events[2:]

	if err := insertTRCEvents(ctx, db, batch1, sessionID, baseID); err != nil {
		t.Fatalf("insertTRCEvents batch1: %v", err)
	}
	if err := insertTRCEvents(ctx, db, batch2, sessionID, baseID); err != nil {
		t.Fatalf("insertTRCEvents batch2: %v", err)
	}

	// Проверяем parent_id в БД.
	rows, err := db.Query(
		`SELECT id, parent_id, event_name FROM trc_events
		 WHERE session_id = $1 ORDER BY id`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()

	type dbRow struct {
		id       int64
		parentID *int64
		name     string
	}
	var dbRows []dbRow
	for rows.Next() {
		var r dbRow
		if err := rows.Scan(&r.id, &r.parentID, &r.name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		dbRows = append(dbRows, r)
	}
	if len(dbRows) != 3 {
		t.Fatalf("expected 3 events, got %d", len(dbRows))
	}

	// event[0]: root, parent_id = NULL
	if dbRows[0].parentID != nil {
		t.Errorf("event 0 (%s): parent_id = %v, want NULL", dbRows[0].name, dbRows[0].parentID)
	}
	// event[1]: child of event[0], parent_id = id of event[0]
	if dbRows[1].parentID == nil {
		t.Errorf("event 1 (%s): parent_id is NULL, want %d", dbRows[1].name, dbRows[0].id)
	} else if *dbRows[1].parentID != dbRows[0].id {
		t.Errorf("event 1 (%s): parent_id = %d, want %d", dbRows[1].name, *dbRows[1].parentID, dbRows[0].id)
	}
	// event[2]: child of event[1] (cross-batch!), parent_id = id of event[1]
	if dbRows[2].parentID == nil {
		t.Errorf("event 2 (%s): parent_id is NULL, want %d (cross-batch)", dbRows[2].name, dbRows[1].id)
	} else if *dbRows[2].parentID != dbRows[1].id {
		t.Errorf("event 2 (%s): parent_id = %d, want %d (cross-batch)", dbRows[2].name, *dbRows[2].parentID, dbRows[1].id)
	}
}

// TestLoadEventsForTree_DirectJoinNoNumbered проверяет, что LoadEventsForTree
// корректно строит дерево из 3 уровней через прямой JOIN по parent_id.
func TestLoadEventsForTree_DirectJoinNoNumbered(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 6) RETURNING id`,
		"test-direct-join.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// Вставляем события и получаем их id.
	eventNames := []struct {
		name      string
		class     int
		spid      int
		parentID  *int64
		procedure string
	}{
		{"RPC:Starting", 10, 55, nil, "ProcA"},
		{"SP:Starting", 43, 55, nil, "ProcB"},
		{"SP:StmtStarting", 45, 55, nil, "ProcC"},
		{"SP:StmtCompleted", 45, 55, nil, "ProcC"},
		{"SP:Completed", 43, 55, nil, "ProcB"},
		{"RPC:Completed", 11, 55, nil, "ProcA"},
	}

	var ids []int64
	for _, e := range eventNames {
		var id int64
		err := db.QueryRow(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
			 VALUES ($1, $2, $3, $4, $5, $6, 0, 0) RETURNING id`,
			sessionID, e.class, e.name, e.spid, e.parentID, e.procedure,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert event %s: %v", e.name, err)
		}
		ids = append(ids, id)
	}

	// Устанавливаем parent_id как реальные id:
	// 0: RPC:Starting (root, parent_id = NULL)
	// 1: SP:Starting (child of 0)
	// 2: SP:StmtStarting (child of 1)
	// 3: SP:StmtCompleted (child of 1)
	// 4: SP:Completed (child of 0)
	// 5: RPC:Completed (root, parent_id = NULL)
	updates := []struct {
		idx       int
		parentID  *int64
		depth     int
	}{
		{0, nil, 0},
		{1, &ids[0], 1},
		{2, &ids[1], 2},
		{3, &ids[1], 2},
		{4, &ids[0], 1},
		{5, nil, 0},
	}
	for _, u := range updates {
		_, err := db.Exec(
			`UPDATE trc_events SET parent_id = $2, depth = $3 WHERE id = $1`,
			ids[u.idx], u.parentID, u.depth,
		)
		if err != nil {
			t.Fatalf("update parent_id for event %d: %v", u.idx, err)
		}
	}

	treeEvents, err := LoadEventsForTree(context.Background(), db, sessionID, 55, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}

	// Должны получить все 6 событий (2 корня + 4 ребёнка).
	if len(treeEvents) != 6 {
		t.Fatalf("expected 6 events, got %d", len(treeEvents))
	}

	// Проверяем, что корневые события (RPC:Starting, RPC:Completed) имеют ParentID = -1.
	roots := 0
	for _, ev := range treeEvents {
		if ev.ParentID == -1 {
			roots++
		}
	}
	if roots != 2 {
		t.Errorf("expected 2 root events, got %d", roots)
	}
}

// TestLoadEventsForTree_LargeSession_NoTimeout проверяет, что LoadEventsForTree
// с фильтром по SPID выполняется быстро для сессии с 10K+ событий.
func TestLoadEventsForTree_LargeSession_NoTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large session test in short mode")
	}
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, $2) RETURNING id`,
		"test-large.trc", 12000,
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// 3 SPID: 55 (10K событий), 66 (1K), 77 (1K)
	// SPID 55: 100 пар RPC:Starting/Completed с 50 дочерними SP:Starting/Completed каждая.
	for spid := 0; spid < 3; spid++ {
		actualSpid := 55 + spid*11
		count := 10000
		if spid > 0 {
			count = 1000
		}
		for i := 0; i < count; i++ {
			name := "RPC:Starting"
			if i%2 == 1 {
				name = "RPC:Completed"
			}
			_, err := db.Exec(
				`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
				 VALUES ($1, 10, $2, $3, NULL, $4, 0, 0)`,
				sessionID, name, actualSpid, fmt.Sprintf("Proc%d", i%100),
			)
			if err != nil {
				t.Fatalf("insert event %d: %v", i, err)
			}
		}
	}

	start := time.Now()
	_, err = LoadEventsForTree(context.Background(), db, sessionID, 55, 0, 0, "")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("LoadEventsForTree took %v, expected < 5s", elapsed)
	}
	t.Logf("LoadEventsForTree (12K events, SPID 55) completed in %v", elapsed)
}

// TestLoadEventsForTree_ProcedureFilter проверяет фильтрацию по имени процедуры.
func TestLoadEventsForTree_ProcedureFilter(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 6) RETURNING id`,
		"test-proc-filter.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// 3 корневых Starting-события с разными процедурами.
	// Children имеют другие procedure names чтобы избежать дубликатов в UNION ALL.
	procs := []string{"ProcA", "ProcB", "ProcC"}
	var ids []int64
	for _, proc := range procs {
		var id int64
		err := db.QueryRow(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
			 VALUES ($1, 10, 'RPC:Starting', 55, NULL, $2, 0, 0) RETURNING id`,
			sessionID, proc,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert event %s: %v", proc, err)
		}
		ids = append(ids, id)
	}
	// Добавляем Completed с другой procedure (child) для каждого корня.
	for i, proc := range procs {
		_, err := db.Exec(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
			 VALUES ($1, 11, 'RPC:Completed', 55, $2, $3, 0, 1)`,
			sessionID, ids[i], proc+"_Done",
		)
		if err != nil {
			t.Fatalf("insert completed event %s: %v", proc, err)
		}
	}

	// Запрос только для ProcB.
	treeEvents, err := LoadEventsForTree(context.Background(), db, sessionID, 55, 0, 0, "ProcB")
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}

	// Должны получить 2 события: RPC:Starting ProcB (anchor) + RPC:Completed ProcB_Done (recursive child).
	if len(treeEvents) != 2 {
		t.Fatalf("expected 2 events for ProcB, got %d", len(treeEvents))
	}
	// Хотя бы одно событие должно иметь procedure = ProcB.
	foundProcB := false
	for _, ev := range treeEvents {
		if ev.Procedure == "ProcB" {
			foundProcB = true
		}
	}
	if !foundProcB {
		t.Errorf("no event with procedure ProcB in results")
	}
}

// TestLoadEventsForTree_ParentChildSameSPID проверяет, что parent-child
// пары всегда на одном SPID — фильтр по SPID не ломает дерево.
func TestLoadEventsForTree_ParentChildSameSPID(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 4) RETURNING id`,
		"test-spid.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// SPID 55: RPC:Starting (root) + SP:Starting (child)
	// SPID 66: RPC:Starting (root) + SP:Starting (child)
	type ev struct {
		name     string
		spid     int
		proc     string
	}
	evtSpecs := []ev{
		{"RPC:Starting", 55, "ProcA"},
		{"SP:Starting", 55, "ProcB"},
		{"RPC:Starting", 66, "ProcC"},
		{"SP:Starting", 66, "ProcD"},
	}

	var ids []int64
	for _, e := range evtSpecs {
		var id int64
		err := db.QueryRow(
			`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
			 VALUES ($1, 10, $2, $3, NULL, $4, 0, 0) RETURNING id`,
			sessionID, e.name, e.spid, e.proc,
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert event %s: %v", e.name, err)
		}
		ids = append(ids, id)
	}

	// Set parent_id: SP:Starting (idx 1) → RPC:Starting (idx 0), same SPID 55
	// SP:Starting (idx 3) → RPC:Starting (idx 2), same SPID 66
	_, err = db.Exec(`UPDATE trc_events SET parent_id = $2, depth = 1 WHERE id = $1`, ids[1], ids[0])
	if err != nil {
		t.Fatalf("update parent_id: %v", err)
	}
	_, err = db.Exec(`UPDATE trc_events SET parent_id = $2, depth = 1 WHERE id = $1`, ids[3], ids[2])
	if err != nil {
		t.Fatalf("update parent_id: %v", err)
	}

	// Запрос для SPID 55 — должны получить 2 события.
	treeEvents55, err := LoadEventsForTree(context.Background(), db, sessionID, 55, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree SPID 55: %v", err)
	}
	if len(treeEvents55) != 2 {
		t.Errorf("SPID 55: expected 2 events, got %d", len(treeEvents55))
	}

	// Запрос для SPID 66 — должны получить 2 события.
	treeEvents66, err := LoadEventsForTree(context.Background(), db, sessionID, 66, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree SPID 66: %v", err)
	}
	if len(treeEvents66) != 2 {
		t.Errorf("SPID 66: expected 2 events, got %d", len(treeEvents66))
	}
}

// TestLoadEventsForTree_RootEventsHaveNullParent проверяет, что root-события
// имеют parent_id IS NULL и попадают в anchor CTE.
func TestLoadEventsForTree_RootEventsHaveNullParent(t *testing.T) {
	db := testutil.Open(t)

	var sessionID int64
	err := db.QueryRow(
		`INSERT INTO trc_sessions (file_path, file_size, total_events)
		 VALUES ($1, 10, 2) RETURNING id`,
		"test-null-parent.trc",
	).Scan(&sessionID)
	if err != nil {
		t.Fatalf("insert session: %v", err)
	}
	defer func() { _ = DeleteSession(context.Background(), db, sessionID) }()

	// 2 root события: RPC:Starting и RPC:Completed, parent_id = NULL.
	_, err = db.Exec(
		`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
		 VALUES ($1, 10, 'RPC:Starting', 55, NULL, 'ProcA', 0, 0)`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("insert RPC:Starting: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO trc_events (session_id, event_class, event_name, spid, parent_id, procedure, duration_ms, depth)
		 VALUES ($1, 11, 'RPC:Completed', 55, NULL, 'ProcA', 0, 0)`,
		sessionID,
	)
	if err != nil {
		t.Fatalf("insert RPC:Completed: %v", err)
	}

	// Проверяем в БД, что parent_id IS NULL.
	var nullCount int
	err = db.QueryRow(
		`SELECT count(*) FROM trc_events WHERE session_id = $1 AND parent_id IS NULL`,
		sessionID,
	).Scan(&nullCount)
	if err != nil {
		t.Fatalf("query null parent_id: %v", err)
	}
	if nullCount != 2 {
		t.Errorf("expected 2 events with NULL parent_id, got %d", nullCount)
	}

	// LoadEventsForTree должен вернуть оба события как корни.
	treeEvents, err := LoadEventsForTree(context.Background(), db, sessionID, 55, 0, 0, "")
	if err != nil {
		t.Fatalf("LoadEventsForTree: %v", err)
	}
	if len(treeEvents) != 2 {
		t.Fatalf("expected 2 root events, got %d", len(treeEvents))
	}
	for _, ev := range treeEvents {
		if ev.ParentID != -1 {
			t.Errorf("event %s: ParentID = %d, want -1 (root)", ev.EventName, ev.ParentID)
		}
	}
}

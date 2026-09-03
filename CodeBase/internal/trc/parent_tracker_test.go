package trc

import (
	"testing"
)

// makeTestEvent создаёт TRCEvent с заданными EventName и SPID.
func makeTestEvent(eventName string, spid int) TRCEvent {
	return TRCEvent{
		EventClass: 0,
		EventName:  eventName,
		Columns:    map[int]any{12: int32(spid)},
	}
}

// TestIncrementalParentTracker_MatchesComputeParentIDs сравнивает результаты
// IncrementalParentTracker с reference ComputeParentIDs на наборе событий
// с несколькими SPID и вложенными Starting/Completed парами.
func TestIncrementalParentTracker_MatchesComputeParentIDs(t *testing.T) {
	events := []TRCEvent{
		makeTestEvent("RPC:Starting", 51),
		makeTestEvent("SQL:BatchStarting", 51),
		makeTestEvent("SQL:StmtStarting", 51),
		makeTestEvent("SQL:StmtCompleted", 51),
		makeTestEvent("SP:Starting", 51),
		makeTestEvent("SP:StmtStarting", 51),
		makeTestEvent("SP:StmtCompleted", 51),
		makeTestEvent("SP:Completed", 51),
		makeTestEvent("SQL:BatchCompleted", 51),
		makeTestEvent("RPC:Completed", 51),
		// Другой SPID — независимое дерево
		makeTestEvent("SQL:BatchStarting", 60),
		makeTestEvent("SQL:StmtStarting", 60),
		makeTestEvent("SQL:StmtCompleted", 60),
		makeTestEvent("SQL:BatchCompleted", 60),
		// Событие без SPID
		{EventName: "Attention", Columns: map[int]any{}},
		// Ещё один SPID с непарным Starting
		makeTestEvent("RPC:Starting", 70),
		makeTestEvent("RPC:Completed", 70),
		makeTestEvent("SQL:BatchStarting", 70),
		// BatchCompleted без парного Starting (orphan)
		makeTestEvent("SQL:BatchCompleted", 70),
	}

	// Reference: ComputeParentIDs
	ref := make([]TRCEvent, len(events))
	copy(ref, events)
	ComputeParentIDs(ref)

	// Incremental
	inc := make([]TRCEvent, len(events))
	copy(inc, events)
	tracker := NewIncrementalParentTracker()
	for i := range inc {
		tracker.Process(&inc[i])
	}

	for i := range events {
		if inc[i].ParentID != ref[i].ParentID {
			t.Errorf("event %d (%s spid=%v): ParentID incremental=%d, reference=%d",
				i, events[i].EventName, events[i].Columns[12], inc[i].ParentID, ref[i].ParentID)
		}
		if inc[i].Depth != ref[i].Depth {
			t.Errorf("event %d (%s spid=%v): Depth incremental=%d, reference=%d",
				i, events[i].EventName, events[i].Columns[12], inc[i].Depth, ref[i].Depth)
		}
		if inc[i].EventIndex != ref[i].EventIndex {
			t.Errorf("event %d: EventIndex incremental=%d, reference=%d",
				i, inc[i].EventIndex, ref[i].EventIndex)
		}
	}
}

// TestIncrementalParentTracker_NoSPID_RootEvent — событие без SPID получает
// ParentID=-1, Depth=0.
func TestIncrementalParentTracker_NoSPID_RootEvent(t *testing.T) {
	tracker := NewIncrementalParentTracker()
	ev := TRCEvent{
		EventName: "Attention",
		Columns:   map[int]any{},
	}
	tracker.Process(&ev)
	if ev.ParentID != -1 {
		t.Errorf("ParentID = %d, want -1", ev.ParentID)
	}
	if ev.Depth != 0 {
		t.Errorf("Depth = %d, want 0", ev.Depth)
	}
	if ev.EventIndex != 0 {
		t.Errorf("EventIndex = %d, want 0", ev.EventIndex)
	}
}

// TestIncrementalParentTracker_NestedCalls проверяет корректность глубины
// при вложенных вызовах процедур.
func TestIncrementalParentTracker_NestedCalls(t *testing.T) {
	events := []TRCEvent{
		makeTestEvent("RPC:Starting", 51),    // idx=0, depth=1, parent=-1
		makeTestEvent("SP:Starting", 51),     // idx=1, depth=2, parent=0
		makeTestEvent("SP:StmtStarting", 51), // idx=2, depth=3, parent=1
		makeTestEvent("SP:StmtCompleted", 51), // idx=3, depth=3, parent=1
		makeTestEvent("SP:Completed", 51),    // idx=4, depth=2, parent=0
		makeTestEvent("RPC:Completed", 51),   // idx=5, depth=1, parent=-1
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}

	// RPC:Starting — root, depth=1 (ComputeParentIDs gives depth=0 for root
	// because stack is empty when Starting is processed, then it's pushed)
	// Actually in ComputeParentIDs: Starting with empty stack → ParentID=-1,
	// Depth=0 (stack is empty, no parent). Then pushed.
	// Let me check: in ComputeParentIDs, Starting:
	//   if len(stack) > 0: set parent/depth
	//   push frame
	// So root Starting: ParentID=-1, Depth=0.
	// SP:Starting: stack has RPC → ParentID=0, Depth=1.
	// SP:StmtStarting: stack has RPC,SP → ParentID=1, Depth=2.
	// SP:StmtCompleted: pop SP (matching), then stack has RPC → ParentID=0, Depth=1.
	// SP:Completed: pop SP... wait, SP was already popped by StmtCompleted?
	// No: SP:StmtCompleted has family "SP:Stmt", SP:Starting has family "SP".
	// So SP:StmtCompleted pops "SP:Stmt" frame, not "SP".

	// Let me trace carefully:
	// idx=0 RPC:Starting → stack empty → ParentID=-1, Depth=0; push {RPC, 0}
	// idx=1 SP:Starting → stack=[RPC] → ParentID=0, Depth=1; push {SP, 1}
	// idx=2 SP:StmtStarting → stack=[RPC,SP] → ParentID=1, Depth=2; push {SP:Stmt, 2}
	// idx=3 SP:StmtCompleted → family="SP:Stmt", pop {SP:Stmt}; stack=[RPC,SP] → ParentID=1, Depth=2
	// idx=4 SP:Completed → family="SP", pop {SP}; stack=[RPC] → ParentID=0, Depth=1
	// idx=5 RPC:Completed → family="RPC", pop {RPC}; stack=[] → ParentID=-1, Depth=0

	want := []struct {
		parentID int
		depth    int
	}{
		{-1, 0}, // RPC:Starting (root, stack empty → depth=0)
		{0, 1},  // SP:Starting (parent RPC depth=0 → depth=1)
		{1, 2},  // SP:StmtStarting (parent SP depth=1 → depth=2)
		{1, 2},  // SP:StmtCompleted (pop SP:Stmt, parent SP depth=1 → depth=2)
		{0, 1},  // SP:Completed (pop SP, parent RPC depth=0 → depth=1)
		{-1, 0}, // RPC:Completed (pop RPC, stack empty → depth=0)
	}

	for i, w := range want {
		if events[i].ParentID != w.parentID {
			t.Errorf("event %d (%s): ParentID=%d, want %d", i, events[i].EventName, events[i].ParentID, w.parentID)
		}
		if events[i].Depth != w.depth {
			t.Errorf("event %d (%s): Depth=%d, want %d", i, events[i].EventName, events[i].Depth, w.depth)
		}
	}
}

// TestIncrementalParentTracker_CompletedOnlyFallback — SPID с только Completed
// событиями получает ParentID/Depth через интервальный fallback после Flush.
func TestIncrementalParentTracker_CompletedOnlyFallback(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(11, "RPC:Completed", 88, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 88, "ProcB", st(10, 1, 0), st(10, 4, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 88, "ProcC", st(10, 2, 0), st(10, 3, 0)),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("RPC:Completed: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	if events[1].ParentID != 0 || events[1].Depth != 1 {
		t.Errorf("SP:Completed: ParentID=%d, Depth=%d, want 0/1", events[1].ParentID, events[1].Depth)
	}
	if events[2].ParentID != 1 || events[2].Depth != 2 {
		t.Errorf("SP:StmtCompleted: ParentID=%d, Depth=%d, want 1/2", events[2].ParentID, events[2].Depth)
	}
}

// TestIncrementalParentTracker_CompletedOnly_RealWorldPattern — паттерн из
// trc-tree-flat.txt: SP:Completed + SP:StmtCompleted с одинаковыми интервалами.
func TestIncrementalParentTracker_CompletedOnly_RealWorldPattern(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 122, "MassDoc_Add", st(9, 42, 55), st(9, 43, 24)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 122, "MassDoc_Add", st(9, 42, 55), st(9, 43, 24)),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("SP:Completed: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	if events[1].ParentID != 0 || events[1].Depth != 1 {
		t.Errorf("SP:StmtCompleted: ParentID=%d, Depth=%d, want 0/1", events[1].ParentID, events[1].Depth)
	}
}

// TestIncrementalParentTracker_CompletedOnly_SameRankSiblings — одинаковый
// EventClass с одинаковым интервалом → siblings (оба root).
func TestIncrementalParentTracker_CompletedOnly_SameRankSiblings(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcB", st(10, 0, 0), st(10, 5, 0)),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	if events[0].ParentID != -1 {
		t.Errorf("ProcA: ParentID=%d, want -1 (root)", events[0].ParentID)
	}
	if events[1].ParentID != -1 {
		t.Errorf("ProcB: ParentID=%d, want -1 (root)", events[1].ParentID)
	}
}

// TestIncrementalParentTracker_CompletedOnlyDifferentSPID — события из разных
// SPID не образуют parent-child.
func TestIncrementalParentTracker_CompletedOnlyDifferentSPID(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 100, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 200, "ProcB", st(10, 1, 0), st(10, 4, 0)),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("SPID 100: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	if events[1].ParentID != -1 || events[1].Depth != 0 {
		t.Errorf("SPID 200: ParentID=%d, Depth=%d, want -1/0", events[1].ParentID, events[1].Depth)
	}
}

// TestIncrementalParentTracker_MixedSPID_CompletedAndNormal — один SPID с
// Starting, другой Completed-only. Проверка что fallback применяется только
// к Completed-only SPID.
func TestIncrementalParentTracker_MixedSPID_CompletedAndNormal(t *testing.T) {
	events := []TRCEvent{
		// SPID 50 — normal mode (Starting/Completed pairs)
		makeTestEvent("RPC:Starting", 50),
		makeTestEvent("SP:StmtStarting", 50),
		makeTestEvent("SP:StmtCompleted", 50),
		makeTestEvent("RPC:Completed", 50),
		// SPID 60 — completed-only mode
		makeCompletedEventWithTime(43, "SP:Completed", 60, "ProcX", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 60, "ProcY", st(10, 1, 0), st(10, 4, 0)),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	// SPID 50 — normal mode: RPC:Starting root, SP:StmtStarting child
	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("SPID 50 RPC:Starting: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	if events[1].ParentID != 0 || events[1].Depth != 1 {
		t.Errorf("SPID 50 SP:StmtStarting: ParentID=%d, Depth=%d, want 0/1", events[1].ParentID, events[1].Depth)
	}

	// SPID 60 — fallback: SP:Completed root, SP:StmtCompleted child
	if events[4].ParentID != -1 || events[4].Depth != 0 {
		t.Errorf("SPID 60 SP:Completed: ParentID=%d, Depth=%d, want -1/0", events[4].ParentID, events[4].Depth)
	}
	if events[5].ParentID != 4 || events[5].Depth != 1 {
		t.Errorf("SPID 60 SP:StmtCompleted: ParentID=%d, Depth=%d, want 4/1", events[5].ParentID, events[5].Depth)
	}
}

// TestIncrementalParentTracker_CompletedOnly_MatchesComputeParentIDs —
// сравнение IncrementalParentTracker с ComputeParentIDs на Completed-only
// событиях.
func TestIncrementalParentTracker_CompletedOnly_MatchesComputeParentIDs(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(11, "RPC:Completed", 88, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 88, "ProcB", st(10, 1, 0), st(10, 4, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 88, "ProcC", st(10, 2, 0), st(10, 3, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 88, "ProcD", st(10, 1, 0), st(10, 2, 0)),
	}

	// Reference: ComputeParentIDs
	ref := make([]TRCEvent, len(events))
	copy(ref, events)
	ComputeParentIDs(ref)

	// Incremental
	inc := make([]TRCEvent, len(events))
	copy(inc, events)
	tracker := NewIncrementalParentTracker()
	for i := range inc {
		tracker.Process(&inc[i])
	}
	tracker.Flush()

	for i := range events {
		if inc[i].ParentID != ref[i].ParentID {
			t.Errorf("event %d (%s): ParentID incremental=%d, reference=%d",
				i, events[i].EventName, inc[i].ParentID, ref[i].ParentID)
		}
		if inc[i].Depth != ref[i].Depth {
			t.Errorf("event %d (%s): Depth incremental=%d, reference=%d",
				i, events[i].EventName, inc[i].Depth, ref[i].Depth)
		}
	}
}

// TestIncrementalParentTracker_StartingAfterCompleted — если после Completed-only
// событий приходит Starting, трекер переключается в normal mode.
func TestIncrementalParentTracker_StartingAfterCompleted(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 55, "ProcA", st(10, 0, 0), st(10, 1, 0)),
		makeTestEvent("RPC:Starting", 55),
		makeTestEvent("RPC:Completed", 55),
	}

	tracker := NewIncrementalParentTracker()
	for i := range events {
		tracker.Process(&events[i])
	}
	tracker.Flush()

	// First event (SP:Completed) was in pending buffer, flushed with interval.
	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("SP:Completed: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	// RPC:Starting — normal mode, root.
	if events[1].ParentID != -1 || events[1].Depth != 0 {
		t.Errorf("RPC:Starting: ParentID=%d, Depth=%d, want -1/0", events[1].ParentID, events[1].Depth)
	}
	// RPC:Completed — pops RPC, root.
	if events[2].ParentID != -1 || events[2].Depth != 0 {
		t.Errorf("RPC:Completed: ParentID=%d, Depth=%d, want -1/0", events[2].ParentID, events[2].Depth)
	}
}

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

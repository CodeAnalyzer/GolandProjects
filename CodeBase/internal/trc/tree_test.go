package trc

import (
	"strings"
	"testing"
)

// TestBuildTrees_StartingCompletedNesting — проверка вложенности по
// Starting/Completed парам: RPC:Starting → SP:StmtStarting → SP:StmtCompleted → RPC:Completed.
func TestBuildTrees_StartingCompletedNesting(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(51)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc", DurationMs: 50},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(51)}, DurationMs: 100},
	}
	trees := BuildTrees(events)
	roots, ok := trees[51]
	if !ok {
		t.Fatalf("no tree for SPID 51, got SPIDs: %v", trees)
	}
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	root := roots[0]
	if root.Start.EventName != "RPC:Starting" {
		t.Errorf("root event = %q, want RPC:Starting", root.Start.EventName)
	}
	if root.End == nil || root.End.EventName != "RPC:Completed" {
		t.Errorf("root end = %v, want RPC:Completed", root.End)
	}
	if len(root.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(root.Children))
	}
	child := root.Children[0]
	if child.Start.EventName != "SP:StmtStarting" {
		t.Errorf("child event = %q, want SP:StmtStarting", child.Start.EventName)
	}
	if child.End == nil || child.End.EventName != "SP:StmtCompleted" {
		t.Errorf("child end = %v, want SP:StmtCompleted", child.End)
	}
	if len(child.Children) != 0 {
		t.Errorf("expected 0 grandchildren, got %d", len(child.Children))
	}
}

// TestBuildTrees_MultipleSPIDs — события разных SPID попадают в разные деревья.
func TestBuildTrees_MultipleSPIDs(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(2)}},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(2)}},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(1)}},
	}
	trees := BuildTrees(events)
	if len(trees) != 2 {
		t.Fatalf("expected 2 SPIDs, got %d", len(trees))
	}
	if len(trees[1]) != 1 || len(trees[2]) != 1 {
		t.Errorf("expected 1 root per SPID, got %d and %d", len(trees[1]), len(trees[2]))
	}
}

// TestBuildTrees_UnmatchedCompleted — Completed без парного Starting
// становится отдельным узлом на текущем уровне.
func TestBuildTrees_UnmatchedCompleted(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(7)}},
	}
	trees := BuildTrees(events)
	roots := trees[7]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].Start.EventName != "RPC:Completed" {
		t.Errorf("root = %q, want RPC:Completed", roots[0].Start.EventName)
	}
	if roots[0].End != nil {
		t.Errorf("expected nil End for unmatched Completed")
	}
}

// TestBuildTrees_SimpleEvent — diagnostic-событие без Starting/Completed
// контекста (например, SP:Recompile) не становится корневым узлом.
func TestBuildTrees_SimpleEvent(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 37, EventName: "SP:Recompile", Columns: map[int]any{12: int32(9)}},
	}
	trees := BuildTrees(events)
	roots := trees[9]
	if len(roots) != 0 {
		t.Fatalf("expected 0 roots (diagnostic without parent), got %d", len(roots))
	}
}

// TestFormatTrees_Output — проверка текстового вывода.
func TestFormatTrees_Output(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(1)}, Procedure: "MyProc"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(1)}, Procedure: "MyProc", DurationMs: 42},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(1)}, DurationMs: 100},
	}
	trees := BuildTrees(events)
	out := FormatTrees(trees)
	if !strings.Contains(out, "SPID 1:") {
		t.Errorf("output missing 'SPID 1:', got:\n%s", out)
	}
	if !strings.Contains(out, "RPC:Starting") {
		t.Errorf("output missing 'RPC:Starting', got:\n%s", out)
	}
	if !strings.Contains(out, "exec MyProc") {
		t.Errorf("output missing 'exec MyProc', got:\n%s", out)
	}
	if !strings.Contains(out, "[100ms]") {
		t.Errorf("output missing '[100ms]', got:\n%s", out)
	}
}

// TestBuildTreesWithDepth_MaxDepth1 — max_depth=1 оставляет только корни без детей.
func TestBuildTreesWithDepth_MaxDepth1(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(51)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(51)}},
	}
	trees := BuildTreesWithDepth(events, 1)
	root := trees[51][0]
	if len(root.Children) != 0 {
		t.Fatalf("max_depth=1: expected 0 children, got %d", len(root.Children))
	}
}

// TestBuildTreesWithDepth_MaxDepth2 — max_depth=2 оставляет 1 уровень детей, но без внуков.
func TestBuildTreesWithDepth_MaxDepth2(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(51)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 40, EventName: "SQL:StmtStarting", Columns: map[int]any{12: int32(51)}, Procedure: "DeepProc"},
		{EventClass: 41, EventName: "SQL:StmtCompleted", Columns: map[int]any{12: int32(51)}, Procedure: "DeepProc"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(51)}},
	}
	trees := BuildTreesWithDepth(events, 2)
	root := trees[51][0]
	if len(root.Children) != 1 {
		t.Fatalf("max_depth=2: expected 1 child, got %d", len(root.Children))
	}
	child := root.Children[0]
	if len(child.Children) != 0 {
		t.Fatalf("max_depth=2: expected 0 grandchildren, got %d", len(child.Children))
	}
}

// TestBuildTreesWithDepth_Unlimited — max_depth=0 = без ограничений (как BuildTrees).
func TestBuildTreesWithDepth_Unlimited(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(51)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(51)}, Procedure: "InnerProc"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(51)}},
	}
	trees0 := BuildTreesWithDepth(events, 0)
	treesDefault := BuildTrees(events)
	if len(trees0[51][0].Children) != len(treesDefault[51][0].Children) {
		t.Errorf("max_depth=0 should equal BuildTrees")
	}
}

// TestLimitTrees_RootLimit — limit обрезает количество корневых узлов.
func TestLimitTrees_RootLimit(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 13, EventName: "SQL:BatchStarting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 12, EventName: "SQL:BatchCompleted", Columns: map[int]any{12: int32(1)}},
		{EventClass: 13, EventName: "SQL:BatchStarting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 12, EventName: "SQL:BatchCompleted", Columns: map[int]any{12: int32(1)}},
		{EventClass: 13, EventName: "SQL:BatchStarting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 12, EventName: "SQL:BatchCompleted", Columns: map[int]any{12: int32(1)}},
	}
	trees := BuildTrees(events)
	if len(trees[1]) != 3 {
		t.Fatalf("expected 3 roots, got %d", len(trees[1]))
	}
	LimitTrees(trees, 2)
	if len(trees[1]) != 2 {
		t.Fatalf("limit=2: expected 2 roots, got %d", len(trees[1]))
	}
}

// TestLimitTrees_ChildLimit — limit обрезает количество детей каждого узла.
func TestLimitTrees_ChildLimit(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(1)}},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(1)}, Procedure: "P1"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(1)}, Procedure: "P1"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(1)}, Procedure: "P2"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(1)}, Procedure: "P2"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(1)}, Procedure: "P3"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(1)}, Procedure: "P3"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(1)}},
	}
	trees := BuildTrees(events)
	root := trees[1][0]
	if len(root.Children) != 3 {
		t.Fatalf("expected 3 children, got %d", len(root.Children))
	}
	LimitTrees(trees, 2)
	if len(root.Children) != 2 {
		t.Fatalf("limit=2: expected 2 children, got %d", len(root.Children))
	}
}

// TestFilterTreesByProcedure — дерево ProcA→ProcB→ProcC, фильтр по ProcB
// возвращает поддерево ProcB с ProcC.
func TestFilterTreesByProcedure(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcB"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcC"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(55)}, Procedure: "ProcC"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(55)}, Procedure: "ProcB"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
	}
	trees := BuildTrees(events)
	filtered := FilterTreesByProcedure(trees, "ProcB")
	roots, ok := filtered[55]
	if !ok || len(roots) != 1 {
		t.Fatalf("expected 1 root for ProcB in SPID 55, got %v", filtered)
	}
	if roots[0].Start.Procedure != "ProcB" {
		t.Errorf("root procedure = %q, want ProcB", roots[0].Start.Procedure)
	}
	if len(roots[0].Children) != 1 {
		t.Fatalf("expected 1 child (ProcC), got %d", len(roots[0].Children))
	}
	if roots[0].Children[0].Start.Procedure != "ProcC" {
		t.Errorf("child procedure = %q, want ProcC", roots[0].Children[0].Start.Procedure)
	}
}

// TestFilterTreesByProcedure_MultipleMatches — несколько вызовов одной процедуры
// в одном SPID возвращают несколько поддеревьев.
func TestFilterTreesByProcedure_MultipleMatches(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(55)}, Procedure: "Target"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(55)}, Procedure: "Target"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(55)}, Procedure: "Target"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(55)}, Procedure: "Target"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
	}
	trees := BuildTrees(events)
	filtered := FilterTreesByProcedure(trees, "Target")
	roots, ok := filtered[55]
	if !ok || len(roots) != 2 {
		t.Fatalf("expected 2 roots for Target, got %d", len(roots))
	}
}

// TestFilterTreesByProcedure_NotFound — процедура не найдена → пустой результат.
func TestFilterTreesByProcedure_NotFound(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
	}
	trees := BuildTrees(events)
	filtered := FilterTreesByProcedure(trees, "NonExistent")
	if len(filtered) != 0 {
		t.Fatalf("expected empty map, got %v", filtered)
	}
}

// TestFilterTreesByProcedure_EmptyProcedure — пустая процедура возвращает trees без изменений.
func TestFilterTreesByProcedure_EmptyProcedure(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
	}
	trees := BuildTrees(events)
	filtered := FilterTreesByProcedure(trees, "")
	if len(filtered) != len(trees) {
		t.Fatalf("empty procedure should return trees unchanged, got %v", filtered)
	}
}

// TestBuildSPIDTree_DiagnosticNoRoot — diagnostic-события (SP:Recompile,
// SQL:StmtRecompile) перед любым Starting не должны становиться корневыми узлами.
func TestBuildSPIDTree_DiagnosticNoRoot(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 37, EventName: "SP:Recompile", Columns: map[int]any{12: int32(76)}},
		{EventClass: 166, EventName: "SQL:StmtRecompile", Columns: map[int]any{12: int32(76)}},
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(76)}, Procedure: "ProcA"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(76)}, Procedure: "ProcA", DurationMs: 100},
	}
	trees := BuildTrees(events)
	roots, ok := trees[76]
	if !ok {
		t.Fatalf("no tree for SPID 76, got SPIDs: %v", trees)
	}
	if len(roots) != 1 {
		t.Fatalf("expected 1 root (RPC:Starting), got %d roots", len(roots))
	}
	if roots[0].Start.EventName != "RPC:Starting" {
		t.Errorf("root event = %q, want RPC:Starting", roots[0].Start.EventName)
	}
}

// TestBuildSPIDTree_DiagnosticInsideCall — diagnostic-событие между Starting
// и Completed сохраняется как ребёнок узла Starting.
func TestBuildSPIDTree_DiagnosticInsideCall(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 37, EventName: "SP:Recompile", Columns: map[int]any{12: int32(55)}},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA", DurationMs: 100},
	}
	trees := BuildTrees(events)
	roots, ok := trees[55]
	if !ok {
		t.Fatalf("no tree for SPID 55, got SPIDs: %v", trees)
	}
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	root := roots[0]
	if root.Start.EventName != "RPC:Starting" {
		t.Errorf("root event = %q, want RPC:Starting", root.Start.EventName)
	}
	if len(root.Children) != 1 {
		t.Fatalf("expected 1 child (SP:Recompile), got %d", len(root.Children))
	}
	if root.Children[0].Start.EventName != "SP:Recompile" {
		t.Errorf("child event = %q, want SP:Recompile", root.Children[0].Start.EventName)
	}
}

// makeCompletedEventWithTime создаёт TRCEvent Completed с заданными временами.
func makeCompletedEventWithTime(eventClass int, eventName string, spid int, proc string, start, end SystemTime) TRCEvent {
	return TRCEvent{
		EventClass: eventClass,
		EventName:  eventName,
		Columns:    map[int]any{12: int32(spid), 14: start, 15: end},
		Procedure:  proc,
	}
}

func st(h, m, s int) SystemTime {
	return SystemTime{Year: 2026, Month: 1, Day: 1, Hour: uint16(h), Minute: uint16(m), Second: uint16(s)}
}

// TestEventClassRank — проверка иерархии EventClass.
func TestEventClassRank(t *testing.T) {
	tests := []struct {
		eventClass int
		want       int
	}{
		{11, 4},  // RPC:Completed
		{43, 3},  // SP:Completed
		{45, 2},  // SP:StmtCompleted
		{12, 1},  // SQL:BatchCompleted
		{41, 0},  // SQL:StmtCompleted
		{99, -1}, // unknown
	}
	for _, tt := range tests {
		got := eventClassRank(tt.eventClass)
		if got != tt.want {
			t.Errorf("eventClassRank(%d) = %d, want %d", tt.eventClass, got, tt.want)
		}
	}
}

// TestHasStartingEvents — проверка детекции Starting-событий.
func TestHasStartingEvents(t *testing.T) {
	completedOnly := []*TRCEvent{
		{EventName: "SP:Completed", Columns: map[int]any{12: int32(1)}},
		{EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(1)}},
	}
	if hasStartingEvents(completedOnly) {
		t.Error("expected false for Completed-only events")
	}

	mixed := []*TRCEvent{
		{EventName: "SP:Completed", Columns: map[int]any{12: int32(1)}},
		{EventName: "SP:Starting", Columns: map[int]any{12: int32(1)}},
	}
	if !hasStartingEvents(mixed) {
		t.Error("expected true for mixed events")
	}
}

// TestBuildTrees_CompletedOnlyNesting — 3 уровня вложенности (RPC → SP → SP:Stmt).
func TestBuildTrees_CompletedOnlyNesting(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(11, "RPC:Completed", 88, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 88, "ProcB", st(10, 1, 0), st(10, 4, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 88, "ProcC", st(10, 2, 0), st(10, 3, 0)),
	}
	trees := BuildTrees(events)
	roots := trees[88]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].Start.EventName != "RPC:Completed" {
		t.Errorf("root = %q, want RPC:Completed", roots[0].Start.EventName)
	}
	if len(roots[0].Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(roots[0].Children))
	}
	child := roots[0].Children[0]
	if child.Start.EventName != "SP:Completed" {
		t.Errorf("child = %q, want SP:Completed", child.Start.EventName)
	}
	if len(child.Children) != 1 {
		t.Fatalf("expected 1 grandchild, got %d", len(child.Children))
	}
	grandchild := child.Children[0]
	if grandchild.Start.EventName != "SP:StmtCompleted" {
		t.Errorf("grandchild = %q, want SP:StmtCompleted", grandchild.Start.EventName)
	}
}

// TestBuildTrees_CompletedOnly_RealWorldPattern — паттерн из trc-tree-flat.txt:
// SP:Completed + SP:StmtCompleted с совпадающими интервалами.
func TestBuildTrees_CompletedOnly_RealWorldPattern(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 122, "MassDoc_Add", st(9, 42, 55), st(9, 43, 24)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 122, "MassDoc_Add", st(9, 42, 55), st(9, 43, 24)),
	}
	trees := BuildTrees(events)
	roots := trees[122]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].Start.EventName != "SP:Completed" {
		t.Errorf("root = %q, want SP:Completed", roots[0].Start.EventName)
	}
	if len(roots[0].Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(roots[0].Children))
	}
	if roots[0].Children[0].Start.EventName != "SP:StmtCompleted" {
		t.Errorf("child = %q, want SP:StmtCompleted", roots[0].Children[0].Start.EventName)
	}
}

// TestBuildTrees_CompletedOnly_OverlappingNotNested — перекрывающиеся, но не
// вложенные интервалы → siblings (оба root).
func TestBuildTrees_CompletedOnly_OverlappingNotNested(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 77, "ProcA", st(10, 0, 0), st(10, 3, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 77, "ProcB", st(10, 1, 0), st(10, 4, 0)),
	}
	trees := BuildTrees(events)
	roots := trees[77]
	if len(roots) != 2 {
		t.Fatalf("expected 2 roots (siblings), got %d", len(roots))
	}
}

// TestBuildTrees_CompletedOnly_SameEventClassSameInterval — одинаковый EventClass
// с одинаковым интервалом → siblings.
func TestBuildTrees_CompletedOnly_SameEventClassSameInterval(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcB", st(10, 0, 0), st(10, 5, 0)),
	}
	trees := BuildTrees(events)
	roots := trees[99]
	if len(roots) != 2 {
		t.Fatalf("expected 2 roots (same rank = siblings), got %d", len(roots))
	}
}

// TestBuildTrees_CompletedOnly_DifferentSPIDNoNesting — события из разных SPID
// не образуют parent-child, даже если интервалы вложены.
func TestBuildTrees_CompletedOnly_DifferentSPIDNoNesting(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 100, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 200, "ProcB", st(10, 1, 0), st(10, 4, 0)),
	}
	trees := BuildTrees(events)
	if len(trees) != 2 {
		t.Fatalf("expected 2 SPIDs, got %d", len(trees))
	}
	if len(trees[100]) != 1 || trees[100][0].Start.Procedure != "ProcA" {
		t.Errorf("SPID 100 should have 1 root ProcA")
	}
	if len(trees[200]) != 1 || trees[200][0].Start.Procedure != "ProcB" {
		t.Errorf("SPID 200 should have 1 root ProcB")
	}
}

// TestBuildTrees_MixedStartingCompleted_NoFallback — при наличии Starting
// используется стековый алгоритм, не интервальный.
func TestBuildTrees_MixedStartingCompleted_NoFallback(t *testing.T) {
	events := []TRCEvent{
		{EventClass: 10, EventName: "RPC:Starting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
		{EventClass: 44, EventName: "SP:StmtStarting", Columns: map[int]any{12: int32(55)}, Procedure: "ProcB"},
		{EventClass: 45, EventName: "SP:StmtCompleted", Columns: map[int]any{12: int32(55)}, Procedure: "ProcB"},
		{EventClass: 11, EventName: "RPC:Completed", Columns: map[int]any{12: int32(55)}, Procedure: "ProcA"},
	}
	trees := BuildTrees(events)
	roots := trees[55]
	if len(roots) != 1 {
		t.Fatalf("expected 1 root, got %d", len(roots))
	}
	if roots[0].Start.EventName != "RPC:Starting" {
		t.Errorf("root = %q, want RPC:Starting (stack algorithm)", roots[0].Start.EventName)
	}
	if len(roots[0].Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(roots[0].Children))
	}
}

// TestComputeParentIDs_IntervalNesting — 3 события (RPC → SP → SP:Stmt) с
// вложенными интервалами, проверка ParentID и Depth.
func TestComputeParentIDs_IntervalNesting(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(11, "RPC:Completed", 88, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 88, "ProcB", st(10, 1, 0), st(10, 4, 0)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 88, "ProcC", st(10, 2, 0), st(10, 3, 0)),
	}
	ComputeParentIDs(events)
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

// TestComputeParentIDs_CompletedOnlySPID — SPID с только Completed-событиями
// получает корректные ParentID/Depth через интервальный fallback.
func TestComputeParentIDs_CompletedOnlySPID(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 122, "ProcA", st(9, 42, 55), st(9, 43, 24)),
		makeCompletedEventWithTime(45, "SP:StmtCompleted", 122, "MassDoc_Add", st(9, 42, 55), st(9, 43, 24)),
	}
	ComputeParentIDs(events)
	if events[0].ParentID != -1 || events[0].Depth != 0 {
		t.Errorf("SP:Completed: ParentID=%d, Depth=%d, want -1/0", events[0].ParentID, events[0].Depth)
	}
	if events[1].ParentID != 0 || events[1].Depth != 1 {
		t.Errorf("SP:StmtCompleted: ParentID=%d, Depth=%d, want 0/1", events[1].ParentID, events[1].Depth)
	}
}

// TestComputeParentIDs_OverlappingNotNested — перекрывающиеся, но не вложенные
// интервалы → siblings (оба root).
func TestComputeParentIDs_OverlappingNotNested(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 77, "ProcA", st(10, 0, 0), st(10, 3, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 77, "ProcB", st(10, 1, 0), st(10, 4, 0)),
	}
	ComputeParentIDs(events)
	if events[0].ParentID != -1 {
		t.Errorf("ProcA: ParentID=%d, want -1 (root)", events[0].ParentID)
	}
	if events[1].ParentID != -1 {
		t.Errorf("ProcB: ParentID=%d, want -1 (root)", events[1].ParentID)
	}
}

// TestComputeParentIDs_SameEventClassSameInterval — одинаковый EventClass с
// одинаковым интервалом → siblings.
func TestComputeParentIDs_SameEventClassSameInterval(t *testing.T) {
	events := []TRCEvent{
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcA", st(10, 0, 0), st(10, 5, 0)),
		makeCompletedEventWithTime(43, "SP:Completed", 99, "ProcB", st(10, 0, 0), st(10, 5, 0)),
	}
	ComputeParentIDs(events)
	if events[0].ParentID != -1 {
		t.Errorf("ProcA: ParentID=%d, want -1 (root)", events[0].ParentID)
	}
	if events[1].ParentID != -1 {
		t.Errorf("ProcB: ParentID=%d, want -1 (root)", events[1].ParentID)
	}
}

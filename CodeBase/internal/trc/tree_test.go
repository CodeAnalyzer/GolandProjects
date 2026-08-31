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

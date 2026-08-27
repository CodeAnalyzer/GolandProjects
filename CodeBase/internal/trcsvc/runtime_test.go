package trcsvc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

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
	if result.TotalCount <= 0 {
		t.Errorf("TotalCount = %d, want > 0", result.TotalCount)
	}
	if result.FilteredCount > 10 {
		t.Errorf("FilteredCount = %d, want <= 10", result.FilteredCount)
	}
	if len(result.Events) > 10 {
		t.Errorf("Events len = %d, want <= 10", len(result.Events))
	}
}

func TestExecuteEvents_FileMode_WithEventNameFilter(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteEvents(ctx, nil, EventsParams{
		Source:    SessionSource{FilePath: p},
		EventName: "RPC:Completed",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("ExecuteEvents error: %v", err)
	}
	for _, ev := range result.Events {
		if ev.EventName != "RPC:Completed" {
			t.Errorf("EventName = %q, want RPC:Completed", ev.EventName)
		}
	}
}

func TestExecuteProcedures_FileMode(t *testing.T) {
	p := trcTestPath(t)
	ctx := context.Background()
	result, err := ExecuteProcedures(ctx, nil, SessionSource{FilePath: p})
	if err != nil {
		t.Fatalf("ExecuteProcedures error: %v", err)
	}
	if result.Count <= 0 {
		t.Errorf("Count = %d, want > 0", result.Count)
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
	procResult, err := ExecuteProcedures(ctx, nil, SessionSource{FilePath: p})
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

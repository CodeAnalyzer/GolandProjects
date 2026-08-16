package rtisvc

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// writeTempFile creates a temp file with the given content and returns its path.
func writeTempFile(t *testing.T, suffix, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "test"+suffix)
	if err := os.WriteFile(p, []byte(content), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}

const rtiSample = "04.03.2026 16:59:03.173\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t349\t357325\t\t0\t245\n" +
	"Enter Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
	"Elapsed, ms: 17\n" +
	"@InterfaceObjectID             : DSIDENTIFIER                   = 161\n" +
	"04.03.2026 16:59:03.203\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t349\t357327\t\t0\t196\n" +
	"Exit Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 39\n" +
	"Elapsed, ms: 34\n" +
	"Return 0\n" +
	"04.03.2026 16:59:03.237\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357338\t\t0\t1264\n" +
	"Enter DealProtocol_Select @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
	"Elapsed, ms: 0\n" +
	"04.03.2026 16:59:03.250\tINFO\tTrace.Server.Proc\t\tActionPlan_CheckAction\t349\t357346\t\t0\t705\n" +
	"Enter ActionPlan_CheckAction @@TranCount = 0 @@NestLevel = 2 @@DsSysModuleID = 10\n" +
	"Elapsed, ms: 0\n" +
	"04.03.2026 16:59:03.267\tINFO\tTrace.Server.Proc\t\tActionPlan_CheckAction\t349\t357349\t\t0\t268\n" +
	"Exit ActionPlan_CheckAction @@TranCount = 0 @@NestLevel = 2@BeginCnt = 0 @@DsSysModuleID = 10\n" +
	"Elapsed, ms: 16\n" +
	"Return 0\n" +
	"04.03.2026 16:59:03.267\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357351\t\t0\t119\n" +
	"Exit DealProtocol_Select @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
	"Elapsed, ms: 30\n" +
	"Return 0\n"

func TestExecuteParse_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteParse(ctx, nil, p)
	if err != nil {
		t.Fatalf("ExecuteParse error: %v", err)
	}
	if result.SessionID != 0 {
		t.Errorf("SessionID = %d, want 0 (no DB)", result.SessionID)
	}
	if result.TotalCalls != 3 {
		t.Errorf("TotalCalls = %d, want 3", result.TotalCalls)
	}
	if result.Summary.TotalCalls != 3 {
		t.Errorf("Summary.TotalCalls = %d, want 3", result.Summary.TotalCalls)
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
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteSummary(ctx, nil, SessionSource{FilePath: p})
	if err != nil {
		t.Fatalf("ExecuteSummary error: %v", err)
	}
	if result.Summary.TotalCalls != 3 {
		t.Errorf("Summary.TotalCalls = %d, want 3", result.Summary.TotalCalls)
	}
}

func TestExecuteSummary_NoSource(t *testing.T) {
	_, err := ExecuteSummary(context.Background(), nil, SessionSource{})
	if err == nil {
		t.Fatal("expected error for empty source, got nil")
	}
}

func TestExecuteTree_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "DealProtocol_Select",
		MaxDepth:  0,
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}
	if result.Tree == nil {
		t.Fatal("expected non-nil tree")
	}
	if result.Tree.Call == nil {
		t.Fatal("expected non-nil root call")
	}
	if result.Tree.Call.Procedure != "DealProtocol_Select" {
		t.Errorf("Tree.Call.Procedure = %q, want DealProtocol_Select", result.Tree.Call.Procedure)
	}
}

func TestExecuteTree_NotFound(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteTree(ctx, nil, TreeParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "NonExistentProc",
		MaxDepth:  0,
	})
	if err != nil {
		t.Fatalf("ExecuteTree error: %v", err)
	}
	if result.Tree != nil {
		t.Fatal("expected nil tree for non-existent procedure")
	}
}

func TestExecuteErrors_FileMode_NoErrors(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteErrors(ctx, nil, ErrorsParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteErrors error: %v", err)
	}
	if result.ServerErrorCount != 0 {
		t.Errorf("ServerErrorCount = %d, want 0", result.ServerErrorCount)
	}
	if result.ClientErrorCount != 0 {
		t.Errorf("ClientErrorCount = %d, want 0", result.ClientErrorCount)
	}
}

func TestExecuteErrors_FileMode_WithErrors(t *testing.T) {
	content := "04.03.2026 16:59:03.173\tINFO\tTrace.Server.Proc\t\tFailingProc\t349\t357325\t\t0\t245\n" +
		"Enter FailingProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 17\n" +
		"04.03.2026 16:59:03.203\tINFO\tTrace.Server.Proc\t\tFailingProc\t349\t357327\t\t0\t196\n" +
		"Exit FailingProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 34\n" +
		"Return 500\n"
	p := writeTempFile(t, ".rti", content)
	ctx := context.Background()
	result, err := ExecuteErrors(ctx, nil, ErrorsParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteErrors error: %v", err)
	}
	if result.ServerErrorCount != 1 {
		t.Errorf("ServerErrorCount = %d, want 1", result.ServerErrorCount)
	}
	if result.ServerErrors[0].Procedure != "FailingProc" {
		t.Errorf("ServerErrors[0].Procedure = %q, want FailingProc", result.ServerErrors[0].Procedure)
	}
}

func TestExecuteSlow_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteSlow(ctx, nil, SlowParams{
		Source:      SessionSource{FilePath: p},
		ThresholdMs: 20,
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("ExecuteSlow error: %v", err)
	}
	// DealProtocol_Select (30ms) and Cons_Get_ProtocolNumber (34ms) should be slow
	if result.ServerCallCount != 2 {
		t.Errorf("ServerCallCount = %d, want 2", result.ServerCallCount)
	}
	if result.Threshold != 20 {
		t.Errorf("Threshold = %d, want 20", result.Threshold)
	}
	// Should be sorted descending by elapsed
	if result.ServerCalls[0].ElapsedMs < result.ServerCalls[1].ElapsedMs {
		t.Error("expected descending sort by elapsed time")
	}
}

func TestExecuteSlow_FileMode_NoSlowCalls(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteSlow(ctx, nil, SlowParams{
		Source:      SessionSource{FilePath: p},
		ThresholdMs: 100,
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("ExecuteSlow error: %v", err)
	}
	if result.ServerCallCount != 0 {
		t.Errorf("ServerCallCount = %d, want 0", result.ServerCallCount)
	}
}

func TestExecuteDetails_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteDetails(ctx, nil, DetailsParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "Cons_Get_ProtocolNumber",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("ExecuteDetails error: %v", err)
	}
	if result.Count != 1 {
		t.Errorf("Count = %d, want 1", result.Count)
	}
	if result.Procedure != "Cons_Get_ProtocolNumber" {
		t.Errorf("Procedure = %q, want Cons_Get_ProtocolNumber", result.Procedure)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("Calls len = %d, want 1", len(result.Calls))
	}
	if result.Calls[0].Procedure != "Cons_Get_ProtocolNumber" {
		t.Errorf("Calls[0].Procedure = %q, want Cons_Get_ProtocolNumber", result.Calls[0].Procedure)
	}
}

func TestExecuteDetails_FileMode_NotFound(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteDetails(ctx, nil, DetailsParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "NonExistentProc",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("ExecuteDetails error: %v", err)
	}
	if result.Count != 0 {
		t.Errorf("Count = %d, want 0", result.Count)
	}
}

func TestExecuteBlog_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteBlog(ctx, nil, BlogParams{
		Source:    SessionSource{FilePath: p},
		Procedure: "Cons_Get_ProtocolNumber",
		Limit:     100,
	})
	if err != nil {
		t.Fatalf("ExecuteBlog error: %v", err)
	}
	if result.Count != 1 {
		t.Errorf("Count = %d, want 1", result.Count)
	}
	if result.Procedure != "Cons_Get_ProtocolNumber" {
		t.Errorf("Procedure = %q, want Cons_Get_ProtocolNumber", result.Procedure)
	}
}

func TestExecuteClientTree_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteClientTree(ctx, nil, ClientTreeParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteClientTree error: %v", err)
	}
	// No client events in this sample
	if result.FilteredEventsCount != 0 {
		t.Errorf("FilteredEventsCount = %d, want 0", result.FilteredEventsCount)
	}
}

func TestExecuteTimeline_FileMode(t *testing.T) {
	p := writeTempFile(t, ".rti", rtiSample)
	ctx := context.Background()
	result, err := ExecuteTimeline(ctx, nil, TimelineParams{
		Source: SessionSource{FilePath: p},
		Limit:  100,
	})
	if err != nil {
		t.Fatalf("ExecuteTimeline error: %v", err)
	}
	if result.FilteredCallsCount != 3 {
		t.Errorf("FilteredCallsCount = %d, want 3", result.FilteredCallsCount)
	}
	if result.FilteredEventsCount != 0 {
		t.Errorf("FilteredEventsCount = %d, want 0", result.FilteredEventsCount)
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

func TestToCallSlims_Empty(t *testing.T) {
	got := toCallSlims(nil)
	if got != nil {
		t.Errorf("toCallSlims(nil) = %v, want nil", got)
	}
}

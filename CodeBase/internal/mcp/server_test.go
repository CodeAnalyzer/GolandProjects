package mcp

import (
	"context"
	"encoding/json"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestToolRegistryContainsPing(t *testing.T) {
	tool, ok := toolRegistry["codebase_ping"]
	if !ok {
		t.Fatal("codebase_ping not in toolRegistry")
	}
	if tool.Definition.Name != "codebase_ping" {
		t.Fatalf("unexpected name: %s", tool.Definition.Name)
	}
}

func TestToolsListIncludesPing(t *testing.T) {
	list := tools()
	found := false
	for _, tool := range list {
		if tool.Name == "codebase_ping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("codebase_ping not in tools()")
	}
}

func TestPingHandlerReturnsOK(t *testing.T) {
	tool, ok := toolRegistry["codebase_ping"]
	if !ok {
		t.Fatal("codebase_ping not in toolRegistry")
	}
	result, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	m, ok := result.(map[string]interface{})
	if !ok {
		t.Fatalf("unexpected result type: %T", result)
	}
	if v, _ := m["ok"].(bool); !v {
		t.Fatal("expected ok=true in ping response")
	}
}

func TestUnknownToolNotInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_unknown"]; ok {
		t.Fatal("unexpected tool codebase_unknown found in registry")
	}
}

func TestDecodeSDKToolArgsNil(t *testing.T) {
	args, err := decodeSDKToolArgs(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args == nil {
		t.Fatal("expected non-nil args map")
	}
}

func TestDecodeSDKToolArgsValid(t *testing.T) {
	raw, _ := json.Marshal(map[string]interface{}{"name": "foo"})
	req := &mcpsdk.CallToolRequest{}
	req.Params = &mcpsdk.CallToolParamsRaw{Arguments: raw}
	args, err := decodeSDKToolArgs(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["name"] != "foo" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestRTIToolsInRegistry(t *testing.T) {
	expected := []string{
		"codebase_rti_parse",
		"codebase_rti_list",
		"codebase_rti_summary",
		"codebase_rti_tree",
		"codebase_rti_errors",
		"codebase_rti_slow",
		"codebase_rti_details",
		"codebase_rti_delete",
		"codebase_rti_prune",
	}
	for _, name := range expected {
		if _, ok := toolRegistry[name]; !ok {
			t.Errorf("tool %s not in registry", name)
		}
	}
}

func TestRTIToolsListIncludesAll(t *testing.T) {
	list := tools()
	got := make(map[string]bool)
	for _, tool := range list {
		got[tool.Name] = true
	}
	expected := []string{
		"codebase_rti_parse",
		"codebase_rti_list",
		"codebase_rti_summary",
		"codebase_rti_tree",
		"codebase_rti_errors",
		"codebase_rti_slow",
		"codebase_rti_details",
		"codebase_rti_delete",
		"codebase_rti_prune",
	}
	for _, name := range expected {
		if !got[name] {
			t.Errorf("tool %s not in tools()", name)
		}
	}
}

func TestRTIParseHandlerRequiresFilePath(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_parse"]
	if !ok {
		t.Fatal("codebase_rti_parse not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing file_path")
	}
}

func TestRTISlowHandlerRequiresSessionOrFile(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_slow"]
	if !ok {
		t.Fatal("codebase_rti_slow not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing session_id and file_path")
	}
}

func TestRTIDetailsHandlerRequiresProcedure(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_details"]
	if !ok {
		t.Fatal("codebase_rti_details not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing procedure")
	}
}

func TestRetCodeToolInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_query_retcode"]; !ok {
		t.Fatal("codebase_query_retcode not in toolRegistry")
	}
}

func TestRetCodeHandlerRequiresArg(t *testing.T) {
	tool, ok := toolRegistry["codebase_query_retcode"]
	if !ok {
		t.Fatal("codebase_query_retcode not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	// Without DB, runQueryOpt returns "database not available"
	if err == nil {
		t.Fatal("expected error for no args without DB")
	}
}

func TestReadMoreToolInRegistry(t *testing.T) {
	if _, ok := toolRegistry["codebase_read_more"]; !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
}

func TestReadMoreInToolsList(t *testing.T) {
	list := tools()
	for _, tool := range list {
		if tool.Name == "codebase_read_more" {
			return
		}
	}
	t.Fatal("codebase_read_more not in tools()")
}

func TestReadMoreHandlerRequiresContinuationID(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing continuation_id")
	}
}

func TestReadMoreHandlerUnknownID(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "does-not-exist",
		"chunk":           float64(2),
	})
	if err == nil {
		t.Fatal("expected error for unknown continuation_id")
	}
}

func TestReadMoreHandlerRequiresChunk(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "abc123",
	})
	if err == nil {
		t.Fatal("expected error when chunk is missing")
	}
}

func TestReadMoreHandlerChunkWrongType(t *testing.T) {
	tool, ok := toolRegistry["codebase_read_more"]
	if !ok {
		t.Fatal("codebase_read_more not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"continuation_id": "abc123",
		"chunk":           "abc",
	})
	if err == nil {
		t.Fatal("expected error for non-integer chunk")
	}
}

func TestRTIPruneHandlerRejectsNegativeKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_prune"]
	if !ok {
		t.Fatal("codebase_rti_prune not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(-1),
	})
	if err == nil {
		t.Fatal("expected error for negative keep_last")
	}
}

func TestRTIPruneHandlerAcceptsZeroKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_prune"]
	if !ok {
		t.Fatal("codebase_rti_prune not in toolRegistry")
	}
	// keep_last=0 is now valid (TRUNCATE all). Without DB, it will fail
	// with "database not available", but NOT with "keep_last must be positive".
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(0),
	})
	if err != nil && err.Error() == "keep_last must be >= 0" {
		t.Fatalf("keep_last=0 should be accepted, got: %v", err)
	}
}

func TestTRCToolsInRegistry(t *testing.T) {
	expected := []string{
		"codebase_trc_parse",
		"codebase_trc_list",
		"codebase_trc_summary",
		"codebase_trc_tree",
		"codebase_trc_errors",
		"codebase_trc_slow",
		"codebase_trc_events",
		"codebase_trc_procedures",
		"codebase_trc_delete",
		"codebase_trc_prune",
	}
	for _, name := range expected {
		if _, ok := toolRegistry[name]; !ok {
			t.Errorf("tool %s not in registry", name)
		}
	}
}

func TestTRCPruneHandlerRejectsNegativeKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_prune"]
	if !ok {
		t.Fatal("codebase_trc_prune not in toolRegistry")
	}
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(-1),
	})
	if err == nil {
		t.Fatal("expected error for negative keep_last")
	}
}

func TestTRCPruneHandlerAcceptsZeroKeepLast(t *testing.T) {
	tool, ok := toolRegistry["codebase_trc_prune"]
	if !ok {
		t.Fatal("codebase_trc_prune not in toolRegistry")
	}
	// keep_last=0 is now valid (TRUNCATE all). Without DB, it will fail
	// with "database not available", but NOT with "keep_last must be positive".
	_, err := tool.Handler(context.Background(), map[string]interface{}{
		"keep_last": float64(0),
	})
	if err != nil && err.Error() == "keep_last must be >= 0" {
		t.Fatalf("keep_last=0 should be accepted, got: %v", err)
	}
}

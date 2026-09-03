package mcp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/codebase/internal/config"
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

func newSDKTestClientSession(t *testing.T, profile string) *mcpsdk.ClientSession {
	t.Helper()
	config.CreateDefault("")
	server := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "codebase-test", Version: "test"}, nil)
	registry, err := buildToolRegistryForProfile(nil, profile)
	if err != nil {
		t.Fatalf("buildToolRegistryForProfile: %v", err)
	}
	registerSDKCoreTools(server, registry, profile, nil)
	serverTransport, clientTransport := mcpsdk.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = serverSession.Close() })
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "client-test", Version: "test"}, nil)
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = clientSession.Close() })
	return clientSession
}

func TestSDKToolsList_NoOutputSchema(t *testing.T) {
	for _, profile := range []string{"", "query", "rti", "trc", "review"} {
		t.Run("profile="+profile, func(t *testing.T) {
			cs := newSDKTestClientSession(t, profile)
			res, err := cs.ListTools(context.Background(), &mcpsdk.ListToolsParams{})
			if err != nil {
				t.Fatalf("ListTools: %v", err)
			}
			if len(res.Tools) == 0 {
				t.Fatal("no tools registered")
			}
			for _, tool := range res.Tools {
				if tool.OutputSchema != nil {
					t.Errorf("tool %s declares outputSchema", tool.Name)
				}
			}
		})
	}
}

func TestSDKCallTool_Ping_NoValidationError(t *testing.T) {
	cs := newSDKTestClientSession(t, "")
	res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{Name: "codebase_ping"})
	if err != nil {
		t.Fatalf("CallTool codebase_ping: %v", err)
	}
	if res.IsError {
		t.Fatal("unexpected isError result for codebase_ping")
	}
	if len(res.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d", len(res.Content))
	}
	if _, ok := res.Content[0].(*mcpsdk.TextContent); !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if res.StructuredContent != nil {
		t.Fatal("expected no structuredContent in ping result")
	}
}

func TestSDKCallTool_ErrorPath_TextOnly(t *testing.T) {
	cs := newSDKTestClientSession(t, "rti")
	res, err := cs.CallTool(context.Background(), &mcpsdk.CallToolParams{
		Name:      "codebase_rti_parse",
		Arguments: map[string]interface{}{"file_path": filepath.Join(t.TempDir(), "missing.rti")},
	})
	if err != nil {
		t.Fatalf("CallTool must not fail with protocol error, got: %v", err)
	}
	if !res.IsError {
		t.Fatal("expected isError=true for missing file")
	}
	if len(res.Content) != 1 {
		t.Fatalf("expected exactly 1 content block, got %d", len(res.Content))
	}
	if _, ok := res.Content[0].(*mcpsdk.TextContent); !ok {
		t.Fatalf("expected TextContent, got %T", res.Content[0])
	}
	if res.StructuredContent != nil {
		t.Fatal("expected no structuredContent in error result")
	}
}

func TestTimeoutForTool_ParseTRC(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_trc_parse", cfg)
	want := 300 * time.Second
	if got != want {
		t.Fatalf("codebase_trc_parse: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_ParseRTI(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_rti_parse", cfg)
	want := 300 * time.Second
	if got != want {
		t.Fatalf("codebase_rti_parse: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_ReviewSQL(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	got := timeoutForTool("codebase_review_sql", cfg)
	want := 120 * time.Second
	if got != want {
		t.Fatalf("codebase_review_sql: got %v, want %v", got, want)
	}
}

func TestTimeoutForTool_DefaultQuery(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 30, ReviewTimeoutSec: 120},
		TRC: config.TRCConfig{ParseTimeoutSec: 300},
		RTI: config.RTIConfig{ParseTimeoutSec: 300},
	}
	for _, name := range []string{
		"codebase_query_symbol",
		"codebase_trc_summary",
		"codebase_rti_tree",
		"codebase_ping",
	} {
		got := timeoutForTool(name, cfg)
		want := 30 * time.Second
		if got != want {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
	}
}

func TestTimeoutForTool_ZeroDisablesTimeout(t *testing.T) {
	cfg := &config.Config{
		MCP: config.MCPConfig{QueryTimeoutSec: 0, ReviewTimeoutSec: 0},
		TRC: config.TRCConfig{ParseTimeoutSec: 0},
		RTI: config.RTIConfig{ParseTimeoutSec: 0},
	}
	for _, name := range []string{
		"codebase_query_symbol",
		"codebase_trc_parse",
		"codebase_rti_parse",
		"codebase_review_sql",
	} {
		got := timeoutForTool(name, cfg)
		if got != 0 {
			t.Fatalf("%s: got %v, want 0", name, got)
		}
	}
}

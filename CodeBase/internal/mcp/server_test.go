package mcp

import (
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
	result, err := tool.Handler(map[string]interface{}{})
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
	_, err := tool.Handler(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing file_path")
	}
}

func TestRTISlowHandlerRequiresSessionOrFile(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_slow"]
	if !ok {
		t.Fatal("codebase_rti_slow not in toolRegistry")
	}
	_, err := tool.Handler(map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for missing session_id and file_path")
	}
}

func TestRTIDetailsHandlerRequiresProcedure(t *testing.T) {
	tool, ok := toolRegistry["codebase_rti_details"]
	if !ok {
		t.Fatal("codebase_rti_details not in toolRegistry")
	}
	_, err := tool.Handler(map[string]interface{}{})
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
	_, err := tool.Handler(map[string]interface{}{})
	// Without DB, runQueryOpt returns "database not available"
	if err == nil {
		t.Fatal("expected error for no args without DB")
	}
}

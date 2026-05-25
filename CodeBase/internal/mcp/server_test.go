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

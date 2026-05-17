package mcp

import (
	"encoding/json"
	"testing"
)

func TestHandleRequestInitialize(t *testing.T) {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage("1"),
		Method:  "initialize",
	}

	resp := handleRequest(req, "0.7.1")
	if resp == nil {
		t.Fatal("expected response")
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %+v", resp.Error)
	}

	result, ok := resp.Result.(initializeResult)
	if !ok {
		t.Fatalf("unexpected result type: %T", resp.Result)
	}
	if result.ServerInfo.Name != "codebase-mcp" {
		t.Fatalf("unexpected server name: %s", result.ServerInfo.Name)
	}
	if result.ServerInfo.Version != "0.7.1" {
		t.Fatalf("unexpected server version: %s", result.ServerInfo.Version)
	}
}

func TestHandleRequestToolsListIncludesPing(t *testing.T) {
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage("2"),
		Method:  "tools/list",
	}

	resp := handleRequest(req, "0.7.1")
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %+v", resp)
	}

	result, ok := resp.Result.(listToolsResult)
	if !ok {
		t.Fatalf("unexpected result type: %T", resp.Result)
	}

	found := false
	for _, tool := range result.Tools {
		if tool.Name == "codebase.ping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("codebase.ping tool is not registered")
	}
}

func TestHandleRequestToolsCallPing(t *testing.T) {
	params := `{"name":"codebase.ping","arguments":{}}`
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage("3"),
		Method:  "tools/call",
		Params:  json.RawMessage(params),
	}

	resp := handleRequest(req, "0.7.1")
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %+v", resp)
	}

	result, ok := resp.Result.(callToolResult)
	if !ok {
		t.Fatalf("unexpected result type: %T", resp.Result)
	}
	if result.IsError {
		t.Fatal("expected non-error tool result")
	}
	if len(result.Content) != 1 {
		t.Fatalf("unexpected content length: %d", len(result.Content))
	}
	if result.Content[0].Type != "text" {
		t.Fatalf("unexpected content type: %s", result.Content[0].Type)
	}
}

func TestHandleRequestToolsCallUnknownToolReturnsToolError(t *testing.T) {
	params := `{"name":"codebase.unknown","arguments":{}}`
	req := rpcRequest{
		JSONRPC: "2.0",
		ID:      json.RawMessage("4"),
		Method:  "tools/call",
		Params:  json.RawMessage(params),
	}

	resp := handleRequest(req, "0.7.1")
	if resp == nil || resp.Error != nil {
		t.Fatalf("unexpected response: %+v", resp)
	}

	result, ok := resp.Result.(callToolResult)
	if !ok {
		t.Fatalf("unexpected result type: %T", resp.Result)
	}
	if !result.IsError {
		t.Fatal("expected tool error")
	}
}

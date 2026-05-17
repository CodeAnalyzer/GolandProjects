package mcp

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// RunStdio запускает минимальный MCP JSON-RPC сервер поверх stdin/stdout.
// stdout используется только для protocol-сообщений.
func RunStdio(serverVersion string) error {
	decoder := json.NewDecoder(bufio.NewReader(os.Stdin))
	encoder := json.NewEncoder(os.Stdout)

	for {
		var req rpcRequest
		if err := decoder.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		resp := handleRequest(req, serverVersion)
		if resp == nil {
			continue
		}
		if err := encoder.Encode(resp); err != nil {
			return err
		}
	}
}

func handleRequest(req rpcRequest, serverVersion string) *rpcResponse {
	id := decodeID(req.ID)

	switch req.Method {
	case "initialize":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result: initializeResult{
				ProtocolVersion: protocolVersion,
				ServerInfo: serverInfo{
					Name:    "codebase-mcp",
					Version: serverVersion,
				},
				Capabilities: map[string]interface{}{
					"tools": map[string]interface{}{
						"listChanged": false,
					},
				},
			},
		}
	case "notifications/initialized":
		return nil
	case "tools/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result: listToolsResult{Tools: tools()},
		}
	case "tools/call":
		var params callToolParams
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return rpcInvalidParams(id, "invalid tools/call params")
			}
		}
		result, err := handleToolCall(params)
		if err != nil {
			return &rpcResponse{
				JSONRPC: "2.0",
				ID:      id,
				Result: callToolResult{
					Content: []toolContent{{Type: "text", Text: err.Error()}},
					IsError: true,
				},
			}
		}
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result:  result,
		}
	default:
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Error: &rpcError{
				Code:    -32601,
				Message: "method not found",
			},
		}
	}
}

func rpcInvalidParams(id interface{}, message string) *rpcResponse {
	return &rpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    -32602,
			Message: message,
		},
	}
}

func decodeID(raw json.RawMessage) interface{} {
	if len(raw) == 0 {
		return nil
	}
	var generic interface{}
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil
	}
	return generic
}

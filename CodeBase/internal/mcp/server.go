package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// RunStdio запускает минимальный MCP JSON-RPC сервер поверх stdin/stdout.
// stdout используется только для protocol-сообщений.
func RunStdio(serverVersion string) error {
	server := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "codebase",
		Version: serverVersion,
	}, nil)

	registerSDKCoreTools(server)

	return server.Run(context.Background(), &mcpsdk.StdioTransport{})
}

func registerSDKCoreTools(server *mcpsdk.Server) {
	for _, tool := range toolRegistry {
		server.AddTool(&mcpsdk.Tool{
			Name:         tool.Definition.Name,
			Description:  tool.Definition.Description,
			InputSchema:  tool.Definition.InputSchema,
			OutputSchema: defaultToolOutputSchema,
		}, func(ctx context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			args, err := decodeSDKToolArgs(req)
			if err != nil {
				return sdkToolErrorResult(err), nil
			}

			result, err := tool.Handler(args)
			if err != nil {
				return sdkToolErrorResult(err), nil
			}
			return sdkToolJSONResult(result)
		})
	}

	registerSDKEmptyFeatures(server)
}

func registerSDKEmptyFeatures(server *mcpsdk.Server) {
	server.AddPrompt(&mcpsdk.Prompt{
		Name: "empty",
	}, func(ctx context.Context, req *mcpsdk.GetPromptRequest) (*mcpsdk.GetPromptResult, error) {
		return &mcpsdk.GetPromptResult{Messages: []*mcpsdk.PromptMessage{}}, nil
	})

	server.AddResource(&mcpsdk.Resource{
		Name: "empty",
		URI:  "file:///empty",
	}, func(ctx context.Context, req *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		return &mcpsdk.ReadResourceResult{Contents: []*mcpsdk.ResourceContents{}}, nil
	})
}

func decodeSDKToolArgs(req *mcpsdk.CallToolRequest) (map[string]interface{}, error) {
	args := map[string]interface{}{}
	if req == nil || req.Params == nil || len(req.Params.Arguments) == 0 {
		return args, nil
	}
	if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
		return nil, fmt.Errorf("invalid tool arguments: %w", err)
	}
	return args, nil
}

func sdkToolJSONResult(value interface{}) (*mcpsdk.CallToolResult, error) {
	text, err := toJSONText(value)
	if err != nil {
		return nil, err
	}
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}},
	}, nil
}

func sdkToolErrorResult(err error) *mcpsdk.CallToolResult {
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: err.Error()}},
		IsError: true,
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
					"experimental": map[string]interface{}{},
					"prompts": map[string]interface{}{
						"listChanged": false,
					},
					"resources": map[string]interface{}{
						"subscribe":   false,
						"listChanged": false,
					},
					"tools": map[string]interface{}{
						"listChanged": false,
					},
				},
			},
		}
	case "notifications/initialized":
		return nil
	case "prompts/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result: map[string]interface{}{
				"prompts": []interface{}{},
			},
		}
	case "resources/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result: map[string]interface{}{
				"resources": []interface{}{},
			},
		}
	case "resources/templates/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result: map[string]interface{}{
				"resourceTemplates": []interface{}{},
			},
		}
	case "tools/list":
		return &rpcResponse{
			JSONRPC: "2.0",
			ID:      id,
			Result:  listToolsResult{Tools: tools()},
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

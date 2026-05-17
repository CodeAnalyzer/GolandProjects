package mcp

import "fmt"

func handleToolCall(params callToolParams) (callToolResult, error) {
	tool, ok := toolRegistry[params.Name]
	if !ok {
		return callToolResult{}, fmt.Errorf("unknown tool: %s", params.Name)
	}
	result, err := tool.Handler(params.Arguments)
	if err != nil {
		return callToolResult{}, err
	}
	text, err := toJSONText(result)
	if err != nil {
		return callToolResult{}, err
	}
	return callToolResult{
		Content: []toolContent{{Type: "text", Text: text}},
		IsError: false,
	}, nil
}

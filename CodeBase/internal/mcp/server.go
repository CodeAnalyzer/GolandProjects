package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/codebase/internal/config"
	"github.com/codebase/internal/store"
)

// RunStdio запускает минимальный MCP JSON-RPC сервер поверх stdin/stdout.
// stdout используется только для protocol-сообщений.
// logger — опциональный логгер для записи длительности каждого tool-вызова; nil отключает логирование.
func RunStdio(serverVersion string, profile string, logger *log.Logger) error {
	cfg := config.Get()
	if cfg == nil {
		return fmt.Errorf("config not loaded")
	}

	if cfg.MCP.PaginationChunkSize > 0 {
		globalPages = newPageStore(cfg.MCP.PaginationChunkSize)
	}
	globalPages.startGCLoop()
	defer globalPages.stopGCLoop()

	db, err := store.NewDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}

	server := mcpsdk.NewServer(&mcpsdk.Implementation{
		Name:    "codebase",
		Version: serverVersion,
	}, nil)

	registry, err := buildToolRegistryForProfile(db, profile)
	if err != nil {
		return err
	}
	registerSDKCoreTools(server, registry, profile, logger)

	return server.Run(context.Background(), &mcpsdk.StdioTransport{})
}

func registerSDKCoreTools(server *mcpsdk.Server, registry map[string]registeredTool, profile string, logger *log.Logger) {
	for _, tool := range registry {
		server.AddTool(&mcpsdk.Tool{
			Name:        tool.Definition.Name,
			Description: tool.Definition.Description,
			InputSchema: tool.Definition.InputSchema,
		}, func(ctx context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			start := time.Now()

			args, err := decodeSDKToolArgs(req)
			if err != nil {
				logMCPToolCall(logger, profile, tool.Definition.Name, nil, time.Since(start), err)
				return sdkToolErrorResult(err), nil
			}

			cfg := config.Get()
			timeout := time.Duration(cfg.MCP.QueryTimeoutSec) * time.Second
			if tool.Definition.Name == "codebase_review_sql" {
				timeout = time.Duration(cfg.MCP.ReviewTimeoutSec) * time.Second
			}
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			result, err := tool.Handler(ctx, args)
			elapsed := time.Since(start)
			logMCPToolCall(logger, profile, tool.Definition.Name, args, elapsed, err)
			if err != nil {
				return sdkToolErrorResult(err), nil
			}
			return sdkToolPagedResult(result)
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

// sdkToolPagedResult сериализует value в JSON и применяет пагинацию если ответ превышает лимит.
// rawMCPText возвращается verbatim без маршалинга и без повторной пагинации.
func sdkToolPagedResult(value interface{}) (*mcpsdk.CallToolResult, error) {
	if raw, ok := value.(rawMCPText); ok {
		return &mcpsdk.CallToolResult{
			Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: string(raw)}},
		}, nil
	}
	text, err := toJSONText(value)
	if err != nil {
		return nil, err
	}
	text = globalPages.maybePaginate(text)
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}},
	}, nil
}

func logMCPToolCall(logger *log.Logger, profile string, toolName string, args map[string]interface{}, duration time.Duration, callErr error) {
	if logger == nil {
		return
	}
	if profile == "" {
		profile = "all"
	}
	status := "success"
	errorText := ""
	if callErr != nil {
		status = "error"
		errorText = strings.Join(strings.Fields(callErr.Error()), " ")
	}
	logger.Printf(
		"profile=%s tool=%s args=%s duration=%s duration_ms=%d status=%s error=%q",
		profile,
		toolName,
		formatToolArgs(args),
		duration.Round(time.Millisecond),
		duration.Milliseconds(),
		status,
		errorText,
	)
}

func formatToolArgs(args map[string]interface{}) string {
	if len(args) == 0 {
		return "-"
	}
	for _, key := range []string{"name", "procedure", "text", "event", "table", "type"} {
		if v, ok := args[key]; ok {
			return fmt.Sprintf("%s=%q", key, fmt.Sprintf("%v", v))
		}
	}
	for k, v := range args {
		return fmt.Sprintf("%s=%q", k, fmt.Sprintf("%v", v))
	}
	return "-"
}

func sdkToolErrorResult(err error) *mcpsdk.CallToolResult {
	return &mcpsdk.CallToolResult{
		Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: err.Error()}},
		IsError: true,
	}
}


## 1. Удаление декларации outputSchema

- [x] 1.1 В `internal/mcp/server.go` (`registerSDKCoreTools`, строка ~62) удалить `OutputSchema: defaultToolOutputSchema` из `server.AddTool(...)`; проверить `go build ./...` — чисто
- [x] 1.2 В `internal/mcp/tools.go` удалить переменную `defaultToolOutputSchema` и подстановку схемы в `tools()` (блок `if def.OutputSchema == nil {...}`); проверить `go build ./...` — чисто
- [x] 1.3 В `internal/mcp/types.go` удалить поле `OutputSchema` из `toolDefinition`; убедиться grep `OutputSchema` по `internal/` возвращает 0 совпадений и `go build ./...` + `go vet ./...` чисты

## 2. Regression-тесты (wire-level, без БД)

- [x] 2.1 Добавить в `internal/mcp/server_test.go` тест `TestSDKToolsList_NoOutputSchema`: для каждого профиля (`""`, `query`, `rti`, `trc`, `review`) поднять `mcpsdk.NewServer` + `registerSDKCoreTools` + клиент через `mcpsdk.NewInMemoryTransports()`, вызвать `tools/list` и убедиться, что ни один tool не содержит `outputSchema`; проверить `go test ./internal/mcp/ -run TestSDKToolsList -v` — PASS
- [x] 2.2 Добавить тест `TestSDKCallTool_Ping_NoValidationError`: через ту же in-memory пару вызвать `codebase_ping` и убедиться, что вызов завершается без ошибки (нет -32600), результат содержит один `TextContent` и не содержит `structuredContent`; проверить `go test ./internal/mcp/ -run TestSDKCallTool_Ping -v` — PASS
- [x] 2.3 Добавить тест `TestSDKCallTool_ErrorPath_TextOnly`: вызвать `codebase_rti_parse` с несуществующим `file_path` и убедиться, что результат имеет `isError=true`, text-only `content`, без `structuredContent` и без протокольной ошибки; проверить `go test ./internal/mcp/ -run TestSDKCallTool_ErrorPath -v` — PASS

## 3. Верификация и закрытие bug report

- [x] 3.1 Прогнать `go build ./...`, `go vet ./...`, `go test ./internal/mcp/... -count=1` — все PASS, существующие тесты (`TestToolsListIncludesPing`, `TestRTIToolsListIncludesAll`, `TestReadMoreInToolsList`) не сломаны
- [x] 3.2 Проверить в валидирующем MCP-клиенте (opencode): вызов `codebase_ping` проходит без `-32600`; обновить статус в `Bug-reports/BUG-mcp-output-schema-no-structured-content-20260902.md` на «Исправлено» (указать коммит)

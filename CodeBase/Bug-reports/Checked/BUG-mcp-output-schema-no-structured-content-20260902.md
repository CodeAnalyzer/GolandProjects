# Bug Report: все MCP-инструменты падают с протокольной ошибкой -32600 «has an output schema but did not return structured content»

**Дата:** 2026-09-02
**Файл:** `internal/mcp/server.go` (`registerSDKCoreTools`, строка ~62), `internal/mcp/tools.go` (`defaultToolOutputSchema`, строки 5–20)
**Версия CodeBase:** 0.9.0
**Статус:** Исправлено (2026-09-02, change `openspec/changes/archive/2026-09-02-fix-mcp-output-schema`): декларация `outputSchema` удалена из `registerSDKCoreTools`, `defaultToolOutputSchema` и поле `toolDefinition.OutputSchema` удалены; добавлены wire-level regression-тесты `TestSDKToolsList_NoOutputSchema` (все профили), `TestSDKCallTool_Ping_NoValidationError`, `TestSDKCallTool_ErrorPath_TextOnly` (`internal/mcp/server_test.go`, in-memory транспорт go-sdk). Верифицировано в opencode 1.18.26: `codebase_ping` по 4 профилям (query/review/rti/trc) + `codebase_health` — без `-32600`, все серверы Connected.

## Summary

Любой вызов любого MCP-инструмента сервера (`codebase_ping`, `codebase_stats`, `codebase_query_*`, `codebase_rti_*`, `codebase_trc_*`, `codebase_review_sql` — все профили) завершается протокольной ошибкой клиента:

```
MCP error -32600: Tool codebase_ping has an output schema but did not return structured content
```

Сервер при регистрации каждого tool декларирует `outputSchema`, но ни один обработчик не возвращает `structuredContent` — прямое нарушение спецификации MCP 2025-06-18 (Server Tools → Structured Content): *"If a tool declares an output schema, the tool MUST return structuredContent that validates against the schema"*. Клиенты, валидирующие контракт (opencode), отклоняют ответ ошибкой JSON-RPC -32600 (Invalid Request). Сервер полностью неработоспособен через совместимые клиенты.

---

## Environment

- Сервер: CodeBase 0.9.0, stdio-транспорт
- SDK: `github.com/modelcontextprotocol/go-sdk v1.6.0`
- Клиент: opencode (валидирует outputSchema/structuredContent контракт)

---

## Reproduction Steps

1. Подключить сервер как MCP (stdio) в opencode.
2. Вызвать любой инструмент, например `codebase_ping` или `codebase_stats`.
3. Получить ошибку: `MCP error -32600: Tool <name> has an output schema but did not return structured content`.

## Expected Result

Tool либо не декларирует `outputSchema` (тогда text-only результат валиден), либо возвращает `structuredContent`, валидируемый по объявленной схеме.

## Actual Result

Каждый tool декларирует `outputSchema`, результат содержит только `content` (TextContent), поле `structuredContent` отсутствует → клиент отклоняет вызов с -32600.

---

## Root Cause Analysis

### 1. Схема навешивается всем инструментам

`internal/mcp/server.go`, `registerSDKCoreTools` (строки 56–62):

```go
server.AddTool(&mcpsdk.Tool{
    Name:         tool.Definition.Name,
    Description:  tool.Definition.Description,
    InputSchema:  tool.Definition.InputSchema,
    OutputSchema: defaultToolOutputSchema,   // <-- декларация схемы
}, func(ctx context.Context, req *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
    ...
})
```

Дублирующая подстановка — `internal/mcp/tools.go` (строки 22–30): если `def.OutputSchema == nil`, подставляется `defaultToolOutputSchema`.

### 2. `structuredContent` не заполняется нигде

Оба пути возврата результата отдают только текстовый `Content`:

- `sdkToolPagedResult` (server.go:123–137):

```go
return &mcpsdk.CallToolResult{
    Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: text}},
}, nil
```

- `sdkToolErrorResult` (server.go:179–184):

```go
return &mcpsdk.CallToolResult{
    Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: err.Error()}},
    IsError: true,
}
```

Поиск по всему проекту: `StructuredContent` / `CallToolResultFor` — **0 упоминаний**.

### 3. SDK не заполняет поле автоматически

go-sdk v1.6.0 автоматически маршалит результат в `res.StructuredContent` только для **типизированных** хендлеров (`AddTool` с `Out`-параметром / `CallToolResultFor[T]` — см. `mcp/server.go:360–394` SDK: *"Marshal the output and put the RawMessage in the StructuredContent field"*). Проект использует нетипизированный `func(...) (*mcpsdk.CallToolResult, error)`, для которого документация SDK (server.go:233) прямо переносит ответственность на вызывающего: *"Setting the result's Content, StructuredContent and IsError fields are the caller's [responsibility]"*.

### 4. Сама схема семантически неверна

`defaultToolOutputSchema` (tools.go:5–20) описывает **конверт** ответа:

```go
var defaultToolOutputSchema = map[string]interface{}{
    "type": "object",
    "properties": map[string]interface{}{
        "content":  {...массив {type, text}...},
        "isError":  {"type": "boolean"},
    },
}
```

По спецификации `outputSchema` описывает форму `structuredContent` (полезную нагрузку), а не конверт `CallToolResult` (поля `content`/`isError` уже входят в протокольный тип результата). То есть даже при попытке «починить» заполнением `StructuredContent` реальными данными ответ не прошёл бы валидацию по этой схеме.

Итоговая цепочка: tool декларирует outputSchema → результат содержит только text content → клиент фиксирует нарушение спецификации → -32600 Invalid Request.

---

## Impact

- **Критично:** сервер полностью неработоспособен через MCP-клиенты, валидирующие контракт (opencode) — все инструменты, все профили (query/review/rti/trc).
- Отчёты о совместимости вводят в заблуждение: клиенты без строгой валидации (некоторые SDK) будут работать, маскируя проблему.
- `sdkToolErrorResult` также нарушает контракт: isError-результаты от инструментов с outputSchema обязаны содержать structuredContent.

---

## Suggested Fix

### Вариант A (минимальный, рекомендуется): убрать декларацию outputSchema

Text-only результат без схемы полностью валиден по спецификации.

1. `internal/mcp/server.go:62` — удалить `OutputSchema: defaultToolOutputSchema` из `AddTool`.
2. `internal/mcp/tools.go:22–34` — убрать подстановку `defaultToolOutputSchema` в `tools()`.
3. Удалить `defaultToolOutputSchema` и поле `OutputSchema` из `toolDefinition` (`internal/mcp/types.go`), если оно не используется в других местах.

### Вариант B: честный structured output

1. Переписать схему под реальную структуру ответа (например, `{type: "object", properties: {...фактические поля ответа...}}`).
2. В `sdkToolPagedResult` заполнять `StructuredContent` (значение до пагинации/сериализации в текст). Учесть, что `rawMCPText` и пагинация ломают соответствие схеме — для них structured output не подходит.

Вариант A предпочтителен: ответы инструментов — гетерогенный текст (Markdown/JSON-строки с пагинацией), единая outputSchema для них не имеет смысла.

### Тесты

- `TestRegisterSDKCoreTools_NoOutputSchemaWithoutStructured` — после регистрации всех tools профеля убедиться, что либо `OutputSchema == nil`, либо хендлер возвращает результат с непустым `StructuredContent`.
- Сквозной тест через `mcp.NewClient` (go-sdk): `CallTool` для `codebase_ping` не возвращает ошибку валидации.

## Файлы для изменения

1. **`internal/mcp/server.go`** — `registerSDKCoreTools` (строка 62): убрать `OutputSchema: defaultToolOutputSchema`.
2. **`internal/mcp/tools.go`** — `tools()` (строки 26–28) и `defaultToolOutputSchema` (строки 5–20): убрать подстановку/удалить.
3. **`internal/mcp/types.go`** — `toolDefinition.OutputSchema`: удалить при переходе на вариант A.

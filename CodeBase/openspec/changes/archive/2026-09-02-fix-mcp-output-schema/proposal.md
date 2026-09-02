## Why

Сервер декларирует `outputSchema` для каждого MCP-инструмента, но ни один обработчик не возвращает `structuredContent` — прямое нарушение спецификации MCP 2025-06-18 (Server Tools → Structured Content: *"If a tool declares an output schema, the tool MUST return structuredContent that validates against the schema"*). Клиенты, валидирующие контракт (opencode), отклоняют **любой** вызов **любого** инструмента ошибкой JSON-RPC `-32600` — сервер полностью неработоспособен через такие клиенты. Клиенты без строгой валидации (Windsurf) работают, маскируя проблему. Подробности: `Bug-reports/BUG-mcp-output-schema-no-structured-content-20260902.md` (все утверждения отчёта верифицированы по коду проекта, исходникам go-sdk v1.6.0 и git-истории).

## What Changes

- Убрать декларацию `OutputSchema: defaultToolOutputSchema` для всех инструментов в `registerSDKCoreTools` (`internal/mcp/server.go`): инструменты становятся text-only, что полностью валидно по спецификации.
- Удалить `defaultToolOutputSchema` и подстановку схемы в `tools()` (`internal/mcp/tools.go`) — легаси-путь `tools()` сейчас используется только тестами.
- Удалить поле `OutputSchema` из `toolDefinition` (`internal/mcp/types.go`) — других производственных использований нет.
- Добавить regression-тесты: после регистрации инструментов ни один tool не имеет `OutputSchema` без возвращаемого `StructuredContent`; сквозной вызов `codebase_ping` через клиент go-sdk (`mcp.NewClient`) не возвращает ошибку валидации.

Выбран минимальный вариант A (убрать декларацию схемы) вместо варианта B (честный structured output): ответы инструментов — гетерогенный текст (JSON-строки, пагинационные чанки, `rawMCPText` verbatim), для которого единая payload-схема не строится и который противоречил бы дизайну пагинации.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `mcp-server/mcp-transport-tools`: добавляется контракт result-формата инструментов: инструменты регистрируются без `outputSchema` и возвращают text-only результат (`Content` с `TextContent`); декларация `outputSchema` без `structuredContent` запрещена (нарушение спецификации MCP).

## Impact

- **Код:** `internal/mcp/server.go` (строка ~62), `internal/mcp/tools.go`, `internal/mcp/types.go`, `internal/mcp/server_test.go` (новые тесты).
- **Wire-формат:** поле `outputSchema` исчезает из `tools/list` — видимое изменение протокольного ответа; не ломает валидных клиентов (устраняется невалидная декларация), чинит валидирующих (opencode).
- **Зависимости:** без изменений (go-sdk v1.6.0 остаётся).
- **Не затрагивается:** CLI, сервисный слой, пагинация (`mcp-pagination`), RTI/TRC/review runtime.

## ADDED Requirements

### Requirement: Контракт outputSchema и structuredContent

Система SHALL регистрировать MCP-инструменты без декларации `outputSchema`: результат каждого инструмента — text-only (`content` с одним `TextContent`; для ошибок дополнительно `isError = true`). Декларация `outputSchema` без возвращаемого `structuredContent` запрещена (нарушение спецификации MCP 2025-06-18: *"If a tool declares an output schema, the tool MUST return structuredContent that validates against the schema"*). Если в будущем инструмент станет декларировать `outputSchema`, он MUST возвращать `structuredContent`, валидируемый по объявленной схеме, во всех результатах (включая isError-результаты).

#### Scenario: tools/list без outputSchema

- **GIVEN** запущенный MCP-сервер с любым профилем (или без профиля)
- **WHEN** клиент запрашивает `tools/list`
- **THEN** ни один зарегистрированный инструмент не содержит поле `outputSchema`
- **AND** каждый инструмент описывает только `name`, `description`, `inputSchema`

#### Scenario: Text-only результат принимается валидирующим клиентом

- **GIVEN** MCP-клиент, валидирующий контракт outputSchema/structuredContent (например opencode)
- **WHEN** вызывается любой инструмент, например `codebase_ping`
- **THEN** результат содержит `content` (один `TextContent`) и не содержит `structuredContent`
- **AND** клиент принимает результат без ошибки JSON-RPC `-32600`

#### Scenario: Ошибка инструмента без structuredContent

- **GIVEN** запущенный MCP-сервер
- **WHEN** tool-вызов завершается ошибкой (например `codebase_rti_parse` с несуществующим `file_path`)
- **THEN** результат содержит `content` (текст ошибки) и `isError = true`, без `structuredContent`
- **AND** клиент принимает isError-результат без протокольной ошибки

#### Scenario: Пагинированный и raw-ответ остаются text-only

- **GIVEN** запущенный MCP-сервер и ответ инструмента, превышающий лимит пагинации
- **WHEN** вызывается инструмент (например `codebase_query_callers` с большим результатом) или `codebase_read_more`
- **THEN** результат содержит текстовый чанк в `content` без `structuredContent`
- **AND** инструмент не декларирует `outputSchema`

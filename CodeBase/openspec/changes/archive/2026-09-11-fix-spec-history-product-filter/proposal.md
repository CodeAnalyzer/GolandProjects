## Why

MCP-инструмент `codebase_query_spec_history` объявляет параметр `product` в своей InputSchema, но handler в `internal/mcp/registry.go` не читает его из args и не передаёт в `ExecuteSpecHistory`. В результате через MCP-вызов фильтр по продукту не работает: если одинаковые slug capability существуют в разных продуктах, результаты смешиваются. CLI-команда `codebase query spec history` при этом передаёт `--product` корректно — расхождение существует только в MCP-слое. Кроме того, спека `spec-queries` не документирует параметр `product` для `spec_history`.

## What Changes

- MCP handler `codebase_query_spec_history` читает опциональный параметр `product` из args и передаёт его в `ExecuteSpecHistory` как variadic-аргумент.
- Спека `query/spec-queries` (требование «История capability по changes») дополняется описанием опционального параметра `product` и сценариями фильтрации по продукту.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-queries`: требование «История capability по changes (spec_history)» — добавляется опциональный параметр `product` и сценарии фильтрации.

## Impact

- **Код**: `internal/mcp/registry.go` — handler `codebase_query_spec_history` (3 строки: чтение `product` + передача в вызов).
- **Спеки**: `openspec/specs/query/spec-queries/spec.md` — MODIFIED требование `spec_history`.
- **API**: поведение MCP-инструмента `codebase_query_spec_history` меняется — параметр `product` начинает работать как заявлено в schema. Обратная совместимость сохраняется: `product` остаётся опциональным, без него результаты не фильтруются.
- **CLI**: без изменений — `--product` уже работает.

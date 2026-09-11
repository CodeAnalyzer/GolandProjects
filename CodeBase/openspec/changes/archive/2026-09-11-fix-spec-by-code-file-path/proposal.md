## Why

MCP-инструмент `codebase_query_spec_by_code` обещает в описании вернуть `file path` спецификации, ссылающейся на код-сущность, но реализация `ExecuteSpecByCode` не выбирает путь файла из БД и не включает его в структуру результата `SpecByCodeHit`. MCP-клиент не получает путь к spec-файлу, хотя данные доступны через тривиальный JOIN `spec_code_mentions.file_id → files.rel_path`. Все остальные query-инструменты проекта возвращают путь файла — это устоявшийся паттерн, нарушенный только для данного инструмента.

## What Changes

- SQL-запрос `ExecuteSpecByCode` дополняется `JOIN files f ON f.id = m.file_id` и выборкой `f.rel_path`.
- Структура `SpecByCodeHit` получает поле `File string` с JSON-тегом `json:"file"` — по аналогии со всеми остальными query-результатами (`TableResult`, `MethodResult`, `ProcedureResult` и т.д.).
- Спецификация `query/spec-queries` требования «Спеки по коду (spec_by_code)» дополняется: результат SHALL содержать путь к spec-файлу, в котором найдено упоминание.
- Интеграционные тесты `ExecuteSpecByCode` обновляются для проверки наличия поля `file`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-queries`: требование «Спеки по коду (spec_by_code)» расширяется — результат SHALL включать путь к spec-файлу упоминания (`file`), наряду с существующими полями (capability, snippet, product, границы строк).

## Impact

- **Код**: `internal/specsvc/specsvc.go` — функция `ExecuteSpecByCode` (SQL-запрос + scan), структура `SpecByCodeHit`.
- **Тесты**: `internal/specsvc/specsvc_integration_test.go` — проверка поля `file` в результате.
- **MCP-описание**: `internal/mcp/registry.go` — описание уже корректно упоминает `file path`, изменений не требуется.
- **Спецификация**: `openspec/specs/query/spec-queries/spec.md` — обновление требования.
- **Обратная совместимость**: добавление поля — обратно совместимое изменение; существующие потребители результата получают новое поле без нарушения контракта.

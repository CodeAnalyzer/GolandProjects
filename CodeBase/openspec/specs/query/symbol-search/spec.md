# Symbol Search

## Purpose

Унифицированный индекс символов (unified symbols index) для поиска сущностей любого типа по имени — SQL procedures/tables/indexes/columns, H defines, PAS units/classes/methods, JS functions/constants, DFM forms/components, report forms/params/VB functions, API business objects и XML/API symbols.

## Requirements

### Requirement: Unified symbols index

Система SHALL предоставлять единый индекс `symbols` для поиска сущностей всех поддерживаемых типов по имени, с указанием типа сущности, файла и строки.

#### Scenario: Точный поиск по имени

- **GIVEN** проиндексированный проект с процедурой `MassAccrual_Start`
- **WHEN** выполняется `query symbol --name MassAccrual_Start`
- **THEN** возвращена сущность типа `procedure` с указанием файла и строки

#### Scenario: Поиск по подстроке

- **GIVEN** проиндексированный проект с процедурами `MassAccrual_Start`, `MassAccrual_End`
- **WHEN** выполняется `query symbol --name MassAccrual --like`
- **THEN** возвращены обе процедуры

### Requirement: Фильтрация по типу сущности

Система SHALL поддерживать фильтрацию результатов поиска по типу сущности через флаг `--type`.

#### Scenario: Поиск класса

- **GIVEN** проиндексированный проект с классом `TMyForm`
- **WHEN** выполняется `query symbol --name TMyForm --type class`
- **THEN** возвращена сущность типа `class` с указанием unit, файла и строки

#### Scenario: Поиск метода по подстроке

- **GIVEN** проиндексированный проект с методами `Execute`, `ExecuteQuery`
- **WHEN** выполняется `query symbol --name Execute --type method --like`
- **THEN** возвращены оба метода с указанием unit, class, visibility и signature

### Requirement: Поддерживаемые типы символов

Система SHALL индексировать в `symbols` следующие типы сущностей: `procedure`, `table`, `index`, `column_definition`, `define`, `unit`, `class`, `method`, `js_function`, `constant`, `form`, `component`, `report_form`, `report_param`, `vb_function`, `api_business_object`, и XML/API symbols.

#### Scenario: Поиск компонента формы

- **GIVEN** проиндексированный проект с компонентом `dlName`
- **WHEN** выполняется `query symbol --name dlName --type component`
- **THEN** возвращена сущность типа `component` с указанием формы, файла и строки

### Requirement: Форматы вывода

Система SHALL поддерживать форматы вывода: текстовый (по умолчанию), `--json` (JSON envelope), `--summary` (сводный JSON), `--ndjson` (построчный JSON для pipeline).

#### Scenario: JSON вывод

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query symbol --name API --json`
- **THEN** результат возвращён в JSON envelope с полями `success`, `format_version`, `command`, `count`, `items`

#### Scenario: NDJSON вывод

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query symbol --name API --ndjson`
- **THEN** каждый результат возвращён как отдельная JSON-строка

### Requirement: Поведение при пустых и неоднозначных результатах

Система SHALL возвращать пустой массив `[]` (не `null`) при отсутствии результатов и ограничивать количество возвращаемых сущностей через `--limit` (по умолчанию 100, максимум 1000).

#### Scenario: Сущность не найдена

- **GIVEN** проиндексированный проект без сущности с именем `NonExistent`
- **WHEN** выполняется `query symbol --name NonExistent --json`
- **THEN** возвращён JSON с `"count": 0` и `"items": []`

#### Scenario: Превышение лимита

- **GIVEN** проиндексированный проект с 2000 процедурами, начинающимися на `API_`
- **WHEN** выполняется `query symbol --name API_ --like --limit 100`
- **THEN** возвращено не более 100 результатов

#### Scenario: Неоднозначный поиск без типа

- **GIVEN** проиндексированный проект с процедурой `Execute` и PAS-методом `Execute`
- **WHEN** выполняется `query symbol --name Execute` (без `--type`)
- **THEN** возвращены оба символа с разными `symbol_type`

## Related code

- `internal/query/query.go` — `SearchSymbol`, unified symbols search
- `internal/store/db_lookup_keys.go` — lookup key builders для unified symbol index
- `internal/store/db_schema.go` — таблица `symbols`
- `cmd/query_commands.go` — CLI command `query symbol`

## Notes

- Без `--like` — точный поиск по `name`; с `--like` — поиск по подстроке
- Сущности появляются в `symbols` после переиндексации соответствующих файлов
- `format_version` зафиксирован как `1.0` для machine-readable modes
- `GetFileByPath` и `SearchInContent` — внутренние методы `Query`, не вызываются из CLI/MCP; `SearchInContent` выполняет поиск по symbol names/signatures в unified index, не по сырому содержимому файлов
- Execution-слой `internal/querysvc` (`runtime.go`, `inspect.go`) — общая точка входа для CLI (`cmd/query_execution.go`) и MCP (`internal/mcp/registry.go`); устраняет дублирование оркестрации. Поведение команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

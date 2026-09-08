## ADDED Requirements

### Requirement: Spec инструменты

Система SHALL предоставлять MCP-инструменты для навигации по спекам: `codebase_query_spec_search` (двухслойный полнотекстовый поиск — см. `query/spec-search`), `codebase_query_spec_by_code` (спеки по имени код-сущности), `codebase_query_spec_deps` (граф зависимостей capability), `codebase_query_spec_usecase` (usecase-слой с involved capabilities), `codebase_query_spec_coverage` (покрытие кода спеками и пробелы), `codebase_query_spec_history` (история capability по changes). Инструменты реализованы поверх сервисного слоя, возвращают чистые доменные данные (без CLI envelope), наследуют общий per-tool таймаут `query_timeout_sec` и существующую пагинацию больших ответов.

#### Scenario: Поиск спеки через MCP
- **GIVEN** запущенный MCP-сервер и проиндексированные спеки финпродуктов
- **WHEN** вызывается `codebase_query_spec_search` с `text = "отключение SMS-уведомлений"`
- **THEN** возвращены точные и семантические совпадения в доменном формате (без envelope) с указанием продукта и границ строк

#### Scenario: Навигация от кода к требованию через MCP
- **GIVEN** запущенный MCP-сервер, проиндексированные спеки и код
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требования, ссылающиеся на процедуру, с текстами строк упоминания

#### Scenario: Пагинация большого ответа
- **GIVEN** запрос spec_search, чей результат превышает лимит чанка MCP-ответа
- **WHEN** инструмент формирует ответ
- **THEN** применён существующий механизм пагинации (continuation_id + `codebase_read_more`), без изменений механизма

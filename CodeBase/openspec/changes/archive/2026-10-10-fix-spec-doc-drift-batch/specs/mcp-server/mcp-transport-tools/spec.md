## MODIFIED Requirements

### Requirement: Query инструменты

Система SHALL предоставлять MCP-инструменты `codebase_query_*` для всех query-подкоманд CLI: `codebase_query_symbol`, `codebase_query_table`, `codebase_query_table_schema`, `codebase_query_table_index`, `codebase_query_procedure`, `codebase_query_callers`, `codebase_query_method`, `codebase_query_methods`, `codebase_query_sql_fragment`, `codebase_query_form`, `codebase_query_form_component`, `codebase_query_report_form`, `codebase_query_report_field`, `codebase_query_report_param`, `codebase_query_vb_function`, `codebase_query_js_function`, `codebase_query_smf_instrument`, `codebase_query_smf_type`, `codebase_query_api_contract`, `codebase_query_api_table`, `codebase_query_api_param`, `codebase_query_api_table_index`, `codebase_query_api_impl`, `codebase_query_api_publishers`, `codebase_query_api_consumers`, `codebase_query_relations`, `codebase_query_inspect`, `codebase_query_retcode`, `codebase_query_desc_search` (полнотекстовый поиск по человекочитаемым описаниям процедур и API-контрактов — см. `query/description-search`).

#### Scenario: Query symbol через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированный проект
- **WHEN** вызывается `codebase_query_symbol` с `name = "MyProc"`
- **THEN** возвращены данные символа в формате чистых доменных данных (без CLI envelope)

### Requirement: Профильная регистрация инструментов

Система SHALL поддерживать необязательный флаг `--profile` на команде `codebase mcp`. Без флага регистрируются все доступные инструменты (текущее поведение). При указании `--profile=<name>` регистрируется только подмножество инструментов, релевантное профилю, плюс базовые (`codebase_ping`, `codebase_health`, `codebase_stats`, `codebase_read_more`).

#### Scenario: Запуск без профиля — все инструменты

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp` без флага `--profile`
- **THEN** MCP-сервер регистрирует полный реестр инструментов: базовые, query, spec, rti, trc и review
- **AND** `tools/list` response идентичен текущему поведению

#### Scenario: Запуск с профилем rti

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=rti`
- **THEN** MCP-сервер регистрирует только базовые + RTI инструменты
- **AND** `tools/list` response не содержит query, trc, review инструменты

#### Scenario: Запуск с профилем query

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=query`
- **THEN** MCP-сервер регистрирует только базовые + query инструменты
- **AND** `tools/list` response не содержит rti, trc, review инструменты

#### Scenario: Запуск с профилем trc

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=trc`
- **THEN** MCP-сервер регистрирует только базовые + TRC инструменты
- **AND** `tools/list` response не содержит query, rti, review инструменты

#### Scenario: Запуск с профилем review

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=review`
- **THEN** MCP-сервер регистрирует только базовые + review инструмент
- **AND** `tools/list` response не содержит query, rti, trc инструменты

#### Scenario: Неизвестный профиль

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=unknown`
- **THEN** возвращается ошибка с перечислением доступных профилей: `query`, `rti`, `trc`, `review`
- **AND** MCP-сервер не запускается

### Requirement: Spec инструменты

Система SHALL предоставлять MCP-инструменты для навигации по спекам: `codebase_query_spec_search` (двухслойный полнотекстовый поиск — см. `query/spec-search`), `codebase_query_spec_by_code` (спеки по имени код-сущности), `codebase_query_spec_deps` (граф зависимостей capability), `codebase_query_spec_usecase` (usecase-слой с involved capabilities), `codebase_query_spec_coverage` (покрытие кода спеками и пробелы), `codebase_query_spec_history` (история capability по changes), `codebase_query_spec_config` (профиль продукта и иерархия capabilities — см. `query/spec-queries`). Инструменты реализованы поверх сервисного слоя, возвращают чистые доменные данные (без CLI envelope), наследуют общий per-tool таймаут `query_timeout_sec` и существующую пагинацию больших ответов.

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

## MODIFIED Requirements

### Requirement: TRC инструменты

Система SHALL предоставлять MCP-инструменты для TRC-анализа: `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events`, `codebase_trc_procedures`, `codebase_trc_tree`, `codebase_trc_errors`, `codebase_trc_slow`, `codebase_trc_delete`, `codebase_trc_prune`. Каждый TRC-обработчик SHALL возвращать клиенту ошибку разбора переданного optional-аргумента вместо подстановки его нулевого значения. `codebase_trc_events` SHALL возвращать полное `filtered_count` до `limit` и фактическое `returned_count`; `codebase_trc_procedures` SHALL возвращать enrichment и агрегировать только завершённые вызовы `SP:Completed`.

#### Scenario: TRC procedures через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** вызывается `codebase_trc_procedures` с `session_id = 42`
- **THEN** возвращена агрегация завершённых вызовов процедур с enrichment

#### Scenario: Некорректный optional-аргумент TRC-инструмента

- **GIVEN** запущенный MCP-сервер
- **WHEN** TRC-инструмент вызывается с optional-аргументом неверного типа
- **THEN** инструмент возвращает ошибку с именем некорректного аргумента
- **AND** обработчик не выполняет запрос с нулевым значением вместо переданного аргумента

#### Scenario: Счётчики ограниченной выдачи TRC events

- **GIVEN** фильтру TRC events соответствуют 1500 событий
- **WHEN** вызывается `codebase_trc_events` с `limit=1000`
- **THEN** ответ содержит `filtered_count=1500` и `returned_count=1000`

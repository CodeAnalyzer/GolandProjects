# Delta: mcp-transport-tools

## MODIFIED Requirements

### Requirement: TRC инструменты

Система SHALL предоставлять MCP-инструменты для TRC-анализа: `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events`, `codebase_trc_procedures`, `codebase_trc_tree`, `codebase_trc_errors`, `codebase_trc_slow`, `codebase_trc_delete`, `codebase_trc_prune`. Каждый TRC-обработчик SHALL возвращать клиенту ошибку разбора переданного optional-аргумента вместо подстановки его нулевого значения.

`codebase_trc_events` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `time_from`, `time_to` (RFC3339), `min_duration_ms`, `after_id`, `format=full|short` и SHALL возвращать полное `filtered_count` до `limit`, фактический `returned_count`, `has_more` и `next_after_id`. Существующие скалярные `spid` и `event_name` SHALL сохраняться у `codebase_trc_events` только как legacy-алиасы и нормализоваться в массивы из одного элемента; одновременная передача legacy-алиаса и соответствующего массива SHALL возвращать ошибку.

`codebase_trc_procedures` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `top`, `sort_by` и `group_by_spid`; скалярные alias-параметры для него SHALL NOT вводиться. `codebase_trc_procedures` SHALL возвращать enrichment и по умолчанию (без `event_names`) агрегировать только завершённые вызовы `SP:Completed`.

Array-параметры SHALL приниматься только массивами соответствующего типа со строгой валидацией каждого элемента: ошибка SHALL содержать имя аргумента и индекс некорректного элемента; пустые строки, дробные значения и переполнение SHALL NOT игнорироваться.

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

#### Scenario: Нормализация legacy-алиасов events

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** `codebase_trc_events` вызывается с `spid=728`
- **THEN** фильтрация эквивалентна вызову с `spids=[728]`

#### Scenario: Конфликт scalar и array параметров

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается одновременно с `event_name` и `event_names`
- **THEN** возвращена ошибка с предложением использовать `event_names`

#### Scenario: Некорректный элемент массива SPID

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается с `spids=[728, -1]`
- **THEN** возвращена ошибка, содержащая имя аргумента `spids` и индекс некорректного элемента
- **AND** запрос к данным не выполняется

#### Scenario: Массивы и время в procedures через MCP

- **GIVEN** сохранённая TRC-сессия содержит события SPID 728 и 700
- **WHEN** вызывается `codebase_trc_procedures` с `session_id`, `spids=[728, 700]` и `group_by_spid=true`
- **THEN** возвращены агрегаты `(spid, procedure)` для указанных SPID

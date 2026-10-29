# Дельта: mcp-transport-tools — параметр dsinfo_id

## ADDED Requirements

### Requirement: Параметр dsinfo_id у инструментов анализа

MCP-инструменты, выполняющие выбор копии сущности или enrichment из индекса,
SHALL принимать необязательный параметр `dsinfo_id` (целое число) — контекст
профиля развёртывания с семантикой `dsinfo-analysis/dsinfo-resolution`:
entity/API/retcode/desc/symbol query-инструменты и rti/trc-инструменты с
enrichment. Инструменты, не выполняющие выбор копии
(`codebase_query_callers`, `codebase_query_relations`,
`codebase_query_inspect`, spec-инструменты), а также `dsinfo_*` и review
параметр `dsinfo_id` не SHALL принимать. Ответы с применённым профилем SHALL
содержать `branch_requested`/`branch_used` у выбранных элементов и список
`warnings` верхнего уровня, агрегированный по фактическим элементам ответа
(одна строка на (продукт, запрошенные линии, использованная линия));
инструменты вне границ профиля `warnings` не добавляют.

#### Scenario: Вызов с dsinfo_id

- **GIVEN** профиль 3 с линией `fa-contracts-11-01-7` и MCP-инструмент поиска процедуры
- **WHEN** вызывается инструмент с параметрами `name = MyProc`, `dsinfo_id = 3`
- **THEN** ответ содержит копию из линии профиля, `branch_requested` и `branch_used`

#### Scenario: Вызов без dsinfo_id

- **GIVEN** тот же индекс и инструмент
- **WHEN** вызывается инструмент без `dsinfo_id`
- **THEN** поведение идентично CLI без контекста профиля (default-ветка; активный профиль применяется по цепочке разрешения)

#### Scenario: Инструмент вне границ профиля

- **GIVEN** заданный профиль
- **WHEN** вызывается `codebase_query_callers` или `codebase_query_inspect`
- **THEN** схема инструмента не содержит `dsinfo_id`, результат не зависит от профиля
- **AND** `warnings` о ветках отсутствует

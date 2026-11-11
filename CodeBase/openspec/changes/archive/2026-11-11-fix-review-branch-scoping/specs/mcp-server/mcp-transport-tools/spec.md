## MODIFIED Requirements

### Requirement: Review инструмент

Система SHALL предоставлять MCP-инструмент `codebase_review_sql` для статического анализа SQL-файла с настраиваемым набором правил и необязательным контекстом профиля `dsinfo_id` для разрешения веток чужих продуктов по семантике `review/review-scope`.

#### Scenario: Review через MCP

- **GIVEN** запущенный MCP-сервер и SQL-файл
- **WHEN** вызывается `codebase_review_sql` с `file_path` и опциональными `rules`
- **THEN** возвращены findings в формате доменных данных

#### Scenario: Review через MCP с профилем

- **GIVEN** запущенный MCP-сервер, SQL-файл продукта `fa-contracts` и профиль 3
- **WHEN** вызывается `codebase_review_sql` с `file_path` и `dsinfo_id = 3`
- **THEN** ветки чужих продуктов разрешаются по профилю 3
- **AND** ветка продукта проверяемого файла берётся из файла

### Requirement: Параметр dsinfo_id у инструментов анализа

MCP-инструменты, выполняющие выбор копии сущности или enrichment из индекса,
SHALL принимать необязательный параметр `dsinfo_id` (целое число) — контекст
профиля развёртывания с семантикой `dsinfo-analysis/dsinfo-resolution`:
entity/API/retcode/desc/symbol query-инструменты, rti/trc-инструменты с
enrichment и `codebase_review_sql` (для DB-lookup'ов чужих продуктов).
Инструменты, не выполняющие выбор копии
(`codebase_query_callers`, `codebase_query_relations`,
`codebase_query_inspect`, spec-инструменты), а также `dsinfo_*`
параметр `dsinfo_id` не SHALL принимать. Ответы с применённым профилем SHALL
содержать `branch_requested`/`branch_used` у выбранных элементов и список
`warnings` верхнего уровня, агрегированный по фактическим элементам ответа
(одна строка на (продукт, запрошенные линии, использованная линия));
инструменты вне границ профиля `warnings` не добавляют. Review SHALL NOT
добавлять `warnings` о ветках: ветка продукта проверяемого файла берётся из
файла, а профиль применяется только к чужим продуктам.

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

#### Scenario: Review с dsinfo_id

- **GIVEN** профиль 3 и SQL-файл продукта `fa-contracts`
- **WHEN** вызывается `codebase_review_sql` с `file_path`, `rules` и `dsinfo_id = 3`
- **THEN** схема инструмента принимает `dsinfo_id`
- **AND** ветки чужих продуктов разрешаются по профилю 3
- **AND** ответ не содержит `warnings` о ветках

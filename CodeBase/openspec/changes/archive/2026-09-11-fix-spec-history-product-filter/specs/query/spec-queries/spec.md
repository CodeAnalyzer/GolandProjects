## MODIFIED Requirements

### Requirement: История capability по changes (spec_history)

Система SHALL по slug capability возвращать хронологию изменений: все `spec_changes` (активные и архивные) с рёбрами `change_modifies`, для каждого — статус, артефакты и delta-секции (ADDED/MODIFIED/REMOVED) с именами и телами затронутых требований. Обратный запрос — по change его затронутые capability с delta-сциями (ADDED/MODIFIED/REMOVED) по каждой capability: плоский список `changes` с полями `capability_name`, `section`, `requirement_name`, `body_text`, а также summary-список `capabilities` (id, name, title, product). Для change с `skip_specs: true` (нет записей в delta) система SHALL возвращать по каждой связанной capability одну entry с `skip_specs = true` и `delta_source = "proposal"` (пустые `section`/`requirement_name`) — пометка, что связь извлечена из proposal, delta-требований нет. Поле `changes` SHALL быть пустым массивом `[]`, а не `null`, когда delta-секций нет. Опциональный параметр `product` фильтрует результаты по DS-продукту. Если `product` не передан, система SHALL возвращать изменения из всех продуктов. Параметр `product` применяется к обоим режимам — по capability и по change. Это даёт ответ «какие требования менялись и в каких change» без чтения файлов changes вручную.

#### Scenario: Хронология требований capability

- **GIVEN** capability `card-limits`, затронутая тремя архивными change (один с MODIFIED-требованием «Лимиты по операциям»)
- **WHEN** вызывается `codebase_query_spec_history` с `slug = "card-limits"`
- **THEN** возвращены три change в хронологическом порядке; у второго — delta-секция MODIFIED с телом требования

#### Scenario: Delta-секции по change

- **GIVEN** archived change `2026-09-02-rms-3965352-parallel-accrual-purchase` с ADDED-требованием «Параллельная обработка начислений при покупке портфеля» для capability `consumer-cession/purchase`
- **WHEN** вызывается `codebase_query_spec_history` с `change = "2026-09-02-rms-3965352-parallel-accrual-purchase"`
- **THEN** поле `changes` содержит delta-секцию с `capability_name = "consumer-cession/purchase"`, `section = "ADDED"`, `requirement_name = "Параллельная обработка начислений при покупке портфеля"` и телом требования
- **AND** поле `capabilities` содержит summary-запись `consumer-cession/purchase` с id, title и product

#### Scenario: Change со skip_specs

- **GIVEN** change с `skip_specs: true`, чей proposal упоминал `openspec/specs/CORE/data`
- **WHEN** вызывается spec_history по этому change
- **THEN** возвращена связь с capability `CORE/data` с пометкой, что связь извлечена из proposal (delta-требований нет)
- **AND** в `changes` присутствует entry с `skip_specs = true`, `delta_source = "proposal"`, пустыми `section` и `requirement_name`

#### Scenario: Change без delta и без skip_specs

- **GIVEN** change без записей в `spec_change_delta` и без `skip_specs: true`
- **WHEN** вызывается spec_history по этому change
- **THEN** поле `changes` равно пустому массиву `[]`, а не `null`
- **AND** поле `capabilities` содержит список связанных capability

#### Scenario: Фильтр по продукту при поиске по capability

- **GIVEN** capability slug `card-limits` существует в продуктах `fa-cards` и `fa-payments`, и в `fa-cards` затронута двумя change
- **WHEN** вызывается `codebase_query_spec_history` с `name = "card-limits"` и `product = "fa-cards"`
- **THEN** возвращены только change, затрагивающие capability `card-limits` в продукте `fa-cards`
- **AND** change из продукта `fa-payments` не входят в результат

#### Scenario: Фильтр по продукту при поиске по change

- **GIVEN** change затрагивает capabilities в двух продуктах: `fa-cards` и `fa-payments`
- **WHEN** вызывается `codebase_query_spec_history` с `change = "<change-name>"` и `product = "fa-cards"`
- **THEN** поле `capabilities` содержит только capability из продукта `fa-cards`
- **AND** поле `changes` содержит delta-секции только по capability из продукта `fa-cards`

#### Scenario: Поиск без фильтра по продукту

- **GIVEN** capability slug `card-limits` существует в продуктах `fa-cards` и `fa-payments`
- **WHEN** вызывается `codebase_query_spec_history` с `name = "card-limits"` без параметра `product`
- **THEN** возвращены change из всех продуктов, где есть capability с этим slug

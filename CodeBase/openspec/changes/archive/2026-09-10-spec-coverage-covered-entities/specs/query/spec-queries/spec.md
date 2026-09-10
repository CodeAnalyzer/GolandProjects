## MODIFIED Requirements

### Requirement: Покрытие кода спеками (spec_coverage)

Система SHALL по продукту (обязательно) и опционально capability возвращать перечень покрытых код-сущностей, сгруппированных по capability. Для каждой capability продукта (или одной указанной capability) возвращается список уникальных код-сущностей, на которые ссылаются спеки этой capability через разрешённые `references_code` relations (от источников `spec_capability`, `spec_requirement`, `spec_scenario`). Каждая запись содержит имя сущности и её тип (`kind`). Опциональный параметр `kind` фильтрует сущности по типу: `api_contract | sql_procedure | sql_table | dfm_form | smf_instrument | pas_method | js_function | report_form`. Записи дедуплицируются по паре `name|kind` внутри capability. Метрики покрытия (saved-значения, проценты) и режим gaps этим инструментом не возвращаются.

#### Scenario: Вычисляемое покрытие по API-контрактам

- **GIVEN** продукт `fa-contracts` с 69 API-сервисами и спеками, разрешившими 55 упоминаний контрактов через `references_code`
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-contracts"` без `name`
- **THEN** возвращён список capability продукта, каждая со своим перечнем покрытых API-контрактов `{name, kind: "api_contract"}`
- **AND** сущности внутри каждой capability уникальны по паре `name|kind`

#### Scenario: Покрытие по продукту — все capability

- **GIVEN** продукт `fa-contracts` с capability `consumer-event` (покрывает 3 API-контракта и 8 процедур) и capability `consumer-cession` (покрывает 12 API-контрактов)
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-contracts"` без `name`
- **THEN** возвращён список из двух capability, каждая со своим перечнем покрытых сущностей `{name, kind}`
- **AND** сущности внутри каждой capability уникальны по паре `name|kind`

#### Scenario: Покрытие одной capability

- **GIVEN** продукт `fa-contracts` с capability `consumer-event`, покрывающей 3 API-контракта и 8 процедур
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-contracts"` и `name = "consumer-event"`
- **THEN** возвращён список из одной capability `consumer-event` с 11 записями (3 `api_contract` + 8 `sql_procedure`)

#### Scenario: Фильтр по типу сущности

- **GIVEN** продукт `fa-contracts` с capability `consumer-event`, покрывающей 3 API-контракта и 8 процедур
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-contracts"`, `name = "consumer-event"`, `kind = "api_contract"`
- **THEN** возвращён список из одной capability `consumer-event` с 3 записями типа `api_contract`

#### Scenario: Capability не найдена

- **GIVEN** продукт `fa-contracts` без capability `nonexistent`
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-contracts"` и `name = "nonexistent"`
- **THEN** возвращён пустой список capability (`[]`), не ошибка

#### Scenario: Продукт без покрытого кода

- **GIVEN** продукт `fa-empty` без разрешённых `references_code` relations
- **WHEN** вызывается `codebase_query_spec_coverage` с `product = "fa-empty"` без `name`
- **THEN** возвращён список capability продукта, каждая с пустым массивом `covered`

#### Scenario: Пробелы покрытия

- **GIVEN** спеки с 12 нерезолвнутыми mention-записями (упоминания кода в тексте спек, не сопоставленные с сущностями в индексе)
- **WHEN** вызывается `codebase_query_spec_coverage` по продукту этих спек
- **THEN** нерезолвнутые упоминания не включены в перечень покрытых сущностей (покрытие строится только по разрешённым `references_code` relations)
- **AND** инструмент не возвращает отдельный список gaps (режим `gaps` удалён)

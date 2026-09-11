## MODIFIED Requirements

### Requirement: Usecase-слой (spec_usecase)

Система SHALL по имени/идентификатору usecase (имя файла, `REQ-001-SC-001`, pageId) возвращать его шаги с потоками (main|alternative), актёров и связанные capability (usecase_involves); по фильтру продукта (без name) — список usecase слоя с их видами (`source_dir`, `usecase_kind`) и заголовками. Параметр `product` опционален: при передаче только `product` (без `name`) возвращается список usecase'ов продукта; при передаче `name` (с или без `product`) — детали одного usecase. Если usecase не найден, система SHALL возвращать `ErrSpecNotFound` — единообразно с другими spec-инструментами (`spec_deps`, `spec_history`).

#### Scenario: Сценарий с шагами и involved capabilities

- **GIVEN** проиндексированный `scenarios/scenario-sms-disable.md` с основным и альтернативным потоками и ссылками `../specs/card-service/spec.md`
- **WHEN** вызывается `codebase_query_spec_usecase` с `name = "scenario-sms-disable"`
- **THEN** возвращены актёры, шаги обоих потоков в порядке, involved capability `card-service`

#### Scenario: Продукт без usecase-слоя

- **GIVEN** продукт с профилем `usecase_layout = none` (usecase-слой отсутствует)
- **WHEN** вызывается `codebase_query_spec_usecase` с именем usecase из этого продукта
- **THEN** возвращён `ErrSpecNotFound`, так как usecase не существует в индексе

#### Scenario: Usecase не найден

- **GIVEN** usecase `nonexistent-scenario` отсутствует в индексе
- **WHEN** вызывается `codebase_query_spec_usecase` с `name = "nonexistent-scenario"`
- **THEN** возвращён `ErrSpecNotFound`

#### Scenario: Список usecase'ов по продукту

- **GIVEN** продукт `fa-financialasset` с 33 проиндексированными usecase'ами в `usecases/fot/` и `usecases/msfo/`
- **WHEN** вызывается `codebase_query_spec_usecase` с `product = "fa-financialasset"` без `name`
- **THEN** возвращён список из 33 usecase'ов с полями `usecase_name`, `title`, `source_dir`, `usecase_kind`

#### Scenario: Поиск usecase по pageId

- **GIVEN** проиндексированный usecase с `page_id = 467512912`
- **WHEN** вызывается `codebase_query_spec_usecase` с `name = "467512912"`
- **THEN** возвращён usecase с соответствующим `page_id`

#### Scenario: Usecase с заполненными inline-метаданными

- **GIVEN** проиндексированный `usecases/fot/REQ-001-SC-001 — ....md` с заполненными description, actors, business_value, preconditions, postconditions и шагами
- **WHEN** вызывается `codebase_query_spec_usecase` с `name = "REQ-001-SC-001 — Расчет ставки налога..."`
- **THEN** возвращены заполненные description, actors, business_value, preconditions, postconditions
- **AND** шаги `**Шаг 1**.`, `**Шаг 2**.` — в порядке с `flow_kind = main`

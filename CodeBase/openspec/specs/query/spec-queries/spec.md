# Spec Queries

## Purpose

Навигационные запросы по спекам поверх общего графа индекса: от кода к требованиям (обратные references_code), граф зависимостей capability, usecase-слой с involved capabilities, покрытие кода спеками и история изменения capability по changes. Все запросы доступны как MCP-инструменты `codebase_query_spec_*` и зеркальные CLI-подкоманды `codebase query spec …`.

## Requirements

### Requirement: Спеки по коду (spec_by_code)

Система SHALL по имени код-сущности (SQL-процедура, API-контракт, таблица, PAS-метод, DFM-форма, JS-функция, SMF-инструмент, отчёт) находить спеки, ссылающиеся на неё: обратный проход по `relations` `references_code` до сущности-источника (capability/requirement/scenario/usecase) с текстом-фрагментом строки упоминания, продуктом и границами строк. Результат группируется по capability; внутри — требования и сценарии с указанием, из какой части спеки пришла ссылка (Related code / текст требования / текст сценария).

#### Scenario: Требования по процедуре

- **GIVEN** спека fa-custody с требованием, чей сценарий WHEN упоминает `API_DepoAccount_MassInsert`, и проиндексированная процедура
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требование с текстом строки упоминания и границами строк

#### Scenario: Код-сущность не упоминается в спеках

- **GIVEN** процедура `SomeInternalProc`, не встречающаяся ни в одном тексте спек
- **WHEN** вызывается spec_by_code
- **THEN** возвращён пустой результат (`count = 0`), не ошибка

### Requirement: Зависимости capability (spec_deps)

Система SHALL по slug capability (с опциональным фильтром продукта) возвращать её зависимости: прямые и транзитивные `depends_on_capability` в заданном направлении (depends_on | depended_by, с глубиной), вложенность по `parent_id` (родители и дети). Каждое ребро содержит confidence (explicit|notes|inline), источник (заголовок секции и строка маркера) и продукт. Транзитивные зависимости ограничиваются глубиной (по умолчанию 2) с защитой от циклов.

#### Scenario: Прямые и транзитивные зависимости

- **GIVEN** capability `card-limits` зависит от `card-service`, который зависит от `limits`
- **WHEN** вызывается `codebase_query_spec_deps` с `slug = "card-limits"` и `direction = depends_on`, depth 2
- **THEN** возвращены оба уровня зависимостей с пометкой уровня и confidence каждого ребра

#### Scenario: Кто зависит от capability

- **GIVEN** capability `reserve-references`, на которую ссылаются спеки reserve-elements
- **WHEN** вызывается spec_deps с `direction = depended_by`
- **THEN** возвращены все зависящие capability с источниками ссылок

### Requirement: Usecase-слой (spec_usecase)

Система SHALL по имени/идентификатору usecase (имя файла, `REQ-001-SC-001`, pageId) возвращать его шаги с потоками (main|alternative), актёров и связанные capability (usecase_involves); по фильтру продукта — список usecase слоя с их видами (`source_dir`). Если usecase не найден, система SHALL возвращать `ErrSpecNotFound` — единообразно с другими spec-инструментами (`spec_deps`, `spec_history`).

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

### Requirement: Профиль продукта и иерархия capabilities (spec_config)

Система SHALL по имени продукта возвращать профиль spec-конфигурации: `product_name`, `schema_name`, `root_dir`, `usecase_layout` (scenarios|usecases|business-processes|none), `id_style` (dir|full_path), `cross_ref_style` (explicit|notes|inline|mixed), `normative_lang` (en|ru), `traceability` (html_comment|pageid|none), `has_changes`, `has_audit`, `has_adr`, `coverage_metrics`, `context_text`. Дополнительно система SHALL возвращать статистику продукта: количество capabilities, requirements, scenarios, usecases и changes. Опционально (параметр `include_hierarchy`, по умолчанию `true`) система SHALL возвращать дерево иерархии capabilities по `parent_id`: каждый узел содержит `capability_name`, `title`, `is_container` (true для директорий-контейнеров без spec.md), `children_count` (общее количество прямых детей) и `children` (массив, раскрытый до глубины `depth`, по умолчанию 2). Если продукт не найден, система SHALL возвращать `ErrSpecNotFound`.

#### Scenario: Профиль и иерархия продукта с плоской структурой

- **GIVEN** продукт `fa-cards` с 73 capabilities без вложенности (parent_id = NULL у всех) и профилем `usecase_layout = none`, `normative_lang = ru`
- **WHEN** вызывается `codebase_query_spec_config` с `product = "fa-cards"`
- **THEN** возвращён профиль с `usecase_layout = "none"`, `normative_lang = "ru"`
- **AND** возвращена статистика: `capabilities = 73`
- **AND** возвращена иерархия из 73 корневых узлов, каждый с `children_count = 0` и `children = null`

#### Scenario: Профиль и иерархия продукта с глубоким деревом

- **GIVEN** продукт `fa-reports` с 7 корневыми capabilities, 139 детьми на уровне 2 и 1748 на уровне 3
- **WHEN** вызывается `codebase_query_spec_config` с `product = "fa-reports"` и `depth = 2`
- **THEN** возвращён профиль продукта
- **AND** возвращена иерархия из 7 корневых узлов, каждый с раскрытыми детьми (уровень 2)
- **AND** узлы уровня 2 содержат `children_count` с количеством детей, но `children = null` (глубина 2 достигнута)

#### Scenario: Иерархия отключена

- **GIVEN** продукт `fa-cards`
- **WHEN** вызывается `codebase_query_spec_config` с `product = "fa-cards"` и `include_hierarchy = false`
- **THEN** возвращён профиль и статистика без поля `hierarchy`

#### Scenario: Продукт не найден

- **GIVEN** продукт `nonexistent-product` отсутствует в индексе
- **WHEN** вызывается `codebase_query_spec_config` с `product = "nonexistent-product"`
- **THEN** возвращён `ErrSpecNotFound`

#### Scenario: Контейнерные узлы в иерархии

- **GIVEN** продукт с директорией `specs/billing/` без `spec.md` (контейнер) и `specs/billing/invoicing/spec.md` (реальная capability)
- **WHEN** вызывается `codebase_query_spec_config` с `include_hierarchy = true`
- **THEN** узел `billing` имеет `is_container = true`, `title = ""`
- **AND** узел `billing/invoicing` имеет `is_container = false` и непустой `title`

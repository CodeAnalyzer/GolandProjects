## ADDED Requirements

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

## MODIFIED Requirements

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

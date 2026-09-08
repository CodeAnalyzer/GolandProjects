# Database Schema

## Purpose

Схема БД PostgreSQL: создание всех таблиц и индексов через `InitSchema` (идемпотентная), CRUD для файлов, batch insert для всех типов сущностей, lookup-запросы, статистика, управление scan runs.

## Requirements

### Requirement: Идемпотентная инициализация схемы

Система SHALL создавать все таблицы и индексы БД при первом запуске `codebase init` через `InitSchemaCtx(ctx)`, идемпотентно (CREATE TABLE IF NOT EXISTS, CREATE INDEX IF NOT EXISTS). Все DDL-выражения выполняются через `ExecContext` с переданным `context.Context`, что позволяет отменить длительную инициализацию. `InitSchema()` без контекста сохранён как deprecated-обёртка поверх `InitSchemaCtx(context.Background())` для обратной совместимости.

#### Scenario: Первый запуск

- **GIVEN** пустая БД PostgreSQL
- **WHEN** выполняется `codebase init`
- **THEN** все таблицы и индексы созданы без ошибок через `InitSchemaCtx(ctx)`

#### Scenario: Повторный запуск

- **GIVEN** БД с уже созданной схемой
- **WHEN** выполняется `codebase init` повторно
- **THEN** схема не пересоздаётся, существующие таблицы и индексы сохраняются

#### Scenario: Отмена длительной инициализации

- **GIVEN** запущенный `codebase init` на крупном проекте
- **WHEN** пользователь нажимает Ctrl+C во время `InitSchemaCtx`
- **THEN** контекст отменён, выполнение DDL прекращается без зависания

### Requirement: Таблицы сущностей

Система SHALL создавать таблицы для всех типов сущностей: `files`, `scan_runs`, `ds_products`, `sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`, `pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`, `report_forms`, `report_fields`, `report_params`, `vb_functions`, `h_files_defines`, `api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`, `symbols`, `relations`, `query_fragments`, `include_directives`, а также таблицы спек: `spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`.

#### Scenario: Создание таблицы процедур

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** таблица `sql_procedures` создана с колонками: id, file_id, procedure_name, body, start_line, end_line, ...

#### Scenario: Каталог продуктов и JS-константы

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `ds_products` (каталог продуктов Diasoft для review `foreign*`) и `js_constants` (константы из JS, попадающие в `symbols` как `constant`)

#### Scenario: Создание таблиц спек

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `spec_configs`, `spec_capabilities` (parent_id, title, purpose, notes, related_code, метрики покрытия), `spec_requirements`, `spec_scenarios` (given/when/then), `spec_usecases` (+ source_dir, usecase_kind, page_id), `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, staging `spec_code_mentions`, а также `spec_vocab` и `spec_embeddings` полнотекстового слоя

### Requirement: Таблицы анализаторов

Система SHALL создавать таблицы для RTI и TRC анализаторов: `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`, `trc_sessions`, `trc_events`, `ds_return_codes`.

#### Scenario: Создание таблиц RTI

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** таблицы `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events` созданы

### Requirement: Batch insert

Система SHALL использовать batch insert (COPY IN через `pq.CopyIn`) для эффективной массовой загрузки сущностей в БД с настраиваемым размером batch (`batch_insert_size`, по умолчанию 50000).

#### Scenario: Batch insert процедур

- **GIVEN** 1000 SQL-процедур для загрузки
- **WHEN** выполняется `BatchInsertSQLProcedures`
- **THEN** процедуры загружены через COPY IN за один batch

### Requirement: Управление файлами

Система SHALL предоставлять CRUD для файлов: сохранение новых, обновление существующих, удаление по пути (`DeleteFilesByPath`), проверка существования.

#### Scenario: Удаление файлов по пути

- **GIVEN** проиндексированный проект с файлом `path/to/OldFile.sql`
- **WHEN** файл удалён и выполняется `codebase update`
- **THEN** `DeleteFilesByPath` удаляет запись файла и все связанные сущности

### Requirement: Управление scan runs

Система SHALL отслеживать запуски сканирования через таблицу `scan_runs` с метаданными: start_time, end_time, status, total_files, processed_files.

#### Scenario: Завершённый scan run

- **GIVEN** запущенный `codebase init`
- **WHEN** инициализация завершена
- **THEN** в `scan_runs` создана запись со status `completed` и метаданными

### Requirement: Индексы для производительности

Система SHALL создавать индексы для оптимизации запросов: GIN-индекс `pg_trgm` для `query_fragments.query_text`, составные индексы для часто используемых запросов, индексы на `session_id` для RTI/TRC таблиц.

#### Scenario: GIN-индекс для полнотекстового поиска

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан GIN-индекс `pg_trgm` на `query_fragments.query_text`

### Requirement: Nullable helpers

Система SHALL предоставлять nullable-хелперы (`NullableString`, `NullableInt`, `NullableInt64`) для корректной работы с NULL-значениями в PostgreSQL.

#### Scenario: Nullable string

- **GIVEN** значение, которое может быть NULL
- **WHEN** выполняется вставка через `NullableString`
- **THEN** в БД сохранён NULL, а не пустая строка

### Requirement: Context propagation во всех методах store

Система SHALL принимать `context.Context` первым аргументом во всех методах слоя store: `InitSchemaCtx`, `WithBatchTxCtx`, lookup-методы (`FindLatest*`, `Find*ByFile` в `db_lookup_*.go`), batch insert (`db_insert_*.go`), CRUD для файлов и scan runs. Все SQL-вызовы выполняются через `ExecContext`/`QueryContext`/`QueryRowContext` (а не через `Exec`/`Query` без контекста). Это позволяет проксировать ctx таймаутов tool-ов (см. `mcp-transport-tools`), отмену CLI-pipeline и shutdown-сигналы вплоть до SQL-запросов. `InitSchema()` без ctx сохранён только как deprecated-обёртка.

#### Scenario: Контекст доходит до SQL

- **GIVEN** вызывается `WithBatchTxCtx(ctx, fn)` для batch insert
- **WHEN** ctx отменяется во время выполнения `fn`
- **THEN** SQL-операции внутри `fn` прерываются (`ExecContext` возвращает `ctx.Err()`)
- **AND** транзакция откатывается

#### Scenario: Lookup с контекстом

- **GIVEN** вызывается `FindLatestSQLProcedureIDsByNames(ctx, names)` во время постобработки
- **WHEN** ctx отменён пользователем (Ctrl+C)
- **THEN** SQL-запрос прерывается, возвращается `ctx.Err()`, горутины постобработки завершаются

### Requirement: DSN-экранирование значений

Система SHALL строить libpq keyword/value DSN из конфигурации через `FormatDSN(cfg)` с экранированием каждого значения через `quoteDSNValue`. Значения с пробелами, одинарной кавычкой или backslash оборачиваются в одинарные кавычки, внутри которых `'` и `\` экранируются backslash-ом (`\'`, `\\`). Удвоение кавычки (как в SQL-литералах) НЕ используется, т.к. парсер `lib/pq` `parseOpts` трактует любую неэкранированную `'` как конец значения, а одиночный `\` съедает как escape. Это критично для паролей, содержащих спецсимволы (доработка «review-dsn-race-fix»).

#### Scenario: Пароль с пробелом

- **GIVEN** конфигурация с `password = "my secret"`
- **WHEN** `NewDB` вызывает `FormatDSN(cfg)`
- **THEN** DSN содержит `password='my secret'` (значение в кавычках)

#### Scenario: Пароль с одинарной кавычкой

- **GIVEN** конфигурация с `password = "p'ass"`
- **WHEN** `FormatDSN(cfg)` строит DSN
- **THEN** DSN содержит `password='p\'ass'` (кавычка экранирована backslash-ом)
- **AND** `lib/pq` корректно парсит DSN, подключение устанавливается

#### Scenario: Простое значение без экранирования

- **GIVEN** конфигурация с `user = "codebase"` (без спецсимволов)
- **WHEN** `FormatDSN(cfg)` строит DSN
- **THEN** DSN содержит `user=codebase` (без кавычек)

### Requirement: HasCompletedInit — признак завершённой инициализации

Система SHALL предоставлять метод `HasCompletedInit(ctx)`, проверяющий, была ли уже завершена первичная инициализация индекса (наличие записи `scan_runs` со статусом `completed`). Используется в `systemsvc.ExecuteHealth` для определения `index readiness` без необходимости отдельных запросов по сущностям.

#### Scenario: Проект не инициализирован

- **GIVEN** пустая БД (только схема)
- **WHEN** `systemsvc` вызывает `HasCompletedInit(ctx)`
- **THEN** возвращено `false, nil`

#### Scenario: Проект инициализирован

- **GIVEN** БД с завершённой записью `scan_runs.status = completed`
- **WHEN** `systemsvc.ExecuteHealth` вызывает `HasCompletedInit(ctx)`
- **THEN** возвращено `true, nil`, и `index readiness` отмечен как готов

### Requirement: Сущности спек с привязкой к продукту

Система SHALL создавать таблицы спек по общей модели сущностей: каждая с FK `file_id` REFERENCES files ON DELETE CASCADE (повторная индексация файла каскадно пересоздаёт его сущности спек); `spec_configs` и `spec_capabilities` дополнительно несут `ds_product_id`. `spec_capabilities.parent_id` ссылается на `spec_capabilities` (ON DELETE SET NULL) для иерархии вложенных директорий; `spec_requirements.capability_id`, `spec_scenarios.requirement_id`, `spec_usecase_steps.usecase_id`, `spec_change_delta.change_id` — обязательные FK с CASCADE. Связи между сущностями спек и с кодом хранятся ТОЛЬКО в существующей полиморфной `relations` (новые relation_type: `depends_on_capability`, `usecase_involves`, `references_code`, `change_modifies`); иерархия, выражаемая FK, в relations НЕ дублируется.

#### Scenario: Каскадное обновление spec.md

- **GIVEN** ранее проиндексированный `spec.md` с capability, требованиями и сценариями
- **WHEN** файл переиндексируется (новый file_id)
- **THEN** старые записи spec_capabilities/spec_requirements/spec_scenarios удалены каскадно по file_id, новые вставлены

#### Scenario: Иерархия через parent_id

- **GIVEN** capability `FORMS/0409120/usage` внутри контейнера `FORMS/0409120`
- **WHEN** выполняется запрос иерархии
- **THEN** связь ребёнок→родитель читается из `parent_id` без обращения к relations

### Requirement: Полнотекстовые индексы спек

Система SHALL создавать для полнотекстового слоя спек: GIN-индексы на `search_vector` (tsvector, конфигурация 'russian') таблиц `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`; GIN-индекс pg_trgm на текстовых полях спек для поиска технических идентификаторов; btree-индексы на `spec_capabilities (LOWER(capability_name))`, `(spec_config_id)`, `(parent_id)`, `spec_requirements (capability_id)`, `spec_scenarios (requirement_id)`, `spec_usecases (spec_config_id)`, `spec_usecase_steps (usecase_id, flow_kind, step_order)`, `spec_code_mentions (LOWER(mention_name))`, `(source_type, source_id)`, `spec_change_delta (change_id)`, `spec_vocab (term)` (уникальный), `spec_embeddings (spec_id, embed_level)`. Новые relation_type покрываются существующими составными индексами `relations (source_type, source_id)` / `(target_type, target_id)` — отдельный индекс под relation_type не создаётся.

#### Scenario: GIN-индексы полнотекста

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы GIN-индексы search_vector на четырёх таблицах спек и pg_trgm-индекс для технических идентификаторов

#### Scenario: Уникальность словаря

- **GIVEN** повторная вставка термина `CON_STP_MassAccrual` в `spec_vocab`
- **WHEN** выполняется batch insert словаря
- **THEN** дубликат не создаётся (уникальный индекс по term)

### Requirement: Batch insert для сущностей спек

Система SHALL загружать сущности спек batch insert (COPY IN, `pq.CopyIn`) по образцу существующих сущностей: `BatchInsertSpecCapabilities`, `BatchInsertSpecRequirements`, `BatchInsertSpecScenarios`, `BatchInsertSpecUsecases` (+steps), `BatchInsertSpecChanges` (+delta), `BatchInsertSpecCodeMentions`, `BatchInsertSpecVocab`, `BatchInsertSpecEmbeddings`. Размер batch — общий `batch_insert_size`. tsvector-колонки `search_vector` заполняются при вставке через `to_tsvector('russian', …)` от конкатенации весовых полей.

#### Scenario: Массовая загрузка требований

- **GIVEN** 60 000 распарсенных требований
- **WHEN** выполняется `BatchInsertSpecRequirements`
- **THEN** требования загружены через COPY IN батчами, search_vector заполнен для каждой записи

## Related code

- `internal/store/db.go` — `NewDB`, `FormatDSN`, `quoteDSNValue`, `dsnValueEscaper`, подключение к БД
- `internal/store/db_schema.go` — `InitSchemaCtx`, `InitSchema` (deprecated-обёртка), все CREATE TABLE/INDEX через `ExecContext`
- `internal/store/db_tx.go` — `WithBatchTxCtx`, `WithBatchTx` (deprecated), `ExecContext`, `QueryContext`
- `internal/store/db_files.go` — CRUD для files, `DeleteFilesByPath`, `DeleteFilesByPaths`, `DeleteFilesByPathsExcept` (все с ctx)
- `internal/store/db_insert_*.go` — batch insert (COPY IN) для всех типов сущностей (все с ctx)
- `internal/store/db_lookup_*.go` — lookup-запросы `Find*`/`FindLatest*` (все с ctx)
- `internal/store/db_scan_runs.go` — `CreateScanRun`, `UpdateScanRun`, `HasCompletedInit`
- `internal/store/db_stats.go` — агрегированная статистика
- `internal/store/db_nullable.go` — nullable helpers
- `internal/store/db_products.go` — каталог продуктов Diasoft
- `internal/store/api_store.go` — persistence для API/DSArchitect сущностей

## Notes

- `InitSchemaCtx` идемпотентна — безопасна для повторного запуска; `InitSchema()` без ctx — deprecated-обёртка для обратной совместимости
- Batch insert использует `pq.CopyIn` для максимальной производительности
- GIN-индекс `pg_trgm` требует расширения `pg_trgm` в PostgreSQL
- Удалены избыточные индексы в рамках оптимизации (оставлены только используемые)
- При `codebase update` удалённые файлы обрабатываются через `DeleteFilesByPath`/`DeleteFilesByPaths`
- Context propagation — сквозная доработка («context-propagation-db-layer»): все SQL-операции принимают ctx, что позволяет проксировать таймауты/отмену от CLI/MCP до SQL-запросов
- DSN-экранирование (`quoteDSNValue`/`FormatDSN`) — отдельная доработка «review-dsn-race-fix»: критично для паролей со спецсимволами, удвоение кавычки НЕ работает с `lib/pq`
- `HasCompletedInit` используется только в `systemsvc.ExecuteHealth` для cheap-проверки readiness

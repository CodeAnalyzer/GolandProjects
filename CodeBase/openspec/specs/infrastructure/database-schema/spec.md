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

Система SHALL создавать таблицы для всех типов сущностей: `files`, `scan_runs`, `ds_products`, `sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`, `pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`, `report_forms`, `report_fields`, `report_params`, `vb_functions`, `h_files_defines`, `api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`, `symbols`, `relations`, `query_fragments`, `include_directives`.

#### Scenario: Создание таблицы процедур

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** таблица `sql_procedures` создана с колонками: id, file_id, procedure_name, body, start_line, end_line, ...

#### Scenario: Каталог продуктов и JS-константы

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `ds_products` (каталог продуктов Diasoft для review `foreign*`) и `js_constants` (константы из JS, попадающие в `symbols` как `constant`)

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

# Integration-тесты store / indexer

Укрепить автотесты слоёв `internal/store` и `internal/indexer`: сейчас почти только pure helpers (ключи lookup, nullable, макро-строки, сборка relations из готовых ID). Регрессии схемы, `pq.CopyIn`, prune файлов и post-process Init/Update автотестами не ловятся.

Продолжение `parser-unit-tests-plan-0e900c.md` (там явно исключены DB-тесты store/indexer).

## Контекст проблемы

Текущее покрытие:

- `internal/store/db_test.go` — ключи lookup, `Nullable*`, `sanitizeUTF8String`.
- `internal/store/api_store_test.go` — ключи API lookup.
- `internal/store/db_products_test.go` — `normalizeDSProductName`.
- `internal/indexer/*_test.go` — `#define`, SQL/JS call builders, `SELECT INTO`, relation helpers.
- `internal/rti/store_test.go` / `internal/trc/store_test.go` — константа `batchDeleteSize` и `_ = PruneSessions`.

Не покрыто:

- `InitSchema` (~700 строк CREATE/ALTER/DROP) — идемпотентность, обязательные таблицы/индексы, миграции колонок.
- `pq.CopyIn` + `WithBatchTx` — FK на незакоммиченный INSERT, битый UTF-8, пустой batch.
- `Update` prune файлов — `DeleteFilesByPaths` / `DeleteFilesByPathsExcept`.
- post-process: `subscribes_to_event`, SQL/JS `calls_procedure`, retcode-константы.
- RTI/TRC `PruneSessions` / `DeleteSession` (TRUNCATE при `keepLast=0`, batch 50K).

`sqlmock` здесь почти бесполезен: COPY и схема проверяются только живым PostgreSQL.

## Принципы

- Обычный `go test` **без** `-tags=integration` БД не трогает.
- Живой индекс `codebase` из `codebase.toml` тесты не используют.
- Сервер тот же (`localhost:5435`, user/password из конфига/env), имя БД всегда тестовое: `codebase_test_<pid>_<nano>`.
- После теста `t.Cleanup` делает `DROP DATABASE`.
- Без testcontainers на первом этапе: локальный Postgres уже есть.
- Не писать новые `TestXxx_FunctionExists`.
- Не индексировать весь `FA/` в автотестах — только крошечный `testdata/mini`.

## Подключение к Postgres

Приоритет параметров:

1. Env `CODEBASE_TEST_DSN` — полный DSN к **системной** БД `postgres` (через неё CREATE/DROP тестовой БД).
2. Иначе host/port/user/password из `codebase.toml` (`[database]`), но `database` **игнорируется**.
3. Если Postgres недоступен — `t.Skip`, не fail.

Пример запуска:

```powershell
$env:CODEBASE_TEST_DSN = "postgres://postgres:123456@localhost:5435/postgres?sslmode=disable"
go test ./internal/store/... ./internal/indexer/... ./internal/rti/... ./internal/trc/... -tags=integration -count=1
```

Без тега:

```powershell
go test ./internal/store/... ./internal/indexer/... -count=1
```

## Затронутые файлы

Новые:

- `internal/store/testutil/testdb.go` — `OpenTestDB(t)`, создание/дроп БД, `InitSchema`
- `internal/store/schema_integration_test.go` (`//go:build integration`)
- `internal/store/copy_integration_test.go`
- `internal/store/files_integration_test.go`
- `internal/indexer/testdata/mini/` — минимальное дерево исходников
- `internal/indexer/pipeline_integration_test.go`
- `internal/rti/store_integration_test.go`
- `internal/trc/store_integration_test.go`

Существующие (точечно, без ломки API):

- `internal/store/db.go` / `db_schema.go` — только если понадобится тестовый конструктор `DB` без `createDatabaseIfNotExists` на целевое имя
- `internal/indexer/*_test.go` — дополнительные pure helpers (шаг 1)
- `internal/rti/store_test.go`, `internal/trc/store_test.go` — убрать «function exists», оставить константу

Production-логика индексации/схемы не меняется, кроме минимальных швов для тестируемости (если без них нельзя открыть `*store.DB` на уже созданную тестовую БД).

## План (7 шагов)

### Шаг 1: Pure helpers без БД

Закрыть дыры, которые не требуют Postgres. Обычный `go test`.

**Indexer:**

- `normalizeParallel` (0, 1, N)
- `mergeScanStats` / `statsCollector.Add` + `Snapshot`
- `addPending*` / `snapshotPending*` (SQL calls, fragment refs, JS calls, T01, API macros) — snapshot обнуляет буфер
- `walkerPatterns` — include только поддерживаемые расширения, exclude из конфига
- callback matching: name+module, fallback name, skip пустого `UsedObjectName`, dedup

**Store (без SQL):**

- вынести нарезку `DeleteFilesByPathsExcept` (chunk 500, split withKeep/withoutKeep) и покрыть границами
- не трогать сами SQL-строки в этом шаге

**RTI/TRC:**

- удалить `TestPruneSessions_FunctionExists` / `TestDeleteSession_FunctionExists`
- оставить проверку `batchDeleteSize == 50000`

**Проверка:** `go test ./internal/store/... ./internal/indexer/... ./internal/rti/... ./internal/trc/... -count=1`

### Шаг 2: testdb helper + InitSchema

`internal/store/testutil/testdb.go`:

1. Разобрать DSN/конфиг (системная БД `postgres`).
2. `CREATE DATABASE codebase_test_<pid>_<nano>`.
3. Открыть `*store.DB` на эту БД, вызвать `InitSchema()`.
4. `t.Cleanup`: закрыть коннект, `DROP DATABASE ... WITH (FORCE)` (или disconnect + DROP).
5. При ошибке ping — `t.Skipf("postgres unavailable: %v", err)`.

Нужен способ создать `*store.DB` **минуя** `NewDB` (тот всегда целится в `cfg.Database` из конфига). Варианты:

- экспортировать/добавить `store.Open(cfg)` где `cfg.Database` уже тестовое имя;
- либо в testdb собрать `config.DBConfig` с подменённым `Database` и вызвать существующий `NewDB`.

Предпочтительно второе: меньше швов, `NewDB` сам создаст БД если её нет. Cleanup всё равно делает DROP.

Тесты (`//go:build integration`):

- `InitSchema` дважды подряд — без ошибки (идемпотентность).
- Обязательные таблицы есть: `scan_runs`, `files`, `symbols`, `sql_procedures`, `relations`, `rti_sessions`, `trc_sessions`, …
- Обязательные индексы есть: `idx_api_contracts_lower_name_kind`, `idx_symbols_symbol_name_type_lower`, составные RTI/TRC.
- Дропнутые индексы отсутствуют: `idx_relations_relation_type`, `idx_rti_calls_session_id`, `idx_trc_events_session_id`, `idx_api_contracts_name_kind`, `idx_symbols_symbol_name_lower`.
- Миграция колонок: создать урезанную `rti_sessions` без `client_events_count` и `trc_events` без `parent_id`/`depth` (на пустой БД до полного Init — либо отдельный сценарий ALTER на уже существующей таблице), затем `InitSchema` добавляет колонки.

**Проверка:** `go test ./internal/store/... -tags=integration -count=1`

### Шаг 3: COPY + WithBatchTx

Integration-тесты на той же testdb:

- `BatchInsertSQLProcedures` + `FindLatestSQLProcedureIDsByNames` — ID находятся, имена case-insensitive.
- Пустой slice — `nil` error, без Prepare COPY.
- Invalid UTF-8 в `ProcName` не валит COPY (sanitize).
- `WithBatchTx`: INSERT процедуры → сразу SELECT ID **в той же tx** (регресс «FK = 0», см. комментарий в `db_tx.go`).
- `BatchInsertRelations` + повторная вставка тех же пар — нет паники; поведение дублей зафиксировать как есть (сейчас UNIQUE на relations нет).
- `CreateScanRun` / `UpdateScanRun` / `HasCompletedInit`.

**Проверка:** `go test ./internal/store/... -tags=integration -count=1`

### Шаг 4: prune файлов (store)

Тесты `DeleteFilesByPath`, `DeleteFilesByPaths`, `DeleteFilesByPathsExcept`:

- Два `files` с одним `path`, разные `id` — except сохраняет `keepID`, второй уходит.
- Children (`sql_procedures`, `symbols`) уходят CASCADE вместе со старым `file_id`.
- Пустой список paths — no-op.
- Нарезка >500 paths не ломает `ANY($1)`.
- `GetLatestFilesByRootPath` возвращает последний id на path (`DISTINCT ON (path) … id DESC`).

**Проверка:** `go test ./internal/store/... -tags=integration -count=1`

### Шаг 5: Indexer golden Init/Update

Минимальное дерево `internal/indexer/testdata/mini/`:

```text
mini/
  a.sql          -- create proc CallerA; exec CalleeB
  b.sql          -- create proc CalleeB
  c.js           -- ExecProc("CalleeB")
  event.xml      -- event + callback_event на это событие
  ret.h          -- #define LOC_RETCODE_X ...
  ret.sql        -- ds_return_codes с константой
```

Файлы — крошечные, кодировки как в проде (SQL/H — смысл важнее байтов; писать ASCII/UTF-8 достаточно, walker проставит encoding).

Сценарии (`//go:build integration`):

1. `Init(root, parallel=1)` — в БД есть procs `CallerA`/`CalleeB`, relation `calls_procedure`, JS `calls_procedure`, `subscribes_to_event`.
2. Повторный `Update(onlyModified=true)` без изменений файлов — нет новых file rows, relations не размножились сверх ожидаемого (для `subscribes_to_event` — полный rebuild, количество стабильно).
3. Правка `a.sql` + `Update(onlyModified=true)` — старый `file_id` удалён, новые relations смотрят на новый ID.
4. Удаление `c.js` с диска + `Update` — path исчез, висячих JS relations нет.
5. Тот же Init с `parallel=4` — тот же набор relations (нет гонки pending-state).

Progress reporter пишет в stdout — для тестов допустимо; не глушить, если не мешает.

**Проверка:** `go test ./internal/indexer/... -tags=integration -count=1`

### Шаг 6: RTI/TRC prune

На testdb после `InitSchema`:

- Вставить 3 лёгкие сессии (по несколько child rows, не миллионы).
- `PruneSessions(keepLast=1)` — остаётся одна, children чужих сессий пустые.
- `PruneSessions(keepLast=0)` — TRUNCATE, следующая вставка жива (identity restart не ломает INSERT).
- `DeleteSession(id)` — чужие сессии не тронуты.
- `keepLast < 0` — ошибка/no-op: зафиксировать фактическое поведение.

Заменить бессмысленные existence-тесты.

**Проверка:** `go test ./internal/rti/... ./internal/trc/... -tags=integration -count=1`

### Шаг 7: Документация запуска и фиксация

- Короткий блок в `README.md` или в этом файле (достаточно этого плана): как задать DSN, тег, что БД создаётся/удаляется.
- Явно: не указывать в DSN имя `codebase`.
- Команды верификации всего контура (см. ниже).

## Проверка

После каждого шага — unit без тега. После шагов 2–6 — integration при доступном Postgres.

Финальный набор:

```powershell
go build ./...
go vet ./internal/store/... ./internal/indexer/... ./internal/rti/... ./internal/trc/...
go test ./internal/store/... ./internal/indexer/... ./internal/rti/... ./internal/trc/... -count=1
go test ./internal/store/... ./internal/indexer/... ./internal/rti/... ./internal/trc/... -tags=integration -count=1
```

На живой `codebase` после прогона не должно появиться БД `codebase_test_*` (кроме сирот после kill -9 — тогда разово `DROP DATABASE`).

## Критерии готовности

- Без `-tags=integration` все тесты зелёные и не требуют Postgres.
- С тегом и доступным Postgres: схема идемпотентна, COPY+tx, file prune, Init/Update mini-tree, RTI/TRC prune — зелёные.
- Рабочая БД `codebase` не изменяется.
- Тестовая БД удаляется в cleanup.
- Нет новых existence-тестов.

## Не включать в этот план

- testcontainers / Docker как обязательный runtime.
- Покрытие всего `internal/query` живой БД (отдельный контур; есть ручной `Tests/codebase-testing-plan.md`).
- E2E CLI на полном `FA/`.
- Рефакторинг production ради процента coverage.
- Нагрузочные тесты prune на миллионах строк.

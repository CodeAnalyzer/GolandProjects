# Сквозное протягивание context.Context в слое БД

Устранение отсутствия context.Context в DB-вызовах: 128 вызовов `.Query(`/.QueryRow(`/.Exec(` без context против 3 с context. MCP-сервер (долгоживущий процесс) не может отменить/ограничить по таймауту ни один запрос. В review `ctx` формально передаётся в `ruleTask.run(ctx)`, но ни одно правило его не использует.

---

## Контекст проблемы

### Текущее состояние по слоям

| Слой | Файлов | Вызовов без ctx | Вызовов с ctx | `context` импортирован |
|---|---|---|---|---|
| `internal/store` (DB-обёртка) | 16 | 98 Exec + 42 Query + 39 QueryRow = **179** | 1 ExecContext + 3 QueryContext (только определения) | да (db_tx.go, db.go) |
| `internal/query` | 4 | **42** Query | 0 | **нет** |
| `internal/review` | 2 | **5** QueryRow + ~15 Query (review_lookup.go) | 0 | да (runner.go), но не используется в правилах |
| `internal/rti/store.go` | 1 | **~25** Query/QueryRow/Exec | 0 | **нет** |
| `internal/trc/store.go` | 1 | **~15** Query/QueryRow/Exec | 0 | **нет** |
| `internal/indexer` | 2 | 0 (делегирует в store) | 0 | **нет** |
| `internal/mcp` | 2 | 0 (делегирует в query/review/rti/trc) | 0 (context.Background в RunStdio) | да (server.go) |
| `cmd` | 6 | 0 (делегирует) | 0 | **нет** |

### Ключевые находки

1. **MCP SDK передаёт `ctx context.Context`** в tool handler (`server.go:57`), но `toolHandler` определён как `func(args map[string]interface{}) (interface{}, error)` (`registry.go:21`) — **без ctx**. `ctx` от SDK игнорируется.

2. **`DB` в `db_tx.go`** уже имеет `QueryContext`/`QueryRowContext` (`:73-86`), но они почти не вызываются. Основные методы — `Query`/`QueryRow`/`exec` без context (`:47-69`). `WithBatchTx` использует `db.Begin()`, а не `db.BeginTx(ctx, nil)`.

3. **Review `runRuleTasks`** (`runner.go:490-518`) передаёт `ctx` в `task.run(ctx)` (`:509`), но ни одна closure в `buildRuleTasks` (`:176-470`) его не принимает — все вызывают `r.checkXxx(parsed, file)` без ctx. `cancel()` останавливает только диспетчеризацию задач по каналу, но не прерывает DB-запрос внутри работающего правила.

4. **CLI** (`cmd/`) не использует `context` вообще — нет таймаутов, нет отмены по Ctrl+C (только SIGKILL).

### Следствия

- **MCP-сервер:** зависший SQL-запрос блокирует tool handler до завершения или обрыва TCP. Нет таймаутов.
- **Review:** `cancel()` не прерывает уже запущенный DB-запрос внутри правила (например, `checkIndexExistsInDB` делает `r.db.Query(...)` — не отменится).
- **Indexer:** нет graceful shutdown по сигналу.

---

## Принцип

- `context.Context` — **первый параметр** всех публичных методов store, query, review, rti, trc.
- Store-слой: `Query`/`QueryRow`/`exec` → deprecated thin wrappers, новые методы `QueryCtx`/`QueryRowCtx`/`execCtx` (или прямые вызовы `QueryContext`/`QueryRowContext`/`ExecContext`).
- `WithBatchTx` → `WithBatchTxCtx(ctx, fn)`.
- MCP: `toolHandler` → `func(ctx context.Context, args map[string]interface{}) (interface{}, error)`. SDK `ctx` пробрасывается во все слои. Дополнительно: `context.WithTimeout` на каждый tool-вызов (настраиваемый, по умолчанию 30s).
- CLI: `cmd.Execute()` создаёт root context с `signal.NotifyContext(SIGINT, SIGTERM)`, передаёт в подкоманды.
- Review: все `checkXxx` принимают `ctx`, передают в `r.db.QueryContext(ctx, ...)`.
- Indexer: `Init`/`Update` принимают `ctx`, передают в `saveFile`/`processFile`/postprocess.

---

## Затронутые файлы

| Файл | Изменение |
|---|---|
| `internal/store/db_tx.go` | +`execCtx`, deprecate `exec`/`Query`/`QueryRow`, `WithBatchTxCtx` |
| `internal/store/db_lookup_sql.go` | все методы → +ctx, `QueryContext`/`QueryRowContext` |
| `internal/store/db_lookup_pas.go` | аналогично |
| `internal/store/db_lookup_dfm.go` | аналогично |
| `internal/store/db_lookup_j.go` | аналогично |
| `internal/store/db_lookup_h.go` | аналогично |
| `internal/store/db_lookup_reports.go` | аналогично |
| `internal/store/db_lookup_retcode.go` | аналогично |
| `internal/store/db_files.go` | аналогично |
| `internal/store/db_stats.go` | аналогично |
| `internal/store/db_scan_runs.go` | аналогично |
| `internal/store/db_products.go` | аналогично |
| `internal/store/db_schema.go` | `InitSchema` → +ctx |
| `internal/store/api_store.go` | все методы → +ctx |
| `internal/store/db_insert_*.go` | `withCopyInTx` → +ctx, `BeginTx` |
| `internal/store/db_resolve_retcode.go` | +ctx |
| `internal/query/query.go` | все `SearchXxx` → +ctx, `q.db.QueryContext` |
| `internal/query/api_query.go` | аналогично |
| `internal/query/query_sql.go` | аналогично |
| `internal/query/query_relations.go` | аналогично |
| `internal/querysvc/runtime.go` | `Execute`/`ExecuteWith` → +ctx |
| `internal/review/runner.go` | `Review` → +ctx, `buildRuleTasks` → ctx в closures |
| `internal/review/review_rules.go` | все `checkXxx` → +ctx |
| `internal/review/review_helpers.go` | все lookup → +ctx |
| `internal/review/review_lookup.go` | все `r.db.Query` → `r.db.QueryContext` |
| `internal/review/review_parser.go` | без изменений (чистый парсинг, без DB) |
| `internal/rti/store.go` | все функции → +ctx, `db.QueryContext` |
| `internal/trc/store.go` | все функции → +ctx, `db.QueryContext` |
| `internal/indexer/indexer.go` | `saveFile`/`processFile` → +ctx |
| `internal/indexer/runner.go` | `Init`/`Update` → +ctx, `runPostProcessingParallel` → +ctx |
| `internal/mcp/registry.go` | `toolHandler` → +ctx, все handler'ы → +ctx |
| `internal/mcp/server.go` | SDK ctx → toolHandler, +timeout |
| `internal/mcp/pagination.go` | без изменений |
| `internal/config/config.go` | +`MCP.QueryTimeoutSec` (по умолчанию 30) |
| `cmd/root.go` | root context с signal.NotifyContext |
| `cmd/query_commands.go` | +ctx в runQueryCommand |
| `cmd/review.go` | +ctx в Review |
| `cmd/rti.go` | +ctx в rti-командах |
| `cmd/trc.go` | +ctx в trc-командах |
| `cmd/stats.go` | +ctx |
| `cmd/init.go` / `cmd/update.go` | +ctx в indexer |

Оценка: ~40 файлов, ~250 сигнатур. Изменения механические (добавить параметр + заменить вызов).

---

## План (6 фаз)

### Фаза 1 — Store: ctx-методы в `db_tx.go` *(остановка — ждать акцепт)*

**Файл:** `internal/store/db_tx.go`

Добавить новые методы с ctx, сохранить старые как deprecated thin-wrappers:

```go
func (db *DB) execCtx(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
    if db.boundTx != nil {
        return db.boundTx.ExecContext(ctx, query, args...)
    }
    return db.DB.ExecContext(ctx, query, args...)
}

func (db *DB) WithBatchTxCtx(ctx context.Context, fn func(txdb *DB) error) (err error) {
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer func() {
        if err != nil {
            _ = tx.Rollback()
            return
        }
        err = tx.Commit()
    }()
    return fn(&DB{DB: db.DB, boundTx: tx})
}
```

`withCopyInTx` → `withCopyInTxCtx(ctx, fn)`.

Старые `exec`/`Query`/`QueryRow`/`WithBatchTx` — остаются как thin wrappers без ctx (вызывают новые с `context.Background()`). Это сохранит обратную совместимость и позволит мигрировать вызовы поэтапно.

**Проверка:** `go build ./internal/store/...` + `go vet ./internal/store/...` + `go test ./internal/store/... -count=1`

---

### Фаза 2 — Store: миграция `db_lookup_*.go`, `api_store.go`, `db_insert_*.go` *(остановка — ждать акцепт)*

**Файлы:** все `internal/store/db_lookup_*.go`, `internal/store/api_store.go`, `internal/store/db_insert_*.go`, `internal/store/db_files.go`, `internal/store/db_stats.go`, `internal/store/db_scan_runs.go`, `internal/store/db_products.go`, `internal/store/db_schema.go`, `internal/store/db_resolve_retcode.go`

Для каждого публичного метода:
- Добавить `ctx context.Context` первым параметром
- Заменить `db.Query(...)` → `db.QueryContext(ctx, ...)`, `db.QueryRow(...)` → `db.QueryRowContext(ctx, ...)`, `db.exec(...)` → `db.execCtx(ctx, ...)`
- `db.WithBatchTx(fn)` → `db.WithBatchTxCtx(ctx, fn)` где применимо

`InitSchema` → `InitSchemaCtx(ctx)`. Старый `InitSchema` — thin wrapper.

**Проверка:** `go build ./internal/store/...` + `go vet` + `go test ./internal/store/... -count=1`

---

### Фаза 3 — Query + сервисы: `query/`, `querysvc/`, `rti/store.go`, `trc/store.go` *(остановка — ждать акцепт)*

**Файлы:**
- `internal/query/query.go`, `api_query.go`, `query_sql.go`, `query_relations.go`
- `internal/querysvc/runtime.go`
- `internal/rti/store.go`
- `internal/trc/store.go`

Для каждого метода `SearchXxx` / `LoadXxx` / `SaveSession` / `DeleteSession` / `PruneSessions` / и т.д.:
- Добавить `ctx context.Context` первым параметром
- Заменить `q.db.Query(...)` → `q.db.QueryContext(ctx, ...)`, `q.db.QueryRow(...)` → `q.db.QueryRowContext(ctx, ...)`
- `db.Exec(...)` → `db.ExecContext(ctx, ...)` (через `execCtx`)

`querysvc.Execute` → `ExecuteCtx(ctx, run)`. `ExecuteWith` → `ExecuteWithCtx(ctx, db, run)`. Старые — thin wrappers.

**Проверка:** `go build ./internal/query/... ./internal/querysvc/... ./internal/rti/... ./internal/trc/...` + `go vet` + `go test ./internal/query/... ./internal/rti/... ./internal/trc/... -count=1`

---

### Фаза 4 — Review: протащить ctx в правила *(остановка — ждать акцепт)*

**Файлы:** `internal/review/runner.go`, `review_rules.go`, `review_helpers.go`, `review_lookup.go`

1. `Runner.Review` → `ReviewCtx(ctx, opts)` (старый `Review` — thin wrapper с `context.Background()`).
2. `buildRuleTasks` — каждая closure принимает `ctx` и передаёт в `r.checkXxx(ctx, parsed, file)`.
3. Все `checkXxx` → добавить `ctx context.Context` первым параметром.
4. Все `r.db.Query(...)` → `r.db.QueryContext(ctx, ...)`, `r.db.QueryRow(...)` → `r.db.QueryRowContext(ctx, ...)`.
5. Все batch-lookup вызовы (`r.db.BatchLookupProcedureParams` и т.д.) → +ctx.
6. `runRuleTasks` уже передаёт `ctx` в `task.run(ctx)` — теперь замыкания будут его реально использовать.

**Проверка:** `go build ./internal/review/...` + `go vet` + `go test ./internal/review/... -count=1`

---

### Фаза 5 — MCP: `toolHandler` + ctx + timeout *(остановка — ждать акцепт)*

**Файлы:** `internal/mcp/registry.go`, `internal/mcp/server.go`, `internal/config/config.go`

1. `toolHandler` → `func(ctx context.Context, args map[string]interface{}) (interface{}, error)`.
2. В `registerSDKCoreTools` (`server.go:57-73`): `tool.Handler(ctx, args)` вместо `tool.Handler(args)`.
3. Конфиг — два независимых таймаута в `MCPConfig` (`config.go`):
   ```go
   type MCPConfig struct {
       PaginationChunkSize int    `toml:"pagination_chunk_size"`
       PaginationTTL       string `toml:"pagination_ttl"`
       QueryTimeoutSec     int    `toml:"query_timeout_sec"`   // default 30
       ReviewTimeoutSec    int    `toml:"review_timeout_sec"`  // default 120
   }
   ```
   Defaults в `defaultConfig()`: `QueryTimeoutSec: 30`, `ReviewTimeoutSec: 120`.
   В `codebase.toml`:
   ```toml
   [mcp]
   pagination_chunk_size = 8000
   pagination_ttl = "15m"
   query_timeout_sec = 30
   review_timeout_sec = 120
   ```
   Review имеет отдельный таймаут (120s), т.к. анализ большого SQL-файла с ~40 правилами и DB-lookup'ами может занимать больше времени, чем точечный query.
4. В SDK handler — выбор таймаута по типу инструмента:
   ```go
   timeout := time.Duration(cfg.MCP.QueryTimeoutSec) * time.Second
   if tool.Definition.Name == "codebase_review_sql" {
       timeout = time.Duration(cfg.MCP.ReviewTimeoutSec) * time.Second
   }
   ctx, cancel := context.WithTimeout(ctx, timeout)
   defer cancel()
   result, err := tool.Handler(ctx, args)
   ```
   `0` = без таймаута (ctx от SDK передаётся как есть).
5. Все handler'ы в `registry.go` — принять `ctx`, передать в `querysvc.ExecuteWithCtx(ctx, db, run)`, `review.Runner.ReviewCtx(ctx, opts)`, `rti.LoadCalls(ctx, db, ...)`, `trc.LoadEvents(ctx, db, ...)`, `store.NewDB` / `db.InitSchemaCtx(ctx)`.
6. `buildToolRegistry(db)` — без изменений (возвращает map, ctx приходит в handler).

**Проверка:** `go build ./internal/mcp/...` + `go vet` + `go test ./internal/mcp/... -count=1`

---

### Фаза 6 — CLI: root context + signal handling *(остановка — ждать акцепт)*

**Файлы:** `cmd/root.go`, `cmd/query_commands.go`, `cmd/review.go`, `cmd/rti.go`, `cmd/trc.go`, `cmd/stats.go`, `cmd/init.go`, `cmd/update.go`, `internal/indexer/indexer.go`, `internal/indexer/runner.go`

1. `cmd/root.go`: в `Execute()` создать root context:
   ```go
   ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
   defer cancel()
   rootCmd.SetContext(ctx)
   ```
2. Каждая подкоманда: `ctx := cmd.Context()` → передать в `querysvc.ExecuteWithCtx(ctx, ...)`, `review.ReviewCtx(ctx, ...)`, `rti.LoadCalls(ctx, ...)`, `indexer.Init(ctx, ...)`, `indexer.Update(ctx, ...)`.
3. `indexer.Init` / `Update` → +ctx, передаёт в `saveFile`, `processFile`, `runPostProcessingParallel`.
4. Воркеры в `processFilesWorkerPoolInit` / `processFilesWorkerPool` — `select <-ctx.Done()` для graceful shutdown.

**Проверка:** `go build ./...` + `go vet ./...` + `go test ./... -count=1`

---

## Откат

Все изменения обратно совместимы: старые методы без ctx остаются как thin wrappers (`context.Background()`). Откат — revert конкретной фазы. Частичный откат (только MCP, без CLI) возможен.

## Ожидаемый результат

| Метрика | Было | Стало |
|---|---|---|
| DB-вызовов без ctx | 179 | 0 (все через QueryContext/ExecContext) |
| MCP tool: отмена по таймауту | невозможно | 30s (настраиваемо) |
| MCP tool: отмена по SIGTERM | невозможно | ctx отменяется, DB-запрос прерывается |
| Review: cancel() прерывает DB-запрос | нет | да |
| CLI: Ctrl+C прерывает операцию | SIGKILL | graceful shutdown через ctx |
| Indexer: graceful shutdown | нет | да (воркеры проверяют ctx.Done()) |

## Риски

- **Производительность:** `context.Background()` в thin wrappers не добавляет overhead. Прямые вызовы `QueryContext` идентичны `Query` по производительности.
- **Тесты:** тесты, вызывающие методы store/query напрямую, нужно обновить (добавить `context.Background()`). Механическое изменение.
- **Indexer parallel workers:** добавление `ctx.Done()` в select воркеров — точка отказа если ctx отменяется случайно. Нужно убедиться, что root ctx в CLI отменяется только по сигналу.
- **MCP timeout:** два независимых таймаута в конфиге — `query_timeout_sec` (30s) для query/rti/trc/tools и `review_timeout_sec` (120s) для `codebase_review_sql`. `0` = без таймаута. Пользователь может увеличить/отключить для тяжёлых запросов (например, `like=true` на большой БД). CLI не использует эти таймауты — только MCP.

# MCP Shared DB Connection

Переход MCP query-инструментов с «новое соединение на каждый вызов» на единое shared `*store.DB` на всё время жизни сервера.

---

## Scope

| Группа | Инструментов | Изменение |
|---|---|---|
| query (symbol, table, callers, …) | 28 | ✅ shared DB |
| codebase_ping | 1 | без изменений |
| codebase_health, codebase_stats | 2 | без изменений |

Поведение при сбое DB: `sql.DB` переподключается автоматически; неудача → tool возвращает `isError`.

---

## Фаза 1 — `querysvc.ExecuteWith` *(остановка — ждать акцепт)*

**Файл:** `internal/querysvc/runtime.go`

Добавить одну новую функцию рядом с существующей `Execute`:

```go
// ExecuteWith выполняет query-runner на уже открытом соединении.
// InitSchema не вызывается — предполагается, что схема уже инициализирована.
func ExecuteWith(db *store.DB, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
    q := query.New(db)
    results, err := run(q)
    if err != nil {
        return nil, fmt.Errorf("query failed: %w", err)
    }
    return results, nil
}
```

`Execute` остаётся без изменений — CLI-путь не трогаем.

**Проверка после фазы:** `go build ./internal/querysvc/...` + `go test ./internal/querysvc/...`

---

## Фаза 2 — рефакторинг `registry.go` *(остановка — ждать акцепт)*

**Файл:** `internal/mcp/registry.go`

### 2a. Добавить `runQueryOpt`

```go
// runQueryOpt использует db если не nil, иначе открывает соединение сам.
func runQueryOpt(db *store.DB, run func(q *query.Query) (interface{}, error)) (interface{}, error) {
    var items interface{}
    var err error
    if db != nil {
        items, err = querysvc.ExecuteWith(db, run)
    } else {
        items, err = querysvc.Execute(run)
    }
    if err != nil {
        return nil, err
    }
    return map[string]interface{}{
        "count": resultCount(items),
        "items": normalizeNilResults(items),
    }, nil
}
```

Существующий `runQuery` переписать через `runQueryOpt(nil, run)` — тесты и обратная совместимость сохраняются.

### 2b. Ввести фабрику `buildToolRegistry`

- Переименовать `var fullToolRegistry = map[string]registeredTool{...}` в функцию:
  ```go
  func buildToolRegistry(db *store.DB) map[string]registeredTool { ... }
  ```
- Внутри: механическая замена `runQuery(func(q)...)` → `runQueryOpt(db, func(q)...)`  
  (`codebase_ping`, `codebase_health`, `codebase_stats` — не трогать)
- Обновить package-level var:
  ```go
  var toolRegistry = buildToolRegistry(nil)   // ← тесты видят тот же реестр
  ```

**Проверка после фазы:** `go build ./internal/mcp/...` + `go test ./internal/mcp/...`

---

## Фаза 3 — `server.go`: открыть DB в `RunStdio` *(остановка — ждать акцепт)*

**Файл:** `internal/mcp/server.go`

```go
func RunStdio(serverVersion string) error {
    cfg := config.Get()
    if cfg == nil {
        return fmt.Errorf("config not loaded")
    }

    db, err := store.NewDB(cfg.DB)
    if err != nil {
        return fmt.Errorf("failed to connect to database: %w", err)
    }
    defer db.Close()

    if err := db.InitSchema(); err != nil {
        return fmt.Errorf("failed to init schema: %w", err)
    }

    server := mcpsdk.NewServer(&mcpsdk.Implementation{
        Name:    "codebase",
        Version: serverVersion,
    }, nil)

    registerSDKCoreTools(server, buildToolRegistry(db))   // ← передаём registry с db

    return server.Run(context.Background(), &mcpsdk.StdioTransport{})
}
```

`registerSDKCoreTools` получает параметр `registry map[string]registeredTool` вместо `toolRegistry` из глобала.

**Проверка после фазы:** `go build ./...` + `go test ./...`

---

## Затронутые файлы

| Файл | Изменение |
|---|---|
| `internal/querysvc/runtime.go` | +`ExecuteWith` |
| `internal/mcp/registry.go` | +`runQueryOpt`, `buildToolRegistry`, обновить `toolRegistry` |
| `internal/mcp/server.go` | открыть DB, передать `registry` в `registerSDKCoreTools` |
| `internal/mcp/server_test.go` | скорее всего без изменений |

CLI-путь (`cmd/`, `querysvc.Execute`) **не меняется**.

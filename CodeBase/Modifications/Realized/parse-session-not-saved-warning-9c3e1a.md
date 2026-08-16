# Предупреждение о несохранённой сессии при недоступной БД (rti parse / trc parse)

Добавление явного предупреждения когда `rti parse` / `trc parse` отрабатывают без БД — парсинг успешен, но сессия не сохранена (`session_id=0`). Пользователь и LLM-агент должны видеть это сразу, а не обнаруживать позже.

## Контекст проблемы

При недоступной БД (`openDB()` возвращает `nil`) `ExecuteParse` в обоих сервисных слоях (`rtisvc` и `trcsvc`) корректно парсит файл, но возвращает `ParseResult{SessionID: 0}` без ошибки. CLI молча печатает summary без строки "Saved session", а MCP-ответ содержит `session_id: 0` без пояснений.

**Текущие пути кода:**

- `cmd/root.go:312-323` — `openDB()`: `nil` если конфига нет или `store.NewDB` упал.
- `cmd/rti.go:127-143` — `runRTIParse`: вызывает `rtisvc.ExecuteParse(ctx, db, args[0])`, печатает summary, `if result.SessionID > 0` — иначе молчит.
- `cmd/trc.go:120-137` — `runTRCParse`: аналогично.
- `internal/rtisvc/runtime.go:67-96` — `ExecuteParse`: при `db == nil` возвращает `ParseResult{SessionID: 0}` без ошибки.
- `internal/trcsvc/runtime.go:49-72` — `ExecuteParse`: аналогично.
- `internal/mcp/registry.go:573,773` — MCP-обработчики возвращают `ParseResult` как есть.

**Последствие:** пользователь видит успешный summary, предполагает что сессия сохранена, позже не находит её в `rti list` / `trc list`.

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/rtisvc/types.go` | добавить поле `Warning string` в `ParseResult` |
| `internal/trcsvc/types.go` | добавить поле `Warning string` в `ParseResult` |
| `internal/rtisvc/runtime.go` | в `ExecuteParse` при `db == nil` ставить `Warning` |
| `internal/trcsvc/runtime.go` | в `ExecuteParse` при `db == nil` ставить `Warning` |
| `cmd/rti.go` | в `runRTIParse` при `SessionID == 0` печатать предупреждение в stderr |
| `cmd/trc.go` | в `runTRCParse` при `SessionID == 0` печатать предупреждение в stderr |
| `internal/rtisvc/runtime_test.go` | обновить `TestExecuteParse_FileMode` — проверить `Warning` |
| `internal/trcsvc/runtime_test.go` | обновить `TestExecuteParse_FileMode` — проверить `Warning` |

## План (3 шага)

### Шаг 1: Поле Warning в ParseResult (rtisvc + trcsvc)

В `internal/rtisvc/types.go`:

```go
type ParseResult struct {
    SessionID  int64          `json:"session_id"`
    TotalCalls int            `json:"total_calls"`
    Summary    rti.RTISummary `json:"summary"`
    Warning    string         `json:"warning,omitempty"`
}
```

В `internal/trcsvc/types.go`:

```go
type ParseResult struct {
    SessionID   int64  `json:"session_id"`
    TotalEvents int    `json:"total_events"`
    Warning     string `json:"warning,omitempty"`
}
```

В `internal/rtisvc/runtime.go` (`ExecuteParse`, ветка `db == nil`, строка 91):

```go
return &ParseResult{
    SessionID:  0,
    TotalCalls: result.Summary.TotalCalls,
    Summary:    result.Summary,
    Warning:    "database unavailable, session not saved",
}, nil
```

В `internal/trcsvc/runtime.go` (`ExecuteParse`, ветка `db == nil`, строка 68):

```go
return &ParseResult{
    SessionID:   0,
    TotalEvents: len(result.Events),
    Warning:     "database unavailable, session not saved",
}, nil
```

### Шаг 2: Предупреждение в CLI (cmd/rti.go + cmd/trc.go)

В `cmd/rti.go` (`runRTIParse`, строки 139-141):

```go
if result.SessionID > 0 {
    fmt.Printf("Saved session: %d\n", result.SessionID)
} else {
    fmt.Fprintf(os.Stderr, "Warning: database unavailable, session not saved\n")
}
```

В `cmd/trc.go` (`runTRCParse`, строки 133-135):

```go
if result.SessionID > 0 {
    fmt.Printf("Saved session: %d\n", result.SessionID)
} else {
    fmt.Fprintf(os.Stderr, "Warning: database unavailable, session not saved\n")
}
```

Убедиться что `os` импортирован в обоих файлах (вероятно уже есть).

### Шаг 3: Тесты

В `internal/rtisvc/runtime_test.go` (`TestExecuteParse_FileMode`):

```go
if result.Warning == "" {
    t.Errorf("Warning should be set when db is nil")
}
```

В `internal/trcsvc/runtime_test.go` (`TestExecuteParse_FileMode`):

```go
if result.Warning == "" {
    t.Errorf("Warning should be set when db is nil")
}
```

## Проверка

1. `go build ./...` — чисто.
2. `go vet ./internal/rtisvc/... ./internal/trcsvc/... ./cmd/...` — чисто.
3. `go test ./internal/rtisvc/... ./internal/trcsvc/... -count=1` — PASS, `Warning` поле заполнено при `db == nil`.
4. CLI smoke (без БД): `codebase rti parse <file>` — в stderr видно "Warning: database unavailable, session not saved".
5. CLI smoke (без БД): `codebase trc parse <file>` — аналогично.
6. MCP smoke: вызов `codebase_rti_parse` / `codebase_trc_parse` без БД — JSON содержит `"warning": "database unavailable, session not saved"`.

## Откат

Изменения обратно совместимы: `Warning` — `omitempty` поле, существующие потребители MCP/CLI не затронуты. Удаление предупреждения в stderr и поля `Warning` возвращает прежнее поведение.

## Ожидаемый результат

| Сценарий | Было | Стало |
|---|---|---|
| `rti parse` без БД (CLI) | summary без "Saved session", молча | summary + stderr "Warning: database unavailable, session not saved" |
| `trc parse` без БД (CLI) | summary без "Saved session", молча | summary + stderr "Warning: database unavailable, session not saved" |
| `codebase_rti_parse` без БД (MCP) | `{"session_id":0, ...}` без пояснения | `{"session_id":0, "warning":"database unavailable, session not saved", ...}` |
| `codebase_trc_parse` без БД (MCP) | `{"session_id":0, ...}` без пояснения | `{"session_id":0, "warning":"database unavailable, session not saved", ...}` |
| `rti parse` с БД (CLI) | без изменений | без изменений |
| `trc parse` с БД (CLI) | без изменений | без изменений |

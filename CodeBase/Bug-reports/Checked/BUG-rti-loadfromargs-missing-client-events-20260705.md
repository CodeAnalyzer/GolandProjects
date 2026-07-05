# `loadRTIFromArgs` не загружает клиентские события из БД — `codebase_rti_client_tree`, `codebase_rti_timeline`, `codebase_rti_errors`, `codebase_rti_slow` возвращают пустые результаты при `session_id`

**Дата:** 2026-07-05
**Файл:** `internal/mcp/registry.go`, функция `loadRTIFromArgs` (строка ~1015)
**Версия CodeBase:** 0.8.3 build 1164
**Статус:** Не исправлено

## Описание

Функция `loadRTIFromArgs` в `internal/mcp/registry.go` при загрузке RTI-данных через `session_id` (из БД) вызывает только `rti.LoadCalls(db, sessionID)`, но **не вызывает** `rti.LoadClientEvents(db, sessionID)`. В результате `RTIParseResult.ClientEvents` остаётся `nil`, и все MCP-инструменты, обращающиеся к `result.ClientEvents`, возвращают пустые результаты.

При этом парсинг клиентских событий реализован полностью: `parser_client.go` парсит BPL-листинги, connection-блоки, SQL-блоки, trancount, memory, ошибки; `SaveSession` корректно сохраняет клиентские события в БД через `insertRTIClientEvents`; функция `LoadClientEvents` существует в `store.go` (строка ~797), но **никогда не вызывается** из `loadRTIFromArgs`.

## Воспроизведение

1. Спарсить клиентский RTI-лог через `codebase_rti_parse` с `file_path` → получить `session_id`. Парсинг сохранит клиентские события в БД (таблица `rti_client_events`).
2. Вызвать `codebase_rti_client_tree` с `session_id=<X>`.
3. Получить пустой результат: `nodes: []`.
4. Вызвать `codebase_rti_timeline` с `session_id=<X>`.
5. Получить `client_events: null` (или пустой массив).
6. Вызвать `codebase_rti_errors` с `session_id=<X>` для лога, содержащего клиентские `SEVERE`-ошибки.
7. Получить `client_errors: null` (или пустой массив).
8. Вызвать `codebase_rti_slow` с `session_id=<X>` для лога, содержащего медленные клиентские SQL-блоки.
9. Получить `client_slow_sql: null` (или пустой массив).

### Альтернативный путь — работает

Вызвать любой из этих инструментов с `file_path` вместо `session_id` → `loadRTIFromArgs` вызовет `rti.ParseFile(filePath)`, который полностью парсит и серверные, и клиентские события. Результаты корректны.

## Ожидаемый результат

При вызове с `session_id`:
- `codebase_rti_client_tree` возвращает дерево клиентских событий, сгруппированных по PID.
- `codebase_rti_timeline` возвращает объединённую хронологическую ленту серверных вызовов и клиентских событий.
- `codebase_rti_errors` возвращает клиентские ошибки (`Kind == "error"`) наряду с серверными.
- `codebase_rti_slow` возвращает медленные клиентские SQL-блоки наряду с серверными.
- `codebase_rti_summary` включает `ClientEventsCount` в сводку.

## Фактический результат

- `result.ClientEvents == nil` → все клиентские данные отсутствуют.
- `codebase_rti_client_tree` → `nodes: []`.
- `codebase_rti_timeline` → `client_events: null`.
- `codebase_rti_errors` → `client_errors: null`.
- `codebase_rti_slow` → `client_slow_sql: null`.
- `codebase_rti_summary` → `ClientEventsCount: 0` (не заполняется из `session.ClientEventsCount`).

## Анализ причины

### `loadRTIFromArgs` — `session_id` путь

Файл: `internal/mcp/registry.go`, строки 1015–1050.

```go
func loadRTIFromArgs(db *store.DB, args map[string]interface{}) (*rti.RTIParseResult, error) {
    sessionID, err := optionalInt64(args, "session_id")
    // ...
    if sessionID > 0 {
        // ...
        calls, err := rti.LoadCalls(db, sessionID)
        // ...
        return &rti.RTIParseResult{
            Calls: calls,
            Summary: rti.RTISummary{
                FilePath:    session.FilePath,
                FileSize:    session.FileSize,
                TotalCalls:  session.TotalCalls,
                ErrorsCount: session.ErrorsCount,
            },
        }, nil  // ← ClientEvents не загружены, ClientEventsCount не заполнен
    }
    // ...
    return rti.ParseFile(filePath)  // ← file_path путь работает корректно
}
```

Проблема: `RTIParseResult.ClientEvents` не заполняется. Функция `rti.LoadClientEvents(db, sessionID)` существует в `store.go` (строка ~797), но не вызывается.

### Цепочка вызовов в MCP-инструментах

Все затронутые инструменты вызывают `loadRTIFromArgs` и затем обращаются к `result.ClientEvents`:

| Инструмент | Файл / строка | Использование `ClientEvents` |
|-----------|---------------|------------------------------|
| `codebase_rti_errors` | registry.go:636 | `for _, ev := range result.ClientEvents { if ev.Kind == "error" ... }` |
| `codebase_rti_slow` | registry.go:679 | `for _, ev := range result.ClientEvents { if ev.Kind == "sql_block" ... }` |
| `codebase_rti_client_tree` | registry.go:833 | `rti.BuildClientTree(result.ClientEvents, pid)` |
| `codebase_rti_timeline` | registry.go:853–855 | `result.ClientEvents` передаётся в ответ |

При `session_id` пути `result.ClientEvents == nil` → все циклы не выполняются, `BuildClientTree` возвращает пустой slice.

### `SaveSession` — корректно сохраняет

Файл: `internal/rti/store.go`, строка 57:

```go
if _, err := insertRTIClientEvents(db, result.ClientEvents, sessionID, callIDs); err != nil {
    return 0, fmt.Errorf("failed to insert rti_client_events: %w", err)
}
```

Данные в БД есть, но не читаются обратно.

### `LoadClientEvents` — существует, но не вызывается

Файл: `internal/rti/store.go`, строка ~797:

```go
func LoadClientEvents(db *store.DB, sessionID int64) ([]*RTIClientEvent, error) {
    rows, err := db.Query(
        `SELECT id, timestamp, level, category, class_name, method_name,
                pid, seq_no, line_no, kind, elapsed_ms, payload, server_call_id
         FROM rti_client_events WHERE session_id = $1 ORDER BY id`,
        sessionID,
    )
    // ...
}
```

Функция полностью реализована, но не имеет ни одного вызова в кодовой базе.

## Влияние

- **`codebase_rti_client_tree`** — полностью нефункционален при `session_id`; возвращает пустое дерево.
- **`codebase_rti_timeline`** — не показывает клиентские события; хронологическая лента содержит только серверные вызовы.
- **`codebase_rti_errors`** — не возвращает клиентские `SEVERE`-ошибки (например, `TCodeProtection.ReportViolation`).
- **`codebase_rti_slow`** — не возвращает медленные клиентские SQL-блоки.
- **`codebase_rti_summary`** — не включает `ClientEventsCount` в сводку.
- Workaround: использовать `file_path` вместо `session_id` — но это повторно парсит файл каждый раз, без использования БД-кэша.

## Предлагаемое исправление

### Изменение в `loadRTIFromArgs`

Файл: `internal/mcp/registry.go`, строки 1028–1040.

**Было:**

```go
calls, err := rti.LoadCalls(db, sessionID)
if err != nil {
    return nil, fmt.Errorf("failed to load calls: %w", err)
}
return &rti.RTIParseResult{
    Calls: calls,
    Summary: rti.RTISummary{
        FilePath:    session.FilePath,
        FileSize:    session.FileSize,
        TotalCalls:  session.TotalCalls,
        ErrorsCount: session.ErrorsCount,
    },
}, nil
```

**Стало:**

```go
calls, err := rti.LoadCalls(db, sessionID)
if err != nil {
    return nil, fmt.Errorf("failed to load calls: %w", err)
}
clientEvents, err := rti.LoadClientEvents(db, sessionID)
if err != nil {
    return nil, fmt.Errorf("failed to load client events: %w", err)
}
return &rti.RTIParseResult{
    Calls:        calls,
    ClientEvents: clientEvents,
    Summary: rti.RTISummary{
        FilePath:          session.FilePath,
        FileSize:          session.FileSize,
        TotalCalls:        session.TotalCalls,
        ErrorsCount:       session.ErrorsCount,
        ClientEventsCount: session.ClientEventsCount,
    },
}, nil
```

### Тесты

- `TestLoadRTIFromArgs_SessionID_LoadsClientEvents` (интеграционный) — после `SaveSession` → `loadRTIFromArgs(session_id)` → `result.ClientEvents` непустой, `result.Summary.ClientEventsCount > 0`.
- `TestLoadRTIFromArgs_FilePath_LoadsClientEvents` — `loadRTIFromArgs(file_path)` → `result.ClientEvents` непустой (regression-тест для уже работающего пути).
- `TestLoadRTIFromArgs_SessionID_NoClientEvents_EmptyResult` — сессия без клиентских событий → `result.ClientEvents` пустой slice (не `nil`), ошибки нет.

## Файлы для изменения

1. **`internal/mcp/registry.go`** — функция `loadRTIFromArgs`, строки ~1028–1040: добавить вызов `rti.LoadClientEvents` и заполнение `ClientEvents` / `ClientEventsCount` в `RTIParseResult`.
2. **Тесты** — `internal/mcp/registry_test.go` (или новый файл): добавить интеграционные тесты для `session_id` пути с клиентскими событиями.

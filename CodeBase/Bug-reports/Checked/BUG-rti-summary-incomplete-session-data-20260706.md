# `rti_summary` возвращает неполные данные при `session_id`: `max_nest_level=0`, `unparsed_lines=0`, `slow_calls_count=0`, `top_slow` отсутствует

**Дата:** 2026-07-06
**Файлы:** `internal/rti/model.go`, `internal/rti/store.go`, `internal/rti/parser.go`, `internal/mcp/registry.go`
**Версия CodeBase:** 0.8.3 build 1164
**Статус:** Не исправлено

## Описание

Команда `codebase_rti_summary` при вызове с `session_id` (загрузка из БД) возвращает неполную сводку: `max_nest_level=0`, `unparsed_lines=0`, `slow_calls_count=0`, `top_slow` отсутствует. При вызове с `file_path` (прямой парсинг) часть полей также нулевая: `slow_calls_count=0` всегда.

### Затронутые поля

| Поле | `session_id` | `file_path` | Ожидаемое значение |
|------|-------------|-------------|-------------------|
| `max_nest_level` | **0** (баг) | корректно | 12 (для тестового лога) |
| `unparsed_lines` | **0** (баг) | корректно | >0 (для тестового лога) |
| `slow_calls_count` | **0** (баг) | **0** (баг) | 134 (для тестового лога, порог 100мс) |
| `top_slow` | **отсутствует** (баг) | корректно | топ-10 вызовов по elapsed_ms |

## Воспроизведение

1. Спарсить RTI-лог через `codebase_rti_parse` с `file_path` → получить `session_id`.
   ```
   rti_parse(file_path="...rti") → session_id=18, slow_calls_count=0
   ```
   **ПРИМЕЧАНИЕ:** `slow_calls_count=0` уже здесь — баг в `parseContent`.

2. Вызвать `codebase_rti_summary` с `session_id=18`.
   ```
   rti_summary(session_id=18) → max_nest_level=0, unparsed_lines=0, slow_calls_count=0, top_slow=null
   ```

3. Вызвать `codebase_rti_summary` с `file_path`.
   ```
   rti_summary(file_path="...rti") → max_nest_level=12, unparsed_lines=N, slow_calls_count=0, top_slow=[...]
   ```

4. Прямой grep по RTI-логу подтверждает: `@@NestLevel = 12` встречается, 134 вызова с `Elapsed, ms:` >= 100.

## Анализ причины

### Баг 1: `RTISession` не содержит `MaxNestLevel` и `UnparsedLines`

**Файл:** `internal/rti/model.go`, строки 167–175

```go
type RTISession struct {
    ID                int64     `json:"id"`
    FilePath          string    `json:"file_path"`
    ParsedAt          time.Time `json:"parsed_at"`
    TotalCalls        int       `json:"total_calls"`
    ErrorsCount       int       `json:"errors_count"`
    FileSize          int64     `json:"file_size"`
    ClientEventsCount int       `json:"client_events_count,omitempty"`
}
```

Структура `RTISession` **не содержит** полей `MaxNestLevel` и `UnparsedLines`, хотя соответствующие колонки существуют в таблице `rti_sessions` (см. `SaveSession`, store.go:20–24).

### Баг 2: `GetSession` и `ListSessions` читают данные из БД, но выбрасывают их

**Файл:** `internal/rti/store.go`, функция `GetSession`, строки 627–639

```go
func GetSession(db *store.DB, sessionID int64) (*RTISession, error) {
    var s RTISession
    var maxNestLevel, unparsedLines int  // ← локальные переменные
    err := db.QueryRow(
        `SELECT id, file_path, file_size, parsed_at, total_calls, errors_count,
                max_nest_level, unparsed_lines, client_events_count
         FROM rti_sessions WHERE id = $1`,
        sessionID,
    ).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
        &s.TotalCalls, &s.ErrorsCount, &maxNestLevel, &unparsedLines, &s.ClientEventsCount)
    // ...
    return &s, nil  // ← maxNestLevel и unparsedLines потеряны
}
```

Значения `max_nest_level` и `unparsed_lines` читаются из БД в локальные переменные `maxNestLevel` и `unparsedLines`, но **никогда не присваиваются** полям структуры `RTISession` (которая этих полей не имеет).

Та же проблема в `ListSessions` (store.go:578–603): локальные переменные `maxNestLevel` и `unparsedLines` сканируются из БД, но выбрасываются.

### Баг 3: `loadRTIFromArgs` не вычисляет `MaxNestLevel`, `UnparsedLines`, `TopSlow`, `SlowCallsCount`

**Файл:** `internal/mcp/registry.go`, функция `loadRTIFromArgs`, строки 1052–1057

```go
summary := rti.RTISummary{
    FilePath:    session.FilePath,
    FileSize:    session.FileSize,
    TotalCalls:  session.TotalCalls,
    ErrorsCount: session.ErrorsCount,
    // ← MaxNestLevel не установлен
    // ← UnparsedLines не установлен
    // ← SlowCallsCount не установлен
    // ← TopSlow не установлен
}
rti.FillClientSummary(&summary, clientEvents)
```

При загрузке из `session_id` сводка заполняется только 4 полями из `RTISession`. Поля `MaxNestLevel`, `UnparsedLines`, `TopSlow`, `SlowCallsCount` остаются нулевыми.

Вызовы (`calls`) загружены из БД через `rti.LoadCalls`, но из них не вычисляются:
- `MaxNestLevel` — можно вычислить итерацией по `calls` (как `maxNestLevel()` в parser.go:484)
- `TopSlow` — можно вычислить через `topSlowCalls(calls, 10)` (как в parser.go:468)
- `SlowCallsCount` — можно вычислить подсчётом вызовов с `ElapsedMs >= 100`

`UnparsedLines` нельзя вычислить из загруженных вызовов (эта информация есть только при парсинге файла), но можно прочитать из БД (колонка `unparsed_lines` в `rti_sessions`).

### Баг 4: `SlowCallsCount` никогда не вычисляется нигде в коде

**Файл:** `internal/rti/parser.go`, строки 464–469

```go
result.Summary.TotalCalls = len(allCalls)
result.Summary.UnparsedLines = result.UnparsedLines
result.Summary.ErrorsCount = countErrors(allCalls)
result.Summary.MaxNestLevel = maxNestLevel(allCalls)
result.Summary.TopSlow = topSlowCalls(allCalls, 10)
FillClientSummary(&result.Summary, allClientEvents)
// ← SlowCallsCount НИКОГДА не устанавливается
```

Поле `SlowCallsCount` (model.go:74) объявлено в структуре `RTISummary`, но **ни в одном месте** кода ему не присваивается значение. Поиск по всей кодовой базе подтверждает: единственное упоминание `SlowCallsCount` — это определение поля в `model.go`.

Это означает, что `slow_calls_count=0` возвращается **всегда** — и при `session_id`, и при `file_path`, и в выводе `rti_parse`.

## Влияние

- **`codebase_rti_summary`** при `session_id` возвращает `max_nest_level=0`, `unparsed_lines=0`, `slow_calls_count=0`, `top_slow=null` — сводка бесполезна для анализа.
- **`codebase_rti_summary`** при `file_path` возвращает `slow_calls_count=0` — неверно.
- **`codebase_rti_parse`** возвращает `slow_calls_count=0` — неверно.
- **`codebase_rti_list`** не возвращает `max_nest_level` и `unparsed_lines` в списке сессий — нет возможности быстро оценить сложность лога без открытия.
- Workaround для `max_nest_level` и `top_slow`: использовать `file_path` вместо `session_id` — но это повторно парсит файл.

## Предлагаемое исправление

### Исправление 1: Добавить поля в `RTISession`

**Файл:** `internal/rti/model.go`, структура `RTISession`

```go
type RTISession struct {
    ID                int64     `json:"id"`
    FilePath          string    `json:"file_path"`
    ParsedAt          time.Time `json:"parsed_at"`
    TotalCalls        int       `json:"total_calls"`
    ErrorsCount       int       `json:"errors_count"`
    FileSize          int64     `json:"file_size"`
    MaxNestLevel      int       `json:"max_nest_level"`       // ← ДОБАВИТЬ
    UnparsedLines     int       `json:"unparsed_lines"`       // ← ДОБАВИТЬ
    ClientEventsCount int       `json:"client_events_count,omitempty"`
}
```

### Исправление 2: Заполнить поля в `GetSession` и `ListSessions`

**Файл:** `internal/rti/store.go`, функция `GetSession`

```go
func GetSession(db *store.DB, sessionID int64) (*RTISession, error) {
    var s RTISession
    err := db.QueryRow(
        `SELECT id, file_path, file_size, parsed_at, total_calls, errors_count,
                max_nest_level, unparsed_lines, client_events_count
         FROM rti_sessions WHERE id = $1`,
        sessionID,
    ).Scan(&s.ID, &s.FilePath, &s.FileSize, &s.ParsedAt,
        &s.TotalCalls, &s.ErrorsCount, &s.MaxNestLevel, &s.UnparsedLines, &s.ClientEventsCount)
    if err != nil {
        return nil, err
    }
    return &s, nil
}
```

Аналогично в `ListSessions` — заменить локальные переменные на `&s.MaxNestLevel` и `&s.UnparsedLines`.

### Исправление 3: Вычислять `MaxNestLevel`, `TopSlow`, `SlowCallsCount` в `loadRTIFromArgs`

**Файл:** `internal/mcp/registry.go`, функция `loadRTIFromArgs`

```go
summary := rti.RTISummary{
    FilePath:       session.FilePath,
    FileSize:       session.FileSize,
    TotalCalls:     session.TotalCalls,
    ErrorsCount:    session.ErrorsCount,
    MaxNestLevel:   session.MaxNestLevel,
    UnparsedLines:  session.UnparsedLines,
}
rti.FillClientSummary(&summary, clientEvents)
// Вычислить поля, зависящие от calls:
summary.TopSlow = rti.TopSlowCallsFromLoaded(calls, 10)
summary.SlowCallsCount = rti.CountSlowCalls(calls, 100)
```

### Исправление 4: Вычислять `SlowCallsCount` в `parseContent`

**Файл:** `internal/rti/parser.go`, строки 464–469

```go
result.Summary.TotalCalls = len(allCalls)
result.Summary.UnparsedLines = result.UnparsedLines
result.Summary.ErrorsCount = countErrors(allCalls)
result.Summary.MaxNestLevel = maxNestLevel(allCalls)
result.Summary.SlowCallsCount = countSlowCalls(allCalls, 100)  // ← ДОБАВИТЬ
result.Summary.TopSlow = topSlowCalls(allCalls, 10)
FillClientSummary(&result.Summary, allClientEvents)
```

### Новые вспомогательные функции

Добавить в `internal/rti/parser.go`:

```go
func countSlowCalls(calls []*RTICall, thresholdMs int) int {
    count := 0
    for _, c := range calls {
        if c.ElapsedMs >= thresholdMs {
            count++
        }
    }
    return count
}
```

Добавить в `internal/rti/parser.go` (экспортируемая версия для `loadRTIFromArgs`):

```go
func TopSlowCallsFromLoaded(calls []*RTICall, n int) []RTICall {
    return topSlowCalls(calls, n)
}

func CountSlowCalls(calls []*RTICall, thresholdMs int) int {
    return countSlowCalls(calls, thresholdMs)
}
```

### Тесты

- `TestRTISummary_SessionID_HasMaxNestLevel` — после `SaveSession` → `loadRTIFromArgs(session_id)` → `summary.MaxNestLevel > 0`.
- `TestRTISummary_SessionID_HasUnparsedLines` — после `SaveSession` → `loadRTIFromArgs(session_id)` → `summary.UnparsedLines >= 0` (совпадает с `ParseFile`).
- `TestRTISummary_SessionID_HasTopSlow` — после `SaveSession` → `loadRTIFromArgs(session_id)` → `len(summary.TopSlow) > 0`.
- `TestRTISummary_SessionID_HasSlowCallsCount` — после `SaveSession` → `loadRTIFromArgs(session_id)` → `summary.SlowCallsCount > 0`.
- `TestParseFile_SlowCallsCount` — `ParseFile` → `summary.SlowCallsCount > 0` для лога с вызовами > 100мс.
- `TestGetSession_ReturnsMaxNestLevel` — `GetSession` → `session.MaxNestLevel > 0`.
- `TestListSessions_ReturnsMaxNestLevel` — `ListSessions` → каждый `session.MaxNestLevel >= 0`.

## Файлы для изменения

1. **`internal/rti/model.go`** — добавить `MaxNestLevel` и `UnparsedLines` в `RTISession` (строки 167–175).
2. **`internal/rti/store.go`** — `GetSession` (строки 627–639) и `ListSessions` (строки 578–603): заменить локальные переменные на поля структуры.
3. **`internal/rti/parser.go`** — добавить `countSlowCalls` и экспортируемые обёртки; добавить `result.Summary.SlowCallsCount = countSlowCalls(allCalls, 100)` в `parseContent` (строка 468).
4. **`internal/mcp/registry.go`** — `loadRTIFromArgs` (строки 1052–1057): заполнить `MaxNestLevel`, `UnparsedLines` из `session`, вычислить `TopSlow` и `SlowCallsCount` из `calls`.
5. **Тесты** — `internal/rti/parser_test.go` и/или `internal/mcp/registry_test.go`.

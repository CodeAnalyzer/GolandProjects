# Экстракция service-слоя для RTI и TRC: устранение дублирования orchestration между CLI и MCP

Выделение бизнес-оркестрации RTI/TRC из `cmd/rti.go` (1063 стр.), `cmd/trc.go` (760 стр.) и `internal/mcp/registry.go` (1924 стр.) в новые пакеты `internal/rtisvc` и `internal/trcsvc` по образцу существующих `querysvc`/`reviewsvc`/`systemsvc`.

## Контекст проблемы

Замечание №8 из ревью кода: для `query`/`review`/`system` существует тонкий service-слой, через который CLI и MCP разделяют бизнес-логику. Для RTI/TRC этого нет — оркестрация (session resolution → load from DB/file → enrich → build response) дублируется между CLI и MCP.

**Дублирование подтверждено:**

| Подсистема | CLI | MCP | Дублируемая логика |
|---|---|---|---|
| RTI errors | `runRTIErrors` ~80 стр. | handler ~90 стр. | load → enrich → retcode → response |
| RTI slow | `runRTISlow` ~56 стр. | handler ~93 стр. | filter → sort → enrich → response |
| RTI tree | `runRTITree` ~43 стр. | handler ~38 стр. | load → BuildTree → enrich |
| RTI details | `runRTIDetails` ~104 стр. | handler ~54 стр. | load by proc → enrich |
| RTI blog | `runRTIBlog` ~105 стр. | handler ~64 стр. | load by proc → extract blog |
| RTI client-tree | `runRTIClientTree` ~61 стр. | handler ~119 стр. | filter → BuildClientTree → enrich |
| RTI timeline | `runRTITimeline` ~65 стр. | handler ~137 стр. | filter calls+events → enrich |
| TRC events | `runTRCEvents` ~85 стр. | handler ~64 стр. | filter → response |
| TRC procedures | `runTRCProcedures` ~68 стр. | handler ~42 стр. | aggregate → enrich |
| TRC tree | `runTRCTree` ~52 стр. | handler ~42 стр. | load → BuildTrees |
| TRC errors | `runTRCErrors` ~66 стр. | handler ~42 стр. | filter → response |
| TRC slow | `runTRCSlow` ~74 стр. | handler ~50 стр. | filter → sort → response |

**Итого:** ~900 строк дублируемой бизнес-логики. `registry.go` — god file (1924 стр.), смешивает schema, validation и бизнес-логику.

**Попутный баг:** `printRTISlow` в `cmd/rti.go:944` открывает **второе** БД-соединение для enrichment — сервис-слой устранит это.

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/rtisvc/types.go` | **новый** — типы результатов: `ErrorsResult`, `SlowResult`, `TreeResult`, `DetailsResult`, `BlogResult`, `ClientTreeResult`, `TimelineResult`, `SummaryResult`, `ListResult`, `DeleteResult`, `PruneResult`, `ParseResult` |
| `internal/rtisvc/runtime.go` | **новый** — функции оркестрации: `ExecuteErrors`, `ExecuteSlow`, `ExecuteTree`, `ExecuteDetails`, `ExecuteBlog`, `ExecuteClientTree`, `ExecuteTimeline`, `ExecuteSummary`, `ExecuteList`, `ExecuteDelete`, `ExecutePrune`, `ExecuteParse` |
| `internal/rtisvc/runtime_test.go` | **новый** — unit-тесты |
| `internal/trcsvc/types.go` | **новый** — типы результатов: `EventsResult`, `ProceduresResult`, `TreeResult`, `ErrorsResult`, `SlowResult`, `SummaryResult`, `ListResult`, `DeleteResult`, `PruneResult`, `ParseResult` |
| `internal/trcsvc/runtime.go` | **новый** — функции оркестрации: `ExecuteEvents`, `ExecuteProcedures`, `ExecuteTree`, `ExecuteErrors`, `ExecuteSlow`, `ExecuteSummary`, `ExecuteList`, `ExecuteDelete`, `ExecutePrune`, `ExecuteParse` |
| `internal/trcsvc/runtime_test.go` | **новый** — unit-тесты |
| `cmd/rti.go` | рефакторинг: `runRTI*` функции вызывают `rtisvc.*`, остаётся CLI flags + output formatting |
| `cmd/trc.go` | рефакторинг: `runTRC*` функции вызывают `trcsvc.*`, остаётся CLI flags + output formatting |
| `internal/mcp/registry.go` | рефакторинг: RTI/TRC handlers вызывают `rtisvc.*`/`trcsvc.*`, удаляются `loadRTIFromArgs`, `loadTRCFromArgs`, `resolveRTISessionID`, `resolveTRCSessionID` |

---

## Шаг 1 — `internal/rtisvc`: типы результатов

### 1.1. `internal/rtisvc/types.go`

Типы результатов, общие для CLI и MCP. Каждый тип — plain struct с JSON-тегами, идентичный текущей структуре ответов MCP.

```go
package rtisvc

import (
    "github.com/codebase/internal/rti"
    "github.com/codebase/internal/store"
)

// SessionSource — источник данных: saved session или file parse.
type SessionSource struct {
    SessionID int64
    FilePath  string
}

// ParseResult — результат парсинга RTI-файла.
type ParseResult struct {
    SessionID  int64          `json:"session_id"`
    TotalCalls int            `json:"total_calls"`
    Summary    rti.RTISummary `json:"summary"`
}

// SummaryResult — статистика по сессии.
type SummaryResult struct {
    Summary rti.RTISummary `json:"summary"`
}

// TreeResult — дерево вызовов.
type TreeResult struct {
    Tree       *rti.RTITreeNode               `json:"tree"`
    Enrichment map[string]*rti.ProcedureEnrichment `json:"enrichment,omitempty"`
}

// ErrorsResult — ошибки с enrich.
type ErrorsResult struct {
    ServerErrors      []RTICallSlim                  `json:"server_errors"`
    ServerErrorCount  int                            `json:"server_error_count"`
    ServerEnrichment  map[string]*rti.ProcedureEnrichment `json:"server_enrichment,omitempty"`
    ClientErrors      []*rti.RTIClientEvent          `json:"client_errors"`
    ClientErrorCount  int                            `json:"client_error_count"`
    ClientEnrichment  map[string]*rti.ClientEnrichment `json:"client_enrichment,omitempty"`
    Limit             int                            `json:"limit"`
}

// RTICallSlim — RTICall без тяжёлых BLog-полей в JSON.
type RTICallSlim struct {
    *rti.RTICall
    BLogTables interface{} `json:"blog_tables,omitempty"`
    BLogBlocks interface{} `json:"blog_blocks,omitempty"`
}

// SlowResult — медленные вызовы.
type SlowResult struct {
    ServerCalls      []RTICallSlim                  `json:"server_calls"`
    ServerCallCount  int                            `json:"server_call_count"`
    ServerEnrichment map[string]*rti.ProcedureEnrichment `json:"server_enrichment,omitempty"`
    ClientSQLBlocks  []*rti.RTIClientEvent          `json:"client_sql_blocks"`
    ClientSQLCount   int                            `json:"client_sql_count"`
    ClientEnrichment map[string]*rti.ClientEnrichment `json:"client_enrichment,omitempty"`
    Threshold        int                            `json:"threshold"`
    Limit            int                            `json:"limit"`
}

// DetailsResult — детали процедуры.
type DetailsResult struct {
    Procedure  string                         `json:"procedure"`
    Calls      []*rti.RTICall                 `json:"calls"`
    Count      int                            `json:"count"`
    Enrichment *rti.ProcedureEnrichment       `json:"enrichment,omitempty"`
}

// BlogResult — business log для процедуры.
type BlogResult struct {
    Procedure string       `json:"procedure"`
    Count     int          `json:"count"`
    Calls     []BlogCallItem `json:"calls"`
}

// BlogCallItem — выжимка BLog-данных из вызова.
type BlogCallItem struct {
    EnterLine   int                 `json:"enter_line"`
    ElapsedMs   int                 `json:"elapsed_ms,omitempty"`
    BLogBlocks  []rti.RTIBLogBlock  `json:"blog_blocks,omitempty"`
    Checkpoints []rti.RTICheckpoint `json:"checkpoints,omitempty"`
    BLogTables  []rti.RTIBLogTable  `json:"blog_tables,omitempty"`
}

// ClientTreeResult — дерево клиентских событий.
type ClientTreeResult struct {
    Nodes              interface{}                     `json:"nodes"`
    Enrichment         map[string]*rti.ClientEnrichment `json:"enrichment,omitempty"`
    FilteredEventsCount int                            `json:"filtered_events_count"`
    Limit              int                             `json:"limit"`
}

// TimelineResult — единый timeline.
type TimelineResult struct {
    Calls               interface{}                     `json:"calls"`
    ClientEvents        interface{}                     `json:"client_events"`
    Enrichment          map[string]*rti.ClientEnrichment `json:"enrichment,omitempty"`
    FilteredCallsCount  int                             `json:"filtered_calls_count"`
    FilteredEventsCount int                             `json:"filtered_events_count"`
    Limit               int                             `json:"limit"`
}

// ListResult — список сессий.
type ListResult struct {
    Sessions []rti.RTISession `json:"sessions"`
}

// DeleteResult — результат удаления.
type DeleteResult struct {
    Deleted    bool   `json:"deleted"`
    SessionID  int64  `json:"session_id"`
    FilePath   string `json:"file_path,omitempty"`
}

// PruneResult — результат очистки.
type PruneResult struct {
    DeletedCount int `json:"deleted_count"`
    KeptLast     int `json:"kept_last"`
}
```

### 1.2. Параметры функций

Каждая функция принимает plain struct параметров вместо `map[string]interface{}` (MCP) или global vars (CLI). Это позволяет CLI и MCP вызывать один и тот же код.

```go
type ErrorsParams struct {
    Source    SessionSource
    Limit     int
}

type SlowParams struct {
    Source       SessionSource
    ThresholdMs  int
    Limit        int
}

type TreeParams struct {
    Source     SessionSource
    Procedure  string
    MaxDepth   int
}

type DetailsParams struct {
    Source     SessionSource
    Procedure  string
    Limit      int
}

type BlogParams struct {
    Source     SessionSource
    Procedure  string
    Limit      int
}

type ClientTreeParams struct {
    Source     SessionSource
    Filter     rti.TimelineFilter
    Limit      int
}

type TimelineParams struct {
    Source     SessionSource
    Filter     rti.TimelineFilter
    Limit      int
}
```

---

## Шаг 2 — `internal/rtisvc/runtime.go`: функции оркестрации

Каждая функция: `(ctx, db, params) → Result`. Инкапсулирует session resolution → load → enrich → build response. CLI и MCP вызывают одну функцию.

### 2.1. Session resolution

```go
// resolveSession загружает данные из БД (session_id > 0) или парсит файл.
// Возвращает calls, clientEvents, parseResult (для file-mode).
func resolveSession(ctx context.Context, db *store.DB, src SessionSource) (
    calls []*rti.RTICall,
    clientEvents []*rti.RTIClientEvent,
    parseResult *rti.RTIParseResult,
    err error,
) {
    if src.SessionID > 0 && db != nil {
        calls, err = rti.LoadCalls(ctx, db, src.SessionID)
        if err != nil {
            return nil, nil, nil, err
        }
        clientEvents, err = rti.LoadClientEvents(ctx, db, src.SessionID)
        if err != nil {
            return nil, nil, nil, err
        }
        return calls, clientEvents, nil, nil
    }
    parseResult, err = rti.ParseFile(src.FilePath)
    if err != nil {
        return nil, nil, nil, err
    }
    return parseResult.Calls, parseResult.ClientEvents, parseResult, nil
}
```

### 2.2. ExecuteErrors

```go
func ExecuteErrors(ctx context.Context, db *store.DB, p ErrorsParams) (*ErrorsResult, error) {
    limit := p.Limit
    if limit <= 0 {
        limit = 100
    }

    var errorCalls []*rti.RTICall
    var clientErrors []*rti.RTIClientEvent

    if p.Source.SessionID > 0 && db != nil {
        var err error
        errorCalls, err = rti.LoadErrorCalls(ctx, db, p.Source.SessionID, limit)
        if err != nil {
            return nil, err
        }
        clientErrors, err = rti.LoadClientErrors(ctx, db, p.Source.SessionID, limit)
        if err != nil {
            return nil, err
        }
    } else {
        calls, events, _, err := resolveSession(ctx, db, p.Source)
        if err != nil {
            return nil, err
        }
        for _, c := range calls {
            if c.RetVal != nil && *c.RetVal != 0 {
                errorCalls = append(errorCalls, c)
                if len(errorCalls) >= limit {
                    break
                }
            }
        }
        for _, ev := range events {
            if ev.Kind == "error" && ev.ErrorText != "" {
                clientErrors = append(clientErrors, ev)
                if len(clientErrors) >= limit {
                    break
                }
            }
        }
    }

    result := &ErrorsResult{
        ServerErrors:     toCallSlims(errorCalls),
        ServerErrorCount: len(errorCalls),
        ClientErrors:     clientErrors,
        ClientErrorCount: len(clientErrors),
        Limit:            limit,
    }

    // Enrich
    if db != nil && (len(errorCalls) > 0 || len(clientErrors) > 0) {
        q := query.New(db)
        if len(errorCalls) > 0 {
            result.ServerEnrichment = rti.EnrichCalls(ctx, q, errorCalls)
            // RetCode lookup (batch — исправляет баг CLI с per-item lookup)
            codes := make([]int64, 0, len(errorCalls))
            for _, c := range errorCalls {
                if c.RetVal != nil {
                    codes = append(codes, int64(*c.RetVal))
                }
            }
            if len(codes) > 0 {
                retCodeMap, _ := db.LookupRetCodes(ctx, codes)
                for _, s := range result.ServerErrors {
                    if s.RetVal != nil {
                        if rc, ok := retCodeMap[int64(*s.RetVal)]; ok && rc != nil {
                            s.RetValMeaning = rc.Message
                            s.ErrorConstant = rc.ProcName
                        }
                    }
                }
            }
        }
        if len(clientErrors) > 0 {
            result.ClientEnrichment = rti.EnrichClientEvents(ctx, q, clientErrors)
        }
    }

    return result, nil
}
```

### 2.3. Остальные функции (аналогично)

Каждая функция следует тому же паттерну:
1. Нормализация параметров (limit, threshold)
2. Branch: `sessionID > 0 && db != nil` → load from DB; else → parse file
3. Transform: filter / sort / build tree / extract blog
4. Enrich (если db доступен)
5. Return typed Result

**ExecuteSlow** — фильтр по threshold, sort по ElapsedMs desc, enrich. Исправляет баг `printRTISlow` (двойное БД-соединение) — использует уже открытое `db`.

**ExecuteTree** — load calls for tree (DB: `LoadCallsForTree` с maxTreeNodes=5000; file: all calls), `BuildTree`, enrich.

**ExecuteDetails** — load by procedure (`LoadCallsByProcedure` / filter), `EnrichProcedure`.

**ExecuteBlog** — load by procedure, extract BLogBlocks/Checkpoints/BLogTables.

**ExecuteClientTree** — `LoadClientEventsFiltered` / `FilterClientEvents`, `BuildClientTree`, enrich, short-format conversion.

**ExecuteTimeline** — `LoadTimelineCalls` + `LoadTimelineClientEvents` / `ApplyTimelineFilter`, enrich, short-format conversion.

**ExecuteSummary** — `LoadSummary` / `parseResult.Summary`.

**ExecuteList** — `ListSessions`.

**ExecuteDelete** — `GetSession` (для file_path в ответе) + `DeleteSession`.

**ExecutePrune** — `PruneSessions`.

**ExecuteParse** — `ParseFile` + `SaveSession` (если db доступен).

---

## Шаг 3 — `internal/trcsvc`: типы и функции

### 3.1. `internal/trcsvc/types.go`

Аналогично RTI, plain structs с JSON-тегами:

```go
package trcsvc

import (
    "github.com/codebase/internal/trc"
)

type SessionSource struct {
    SessionID int64
    FilePath  string
}

type ParseResult struct {
    SessionID   int64  `json:"session_id"`
    TotalEvents int    `json:"total_events"`
}

type SummaryResult struct {
    TotalEvents int                  `json:"total_events"`
    Header      *trc.TRCHeader       `json:"header"`
    Session     *trc.TRCSession      `json:"session,omitempty"`
}

type EventsResult struct {
    Events       []trc.TRCEvent `json:"events"`
    TotalCount   int            `json:"total_count"`
    FilteredCount int           `json:"filtered_count"`
    Limit        int            `json:"limit"`
}

type ProceduresResult struct {
    Procedures []trc.TRCProcedureAgg `json:"procedures"`
    Count      int                   `json:"count"`
}

type TreeResult struct {
    Trees       map[int][]*trc.TRCTreeNode `json:"trees"`
    EventCount  int                        `json:"event_count,omitempty"`
    SPID        int                        `json:"spid,omitempty"`
}

type ErrorsResult struct {
    Events []trc.TRCEvent `json:"events"`
    Count  int            `json:"count"`
    Limit  int            `json:"limit"`
}

type SlowResult struct {
    Events    []trc.TRCEvent `json:"events"`
    Count     int            `json:"count"`
    Threshold int            `json:"threshold"`
    Limit     int            `json:"limit"`
}

type ListResult struct {
    Sessions []trc.TRCSession `json:"sessions"`
}

type DeleteResult struct {
    Deleted   bool   `json:"deleted"`
    SessionID int64  `json:"session_id"`
    FilePath  string `json:"file_path,omitempty"`
}

type PruneResult struct {
    DeletedCount int `json:"deleted_count"`
    KeptLast     int `json:"kept_last"`
}
```

### 3.2. `internal/trcsvc/runtime.go`

Параметры и функции — аналогично RTI:

```go
type EventsParams struct {
    Source     SessionSource
    SPID       int
    Procedure  string
    EventName  string
    Limit      int
}

type TreeParams struct {
    Source     SessionSource
    SPID       int
    MaxDepth   int
    Limit      int
}

type SlowParams struct {
    Source       SessionSource
    ThresholdMs  int
    Limit        int
}

type ErrorsParams struct {
    Source     SessionSource
    Limit      int
}
```

**ExecuteEvents** — `LoadEventsFiltered` / parse+filter, build `EventsResult`.

**ExecuteProcedures** — `LoadProceduresAggregated` / `AggregateByProcedure`, enrich, build `ProceduresResult`.

**ExecuteTree** — `LoadEventsForTree` / `BuildTreesWithDepth`, build `TreeResult`.

**ExecuteErrors** — `LoadErrorEvents` / filter by column[31], build `ErrorsResult`.

**ExecuteSlow** — `LoadSlowEvents` / filter+sort, build `SlowResult`.

**ExecuteSummary** — `GetSession` + `LoadEventCount` / parse, build `SummaryResult`.

**ExecuteList** — `ListSessions`.

**ExecuteDelete** — `GetSession` + `DeleteSession`.

**ExecutePrune** — `PruneSessions`.

**ExecuteParse** — `ParseFileToDB` (streaming, если db) / `ParseFile`, build `ParseResult`.

---

## Шаг 4 — Рефакторинг `internal/mcp/registry.go`

### 4.1. Удаление helper-функций

Удалить из `registry.go`:
- `resolveRTISessionID` (стр. 1819) — заменяется на `rtisvc.SessionSource{SessionID: ...}`
- `resolveTRCSessionID` (стр. 1824) — заменяется на `trcsvc.SessionSource{SessionID: ...}`
- `loadRTIFromArgs` (стр. 1828) — заменяется на `rtisvc.resolveSession` (internal)
- `loadTRCFromArgs` (стр. 1887) — заменяется на `trcsvc.resolveSession` (internal)

### 4.2. Упрощение RTI handlers

Каждый handler сокращается до: parse args → build params struct → call `rtisvc.Execute*` → return result.

**Было** (`codebase_rti_errors`, ~90 стр.):
```go
Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
    limit, _ := optionalInt(args, "limit")
    if limit <= 0 { limit = queryDefaultLimit }
    if limit > queryMaxLimit { limit = queryMaxLimit }
    type callSlim struct { ... }
    var errorCalls []*rti.RTICall
    var clientErrors []*rti.RTIClientEvent
    sessionID, err := resolveRTISessionID(args)
    // ... 70 строк бизнес-логики ...
},
```

**Стало** (~15 стр.):
```go
Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
    limit, _ := optionalInt(args, "limit")
    if limit <= 0 { limit = queryDefaultLimit }
    if limit > queryMaxLimit { limit = queryMaxLimit }
    sessionID, _ := optionalInt64(args, "session_id")
    filePath, _ := optionalString(args, "file_path")
    return rtisvc.ExecuteErrors(ctx, db, rtisvc.ErrorsParams{
        Source: rtisvc.SessionSource{SessionID: sessionID, FilePath: filePath},
        Limit:  limit,
    })
},
```

### 4.3. Упрощение TRC handlers

Аналогично для TRC. Каждый handler — parse args → build params → call `trcsvc.Execute*`.

### 4.4. Ожидаемый эффект на registry.go

- Удаление ~700 строк бизнес-логики RTI
- Удаление ~300 строк бизнес-логики TRC
- Удаление ~80 строк helper-функций
- **registry.go: 1924 → ~850 строк** (остаются schema definitions + thin dispatch)

---

## Шаг 5 — Рефакторинг `cmd/rti.go`

### 5.1. Паттерн рефакторинга

Каждая `runRTI*` функция:
1. Build `rtisvc.SessionSource` из CLI flags (`rtiSessionID`, `args[0]`)
2. Build params struct из CLI flags
3. Call `rtisvc.Execute*`
4. Format output (text или JSON)

**Было** (`runRTIErrors`, ~80 стр.):
```go
func runRTIErrors(cmd *cobra.Command, args []string) error {
    ctx := cmd.Context()
    limit := applyQueryLimit(rtiLimit)
    var errors []*rti.RTICall
    var clientErrors []*rti.RTIClientEvent
    if rtiSessionID > 0 {
        cfg := config.Get()
        if cfg != nil {
            if db, dbErr := store.NewDB(cfg.DB); dbErr == nil {
                defer db.Close()
                // ... 30 строк загрузки из БД + enrich ...
                return printRTIErrors(errors, clientErrors, retCodeMap, enrichMap, clientEnrichMap)
            }
        }
    }
    // ... 30 строк fallback: parse file + enrich ...
    return printRTIErrors(errors, clientErrors, retCodeMap, enrichMap, clientEnrichMap)
}
```

**Стало** (~25 стр.):
```go
func runRTIErrors(cmd *cobra.Command, args []string) error {
    ctx := cmd.Context()
    db := openDBOptional()
    defer closeDBOptional(db)
    result, err := rtisvc.ExecuteErrors(ctx, db, rtisvc.ErrorsParams{
        Source: rtisvc.SessionSource{SessionID: rtiSessionID, FilePath: fileArg(args)},
        Limit:  applyQueryLimit(rtiLimit),
    })
    if err != nil {
        return err
    }
    if rtiOutputJSON {
        return printJSON(result)
    }
    return printRTIErrorsText(result)
}
```

### 5.2. Helper-функции CLI

Добавить в `cmd/rti.go` (или в общий helper):

```go
func openDBOptional() *store.DB {
    cfg := config.Get()
    if cfg == nil {
        return nil
    }
    db, err := store.NewDB(cfg.DB)
    if err != nil {
        return nil
    }
    return db
}

func closeDBOptional(db *store.DB) {
    if db != nil {
        db.Close()
    }
}

func fileArg(args []string) string {
    if len(args) > 0 {
        return args[0]
    }
    return ""
}
```

### 5.3. Удаление дублируемых функций

Удалить из `cmd/rti.go`:
- `loadRTICalls` (стр. 167–207) — заменяется на `rtisvc.resolveSession`
- Внутреннюю логику enrichment из всех `runRTI*` функций

### 5.4. Ожидаемый эффект на rti.go

- `cmd/rti.go`: 1063 → ~550 строк (остаются CLI flags, `init()`, output formatting, `print*` функции)

---

## Шаг 6 — Рефакторинг `cmd/trc.go`

Аналогично RTI. Каждая `runTRC*` функция:
1. Build `trcsvc.SessionSource`
2. Build params struct
3. Call `trcsvc.Execute*`
4. Format output

Удалить `loadTRCResult` (стр. 116–151) — заменяется на `trcsvc.resolveSession`.

**Ожидаемый эффект:** `cmd/trc.go`: 760 → ~400 строк.

---

## Шаг 7 — Тесты

### 7.1. `internal/rtisvc/runtime_test.go`

| Тест | Что проверяет |
|---|---|
| `TestExecuteErrors_FileMode` | парсинг файла → фильтр RetVal != 0 → результат |
| `TestExecuteErrors_EmptyResult` | нет ошибок → пустой result, count=0 |
| `TestExecuteSlow_FileMode` | парсинг → фильтр по threshold → sort desc |
| `TestExecuteSlow_NoSlowCalls` | все вызовы быстрее threshold → пустой result |
| `TestExecuteTree_FileMode` | парсинг → BuildTree → структура tree |
| `TestExecuteTree_ProcNotFound` | несуществующая процедура → error |
| `TestExecuteDetails_FileMode` | парсинг → фильтр по procedure → calls |
| `TestExecuteDetails_ProcNotFound` | несуществующая процедура → error |
| `TestExecuteBlog_FileMode` | парсинг → BLogBlocks/Checkpoints/BLogTables |
| `TestExecuteSummary_FileMode` | парсинг → Summary |
| `TestExecuteClientTree_FileMode` | парсинг → filter → BuildClientTree |
| `TestExecuteTimeline_FileMode` | парсинг → filter → calls + events |
| `TestResolveSession_FileMode` | resolveSession с FilePath → calls + events |
| `TestResolveSession_InvalidFile` | несуществующий файл → error |

Для тестов без БД используется file-mode (parse from file). Тесты с БД требуют PostgreSQL и помечаются `// +build integration` или пропускаются если `CODEBASE_TEST_DB` не задан.

### 7.2. `internal/trcsvc/runtime_test.go`

| Тест | Что проверяет |
|---|---|
| `TestExecuteEvents_FileMode` | парсинг → фильтр SPID/proc/event_name → events |
| `TestExecuteEvents_NoFilter` | парсинг → все events |
| `TestExecuteProcedures_FileMode` | парсинг → aggregate → count > 0 |
| `TestExecuteTree_FileMode` | парсинг → BuildTrees → trees по SPID |
| `TestExecuteErrors_FileMode` | парсинг → filter column[31] != 0 |
| `TestExecuteSlow_FileMode` | парсинг → filter by threshold → sort desc |
| `TestExecuteSummary_FileMode` | парсинг → header + total_events |
| `TestResolveSession_InvalidFile` | несуществующий файл → error |

### 7.3. Регрессионные тесты

Существующие тесты в `internal/mcp/server_test.go` и `cmd/review_test.go` продолжают работать — API MCP tools не меняется, CLI flags не меняются. Проверить:

```
go test ./internal/mcp/... -count=1
go test ./cmd/... -count=1
```

---

## Порядок выполнения

1. **Шаг 1** — `rtisvc/types.go` → `go build`
2. **Шаг 2** — `rtisvc/runtime.go` → `go build` + `go test ./internal/rtisvc/...`
3. **Шаг 3** — `trcsvc/types.go` + `trcsvc/runtime.go` → `go build` + `go test ./internal/trcsvc/...`
4. **Шаг 4** — рефакторинг `registry.go` (RTI handlers → `rtisvc.*`, TRC handlers → `trcsvc.*`) → `go build` + `go test ./internal/mcp/...`
5. **Шаг 5** — рефакторинг `cmd/rti.go` → `go build` + `go test ./cmd/...`
6. **Шаг 6** — рефакторинг `cmd/trc.go` → `go build` + `go test ./cmd/...`
7. **Шаг 7** — тесты `rtisvc`/`trcsvc` → `go test ./internal/rtisvc/... ./internal/trcsvc/...`

После каждого шага — `go build ./...` и `go vet ./...`.

## Проверка

```
go build ./...
go vet ./...
go test ./internal/rtisvc/... ./internal/trcsvc/... ./internal/mcp/... ./cmd/... -count=1
```

## Ожидаемый результат

| Метрика | До | После |
|---|---|---|
| `registry.go` | 1924 стр. | ~850 стр. |
| `cmd/rti.go` | 1063 стр. | ~550 стр. |
| `cmd/trc.go` | 760 стр. | ~400 стр. |
| Дублируемой логики | ~900 стр. | 0 |
| Новых пакетов | — | `rtisvc` (~450 стр.), `trcsvc` (~300 стр.) |
| Баг двойного БД-соединения в `printRTISlow` | есть | устранён |
| RetCode lookup: batch vs per-item | расхождение CLI/MCP | унифицировано (batch) |

## Принципы

- **API не меняется**: MCP tool names, descriptions, input schemas — те же. CLI flags — те же.
- **Типы результатов**: plain structs с JSON-тегами, идентичные текущим MCP-ответам. CLI форматирует text, MCP возвращает struct (JSON через `sdkToolPagedResult`).
- **DB-опциональность**: все функции работают с `db == nil` (file-mode) — как CLI без БД, так и MCP fallback.
- **Batch retcode lookup**: `ExecuteErrors` использует `db.LookupRetCodes` (batch) вместо per-item `q.LookupRetCode` — унификация CLI и MCP, исправление существующего расхождения.
- **Без circular imports**: `rtisvc` импортирует `rti`, `query`, `store`; `trcsvc` импортирует `trc`, `query`, `store`. `mcp` импортирует `rtisvc`/`trcsvc`. `cmd` импортирует `rtisvc`/`trcsvc`.

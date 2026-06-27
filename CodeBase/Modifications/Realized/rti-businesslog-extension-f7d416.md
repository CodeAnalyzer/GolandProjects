# RTI BusinessLog Extension

Расширение RTI-анализатора для парсинга событий бизнес-логирования (`Trace.Server.BusinessLog`, `BusinessLog: Data from TABLE`, checkpoint timestamps) и нового MCP-инструмента `codebase_rti_blog`.

---

## Что сейчас не анализируется

| Источник в RTI-логе | Макрос в SQL | Статус |
|---|---|---|
| `Trace.Server.BusinessLog Enter/Exit` + `RetVal=0#BlockName` | `M_BUSINESSLOG_BLOCK_BEGIN/END` | ❌ игнорируется |
| `ProcName_Begin_N` метки | `M_BUSINESSLOG_CHECKPOINT` | ⚠️ метка есть, timestamp = zero |
| `BusinessLog: Data from TABLE begin/end` + строки | `M_LOG_TABLE / M_LOG_TABLE_LISTID` | ❌ игнорируется |

---

## Шаг 1 — Model: новые структуры (`internal/rti/model.go`)

Добавить:
```go
// RTIBLogBlock — блок бизнес-логирования (BLOCK_BEGIN/END)
type RTIBLogBlock struct {
    BlockName  string    `json:"block_name"`
    EnterTime  time.Time `json:"enter_time"`
    ExitTime   time.Time `json:"exit_time,omitempty"`
    ElapsedMs  int       `json:"elapsed_ms,omitempty"`
    EnterLine  int       `json:"enter_line"`
    ExitLine   int       `json:"exit_line,omitempty"`
}

// RTIBLogTable — дамп таблицы из M_LOG_TABLE / M_LOG_TABLE_LISTID
type RTIBLogTable struct {
    TableName  string   `json:"table_name"`
    Columns    []string `json:"columns,omitempty"`
    Rows       []string `json:"rows,omitempty"`
    RowCount   int      `json:"row_count"`
    EnterLine  int      `json:"enter_line"`
}
```

В `RTICall` добавить:
```go
BLogBlocks []RTIBLogBlock `json:"blog_blocks,omitempty"`
BLogTables []RTIBLogTable `json:"blog_tables,omitempty"`
```

В `RTICheckpoint` timestamp уже есть — просто перестал быть zero.

---

## Шаг 2 — Parser (`internal/rti/parser.go`)

**Новые regex:**
```go
reBLogHeader = regexp.MustCompile(
    `^(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2}:\d{2}\.\d{3})\tINFO\tTrace\.Server\.BusinessLog\t\t\t(\d+)\t(\d+)\t\t(\d+)\t(\d+)`)

reBLogEnter = regexp.MustCompile(`^Enter\s+@@TranCount`)
reBLogExit  = regexp.MustCompile(`^Exit\s+@@TranCount`)
reBLogTable = regexp.MustCompile(`^BusinessLog:\s+Data\s+from\s+(\S+)\s+(begin|end)`)
reBLogTableHeader = regexp.MustCompile(`^Table\s+header\s+(.+)`)
```

**Состояние парсера (новые поля):**
```go
pendingBLog      bool
pendingBLogTS    time.Time
pendingBLogSPID  int
pendingBLogSrcLine int
pendingBLogIsEnter *bool   // nil=unknown, true=Enter, false=Exit
pendingTraceTS   time.Time // timestamp последнего Trace.Server.Trace

captureTable     bool
currentTable     *RTIBLogTable
```

**Логика:**
1. При `reTrace` — сохранять timestamp в `pendingTraceTS`; присваивать его `Checkpoints[last].Timestamp` после `reCheckpoint`
2. При `reBLogHeader` — установить `pendingBLog=true`, сохранить ts/spid/srcLine
3. При `reBLogEnter` + `pendingBLog` — задать `pendingBLogIsEnter=true`
4. При `reBLogExit` + `pendingBLog` — задать `pendingBLogIsEnter=false`
5. При `reRetVal` + `pendingBLog`:
   - Если Enter: `currentCall.BLogBlocks = append(BLogBlocks, {BlockName: ctx, EnterTime: ts, ...})`
   - Если Exit: найти открытый блок с тем же именем, закрыть его
   - Сбросить `pendingBLog`
6. При `reBLogTable` "begin" — создать `currentTable`, `captureTable=true`
7. При `reBLogTableHeader` — распарсить колонки (split `_|_`)
8. При `reBLogTable` "end" — `currentCall.BLogTables = append(...)`, сбросить `captureTable`
9. При `captureTable=true` и не header/begin/end строка — добавить строку как `currentTable.Rows`

---

## Шаг 3 — DB Schema (`internal/store/db_schema.go`)

Новые таблицы:
```sql
CREATE TABLE IF NOT EXISTS rti_blog_blocks (
    id         BIGSERIAL PRIMARY KEY,
    session_id BIGINT NOT NULL REFERENCES rti_sessions(id) ON DELETE CASCADE,
    call_id    BIGINT REFERENCES rti_calls(id) ON DELETE CASCADE,
    block_name TEXT NOT NULL,
    enter_time TIMESTAMPTZ,
    exit_time  TIMESTAMPTZ,
    elapsed_ms INTEGER NOT NULL DEFAULT 0,
    enter_line INTEGER NOT NULL DEFAULT 0,
    exit_line  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS rti_blog_tables (
    id             BIGSERIAL PRIMARY KEY,
    session_id     BIGINT NOT NULL REFERENCES rti_sessions(id) ON DELETE CASCADE,
    call_id        BIGINT REFERENCES rti_calls(id) ON DELETE CASCADE,
    table_name     TEXT NOT NULL,
    columns_header TEXT,
    row_count      INTEGER NOT NULL DEFAULT 0,
    rows_json      TEXT,
    enter_line     INTEGER NOT NULL DEFAULT 0
);
```

Индексы: `idx_rti_blog_blocks_call_id`, `idx_rti_blog_tables_call_id`.

---

## Шаг 4 — Store (`internal/rti/store.go`)

- `insertRTIBLogBlocks(db, blocks, callIDs, sessionID)` — batch insert через `pq.CopyIn`
- `insertRTIBLogTables(db, tables, callIDs, sessionID)` — аналогично
- `LoadBLogBlocks(db, sessionID, callID) ([]RTIBLogBlock, error)`
- `LoadBLogTables(db, sessionID, callID) ([]RTIBLogTable, error)`
- Вызвать оба insert в `SaveSession`

---

## Шаг 5 — MCP tool `codebase_rti_blog` (`internal/mcp/registry.go`)

```json
Input:  { "session_id": int, "file_path": str, "procedure": str }
Output: {
  "procedure": "...",
  "elapsed_ms": N,
  "blog_blocks": [{ "block_name": "...", "enter_time": "...", "exit_time": "...", "elapsed_ms": N }],
  "checkpoints": [{ "label": "...", "timestamp": "...", "elapsed_ms": N }],
  "blog_tables": [{ "table_name": "...", "columns": [...], "row_count": N, "rows": [...] }]
}
```

---

## Шаг 6 — CLI `codebase rti blog` (`cmd/rti.go`)

Новый subcommand `rti blog`:
- Флаги: `--session-id`, `--file`, `--procedure`, `--json`
- Вывод: блоки с timing, чекпоинты с timestamp, дампы таблиц

---

## Файлы и объём

| Файл | Действие | ~строк |
|---|---|---|
| `internal/rti/model.go` | +2 structs, +2 fields | +25 |
| `internal/rti/parser.go` | +5 regex, state machine | +90 |
| `internal/rti/parser_test.go` | +5 тестов | +80 |
| `internal/store/db_schema.go` | +2 таблицы + индексы | +25 |
| `internal/rti/store.go` | +4 функции | +100 |
| `internal/mcp/registry.go` | +1 tool handler | +35 |
| `cmd/rti.go` | +1 subcommand | +40 |

**Итого:** ~395 строк. Рекомендуется реализовать пошагово (шаг 1→2→тест→3→4→5→6).

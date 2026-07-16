# Оптимизация TRC-инструментов для анализа больших трейсов

План оптимизации пайплайна TRC (SQL Server Profiler .trc) для работы с файлами до 5.5 GB: streaming-парсер, server-side SQL с фильтрацией и лимитами, recursive CTE для tree, составные индексы, batch-коммиты при insert.

---

## Фаза T1: Streaming-парсер

**Проблема:** `os.ReadFile` загружает весь файл в RAM → OOM на 5.5 GB.

**Файлы:** `internal/trc/parser.go`, `internal/trc/xml_parser.go`

**Что сделать:**
- `ParseFile`: заменить `os.ReadFile` на `os.Open` + `bufio.NewReader`
- `ParseHeader`: читать заголовок через streaming (читать блоки до `EventsOffset`)
- `ParseEvents`: читать события по одному через `bufio.Reader.Read`, не загружая весь файл
- Для XML: `ParseXML` — заменить `os.ReadFile` на `xml.NewDecoder(bufio.NewReader(f))` (XML decoder уже streaming)
- `findEventHeader` — адаптировать для streaming (читать блоками, искать маркер в буфере)
- Сохранить API: `ParseFile(path) → *TRCParseResult, error` без изменений сигнатуры

## Фаза T2: parent_id при insert + batch-коммиты

**Проблема:** Нет `parent_id` для recursive CTE; одна транзакция на миллионы событий.

**Файлы:** `internal/store/db_schema.go`, `internal/trc/store.go`, `internal/trc/tree.go`

**Что сделать:**
- **Схема:** `ALTER TABLE trc_events ADD COLUMN IF NOT EXISTS parent_id BIGINT` + `ADD COLUMN IF NOT EXISTS depth INTEGER DEFAULT 0`
- **Вычисление parent_id:** после парсинга (в `enrichEventsParallel` или отдельным проходом) вычислить parent-child по парам Starting/Completed (тот же алгоритм `buildSPIDTree`, но заполняющий `parent_id` и `depth` в `TRCEvent`)
- **Insert:** `insertTRCEvents` — коммитить батчами по 50K событий (CopyIn для батча → Exec → повтор), не одной транзакцией
- **Index на parent_id:** `CREATE INDEX IF NOT EXISTS idx_trc_events_parent_id ON trc_events(parent_id)`

## Фаза T3: Server-side SQL — events, slow, errors

**Проблема:** `LoadEvents` грузит ALL rows без фильтров и лимита.

**Файлы:** `internal/trc/store.go`

**Новые функции:**
- `LoadEventsFiltered(db, sessionID, filter, limit) → []TRCEvent` — WHERE по spid, procedure, event_class; ORDER BY event_sequence; LIMIT
- `LoadSlowEvents(db, sessionID, thresholdMs, limit) → []TRCEvent` — WHERE duration_ms >= $2 ORDER BY duration_ms DESC LIMIT
- `LoadErrorEvents(db, sessionID, limit) → []TRCEvent` — WHERE error IS NOT NULL AND error <> 0 ORDER BY id LIMIT
- `LoadEventsByProcedure(db, sessionID, procName, limit) → []TRCEvent` — WHERE procedure = $2 ORDER BY id LIMIT
- `LoadEventCount(db, sessionID) → int` — SELECT count(*) для total_count без загрузки всех событий
- `LoadProceduresAggregated(db, sessionID) → []TRCProcAgg` — SQL GROUP BY procedure: count, min/max/avg/sum(duration_ms)

**Существующие:** `LoadEvents` оставить для обратной совместимости, но не использовать в MCP/CLI.

## Фаза T4: Recursive CTE для tree

**Проблема:** Дерево строится в Go по всем загруженным событиям.

**Файлы:** `internal/trc/store.go`, `internal/trc/tree.go`

**Что сделать:**
- `LoadEventsForTree(db, sessionID, spid, maxDepth, maxNodes) → []TRCEvent` — recursive CTE:
  - Anchor: root events (nest_level=0 или без parent_id) для указанного SPID
  - Recursive: children по parent_id, WHERE depth < maxDepth
  - Outer LIMIT maxNodes
- Если SPID не указан — выбрать SPID с наибольшим числом событий (подзапрос)
- `BuildTreesWithDepth` — оставить для случая работы с файлом (не из БД)
- MCP handler `trc_tree`: при `session_id > 0` использовать `LoadEventsForTree`, затем `BuildTreesWithDepth` на загруженном подмножестве

## Фаза T5: MCP handlers — server-side SQL + limit

**Проблема:** Все MCP tools используют `loadTRCFromArgs` → `LoadEvents` (ALL rows).

**Файлы:** `internal/mcp/registry.go`

**Что изменить:**
- `loadTRCFromArgs` — не менять (fallback для file_path)
- Каждый handler: при `session_id > 0` использовать новые server-side функции вместо `loadTRCFromArgs`
- **`trc_events`**: `LoadEventsFiltered` + limit (default 100, max 1000)
- **`trc_slow`**: `LoadSlowEvents` + limit
- **`trc_errors`**: `LoadErrorEvents` + limit
- **`trc_procedures`**: `LoadProceduresAggregated` (SQL GROUP BY) + enrichment по уникальным процедурам
- **`trc_summary`**: `GetSession` + `LoadEventCount` (без загрузки событий)
- **`trc_tree`**: `LoadEventsForTree` + `BuildTreesWithDepth` на результате
- Добавить параметр `limit` (default 100, max 1000) во все schema definitions

## Фаза T6: CLI — server-side SQL + --limit

**Проблема:** CLI `loadTRCResult` грузит ALL events; нет `--limit`.

**Файлы:** `cmd/trc.go`

**Что изменить:**
- `loadTRCResult` — оставить для file_path fallback
- `runTRCEvents`: при `--session` → `LoadEventsFiltered` + `--limit`
- `runTRCSlow`: при `--session` → `LoadSlowEvents` + `--limit`
- `runTRCErrors`: при `--session` → `LoadErrorEvents` + `--limit`
- `runTRCProcedures`: при `--session` → `LoadProceduresAggregated`
- `runTRCSummary`: при `--session` → `GetSession` + `LoadEventCount`
- `runTRCTree`: при `--session` → `LoadEventsForTree` + `BuildTreesWithDepth`
- Добавить `--limit` флаги на `trcEventsCmd`, `trcSlowCmd`, `trcErrorsCmd`

## Фаза T7: Составные индексы

**Файлы:** `internal/store/db_schema.go`

**Новые индексы:**
- `idx_trc_events_session_duration ON trc_events(session_id, duration_ms DESC)` — для slow
- `idx_trc_events_session_spid ON trc_events(session_id, spid)` — для tree/events filter
- `idx_trc_events_session_proc ON trc_events(session_id, procedure)` — для procedures/events filter
- `idx_trc_events_session_error ON trc_events(session_id) WHERE error IS NOT NULL AND error <> 0` — для errors
- `idx_trc_events_session_seq ON trc_events(session_id, event_sequence)` — для ordered events
- `idx_trc_events_parent_id ON trc_events(parent_id)` — для recursive CTE

---

## Порядок выполнения

1. **T1** (streaming-парсер) — независим, можно делать первым
2. **T2** (parent_id + batch insert) — зависит от T1 (парсер выдаёт события)
3. **T7** (индексы) — независимо, можно параллельно с T3
4. **T3** (server-side SQL функции) — зависит от T2 (нужен parent_id для T4)
5. **T4** (recursive CTE tree) — зависит от T2 + T3
6. **T5** (MCP handlers) — зависит от T3 + T4
7. **T6** (CLI) — зависит от T3 + T4

## Проверка после каждой фазы

```
go build ./...
$env:TMPDIR="C:\Temp"; go test ./internal/trc/... ./internal/mcp/... ./internal/store/... -count=1
```

# Fix: TRC parser crash на больших файлах (9 ГБ)

План устранения двух корневых причин ошибки `ERROR_NO_SYSTEM_RESOURCES` при парсинге .trc файлов размером ~9 ГБ: (1) отсутствие cap на `length` события — garbage-значение вызывает `make([]byte, ~4ГБ)` и исчерпание ресурсов; (2) накопление всех событий в `[]TRCEvent` в RAM — для 9 ГБ файла это миллионы событий с TextData, исчерпающие RAM.

## Phase 1: Cap на length события (быстрый фикс, предотвращает крэш)

**Файл:** `internal/trc/parser.go`

- Добавить константу `maxEventSize = 16 * 1024 * 1024` (16 МБ — TextData в SQL Server трейсах обычно < 1 МБ, 16 МБ с запасом)
- В `ParseEventsStreaming` (строка 175): после чтения `length` проверять `length > maxEventSize`. При превышении — не делать `make([]byte, length)`, а пропустить `length` байт через `io.CopyN(io.Discard, r, int64(length))` и запустить `skipToEventMarker` для ресинхронизации. Логировать warning через `fmt.Fprintf(os.Stderr, ...)`.
- Аналогично в `ParseEvents` (in-memory версия, строка 124): при `length > maxEventSize` — skip + `findEventHeader` для ресинхронизации.
- Проверка `length < 0` (строка 176) — заменить на `length < 0 || length > maxEventSize` с раздельными сообщениями.

**Тесты:** `internal/trc/parser_test.go`
- `TestParseEventsStreaming_LengthCap_SkipsOversizedEvent` — craft event with `length=maxEventSize+1`, verify skip + resync + next event parsed correctly
- `TestParseEvents_LengthCap_SkipsOversizedEvent` — same for in-memory parser

## Phase 2: Streaming parse-and-save (устраняет OOM на больших файлах)

### 2a: IncrementalParentTracker

**Новый файл:** `internal/trc/parent_tracker.go`

Алгоритм `ComputeParentIDs` (tree.go:112) переписан в инкрементальную форму: вместо группировки всех событий по SPID post-factum, поддерживается `map[int][]spidFrame` (SPID → стек) во время стриминга.

```go
type IncrementalParentTracker struct {
    stacks map[int][]spidFrame // spid -> stack of {family, idx}
}

func NewIncrementalParentTracker() *IncrementalParentTracker
func (t *IncrementalParentTracker) Process(ev *TRCEvent, idx int)  // заполняет ParentID, Depth, EventIndex
```

Логика `Process` идентична `ComputeParentIDs` для одного события: Starting → push frame, Completed → pop matching family, ParentID = top of stack (или -1).

**Тесты:** `internal/trc/parent_tracker_test.go`
- `TestIncrementalParentTracker_MatchesComputeParentIDs` — сравнение с reference `ComputeParentIDs` на наборе из ~1000 событий (random SPID, Starting/Completed pairs)
- `TestIncrementalParentTracker_NoSPID_RootEvent`
- `TestIncrementalParentTracker_NestedCalls`

### 2b: ParseFileToDB

**Файл:** `internal/trc/parser.go` (новая функция)

```go
func ParseFileToDB(path string, db *store.DB) (sessionID int64, totalEvents int, err error)
```

Поток:
1. Открыть файл, прочитать заголовок (как в `ParseFile` — переиспользовать существующий код через выделение `parseHeaderFromFile(f) (*TraceHeader, error)`)
2. INSERT в `trc_sessions` с `total_events=0` (placeholder, обновим в конце) → получить `sessionID`
3. Создать `IncrementalParentTracker`
4. Стримить события через модифицированный `ParseEventsStreaming`:
   - Для каждого события: `enrichEvent(&ev)` (одиночный, не parallel — события идут по одному)
   - `tracker.Process(&ev, eventIndex)`
   - Накапливать в локальный батч `[]TRCEvent` размером `streamBatchSize = 10000`
   - При заполнении батча: `insertTRCEvents(db, batch, sessionID)` → очистить батч
5. В конце: вставить остаток батча, `UPDATE trc_sessions SET total_events = $2 WHERE id = $1`
6. Вернуть `sessionID, totalEvents, nil`

Рефакторинг `ParseFile`: выделить `parseHeaderFromFile(f *os.File) (*TraceHeader, error)` из существующего кода (строки 32-90), переиспользовать в обоих функциях.

**Изменение `ParseEventsStreaming`:** добавить внутреннюю версию `parseEventsStreamingCB(r, h, callback func(ev TRCEvent, idx int) error) error`. `ParseEventsStreaming` вызывает её с `callback=nil` (накопление в slice, старое поведение). `ParseFileToDB` вызывает с callback для streaming. Это устраняет дублирование логики парсинга.

### 2c: Интеграция в CLI и MCP

**Файл:** `cmd/trc.go` — `runTRCParse`
- Если DB доступна: вызывать `trc.ParseFileToDB(args[0], db)` вместо `ParseFile` + `SaveSession`
- Если DB недоступна: fallback на `ParseFile` (как сейчас)
- Вывод: `total_events` + `session_id`

**Файл:** `internal/mcp/registry.go` — handler `codebase_trc_parse`
- Аналогично: при наличии DB → `ParseFileToDB`, без DB → `ParseFile` (с ошибкой, т.к. MCP требует DB для сохранения)

### 2d: insertTRCEvents для streaming-батчей

**Файл:** `internal/trc/store.go`

Существующая `insertTRCEvents` уже работает батчами по `batchInsertSize=50000` внутри. Для streaming достаточно вызывать её с батчами по `streamBatchSize=10000` — она сама разобьёт на CopyIn-транзакции. Изменений не требуется, но нужно убедиться что `insertTRCEvents` корректно работает с partial slices (проверено — она принимает `[]TRCEvent`, работает с любым размером).

Единственное: `serializeParallel` внутри `insertTRCEvents` использует индексы events — для partial slices это работает корректно (локальные индексы 0..len(batch)-1).

## Phase 3: Тесты и валидация

- `TestParseFileToDB_StreamBatch` — mock DB (или test DB), проверка что события сохранены батчами, total_events корректен
- `TestParseEventsStreamingCB_Callback` — callback вызывается для каждого события, порядок сохранён
- `TestParseFileToDB_ParentIDsIncremental` — parent_id в БД совпадает с ComputeParentIDs на том же файле
- Регрессия: все существующие тесты `internal/trc/...` — PASS без изменений
- `go build ./...` + `go vet ./internal/trc/...` — чисто

## Порядок реализации

1. Phase 1 (cap на length) — изолированный фикс, можно применить и проверить сразу
2. Phase 2a (IncrementalParentTracker) — новый файл, не затрагивает существующий код
3. Phase 2b (ParseFileToDB + рефакторинг parseHeaderFromFile + parseEventsStreamingCB) — основная работа
4. Phase 2c (интеграция CLI/MCP) — небольшие изменения в вызовах
5. Phase 3 (тесты) — параллельно с каждой фазой

## Риски и заметки

- `streamBatchSize = 10000` — баланс между memory и количеством транзакций. 10000 событий × ~2KB/event ≈ 20MB на батч — комфортно.
- `enrichEvent` вызывается последовательно (не parallel) в streaming-режиме — это OK, т.к. enrich лёгкий (regex по TextData).
- `ParseFile` остаётся без изменений — все существующие тесты и no-DB сценарии работают как раньше.
- `maxEventSize = 16MB` — может потребовать корректировки если встретятся легитимные события > 16MB (маловероятно для SQL Server Profiler).

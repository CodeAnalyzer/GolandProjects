# Парсер бинарных трейсов SQL Server Profiler (.trc) для CodeBase

Реализовать модуль `internal/trc` для бинарного разбора файлов `.trc`, полноценно (все типы колонок, любой набор трассируемых событий) — по аналогии с `internal/rti`, включая сохранение в PostgreSQL и MCP-инструменты `codebase_trc_*`, за один заход (Phase 1+2 вместе).

## Что уже установлено про формат (по итогам ручного реверс-инжиниринга)

Сравнивал `DIAPR-391.trc` (бинарь) с `DIAPR-391.xml` (экспорт того же трейса через Profiler UI, кодировка UTF-16). Формат недокументирован Microsoft, но структура частично восстановлена:

1. **Заголовок** — строки в UTF-16LE с нуль-паддингом до фиксированного размера блока: `TraceProvider name` ("Microsoft SQL Server"), `ServerInformation name` (например "re_test2016"), версия, далее вероятно `TraceStartTime` (8-байтовое значение, похоже на FILETIME/OLE date — тип пока не подтверждён).
2. **Таблица схемы `OrderedColumns`** — маркер `FB FC` (байты в файле: `fc fb`) + 2-байтовая длина + список `ColumnID` (по 2 байта LE каждый). Проверено: длина 0x5E (94 байта) / 2 = 47 записей — совпадает по количеству и порядку с `<OrderedColumns><ID>...` из XML (27,10,9,3,35,...,16,17).
3. **Таблица схемы `TracedEvents` (по одному блоку на EventClass)** — повторяющиеся блоки: маркер `FF FC` (байты `fc ff`) + 1 байт длины (покрывает `EventClassID(2 байта)` + `ColumnID list(2 байта каждый)`) + `EventClassID` (2 байта LE) + список `ColumnID`. Проверено на всех 11 блоках файла — EventClassID и состав колонок **точно совпадают** с `<TracedEvents><Event id="X"><EventColumn id="Y"/>...</Event>` из XML (id 33, 162, 25, 59, 60, 43, 37, 45, 12, 41, 166).
4. Далее идёт блок с GUID-подобной строкой (`SQL Server Profiler - <guid>`), логин/база (`diasoft`, `testday3` и т.п.) — вероятно расширенные метаданные заголовка (connection info), назначение колонок пока не привязано.
5. **Данные событий** — начинаются после заголовка/схемы. Так как схема `TracedEvents` уже фиксирует **набор и порядок колонок для каждого EventClass**, гипотеза: тело события = `EventClassID` + значения колонок **строго в порядке, заданном схемой для этого класса**, без дополнительных тегов типа на каждое поле. Тип кодирования конкретных значений (fixed-width int/bigint/datetime vs length-prefixed UTF-16 строка vs GUID vs binary) **не до конца декодирован** — это первая задача реализации (Phase 0).

Т.к. формат восстанавливается только сравнением hex ↔ XML, а не документацией, вероятны нюансы (NULL-колонки, разные версии формата/build 5622 vs другие, пул строк/дедупликация). Работа ведётся итеративно с постоянной проверкой на golden-паре `DIAPR-391.trc` / `.xml`.

## Архитектура (по аналогии с `internal/rti`)

| Файл | Назначение |
|---|---|
| `internal/trc/model.go` | `TRCEvent`, `TRCParam`, `TRCSummary`, `TRCSession`, `TRCParseResult` |
| `internal/trc/format.go` | Константы формата: маркеры блоков (`0xFFFC`, `0xFFFB`, ...), `eventClassNames` (из `Events list.txt`), таблица `columnID → {name, dataType}` (из `Data Columns.txt`) |
| `internal/trc/header.go` | Разбор заголовка + таблиц схем (`OrderedColumns`, `TracedEvents`) |
| `internal/trc/parser.go` | Разбор потока событий с использованием схемы; декодирование типов колонок |
| `internal/trc/store.go` | Save/Load в PostgreSQL (`trc_sessions`, `trc_events`), паттерн `pq.CopyIn` как в `rti/store.go` |
| `internal/trc/enrich.go` | Переиспользование `ProcedureLookup` из `rti/enrich.go` для обогащения `Procedure → SourceFile/LineStart/LineEnd` |
| `internal/trc/tree.go` | Восстановление иерархии по SPID + Starting/Completed стек (аналог `rti/tree.go`) |
| `internal/trc/aggregate.go` | Агрегация по процедурам: count/min/max/avg duration |

### Изменяемые файлы
- `internal/store/db_schema.go` — таблицы `trc_sessions`, `trc_events` + индексы (схема как в исходном `trc-trace-analyzer.md`).
- `internal/mcp/registry.go` — `codebase_trc_parse/list/summary/events/procedures/tree/slow/errors/delete/prune`.
- `cmd/trc.go` (новый) — CLI-аналог `cmd/rti.go` с теми же подкомандами.
- `cmd/root.go` — регистрация `trcCmd`.

## Дополнительные примеры трейсов

Кроме `DIAPR-391.trc`/`.xml`, найдена и подтверждена **вторая полная golden-пара — `nbki.trc`/`nbki.xml`** (212 МБ / 754 МБ, точная сверка значений возможна, не только структурная):

- **`BuildNumber="4335"`, MajorVersion 15 (SQL Server 2019)** — отличается от `DIAPR-391` (build 5622, SQL 2016). Валидирует устойчивость разбора заголовка к разным версиям формата.
- Схема (`TracedEvents`) объявляет **14 классов**, включая `RPC:Completed`(10)/`RPC:Starting`(11), `SP:Starting`(42), `Exec Prepared SQL`(72) — но по факту в реальных данных (проверено по превью `nbki.xml`) RPC-события **не встретились**: вызовы процедур Diasoft идут как `SQL:BatchStarting/Completed` с текстом `exec ProcName @Param=value...` внутри `TextData`. Значит извлечение `Procedure`/`Params` регуляркой из `TextData` — основной рабочий путь для реальных трейсов; кодирование выделенных RPC-parameter-колонок остаётся **unverified** (не на чём проверить в обоих файлах).
- **Важная находка**: набор колонок в схеме `nbki.trc` НЕ включает `Duration(13)`, `StartTime(14)`, `EndTime(15)`, `CPU(18)`, `Writes(17)` — набор колонок полностью зависит от шаблона трассировки, а не фиксирован форматом. `parser.go`/`aggregate.go`/`tree.go` должны обрабатывать отсутствие `Duration`/`StartTime` как штатный случай (nil/zero), без падений.
- Используется как второй golden-тест (точная сверка событий) + нагрузочный тест (stream-парсинг 212 МБ / 754 МБ, см. риск "Большие файлы").
- GUID/Binary Data/ObjectType — колонки этих типов в обоих файлах не встретились; остаются best-effort/unverified (см. Риски).

## Phase 0 — Завершить реверс-инжиниринг формата (предварительный шаг, блокирует Phase 1)

1. Написать вспомогательную Go-утилиту/тест `internal/trc/reverse_engineer_test.go` (или отдельный `cmd/trcdump`), которая:
   - Парсит `DIAPR-391.xml`/`nbki.xml` через `encoding/xml` (UTF-16, для `nbki.xml` — потоково из-за размера 754 МБ) — получает эталонный список событий с именованными колонками и значениями.
   - Дампит бинарь `DIAPR-391.trc`/`nbki.trc` посегментно (offset, hex, попытка UTF-16 decode) рядом с соответствующим событием из XML — для визуальной сверки типов данных.
2. Используя эту утилиту, определить для каждого встречающегося в файле `ColumnID` (TextData, ClientProcessID, DatabaseID, Duration, SPID, StartTime, EndTime, ApplicationName, HostName, LoginName, ServerName, GroupID, EventSequence, NestLevel, ObjectID и т.д.) точную бинарную кодировку:
   - Числа (int32/int64) — размер, знаковость.
   - Строки — length-prefix (1/2/4 байта?), UTF-16LE, наличие пула строк/дедупликации.
   - Дата/время (`StartTime`/`EndTime`) — FILETIME(100ns since 1601) vs OLE Automation date vs что-то ещё; сверить с известным значением `2026-03-19T15:55:30.02+03:00` из XML.
   - GUID/Binary Data колонки — формат (raw 16 байт GUID?).
   - Как кодируется отсутствие (NULL) колонки, если для конкретного экземпляра события часть declared-колонок не заполнена.
3. Зафиксировать найденные правила в `internal/trc/format.go` как таблицу `columnTypeMap` — только после этого переходить к Phase 1.
4. Риск: если в файле встретятся типы, которых нет в `DIAPR-391.trc` (например Binary Data, GUID) — их кодировку декодировать не на чём проверить; для них реализовать best-effort парсинг по документированному SQL Server описанию типа (`Data Columns.txt`) с явной пометкой "не проверено на реальных данных" и покрыть отдельным synthetic-тестом при возможности.

### Итоги реализации Phase 0 (зафиксировано)

**Сделано и подтверждено golden-тестами (2/2 файла):**
- `internal/trc/model.go` — `TraceHeader`, `EventClassSchema`.
- `internal/trc/format.go` — `columnDefinitions` (64 колонки из `Data Columns.txt`, с грубой категорией типа `TypeString/Int32/Int64/DateTime/GUID/Binary` — категории для типов, не встреченных в реальных данных, помечены как best-effort/unverified прямо в комментарии), `eventClassNames` (основной набор классов, включая все встреченные в обоих файлах).
- `internal/trc/header.go` — `ParseHeader(data []byte) (*TraceHeader, error)`: находит `ProviderName`/`ServerName` (первые две "печатные" UTF-16LE строки в файле, эвристика без привязки к фиксированным офсетам — устойчива к обоим форматам заголовка), затем разбирает подряд идущие блоки `FCFF` (per-EventClass схема: `EventClassID` + список `ColumnID`) и завершающий блок `FCFB` (`OrderedColumns`), до первого несовпадения маркера — на этом `EventsOffset` фиксируется как граница конца таблицы схемы.
- `internal/trc/header_test.go` — 2 golden-теста (`TestParseHeader_DIAPR391_MatchesXML`, `TestParseHeader_NBKI_MatchesXML`): читают `.xml` (потоково декодируя только необходимый префикс UTF-16 через `golang.org/x/text/encoding/unicode`, без загрузки всего 754 МБ файла), парсят `<Header>` секцию через `encoding/xml`, сверяют с результатом `ParseHeader` на соответствующем `.trc`. **Оба теста PASS**: `ProviderName`, `ServerName`, `OrderedColumns` (полный список ID, порядок) и состав колонок каждого `EventClassSchema` совпадают 1:1 с эталоном для 11 классов `DIAPR-391` (SQL2016, build 5622) и 14 классов `nbki` (SQL2019, build 4335).
- `go build ./...` и `go vet ./...` по всему репозиторию — чисто.

**Важное замечание по инструментам:** запуск `go test` без `-timeout` привёл к зависанию системы (ошибка была в `header_test.go` — `transform.Reader` + `io.ReadAll` зависал на UTF-16 декодировании; исправлено через одноразовый `decoder.Bytes()`). Впредь все `go test` в этом проекте запускать только с явным `-timeout`.

### Итоги реализации Phase 0, пункт 2 (декодирование тела события) — ЗАВЕРШЕНО

Формат тела события полностью раскрыт и подтверждён побайтово Go-утилитами (не shell/ручным hex-разбором) на `DIAPR-391.trc`, и golden-тестом на всех событиях этого файла. Инструменты созданы как обычные `_test.go` (не отдельная CLI-утилита) — `internal/trc/reverse_engineer_test.go` и `internal/trc/tlv_scan_test.go` — программный побайтовый поиск известных значений (из XML) в бинаре плюс механический TLV-сканер, вместо визуального чтения hex-дампов.

**Формат записи события** (гипотеза из README, "позиционно по схеме", **не подтвердилась** — реальный формат самоописывающий и компактнее):

```
EventRecord = Marker(2 байта: F6 FF) + FixedByte(1 байт: 0x06, назначение не подтверждено, во всех записях = 0x06)
            + EventClassID(uint16 LE) + Length(uint32 LE, размер Fields в байтах)
            + Fields

Fields = последовательность Field, суммарно Length байт

Field = PropID(uint16 LE, совпадает с ColumnID) + LenByte(1 байт)
        [+ если LenByte == 0xFF: ExtLength(uint32 LE) — расширенная длина для значений ≥255 байт]
        + Value(LenByte, либо ExtLength, байт)
```

Декодирование `Value` по фактической длине (не по декларированному в `columnDefinitions` типу, для устойчивости к вариациям):
- `len(Value) == 16` и колонка типа `TypeDateTime` → `SystemTime` (см. ниже).
- Колонка типа `TypeString` → UTF-16LE **без завершающего null** (в отличие от строк заголовка/преамбула — те null-terminated).
- `len(Value) == 4` → `int32` LE.
- `len(Value) == 8` → `int64` LE.
- Иначе (GUID/Binary, например `LoginSid`(41)/`SqlHandle`(63)) → raw `[]byte`.

**SystemTime** (колонки `StartTime`(14)/`EndTime`(15)) — 16 байт, 8×`uint16` LE в порядке: `Year, Month, DayOfWeek, Day, Hour, Minute, Second, Milliseconds`. Побайтово подтверждено на двух разных значениях: `EA 07 03 00 05 00 13 00 0F 00 37 00 1E 00 14 00` → `2026-03-19T15:55:30.020` (событие `Trace Start`), и второе значение с `Second=32, Milliseconds=757` → `2026-03-19T15:55:32.757` (событие `SP:Recompile`) — оба **точно** совпали с XML.

**Пример полностью декодированного события** (первое информативное событие файла, `EventClass=37` `SP:Recompile`, см. `DIAPR-391.utf8full.txt:394-420`) — **все 29 колонок** (`TextData, ClientProcessID, DatabaseID, LoginName, LineNumber, HostName, ApplicationName, SPID, StartTime, EventSubClass, ObjectID, ObjectType, NestLevel, ObjectName, DatabaseName, LoginSid, RequestID, XactSequence, EventSequence, IntegerData2, Offset, SqlHandle, SessionLoginName, GroupID, ServerName`) декодированы и **побайтово совпали** со значениями из XML, включая точные числа (`ObjectID=1486145921`, `EventSequence=23821372`, 16-байтовый `LoginSid`, 44-байтовый `SqlHandle`) — см. `TestTLVScan_FirstRealEvent`.

**Расширенная длина (≥255 байт)** подтверждена на длинном многострочном `TextData` (~525 символов): байты `01 00 FF 1A 04 00 00` → `PropID=1, LenByte=0xFF(сигнал), ExtLength=0x0000041A=1050 байт` — точно соответствует длине строки (525×2). См. `TestTLVScan_LongTextDataLength`.

**Реализовано** (Phase 1, частично, раньше срока — учитывая, что декодирование тела уже дало рабочий парсер):
- `internal/trc/model.go` — добавлены `TRCEvent{EventClass, EventName, Columns map[int]any}`, `SystemTime`, `TRCParseResult{Header, Events}`.
- `internal/trc/parser.go` — `ParseFile(path) (*TRCParseResult, error)`, `ParseEvents(data, header) ([]TRCEvent, error)`: ищет первый `eventHeaderMarker` (пропуская нерасшифрованный преамбул — см. ниже), затем последовательно разбирает записи событий по формату выше; при разрыве потока (неожиданные байты) выполняет ресинхронизацию поиском следующего валидного маркера, не считая файл повреждённым целиком.
- `internal/trc/parser_test.go` — `TestParseEvents_DIAPR391_MatchesXML`: golden-тест, разбирает **весь** `DIAPR-391.trc` через `ParseFile`, разбирает `<Events>` из XML (`encoding/xml`, без стриминга — файл всего 2.3 МБ), сверяет количество событий, `EventClass`, `TextData` (с нормализацией `\r\n→\n`, которую XML-парсер делает по спецификации XML 1.0, а бинарь хранит сырой текст), `SPID`, `EventSequence` для **каждого** события по порядку. **Тест проходит на 100% событий файла.**

**НЕ расшифровано до конца (не блокирует Phase 1/2, встречается ~1 раз в начале файла, минорный открытый вопрос):**
- Начальный преамбул между концом таблицы схемы (`TraceHeader.EventsOffset`) и первой настоящей записью события содержит блок(и) вида `Marker(FB FB / FB FF, 2 байта) + u16 + u16 + StrLen(uint32 LE) + строка UTF-16LE с завершающим null` (строка `"SQL Server Profiler - <guid>"`), и следом несколько полей в формате, отличном от компактного per-event (те же PropID, но `Type(1 байт) + Length(4 байта)`, а не `LenByte(1 байт)`). Встречается **2 раза на весь файл** (оба — в первых ~1.5 КБ), похоже на служебные метаданные подключения/сессии, не на события. `ParseEvents` **не спотыкается** об этот блок — он просто ищет первый `eventHeaderMarker` дальше по файлу и начинает разбор с него, преамбул целиком игнорируется. Раз он не встречается по всему остальному файлу (проверено `TestTLVScan_TypeConsistency`/сканированием) и не является событием — расшифровка деталей этого блока не требуется для Phase 1/2, отложена как best-effort.
- Назначение фиксированного байта `0x06` в `eventHeaderMarker` не подтверждено (во всех встреченных записях он константен, гипотезы не проверялись за отсутствием вариативности в файле).
- `nbki.trc` (SQL2019, 212 МБ) **не проверен** этим golden-тестом (только заголовок, см. выше) — полная сверка требует потокового XML-парсинга (754 МБ) и оценки памяти/времени, отложено как отдельная задача (см. раздел "Тестирование").

**Проверено:** `go build ./...`, `go vet ./...` — чисто; `go test ./internal/trc/... -v -timeout 120s -count=1` — все 7 тестов (2 header + 1 parser golden + 3 exploratory + 1 length) **PASS**.

## Phase 1 — Модель, парсер, схема БД, сохранение — ЗАВЕРШЕНО

5. ~~`internal/trc/model.go` — структуры~~ **сделано**: `TRCEvent{EventClass, EventName, Columns map[int]any, Procedure, Params, DurationMs}`, `TRCParam{Name, Value}`, `SystemTime` (+ метод `ToTime()`), `TRCParseResult{Header, Events}`, `TRCSession` — все реализованы. `TRCSummary` (агрегированная сводка по сессии) отложена до Phase 2 (там появится `aggregate.go`, который её и будет заполнять).
6. ~~`internal/trc/format.go`~~ **сделано** в Phase 0.
7. ~~`internal/trc/header.go`~~ **сделано** в Phase 0.
8. ~~`internal/trc/parser.go` — `ParseFile`~~ **сделано полностью**, включая ранее отложенные пункты:
   - `internal/trc/extract.go` — `ExtractProcedureAndParams(textData string) (string, []TRCParam)`: regex `execRe` распознаёт `exec [@Ret =] ProcName ...` (в т.ч. с несколькими пробелами/переносами строк), `paramHeaderRe` разбивает список `@Name = value` без lookahead (RE2 не поддерживает `(?=...)` — обойдено вычислением границ значений через `FindAllStringSubmatchIndex` соседних заголовков параметров). `sp_executesql` не покрыт (не встретился ни разу в golden-данных) — явно отмечено как ограничение.
   - `enrichEvent` в `parser.go` вызывается для каждого события сразу после декодирования полей: заполняет `Procedure`/`Params` из `Columns[1]` (TextData) и `DurationMs = Columns[13]/1000` (Duration хранится в микросекундах, см. `Data Columns.txt`).
   - `internal/trc/extract_test.go` — 4 unit-теста (`SimpleExec` с `exec @RetVal = Proc ...`, `WithoutRetVal`, `NonExecStatement`, `NoParams`), все PASS.
9. ~~`internal/store/db_schema.go`~~ **сделано**: таблицы `trc_sessions` (file_path/file_size/parsed_at/total_events/provider_name/server_name/major_version/minor_version/build_number) и `trc_events` (часто используемые колонки вынесены в отдельные поля — event_class/text_data/procedure/spid/database_id/database_name/application_name/login_name/host_name/start_time/end_time/duration_ms/cpu/reads/writes/row_counts/object_id/object_name/event_sequence/nest_level/line_number/error/severity/success/params; **весь** декодированный набор `Columns` дополнительно сохраняется целиком как `columns JSONB`, чтобы не терять данные по колонкам, не вынесенным в отдельные поля — аналог `payload JSONB` в `rti_client_events`) + индексы (`idx_trc_events_session_id/procedure/duration_ms/spid/event_sequence`).
10. ~~`internal/trc/store.go`~~ **сделано**: `SaveSession(db, result, filePath, fileSize) (int64, error)` (batch insert через `pq.CopyIn`, как в `rti/store.go`), `ListSessions`, `GetSession`, `GetLatestSessionID`, `DeleteSession`, `PruneSessions`, `LoadEvents` (восстанавливает `Columns` из JSONB через `marshalColumns`/`unmarshalColumns` — сериализация с явным тегом типа `{"type": "int64"/"string"/"systemtime"/"binary", "value": ...}`, чтобы не терять исходный Go-тип при обратном чтении).

**Проверено:** `go build ./...`, `go vet ./...` — чисто; `go test ./internal/trc/... -v -timeout 120s -count=1` — **11 тестов, все PASS** (2 header golden + 1 parser golden + 4 extract unit + 4 exploratory Phase 0). `store.go`/`db_schema.go` не покрыты автотестами (нужна тестовая БД Postgres) — компилируются и построены строго по паттерну уже работающего `internal/rti/store.go`; функциональная проверка на реальной БД — рекомендуется перед Phase 2.

**Не реализовано в Phase 1 (сознательно, не входило в scope):** `sp_executesql`-вызовы в `ExtractProcedureAndParams`, TRCSummary/агрегация, MCP/CLI-обвязка (`cmd/trc.go`, `codebase_trc_*`) — вынесены в Phase 2 согласно плану.

## Phase 2 — Обогащение, иерархия, агрегация, MCP — ЗАВЕРШЕНО

11. ~~`internal/trc/enrich.go`~~ **сделано**: `ProcedureLookup` (свой интерфейс, структурно совпадающий с `internal/rti/enrich.go`, без импорта пакета `rti` — чтобы не создавать межпакетную зависимость), `EnrichProcedure`, `EnrichEvents` (map: procedure → `*ProcedureEnrichment`), `trimHashSuffix` (отбрасывание суффикса `#...`).
12. ~~`internal/trc/tree.go`~~ **сделано**: `BuildTrees(events) map[int][]*TRCTreeNode` — группировка по SPID (колонка 12), внутри каждой группы стек Starting/Completed по общему префижу имени класса (отбрасывание суффикса `Starting`/`Completed` даёт "family": `RPC:`, `SQL:Batch`, `SQL:Stmt`, `SP:`, `SP:Stmt` — совпадение без хардкода конкретных имён классов). Несовпавшие Completed (нет открытого Starting) и все "простые" события (SP:Recompile, exec-события и т.д.) добавляются как узлы на текущем уровне вложенности. `FormatTrees` — текстовый вывод.
13. ~~`internal/trc/aggregate.go`~~ **сделано**: `AggregateByProcedure(events) []TRCProcAgg` (count/min/max/avg/total duration по `TRCEvent.Procedure`, сортировка по `TotalMs` desc), `EnrichAggregates` — проставляет `SourceFile` из enrich-карты.
14. ~~`internal/mcp/registry.go`~~ **сделано**: зарегистрированы `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events` (фильтры `spid`/`procedure`/`limit`), `codebase_trc_procedures` (с enrich), `codebase_trc_tree` (фильтр `spid`), `codebase_trc_slow`, `codebase_trc_errors` (по колонке `Error`(31) != 0), `codebase_trc_delete`, `codebase_trc_prune`. Добавлен `loadTRCFromArgs` — аналог `loadRTIFromArgs` (session_id → `LoadEvents`/`GetSession` из БД, либо file_path → `ParseFile` на месте).
15. ~~`cmd/trc.go` + `cmd/root.go`~~ **сделано**: подкоманды `parse/summary/events/procedures/tree/errors/slow/list/delete/prune` (аналог `cmd/rti.go`), self-registration через `init()` → `rootCmd.AddCommand(trcCmd)`, упоминание `trc` добавлено в `Long`-описание `rootCmd`.

**Проверено:** `go build ./...`, `go vet ./...` — чисто; `go test ./internal/trc/... ./cmd/... -timeout 300s -count=1` — **33 теста в `internal/trc`** (2 header golden + 2 parser golden incl. nbki 212МБ + 7 decode + 5 tree + 4 aggregate + 6 enrich + 4 extract + 4 exploratory Phase 0) **+ все тесты `cmd`** — **все PASS**.

**Не проверено на реальной БД:** `codebase_trc_*` MCP-инструменты и CLI-подкоманды `list/delete/prune/tree(--session)` требуют Postgres с применённой схемой (`db.InitSchema()`); построены строго по паттерну уже работающих `rti`-аналогов, но end-to-end (`trc parse <file> && trc list && trc tree --session N`) не прогонялся вживую в этой сессии — рекомендуется перед реальным использованием.

## Тестирование (golden-файл обязателен)

- **Golden-тесты** (2 пары): `internal/trc/parser_test.go` — парсит `DIAPR-391.trc`/`nbki.trc`, параллельно парсит соответствующий `.xml` через `encoding/xml`, сравнивает поля событий (`EventClass`, `TextData`, `SPID`, `Duration`, `StartTime`, ...) — построчное соответствие обязательно для всех событий обоих файлов (учитывая, что `nbki` не содержит Duration/StartTime — эти поля просто нулевые в обоих источниках).
- Unit-тесты на `header.go` — проверка разбора `OrderedColumns` и всех блоков `TracedEvents` с точными списками колонок для обоих файлов (11 блоков у `DIAPR-391`, 14 у `nbki`), включая разные `BuildNumber`/версии.
- Unit-тесты на декодирование каждого типа колонки (string/int/bigint/datetime/guid/binary) — synthetic byte-slices с ожидаемым результатом.
- Тест на восстановление иерархии (`tree.go`) — сверка глубины вложенности по известным Starting/Completed парам из golden-файлов.
- Нагрузочный тест на `nbki.trc`/`.xml` (212 МБ / 754 МБ) — время/память потокового парсинга.
- `go build ./...` и `go test ./internal/trc/... ./internal/store/... ./internal/mcp/...` — обязательны после каждой фазы.

## Риски

| Риск | Митигация |
|---|---|
| Формат недокументирован, тип отдельных колонок (GUID/Binary/ObjectType) не встречается в тестовом файле | Best-effort по спецификации `Data Columns.txt`, явная маркировка "unverified", покрытие synthetic-тестами |
| Другие версии .trc (build ≠ 5622/4335, другой набор трассируемых событий) могут иметь иной формат заголовка | Проверено на двух версиях (SQL2016 build 5622, SQL2019 build 4335) — структура схемы идентична; fail-fast с понятной ошибкой при неожиданном формате заголовка вместо тихого неверного разбора |
| RPC:Completed/Starting объявлены в схеме обоих файлов, но реальных экземпляров с параметрами нет ни в одном — кодирование RPC-параметров не проверено | Основной путь извлечения `Procedure`/`Params` — regex по `TextData` (подтверждён на реальных Diasoft exec-вызовах в `nbki.xml`), а не по RPC-колонкам; RPC-колонки — best-effort с пометкой "unverified" |
| Восстановление NestLevel эвристическое | Помечать поле как `estimated`, не выдавать за точное значение из трассировки |

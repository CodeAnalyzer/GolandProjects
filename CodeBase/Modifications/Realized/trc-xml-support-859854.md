# Поддержка XML-формата трейсов в команде `trc`

Расширить парсер `internal/trc` так, чтобы команда `trc` (CLI и MCP) прозрачно принимала на вход как бинарный `.trc`, так и XML-экспорт трейса (`TraceData` из SQL Server Profiler), определяя формат по сигнатуре содержимого и приводя оба источника к единой модели `TRCEvent`/`TraceHeader`, без изменения схемы БД и внешнего API команд.

## Формат XML (обследовано на `Modifications/DIAPR-391.xml`)

- Файл в кодировке UTF-16 (`<?xml version="1.0" encoding="utf-16"?>`), с BOM.
- `<TraceData><Header><TraceProvider name=... MajorVersion=... MinorVersion=... BuildNumber=.../>`, `<ServerInformation name=.../>`.
- `<ProfilerUI><OrderedColumns><ID>N</ID>...</OrderedColumns><TracedEvents><Event id="X"><EventColumn id="Y"/>...</Event>...</TracedEvents></ProfilerUI>` — та же семантика, что и блоки `FCFF`/`FCFB` в бинарнике.
- `<Events><Event id="EventClassID" name="EventName"><Column id="ColumnID" name="ColumnName">значение</Column>...</Event>...</Events>`.
- Значения колонок — текстовые, тип зависит от `ColumnID` (уже есть таблица `columnDefinitions` в `internal/trc/format.go`):
  - `TypeString` — как есть.
  - `TypeInt32`/`TypeInt64` — десятичное число (например `Duration=123310`, микросекунды — совпадает по семантике с бинарным `Columns[13]`).
  - `TypeDateTime` — ISO 8601 с офсетом, например `2026-03-19T15:55:32.757+03:00`.
  - `TypeGUID`/`TypeBinary` — hex-строка без разделителей (например `SqlHandle`, `LoginSid`).
- Имя события и колонки продублированы атрибутом `name` — можно использовать напрямую вместо словарного поиска, но для унификации решено декодировать по `ColumnID` через существующие `columnDefinitions`.

## Детектирование формата

Функция `DetectFormat(data []byte) (isXML bool)` в новом файле `internal/trc/xml_parser.go`:
- Пропустить UTF-16/UTF-8 BOM, если есть.
- Проверить, что после BOM идёт `<?xml` (в UTF-16LE или UTF-8) либо напрямую `<TraceData` — если да, это XML.
- Иначе трактовать как бинарный `.trc`.

`ParseFile` в `internal/trc/parser.go` дорабатывается: читает файл, вызывает `DetectFormat`, и диспетчеризует в `ParseBinary(data)` (текущая логика `ParseHeader`+`ParseEvents`, переименовать/обернуть) либо `ParseXML(data)` (новая). Оба пути возвращают одинаковый `*TRCParseResult{Header, Events}`.

## Новый XML-парсер (`internal/trc/xml_parser.go`)

1. Декодирование UTF-16 → UTF-8 через `golang.org/x/text/encoding/unicode` (или `unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)` + `transform.NewReader`) — зависимость уже может быть в `go.sum`, проверить при реализации; если нет — добавить в `go.mod`.
2. Разбор через `encoding/xml` в промежуточные структуры:
   ```go
   type xmlTraceData struct {
       Header struct {
           TraceProvider struct{ Name string; MajorVersion, MinorVersion, BuildNumber int } `xml:"TraceProvider"`
           ServerInformation struct{ Name string } `xml:"ServerInformation"`
           ProfilerUI struct {
               OrderedColumns struct{ ID []int `xml:"ID"` } `xml:"OrderedColumns"`
               TracedEvents struct {
                   Event []struct {
                       ID int `xml:"id,attr"`
                       EventColumn []struct{ ID int `xml:"id,attr"` } `xml:"EventColumn"`
                   } `xml:"Event"`
               } `xml:"TracedEvents"`
           } `xml:"ProfilerUI"`
       } `xml:"Header"`
       Events struct {
           Event []struct {
               ID   int    `xml:"id,attr"`
               Name string `xml:"name,attr"`
               Column []struct {
                   ID    int    `xml:"id,attr"`
                   Value string `xml:",chardata"`
               } `xml:"Column"`
           } `xml:"Event"`
       } `xml:"Events"`
   }
   ```
3. Собрать `TraceHeader{ProviderName, MajorVersion, MinorVersion, BuildNumber, ServerName, OrderedColumns, EventClasses}` из `Header` — те же поля, что заполняет `ParseHeader` для бинарника (`EventClasses` строится из `TracedEvents/Event/EventColumn`, аналогично `parseSchemaBlock`).
4. Для каждого `<Events><Event>` — собрать `TRCEvent{EventClass: ID, EventName: Name, Columns: map[int]any{}}`, для каждого `<Column>` декодировать `Value` по `ColumnType(ID)`:
   - `TypeString` → как есть.
   - `TypeInt32` → `strconv.ParseInt` → `int32`.
   - `TypeInt64` → `strconv.ParseInt` → `int64`.
   - `TypeDateTime` → `time.Parse(time.RFC3339..., value)`, взять локальные календарные поля **без учёта offset** (пользователь подтвердил: время сохраняется "как есть", без конвертации в UTC) и собрать `SystemTime{Year, Month, DayOfWeek (time.Weekday от распарсенной даты), Day, Hour, Minute, Second, Milliseconds}`. Добавить хелпер `systemTimeFromLocalParts(t time.Time) SystemTime` в `model.go` или `xml_parser.go`.
   - `TypeGUID`/`TypeBinary` → `hex.DecodeString(value)` → `[]byte`.
   - Прочерк/неизвестный тип → сохранить как строку (fallback, не терять данные).
5. Вызвать существующий `enrichEvent(&ev)` (из `parser.go`) для заполнения `Procedure`/`Params`/`DurationMs` — код уже общий, изменений не требует.

## Совместимость с хранением и запросами

- `internal/trc/store.go` (`SaveSession`, `insertTRCEvents`, `LoadEvents`, `marshalColumns`/`unmarshalColumns`) — **без изменений**: работает с уже готовыми `TRCParseResult`/`TRCEvent`, не зависящими от источника формата.
- `internal/trc/tree.go`, `aggregate.go`, `enrich.go`, `extract.go` — без изменений (работают над `[]TRCEvent`).
- CLI (`cmd/trc.go`) и MCP tools (`internal/mcp/registry.go`, `codebase_trc_*`) — без изменений сигнатур; `ParseFile`/`ParseFile` вызывается так же, детект формата — внутри.

## Тесты

- `internal/trc/xml_parser_test.go`:
  - Юнит-тест `DetectFormat` на бинарный сигнатуру vs `<?xml`.
  - Тест разбора малого фикстур-XML (создать урезанный тестовый XML-фикстур с 2-3 событиями на основе структуры `DIAPR-391.xml`, а не гонять полный 11MB файл в unit-тестах) — проверить `EventClass`, `EventName`, декодированные `Columns` (`TextData` строка, `Duration` int64, `StartTime` SystemTime, `SPID` int32).
  - Тест сквозного `ParseFile` на XML-фикстуре → сравнение с ожидаемым `TRCParseResult`.
- Опционально: smoke-тест (не unit, вручную) — прогнать `codebase trc parse Modifications/DIAPR-391.xml` и сравнить `total_events`/`procedures` с результатом `codebase trc parse Modifications/DIAPR-391.trc` (данные должны быть тем же трейсом в двух форматах) — сверка после реализации.

## Файлы к изменению/добавлению

- **Новый**: `internal/trc/xml_parser.go` — `DetectFormat`, `ParseXML`, декодирование колонок/времени.
- **Новый**: `internal/trc/xml_parser_test.go`.
- **Изменить**: `internal/trc/parser.go` — `ParseFile` дорабатывается для диспетчеризации по `DetectFormat`; возможно, переименовать текущую связку `ParseHeader`+`ParseEvents` в приватную `parseBinary`.
- **Изменить** (при необходимости): `internal/trc/model.go` — добавить хелпер конвертации `time.Time` → `SystemTime` без офсета, если такого хелпера ещё нет.
- **Изменить** (при необходимости): `go.mod`/`go.sum` — добавить зависимость для UTF-16 декодирования, если `golang.org/x/text` ещё не используется в проекте.
- Без изменений: `cmd/trc.go`, `internal/mcp/registry.go`, `internal/trc/store.go`, `tree.go`, `aggregate.go`, `enrich.go`, `extract.go`.

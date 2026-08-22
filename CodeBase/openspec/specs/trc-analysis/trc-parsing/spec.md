# TRC Parsing

## Purpose

Парсинг файлов SQL Server Profiler: бинарных .trc, XML-экспортов .xml и Extended Events .xel. Авто-детекция формата по сигнатуре содержимого. Декодирование событий (RPC:Completed, SQL:BatchCompleted, SP:StmtCompleted и др.), извлечение вызовов процедур и параметров из TextData, enrichment из индекса.

## Requirements

### Requirement: Авто-детекция формата

Система SHALL автоматически определять формат файла (.trc, .xml, .xel) по сигнатуре содержимого (content sniffing), без зависимости от расширения файла.

#### Scenario: Бинарный .trc

- **GIVEN** файл с бинарной сигнатурой SQL Server Profiler
- **WHEN** выполняется `codebase trc parse file.trc`
- **THEN** формат определён как бинарный .trc и обработан через `ParseHeader` + `ParseEvents`

#### Scenario: XML-экспорт

- **GIVEN** файл с XML-структурой трейса (UTF-16 или UTF-8)
- **WHEN** выполняется `codebase trc parse file.xml`
- **THEN** формат определён как XML-экспорт и обработан через `ParseXML`

#### Scenario: Extended Events .xel

- **GIVEN** файл с бинарной сигнатурой Extended Events
- **WHEN** выполняется `codebase trc parse file.xel`
- **THEN** формат определён как .xel и обработан через `ParseXEL`

### Requirement: Парсинг бинарного .trc

Система SHALL парсить бинарные .trc файлы: разбор заголовка (TraceHeader), извлечение метаданных (провайдер, сервер, версия), декодирование событий через OrderedColumns и TracedEvents.

#### Scenario: Декодирование события RPC:Completed

- **GIVEN** .trc файл с событием RPC:Completed
- **WHEN** выполняется парсинг
- **THEN** событие декодировано с event_class, event_name, text_data, procedure, spid, duration_ms, cpu, reads, writes, error

### Requirement: Парсинг XML-экспорта

Система SHALL парсить XML-экспорты трейсов с автоматическим определением кодировки UTF-16/UTF-8.

#### Scenario: XML в UTF-16

- **GIVEN** XML-файл трейса в кодировке UTF-16
- **WHEN** выполняется парсинг
- **THEN** кодировка определена как UTF-16 и файл корректно декодирован

### Requirement: Парсинг Extended Events .xel

Система SHALL парсить бинарные .xel файлы (Extended Events): декодирование dictionary-блока, TLV-структур ACTION и DATA, декодирование событий wait_completed, sql_batch_completed, sql_statement_completed, sp_statement_completed, калибровка timestamp.

#### Scenario: Декодирование wait_completed

- **GIVEN** .xel файл с событием wait_completed
- **WHEN** выполняется парсинг
- **THEN** событие декодировано с timestamp, wait_type, duration, signal_duration, wait_resource

#### Scenario: Декодирование sql_batch_completed

- **GIVEN** .xel файл с событием sql_batch_completed
- **WHEN** выполняется парсинг
- **THEN** событие декодировано с timestamp, cpu_time, duration, physical_reads, logical_reads, writes, row_count, batch_text

#### Scenario: Калибровка timestamp

- **GIVEN** .xel файл с калибровочными константами в заголовке (offset 620 = BASE_FILETIME, offset 628 = FREQ)
- **WHEN** выполняется парсинг
- **THEN** timestamps декодированы в UTC с точностью до миллисекунды

### Requirement: Извлечение процедур и параметров из TextData

Система SHALL извлекать имя процедуры и параметры из TextData событий (regex-эвристика для exec-statements) для агрегации и анализа.

#### Scenario: Извлечение процедуры из RPC:Completed

- **GIVEN** .trc файл с событием RPC:Completed, где TextData содержит `exec MyProc @Param1 = 1, @Param2 = 'test'`
- **WHEN** выполняется парсинг
- **THEN** procedure извлечена как `MyProc`, params сохранены как JSONB

### Requirement: Enrichment из индекса

Система SHALL обогащать события и агрегации из индекса: сопоставление имени процедуры с `sql_procedures` для получения пути к файлу и номеров строк.

#### Scenario: Enrichment процедуры

- **GIVEN** .trc файл с событием, где procedure = `MyProc`, и проиндексированный проект
- **WHEN** выполняется enrichment
- **THEN** результат содержит путь к файлу процедуры и номера строк из индекса

### Requirement: Параллельный enrichment для больших трейсов

Система SHALL поддерживать параллельный enrichment для больших трейсов с настраиваемым количеством workers через `trc.max_enrich_workers` и `trc.min_procs_for_parallel_enrich`. Параллельная обработка событий и JSON-сериализация выполняются в `enrich_parallel.go`, включая санитайзинг `\0x00` байт (недопустимых в PostgreSQL `jsonb`) перед записью.

#### Scenario: Параллельный enrichment

- **GIVEN** .trc файл с 10000+ уникальных процедур
- **WHEN** выполняется enrichment
- **THEN** enrichment выполняется параллельно с `max_enrich_workers` воркерами

#### Scenario: Санитайзинг \0x00 для jsonb

- **GIVEN** событие с TextData, содержащим `\0x00` байт (артефакт кодировки)
- **WHEN** выполняется JSON-сериализация параметров перед записью в `trc_events.params` (JSONB)
- **THEN** `\0x00` байты удалены/заменены, JSON валиден для PostgreSQL `jsonb`

### Requirement: Streaming-парсинг с callback и ParseFileToDB

Система SHALL предоставлять streaming-режим парсинга через `parseEventsStreamingCB` (`parser.go:190-265`) с callback-ом на каждое событие, без накопления всех событий в памяти. На основе streaming-парсинга построен `ParseFileToDB` (`parse_to_db.go`) — связка «парсинг + сразу запись в БД», используемая `trcsvc.ExecuteParse` при доступной БД (без промежуточного накопления `parseResult.Events`).

#### Scenario: Streaming-парсинг большого .trc

- **GIVEN** .trc файл с 10M событий
- **WHEN** выполняется `parseEventsStreamingCB` с callback-ом, записывающим события в БД
- **THEN** события обрабатываются по мере парсинга, без накопления всех 10M событий в памяти

#### Scenario: ParseFileToDB при доступной БД

- **GIVEN** .trc файл и доступная БД
- **WHEN** `trcsvc.ExecuteParse` вызывает `trc.ParseFileToDB(ctx, filePath, db)`
- **THEN** события парсятся streaming-режиме и записываются в `trc_events` по мере парсинга
- **AND** возвращается `session_id` и `totalEvents` без удержания всех событий в памяти

### Requirement: Генерация словаря XEL-событий из CSV

Система SHALL генерировать карту XEL-событий (event name → decoder) через генератор `gen_xel_mapping.go` из CSV-источника, итоговый файл `xel_event_map_generated.go` коммитится в репозиторий. Это позволяет обновлять справочник XEL-событий без правок руками — пересоздаётся из CSV при изменении списка событий SQL Server Extended Events.

#### Scenario: Обновление справочника XEL

- **GIVEN** обновлённый CSV с новыми XEL-событиями SQL Server
- **WHEN** запускается `gen_xel_mapping.go` (через `go generate`)
- **THEN** `xel_event_map_generated.go` перегенерирован с обновлённой картой event name → decoder

## Related code

- `internal/trc/parser.go` — `ParseFile`, `DetectFormat`, `parseEventsStreamingCB`, точка входа
- `internal/trc/parse_to_db.go` — `ParseFileToDB` (streaming-парсинг + запись в БД)
- `internal/trc/header.go` — `ParseHeader`, `OrderedColumns`, `TracedEvents`, `EventsOffset`
- `internal/trc/xml_parser.go` — `ParseXML`, `DetectFormat` (UTF-16/UTF-8)
- `internal/trc/xel_parser.go` — `ParseXEL`, декодирование событий
- `internal/trc/xel_format.go` — dictionary-блок, TLV-структуры, калибровка timestamp
- `internal/trc/xel_event_map_generated.go` — карта событий XEL (event name → decoder), сгенерирована из CSV
- `internal/trc/gen_xel_mapping.go` — `go generate` генератор `xel_event_map_generated.go` из CSV
- `internal/trc/extract.go` — извлечение procedure/params из TextData
- `internal/trc/enrich.go` — `EnrichEvents`, `EnrichAggregates`
- `internal/trc/enrich_parallel.go` — параллельный enrichment, JSON-сериализация с санитайзингом `\0x00`
- `internal/trc/utf16.go` — UTF-16LE декодирование для .xel

## Notes

- Формат определяется по содержимому, а не по расширению
- .xel — самоописывающийся формат (dictionary-блок в начале файла)
- Калибровочные константы timestamp хранятся в заголовке .xel (offset 620/628) — могут отличаться для разных версий SQL Server
- События .xel: wait_completed (~83%), sql_batch_completed (~3.7%), sql_statement_completed (~3.8%), sp_statement_completed (~9.7%)
- Execution-слой `internal/trcsvc/runtime.go` — общая точка входа для CLI (`cmd/trc.go`) и MCP-инструментов `codebase_trc_*`; устраняет дублирование оркестрации. Парсинг специфицирован здесь; сохранение сессий и fallback при недоступной БД — в `trc-analysis/trc-storage`; транспорт MCP — в `mcp-server/mcp-transport-tools`.
- `xel_event_map_generated.go` — сгенерированный файл; обновляется через `go generate` (запуск `gen_xel_mapping.go`), не правится руками
- Streaming-парсинг (`parseEventsStreamingCB`/`ParseFileToDB`) — критичен для больших .trc (миллионы событий): без него парсер упирался бы в память
- JSON-санитайзинг `\0x00` байт перед записью в `trc_events.params` (JSONB) — PostgreSQL `jsonb` не принимает `\0x00`

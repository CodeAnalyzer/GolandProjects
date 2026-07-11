# План: фильтры и краткий формат для `codebase_rti_timeline`

Добавить в MCP-инструмент `codebase_rti_timeline` опциональные фильтры (время, PID, процедура, класс/метод) и краткий формат вывода (`format=short`), чтобы сократить объём JSON-ответа для больших RTI-сессий.

## Контекст

- Текущий хэндлер: `@D:/GITHUB/GolandProjects/CodeBase/internal/mcp/registry.go:904-920` — возвращает `result.Calls` и `result.ClientEvents` целиком, без фильтрации.
- Модели: `@D:/GITHUB/GolandProjects/CodeBase/internal/rti/model.go:6-32` (`RTICall`), `:97-126` (`RTIClientEvent`).
- Загрузка данных: `loadRTIFromArgs` (`registry.go:1074-1120`) — грузит всю сессию (calls + client events) целиком из БД или парсит файл; фильтрация будет применяться **после** загрузки, в памяти.
- Существующие хэлперы аргументов в `registry.go`: `optionalString(args, key) (string, error)`, `optionalInt(args, key) (int, error)`, `optionalInt64`, `optionalBool`, `intProp`, `stringProp`, `objectSchema`. **Важно**: `optionalString`/`optionalInt` возвращают `(value, error)` — в тексте исходного proposal это не учтено (там вызовы без обработки ошибки), в реализации это нужно исправить.
- Обогащение (`enrichment`) сейчас считается только для клиентских событий (`rti.EnrichClientEvents`, `@D:/GITHUB/GolandProjects/CodeBase/internal/rti/enrich_client.go:50-63`). Для `calls` `EnrichCalls` в этом хэндлере не вызывается и полей `SourceFile`/`RetValMeaning` там нет.

## Решения по открытым вопросам (согласовано с пользователем)

1. **Фильтр `procedure`** — точное совпадение без учёта регистра (`strings.EqualFold`), как в исходном proposal.
2. **Счётчики** — в ответ добавляются `filtered_calls_count`, `total_calls_count`, `filtered_events_count`, `total_events_count`.
3. **Scope enrichment для calls** — не расширяется. `EnrichCalls` не вызывается в этом хэндлере; короткий формат вызовов не включает `SourceFile`/`RetValMeaning`. Обогащение клиентских событий (`clientEnrich`) остаётся как отдельная map в ответе, без изменений.

## Шаги реализации

### 1. Новый файл `internal/rti/timeline.go`

- `TimelineFilter` struct: `TimeFrom *time.Time`, `TimeTo *time.Time`, `PID *int`, `Procedure string`, `ClassName string`, `MethodName string`, `Format string`.
- `ApplyTimelineFilter(calls []*RTICall, events []*RTIClientEvent, f TimelineFilter) ([]*RTICall, []*RTIClientEvent)`:
  - `calls`: фильтр по `EnterTime` (`TimeFrom`/`TimeTo`), по `Procedure` (`EqualFold`).
  - `events`: фильтр по `Timestamp`, по `PID`, `ClassName`, `MethodName` (`EqualFold`).
  - Раздельная фильтрация по времени для calls (`EnterTime`) и events (`Timestamp`) — обосновано различием часов клиента/сервера (см. примечание в исходном proposal).
- `RTICallShort` struct — без `Params`, `Checkpoints`, `BLogBlocks`, `BLogTables`. Поля: `ID`, `Procedure`, `EnterTime`, `ExitTime`, `ElapsedMs`, `NestLevel`, `SPID`, `ModuleName`, `ModuleID`, `RetVal`, `EnterLine`, `ExitLine`, `ParentID`.
- `RTIClientEventShort` struct — без `RawBody`, полного `SQL.Text` (если тяжёлый), `Memory`/`BPL` деталей. Поля: `ID`, `Timestamp`, `Level`, `Category`, `ClassName`, `MethodName`, `PID`, `SeqNo`, `Line`, `Kind`, `ElapsedMs` (переименовать под `DurationSec`, если применимо — сверить с реальным полем `RTIClientEvent.ElapsedMs`), `ServerCallID`.
  - Уточнение при реализации: в `RTIClientEvent` нет отдельного `DurationSec`, есть `ElapsedMs int`. Использовать реальное поле, не изобретать новое.
- `ToShortCall(c *RTICall) RTICallShort`, `ToShortEvent(ev *RTIClientEvent) RTIClientEventShort` — простые мапперы.

### 2. Схема инструмента (`registry.go`, определение `codebase_rti_timeline`)

Добавить в `InputSchema` (через `objectSchema`/`stringProp`/`intProp`):
- `time_from`, `time_to` (string, RFC3339)
- `pid` (integer)
- `procedure`, `class_name`, `method_name` (string)
- `format` (string: `full` default / `short`)

Обновить `Description` инструмента, упомянув фильтры и `format=short`.

### 3. Хэндлер `codebase_rti_timeline` (`registry.go`)

После `loadRTIFromArgs`:
1. Собрать `rti.TimelineFilter` из аргументов, **корректно обрабатывая ошибки** `optionalString`/`optionalInt`/`optionalInt64` (в отличие от кода в proposal).
2. Парсить `time_from`/`time_to` через `time.Parse(time.RFC3339, v)`; при ошибке парсинга — вернуть явную ошибку хэндлера (не молча игнорировать, как в proposal).
3. Вызвать `rti.ApplyTimelineFilter`.
4. Если `format == "short"` (`EqualFold`) — конвертировать в короткие срезы через `ToShortCall`/`ToShortEvent`.
5. Пересчитать `clientEnrich` **после фильтрации** (обогащать только отфильтрованные события — экономия запросов к БД).
6. Собрать ответ:
   ```go
   map[string]interface{}{
       "calls":                respCalls,
       "client_events":        respEvents,
       "enrichment":           clientEnrich,
       "total_calls_count":    len(result.Calls),
       "filtered_calls_count": len(filteredCalls),
       "total_events_count":   len(result.ClientEvents),
       "filtered_events_count": len(filteredEvents),
   }
   ```

### 4. Тесты (`internal/rti/timeline_test.go`, новый файл)

Базовый набор из proposal + доп. кейсы:
- `TestApplyTimelineFilter_ByTime` — calls и events по `TimeFrom`/`TimeTo`.
- `TestApplyTimelineFilter_ByProcedure` — точное совпадение, разный регистр.
- `TestApplyTimelineFilter_ByPID`
- `TestApplyTimelineFilter_ByClassAndMethod`
- `TestApplyTimelineFilter_NoFilters_ReturnsAll` — регресс на пустой `TimelineFilter{}`.
- `TestToShortCall_DropsHeavyFields`
- `TestToShortEvent_DropsHeavyFields`

### 5. Проверка

```powershell
go build ./...
go test ./internal/rti/... ./internal/mcp/...
```

Ручная проверка сценария из proposal (session_id + time_from/time_to + pid + format=short) — сверить, что `filtered_calls_count`/`filtered_events_count` в ответе соответствуют ожиданиям, а объём JSON заметно меньше.

## Затрагиваемые файлы

| Файл | Действие |
|---|---|
| `internal/rti/timeline.go` | новый: `TimelineFilter`, `ApplyTimelineFilter`, `RTICallShort`, `RTIClientEventShort`, мапперы |
| `internal/rti/timeline_test.go` | новый: unit-тесты |
| `internal/mcp/registry.go` | схема + хэндлер `codebase_rti_timeline`: фильтры, short-формат, счётчики |

## Риски / нюансы, требующие внимания при реализации

- Обязательно обрабатывать ошибки `optionalString`/`optionalInt`/`optionalInt64` (в proposal они игнорировались) — иначе некорректный тип аргумента будет проглочен молча.
- Ошибка парсинга `time.Parse` для `time_from`/`time_to` должна возвращаться пользователю как ошибка инструмента, а не тихо игнорироваться (в proposal `if err == nil` просто пропускает фильтр).
- Проверить реальные имена полей `RTIClientEvent` (`ElapsedMs`, не `DurationSec`) при написании `RTIClientEventShort`, чтобы не рассинхронизироваться с `model.go`.
- `clientEnrich` пересчитывать после фильтрации, а не до — иначе теряется смысл сокращения объёма ответа (в proposal порядок не уточнён явно).

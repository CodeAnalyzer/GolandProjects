# TRC: фильтрация по SPID, сравнение процедур и постраничная выборка событий

## Назначение

Расширить существующие CLI/MCP-инструменты TRC-анализа так, чтобы можно было:

1. строить Top-N завершённых вызовов процедур для одного или нескольких SPID;
2. сравнивать процедуры проблемного SPID с теми же процедурами в параллельных потоках;
3. последовательно выгружать все события, соответствующие расширенному фильтру;
4. получать краткую сводку активности по SPID без выгрузки сырых событий.

Предложение описывает только новый функционал. Корректность базовой агрегации `SP:Completed`, единая формула метрик, точные `filtered_count`/`returned_count`, строгая обработка существующих MCP-аргументов и полное enrichment агрегатов считаются текущим поведением системы и не входят в объём этой доработки.

## Пользовательский сценарий

Для TRC-сессии требуется:

1. определить Top-20 процедур по `total_ms` для `SPID=728`;
2. получить для этих же процедур `count`, `total_ms`, `min_ms`, `max_ms`, `avg_ms` по SPID `700, 179, 741, 748, 751`;
3. сравнить focus-поток с объединённой статистикой peer-потоков;
4. при необходимости выгрузить все события выбранных SPID и классов событий страницами;
5. увидеть временные границы и состав событий каждого SPID.

Текущие инструменты не поддерживают этот сценарий как единый server-side анализ:

- `codebase_trc_procedures` не принимает SPID, Top-N, сортировку и группировку по SPID;
- `codebase_trc_events` не поддерживает несколько SPID, массив event names, временной диапазон и продолжение выборки после первой страницы;
- отдельного инструмента сравнения focus/peer SPID нет;
- отдельной сводки активности по SPID нет;
- `codebase_read_more` продолжает чтение транспортного JSON-ответа, а не выборку строк из `trc_events`.

## Цели

1. Server-side фильтрация и агрегация для saved-session.
2. Эквивалентное поведение для `session_id` и `file_path`.
3. Детерминированный Top-N focus SPID и сравнение тех же процедур с peer SPID.
4. Keyset pagination событий, независимая от MCP transport pagination.
5. Компактный формат событий для аналитических выгрузок.
6. Фактическая, не предположительная сводка по активности SPID.
7. Предсказуемая валидация массивов, enum-параметров, временных границ и CLI CSV-списков.

## Не входит в объём

- изменение бинарного, XML или XEL парсера;
- повторный парсинг сохранённых TRC-сессий;
- вычисление «чистого» времени процедуры без времени дочерних вызовов;
- синтетическая оценка длительности незавершённого вызова;
- автоматическое сопоставление SPID с бизнес-понятием `Batch No` без соответствующих событий трейса;
- изменение `codebase_read_more` и транспортной пагинации MCP;
- расширение `codebase_trc_slow` новыми фильтрами;
- нормализация схем, квадратных скобок или регистра имени процедуры;
- удаление существующих индексов без отдельного подтверждения статистикой их использования.

---

## F1. Общая расширенная модель фильтров событий

### Модель

Привести `TRCEventFilter` к единой внутренней форме без параллельных scalar-полей:

```go
type TRCEventFilter struct {
    SPIDs         []int
    Procedure     string
    EventNames    []string
    TimeFrom      *time.Time
    TimeTo        *time.Time
    MinDurationMs *int64
    AfterID       *int64
}
```

`AfterID` является указателем, чтобы отличать отсутствующий cursor от явно переданного нуля.

### Правила

- внутренняя модель всегда использует `SPIDs` и `EventNames`, в том числе для одного значения;
- каждый SPID должен быть положительным;
- дубликаты в `SPIDs` и `EventNames` удаляются с сохранением порядка первого появления;
- пустые `SPIDs` и `EventNames` эквивалентны отсутствию соответствующего фильтра, кроме контрактов, где массив обязателен;
- существующие публичные `spid` и `event_name` являются только legacy-алиасами и нормализуются в массив из одного элемента до вызова сервисного слоя; новым контрактам scalar-формы не добавляются;
- одновременная передача legacy-алиаса и соответствующего массива возвращает ошибку с предложением использовать array-параметр;
- `Procedure` сохраняет текущее точное case-sensitive совпадение;
- `TimeFrom` фильтрует `start_time >= time_from`;
- `TimeTo` фильтрует `start_time < time_to`;
- при одновременном наличии границ должно выполняться `time_from < time_to`;
- события с `start_time IS NULL` не попадают в заданный временной диапазон;
- `MinDurationMs` должен быть неотрицательным и задаёт `duration_ms >= min_duration_ms`;
- `EventNames` задаёт точное совпадение с любым перечисленным `event_name`;
- пустые строки в массивах запрещены;
- все фильтры, кроме cursor, участвуют в вычислении `filtered_count`.

### Идентичность процедуры

В рамках этой доработки имя процедуры не нормализуется. Значения `MyProc`, `dbo.MyProc` и `[dbo].[MyProc]` считаются разными именами, если именно так они извлечены из трейса. Это сохраняет parity с текущими данными и не требует функционального индекса по нормализованному имени.

### MCP helpers

Добавить:

```go
optionalIntSlice(args, "spids")
optionalStringSlice(args, "event_names")
```

Helpers должны:

- принимать только массивы соответствующего типа;
- применять строгую integer-валидацию к каждому SPID;
- возвращать ошибку с именем аргумента и индексом некорректного элемента;
- не игнорировать пустые строки, дробные значения и переполнение.

---

## F2. Постраничная выборка `codebase_trc_events`

### Контракт запроса

Добавить параметры:

| Параметр | Тип | Назначение |
|---|---|---|
| `after_id` | integer/int64 | непрозрачный keyset cursor предыдущей страницы |
| `spids` | integer[] | фильтр по нескольким SPID |
| `event_names` | string[] | фильтр по нескольким классам событий |
| `time_from` | RFC3339 string | начало интервала включительно |
| `time_to` | RFC3339 string | конец интервала исключительно |
| `min_duration_ms` | integer | минимальная длительность |
| `format` | `full` / `short` | представление события |

Существующие одиночные `spid` и `event_name` сохраняются только как совместимые алиасы. На границе MCP/CLI `spid=728` преобразуется в `SPIDs=[]int{728}`, а `event_name="SP:Completed"` — в `EventNames=[]string{"SP:Completed"}`. Новые клиенты могут всегда использовать массивы, включая массив из одного элемента. Максимальный `limit` одной страницы остаётся равным 1000.

### Контракт ответа

```json
{
  "events": [],
  "total_count": 44560,
  "filtered_count": 7842,
  "returned_count": 1000,
  "limit": 1000,
  "has_more": true,
  "next_after_id": 123456
}
```

Семантика:

- `total_count` — все события источника;
- `filtered_count` — полный размер набора после пользовательских фильтров, без `limit` и без cursor;
- `returned_count` — число событий текущей страницы;
- `has_more` — после текущей страницы существует хотя бы одно совпадающее событие;
- `next_after_id` — cursor последнего возвращённого события, отсутствует при `has_more=false` или пустой странице.

`has_more` определяется запросом `limit + 1`; дополнительная строка не включается в `events`.

### Saved-session

Добавить `StoreID`:

```go
type TRCEvent struct {
    StoreID int64 `json:"id,omitempty"`
    // существующие поля
}
```

DB cursor использует положительный `trc_events.id`:

```sql
SELECT id, event_class, event_name, procedure, duration_ms,
       params, columns, parent_id, depth
FROM trc_events
WHERE session_id = $1
  AND ($after_id_absent OR id > $after_id)
  -- пользовательские фильтры
ORDER BY id
LIMIT $limit_plus_one;
```

`scanEventRow` должен поддерживать проекцию с `id`, не изменяя tree-запросы, которым `StoreID` не требуется.

### File-mode

Для file-mode логический ID равен `EventIndex + 1`, то есть всегда положителен. Первый event имеет ID 1. `after_id=N` возвращает события с логическим ID больше N.

Cursor считается непрозрачным: клиент должен передавать `next_after_id` в следующий вызов без арифметики и без переноса между разными источниками. Исходный файл не должен изменяться между страницами; при изменении файла консистентность последовательности не гарантируется.

### Short format

Не обнулять `TRCEvent.Columns` в доменной модели. Ввести отдельное представление ответа:

```go
type TRCEventView struct {
    ID         int64      `json:"id,omitempty"`
    EventClass int        `json:"event_class"`
    EventName  string     `json:"event_name"`
    SPID       int        `json:"spid,omitempty"`
    Procedure  string     `json:"procedure,omitempty"`
    StartTime  *time.Time `json:"start_time,omitempty"`
    EndTime    *time.Time `json:"end_time,omitempty"`
    DurationMs int64      `json:"duration_ms"`
    Params     []TRCParam `json:"params,omitempty"`
    Columns    map[int]any `json:"columns,omitempty"`
}
```

- `full` возвращает `Params` и `Columns`;
- `short` возвращает идентификатор, класс, имя, SPID, процедуру, время и duration, но исключает `Params` и `Columns`;
- saved-session short-query не должен читать `columns JSONB`, кроме полей, которые нельзя получить из выделенных колонок таблицы;
- default — `full` для совместимости.

---

## F3. Расширение `codebase_trc_procedures`

### Параметры

```go
type ProceduresParams struct {
    Source      SessionSource
    SPIDs       []int
    EventScope  string
    EventNames  []string
    Top         int
    SortBy      string
    GroupBySPID bool
}
```

Один SPID передаётся как `SPIDs=[]int{728}`. У `codebase_trc_procedures` нет унаследованных scalar-фильтров, поэтому публичный контракт сразу получает только array-параметры `spids` и `event_names`; отдельные `spid`/`event_name` для него не вводятся.

### Event scope

| Значение | Фильтр | Назначение |
|---|---|---|
| `sp_completed` | `SP:Completed` | завершённые вызовы SQL-процедур; default |
| `rpc_completed` | `RPC:Completed` | завершённые RPC |
| `completed` | `SP:Completed`, `RPC:Completed`, `SQL:BatchCompleted` | диагностика завершённых единиц разных уровней |
| `all` | без event-фильтра | диагностика извлечения procedure |
| `custom` | обязательный `event_names` | явный набор событий |

Правила:

- default `sp_completed` совпадает с текущим поведением;
- `event_names` разрешён только при `event_scope=custom`;
- `custom` без непустого `event_names` является ошибкой;
- `completed` и `all` могут содержать перекрывающиеся elapsed durations разных уровней; ответ должен содержать предупреждение, что суммы нельзя трактовать как wall-clock time;
- отсутствие event scope не меняет текущий completed-only результат.

### Метрики и группировка

Расширить агрегат полем SPID:

```go
type TRCProcAgg struct {
    SPID       int     `json:"spid,omitempty"`
    Procedure  string  `json:"procedure"`
    Count      int64   `json:"count"`
    TotalMs    int64   `json:"total_ms"`
    MinMs      int64   `json:"min_ms"`
    MaxMs      int64   `json:"max_ms"`
    AvgMs      float64 `json:"avg_ms"`
    SourceFile string  `json:"source_file,omitempty"`
}
```

- `group_by_spid=false`: группировка только по procedure;
- `group_by_spid=true`: группировка по `(spid, procedure)`;
- `sort_by`: `total_ms` (default), `avg_ms`, `max_ms`, `count`;
- secondary sort всегда `procedure`, затем `spid`, чтобы результат был детерминирован;
- `top`: default 100, max 1000;
- `top` применяется глобально после агрегации;
- для Top-N отдельно в каждом SPID используется специализированный compare-инструмент.

Saved-session выполняет фильтрацию, группировку, сортировку и limit в PostgreSQL. File-mode применяет эквивалентные правила в Go.

---

## F4. Новый инструмент `codebase_trc_compare_procedures`

### Назначение

Один вызов отвечает на вопрос: какие Top-N процедуры самые дорогие в focus SPID и как те же процедуры выполнялись в peer SPID.

CLI:

```powershell
codebase trc compare-procedures --session 2 `
  --focus-spid 728 `
  --compare-spids 700,179,741,748,751 `
  --top 20 `
  --event-scope sp_completed `
  --sort total_ms
```

### Параметры

| Параметр | Обязательность | Назначение |
|---|---:|---|
| `session_id` / `file_path` | ровно один | источник событий |
| `focus_spid` | да | SPID, определяющий Top-N |
| `compare_spids` | да | непустой список peer SPID |
| `top` | нет | default 20, max 100 |
| `event_scope` | нет | default `sp_completed` |
| `event_names` | для `custom` | явный набор событий |
| `sort_by` | нет | `total_ms`, `avg_ms`, `max_ms`, `count` |

- `focus_spid` должен быть положительным;
- `compare_spids` дедуплицируется;
- focus удаляется из `compare_spids`;
- если после нормализации peer-список пуст, возвращается ошибка.

### Алгоритм

1. Применить event scope и выбрать только focus/peer SPID.
2. Агрегировать focus SPID по procedure.
3. Выбрать Top-N focus-процедур по `sort_by`, затем по procedure.
4. Для выбранных имён агрегировать каждый peer SPID.
5. Для отсутствующей пары `(spid, procedure)` вернуть `count=0`, `total_ms=0`, а `min_ms`, `max_ms`, `avg_ms` — `null`.
6. Рассчитать `peer_combined` по сырым peer-вызовам всех выбранных SPID.
7. Рассчитать ratios только при наличии ненулевого знаменателя.

### Модель метрик сравнения

```go
type ProcedureMetrics struct {
    Count   int64    `json:"count"`
    TotalMs int64    `json:"total_ms"`
    MinMs   *int64   `json:"min_ms"`
    MaxMs   *int64   `json:"max_ms"`
    AvgMs   *float64 `json:"avg_ms"`
}
```

Nullable-поля отличают отсутствие вызовов от валидного вызова с `duration_ms=0`.

### Ответ

```json
{
  "focus_spid": 728,
  "compare_spids": [700, 179, 741, 748, 751],
  "event_scope": "sp_completed",
  "sort_by": "total_ms",
  "top": 20,
  "procedures": [
    {
      "rank": 1,
      "procedure": "MassProtocolAccrual_Add",
      "focus": {
        "count": 5,
        "total_ms": 1581162,
        "min_ms": 310274,
        "max_ms": 320758,
        "avg_ms": 316232.4
      },
      "peers": [
        {
          "spid": 700,
          "count": 0,
          "total_ms": 0,
          "min_ms": null,
          "max_ms": null,
          "avg_ms": null
        }
      ],
      "peer_combined": {
        "count": 40,
        "total_ms": 120000,
        "min_ms": 0,
        "max_ms": 8000,
        "avg_ms": 3000
      },
      "ratios": {
        "avg_vs_peers": 105.4108,
        "max_vs_peers": 40.0948
      }
    }
  ],
  "warnings": [
    "Only completed events are included; unfinished calls are absent",
    "Elapsed totals include child execution time and are not wall-clock totals"
  ]
}
```

`peer_combined.avg_ms = sum(peer duration_ms) / sum(peer count)`, а не среднее от средних SPID.

```text
avg_vs_peers = focus.avg_ms / peer_combined.avg_ms
max_vs_peers = focus.max_ms / peer_combined.max_ms
```

При отсутствии peer-вызовов или нулевом знаменателе ratio равен `null`.

### Незавершённые вызовы

Инструмент не создаёт фиктивную длительность для Starting-событий без Completed. Если вызов не завершился до конца трейса, он отсутствует в completed-only рейтинге. Завершившиеся дочерние процедуры при этом могут присутствовать.

---

## F5. Новый инструмент `codebase_trc_spids`

### Назначение

Получить список SPID, объём и временные границы их активности без выгрузки всех событий.

### Параметры

- ровно один источник: `session_id` или `file_path`;
- `spids` — необязательный список;
- `time_from`, `time_to`;
- `sort_by`: `first_time`, `last_time`, `event_count`, `max_duration_ms`;
- `limit`: default 100, max 1000.

### Ответ

```json
{
  "spids": [
    {
      "spid": 728,
      "event_count": 7842,
      "first_time": "2026-09-14T13:56:50.107Z",
      "last_time": "2026-09-14T15:23:48.000Z",
      "sp_completed_count": 1234,
      "rpc_completed_count": 0,
      "batch_completed_count": 0,
      "error_count": 0,
      "max_duration_ms": 1027443,
      "application_name": "dcoOwn-Administrator",
      "login_name": "itgev",
      "host_name": "WS10-2404"
    }
  ],
  "warnings": [
    "Absence of BatchCompleted does not prove that a SPID is unfinished"
  ]
}
```

### Семантика

- `event_count` включает все события SPID после временного фильтра;
- `first_time`/`last_time` вычисляются по ненулевым `start_time`; если времени нет ни у одного события, возвращается `null`;
- `error_count` учитывает `error IS NOT NULL AND error <> 0`;
- `max_duration_ms` включает нулевые длительности;
- application/login/host выбираются как наиболее частые непустые значения;
- при равной частоте выбирается значение из самого раннего события по `id`/`EventIndex`;
- если непустых значений нет, поле отсутствует;
- отсутствие `SQL:BatchCompleted` не означает автоматически незавершённый поток;
- инструмент возвращает наблюдаемые факты и предупреждения, но не поле `unfinished=true/false`.

---

## F6. CLI и MCP контракты

### MCP

Расширить schemas существующих инструментов:

- `codebase_trc_events`;
- `codebase_trc_procedures`.

Зарегистрировать:

- `codebase_trc_compare_procedures`;
- `codebase_trc_spids`.

Добавить новые имена в `trcTools` и профиль `trc`. Каждый handler обязан проверять ошибки новых array/time/enum helpers до обращения к сервисному слою.

У `codebase_trc_events` уже существуют scalar-параметры `spid` и `event_name`; они сохраняются как legacy-алиасы, и handler сразу приводит их к каноническим массивам:

```text
spid=728                    -> spids=[728]
event_name="SP:Completed"  -> event_names=["SP:Completed"]
```

`codebase_trc_procedures` унаследованных фильтров не имеет и получает только array-параметры `spids`/`event_names`; scalar-алиасы для него не добавляются.

В сервисные types не добавляются параллельные scalar-поля. Если клиент одновременно передал scalar и array одной сущности, handler возвращает ошибку; неявное объединение не выполняется.

### CLI

Добавить флаги:

```text
trc events --spids --event-name --event-names \
  --after-id --time-from --time-to --min-duration-ms --format

trc procedures --spids --event-scope --event-names \
  --top --sort --group-by-spid

trc compare-procedures --focus-spid --compare-spids \
  --top --event-scope --event-names --sort

trc spids --spids --time-from --time-to --sort --limit
```

Существующие флаги `trc events` (`--spid`, `--proc`, `--limit`) сохраняются без изменений. `--spid` и добавляемый для parity с MCP `--event-name` являются одиночными алиасами только у `trc events`; `trc procedures` унаследованных фильтров не имеет и получает только `--spids`/`--event-names`. Канонические флаги `--spids` и `--event-names` принимают как одно, так и несколько значений; в новых сценариях рекомендуется использовать их всегда.

CSV-списки:

- запрещают пустые элементы;
- запрещают нечисловые, дробные, нулевые и отрицательные SPID;
- дедуплицируются с сохранением порядка;
- сообщают имя флага и некорректное значение.

Время CLI/MCP принимается только в RFC3339. Неявная локальная временная зона не используется.

---

## F7. Индексы и планы выполнения

Кандидаты:

```sql
CREATE INDEX IF NOT EXISTS idx_trc_events_session_cursor
    ON trc_events(session_id, id);

CREATE INDEX IF NOT EXISTS idx_trc_events_session_spid_id
    ON trc_events(session_id, spid, id);

CREATE INDEX IF NOT EXISTS idx_trc_events_spcompleted_spid_proc
    ON trc_events(session_id, spid, procedure)
    INCLUDE (duration_ms)
    WHERE event_name = 'SP:Completed'
      AND procedure IS NOT NULL
      AND procedure <> '';
```

Название `idx_trc_events_session_cursor` намеренно не совпадает с legacy-индексом `idx_trc_events_session_id`, который текущий `InitSchema` удаляет как избыточный.

Назначение:

- `(session_id, id)` — пагинация без SPID-фильтра;
- `(session_id, spid, id)` — пагинация одного или нескольких SPID;
- partial index — focus/peer агрегация completed-процедур без чтения `columns JSONB`.

Перед включением проверить `EXPLAIN (ANALYZE, BUFFERS)`:

1. события без SPID-фильтра;
2. события одного SPID;
3. события нескольких SPID;
4. Top-N focus SPID;
5. compare для 5–20 peer SPID;
6. малая сессия порядка 3K событий;
7. большая сессия не менее 500K событий.

Индекс включается только если план использует его в целевом запросе либо измерения подтверждают снижение чтений/времени. Существующие индексы не удаляются в рамках этой доработки.

---

## F8. Тестирование

### Unit tests

`internal/trc/aggregate_test.go`:

- группировка по `(spid, procedure)`;
- scope `rpc_completed`, `completed`, `all`, `custom`;
- Top/SortBy и детерминированные tie-breakers;
- Top-N compare определяется только focus SPID;
- отсутствующая peer-процедура возвращает nullable metrics;
- weighted `peer_combined.avg_ms`;
- ratio равен `null` при нулевом знаменателе.

`internal/trcsvc/runtime_test.go`:

- нормализация одиночных legacy-алиасов в массивы и ошибка при одновременной передаче обеих форм;
- RFC3339 и `time_from < time_to`;
- parity saved-session/file-mode для новых фильтров и агрегатов;
- file cursor использует положительный `EventIndex + 1`;
- short format сохраняет SPID, время, procedure и duration;
- focus удаляется из peer-списка;
- пустой peer-список после нормализации возвращает ошибку;
- completed-only warning присутствует в compare.

### PostgreSQL integration tests

`internal/trc/store_integration_test.go`:

- одиночный и множественный SPID;
- одиночный и множественный event name;
- временной полуинтервал `[time_from, time_to)`;
- `min_duration_ms`;
- страницы не пересекаются, не теряют события и сохраняют порядок;
- последняя страница возвращает `has_more=false`;
- `filtered_count` не зависит от cursor;
- Top-N строится по focus SPID, затем те же имена выбираются для peers;
- partial index проверяется планом на подходящем наборе данных.

### MCP/CLI tests

- новые инструменты зарегистрированы только в ожидаемых профилях;
- schemas содержат новые параметры и array types;
- неверные массивы, enum и timestamps возвращают ошибки;
- CLI CSV-списки валидируются;
- `codebase_read_more` остаётся только transport pagination;
- full/short JSON-контракты стабильны.

---

## Критерии приёмки

1. `codebase_trc_procedures` строит Top-N по одному/нескольким SPID и поддерживает per-SPID группировку.
2. Отсутствующий `event_scope` сохраняет текущую агрегацию `SP:Completed`.
3. `codebase_trc_compare_procedures` определяет Top-N только по focus SPID и возвращает те же процедуры для каждого peer SPID.
4. Отсутствие peer-вызова различается с валидной нулевой длительностью через nullable metrics.
5. `peer_combined.avg_ms` является взвешенным средним по всем peer-вызовам.
6. Последовательное чтение `codebase_trc_events` по `next_after_id` возвращает каждое совпадающее событие ровно один раз.
7. `filtered_count` остаётся полным размером отфильтрованного набора на каждой странице.
8. Full и short formats имеют одинаковые ID, порядок и число событий; short не содержит `columns`/`params`.
9. Saved-session выполняет фильтрацию, агрегацию и pagination server-side.
10. File-mode возвращает эквивалентный результат при неизменном исходном файле.
11. `codebase_trc_spids` возвращает наблюдаемые счётчики и временные границы без недоказанного статуса незавершённости.
12. Новые параметры CLI/MCP проходят строгую валидацию.
13. Существующие вызовы без новых параметров сохраняют совместимость.
14. Новые запросы проходят performance-проверку на малой и большой сессиях.
15. `go build ./...` и затронутые тесты проходят.

## Предлагаемый порядок реализации

1. F1 — общая модель фильтров и валидация.
2. F2 — keyset pagination и `TRCEventView`.
3. F3 — SPID/scope/top/sort для procedures.
4. F4 — compare-procedures.
5. F5 — SPID summary.
6. F6 — завершение CLI/MCP контрактов.
7. F7 — EXPLAIN и индексы после стабилизации SQL-запросов.
8. F8 — полный regression/integration набор.

Минимальный первый релиз, непосредственно решающий сравнение потоков: F1 + F3 + F4 + соответствующая часть F6/F8. Пагинацию событий и SPID summary можно реализовать отдельными OpenSpec changes, чтобы не связывать независимые пользовательские сценарии одной поставкой.

## Затрагиваемые файлы

| Файл | Изменение |
|---|---|
| `internal/trc/model.go` | `StoreID` и представление cursor metadata |
| `internal/trc/aggregate.go` | scope/SPID/top/sort и compare aggregation |
| `internal/trc/store.go` | расширенные фильтры, keyset pagination, procedures/compare/SPID SQL |
| `internal/trc/aggregate_test.go` | unit-тесты scope/group/compare |
| `internal/trc/store_integration_test.go` | SQL filters, pagination, compare и планы |
| `internal/trcsvc/types.go` | params/results/event view types |
| `internal/trcsvc/runtime.go` | saved-session/file-mode orchestration |
| `internal/trcsvc/runtime_test.go` | validation и parity |
| `internal/store/db_schema.go` | индексы, подтверждённые планами |
| `internal/mcp/registry.go` | schemas, array helpers и handlers |
| `internal/mcp/server_test.go` | registry/input schema validation |
| `cmd/trc.go` | flags и новые подкоманды |
| `cmd/trc_test.go` | CLI parsing и output contracts |
| `README.md` | пользовательская документация |

## Проверка

```powershell
go test ./internal/trc ./internal/trcsvc ./internal/mcp ./cmd -count=1
go test -tags=integration ./internal/trc -count=1
go build ./...
```

Ручной сценарий сравнения:

```json
{
  "session_id": 2,
  "focus_spid": 728,
  "compare_spids": [700, 179, 741, 748, 751],
  "top": 20,
  "event_scope": "sp_completed",
  "sort_by": "total_ms"
}
```

Проверить:

- Top-N определяется только статистикой focus SPID;
- для каждой процедуры присутствуют все peer SPID в исходном нормализованном порядке;
- агрегаты совпадают с контрольным `GROUP BY spid, procedure` для выбранного scope;
- отсутствующие peer-вызовы имеют `count=0`, `total_ms=0`, nullable min/max/avg;
- незавершённые вызовы не получают синтетическую длительность;
- объём ответа не зависит от выгрузки сырых `columns` всех событий.

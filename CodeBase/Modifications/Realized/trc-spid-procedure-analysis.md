# TRC: агрегация процедур по SPID, сравнение потоков и постраничная выборка событий

Расширить инструменты анализа SQL Server Profiler TRC так, чтобы средствами MCP/CLI можно было точно находить самые дорогие процедуры отдельного SPID, сравнивать их с аналогичными вызовами в параллельных потоках и последовательно выгружать все события, соответствующие фильтру.

## Контекст и воспроизводимый сценарий

При анализе TRC-сессии с 44 560 событиями потребовалось:

1. построить Top-20 процедур по суммарному времени для `SPID=728`;
2. для этих процедур сравнить `count`, `total`, `min`, `max`, `avg` с потоками `700, 179, 741, 748, 751`;
3. учитывать только завершения вызовов процедур и не задваивать длительности через соответствующие `SP:StmtCompleted`/`SQL:StmtCompleted`;
4. учитывать, что внешний batch или процедура могли не завершиться до окончания трейса.

Текущий публичный API не позволяет получить такой результат точно:

- `codebase_trc_procedures` агрегирует всю сессию без фильтра по SPID и типу события;
- `codebase_trc_events` фильтрует по одному SPID, но возвращает не более 1000 первых строк;
- `codebase_read_more` делит уже сформированный JSON на транспортные чанки и не выбирает следующие строки из `trc_events`;
- `codebase_trc_slow` не поддерживает SPID/event-фильтры и показывает отдельные события, а не агрегаты;
- `EventsResult.filtered_count` сейчас равен числу возвращённых строк после `LIMIT`, а не полному количеству строк, удовлетворяющих фильтру;
- текущая агрегация учитывает любое событие с непустым `procedure`, поэтому один вызов может одновременно попасть в статистику как `SP:Completed` и как statement-событие;
- in-memory и PostgreSQL-агрегации по-разному считают среднее при `duration_ms=0`.

## Подтверждение по исходникам

| Ограничение | Текущая реализация |
|---|---|
| Общий предел 1000 строк | `internal/trcsvc/runtime.go`: `normalizeLimit`, default 100, max 1000 |
| Нет курсора/offset | `internal/trc/store.go`: `LoadEventsFiltered` выполняет `ORDER BY id LIMIT $N` |
| Неверная семантика `filtered_count` | `internal/trcsvc/runtime.go`: `FilteredCount: len(events)` |
| Нет фильтров у агрегации | `internal/trc/store.go`: `LoadProceduresAggregated(ctx, db, sessionID)` фильтрует только по `session_id` и непустой `procedure` |
| Возможен двойной учёт | `LoadProceduresAggregated` и `AggregateByProcedure` не фильтруют `event_name`/`event_class` |
| Разная формула `avg` | SQL использует `avg(duration_ms) FILTER (WHERE duration_ms > 0)`, Go делит сумму положительных длительностей на количество всех событий |
| MCP-контракт без фильтров | `internal/mcp/registry.go`: `codebase_trc_procedures` принимает только `session_id`/`file_path` |

## Цели

1. Точная server-side агрегация процедур по одному или нескольким SPID.
2. Явная семантика того, какие события считаются вызовами процедур.
3. Один специализированный вызов для сравнения проблемного потока с параллельными потоками.
4. Настоящая постраничная выборка событий независимо от транспортной пагинации JSON.
5. Краткая сводка по SPID для поиска незавершённых/аномальных потоков.
6. Одинаковый результат для saved-session и file-mode.

## Не входит в объём

- повторный парсинг ранее сохранённых TRC-сессий;
- изменение бинарного/XML/XEL-парсера;
- автоматическое утверждение, что SPID является конкретным `Batch No`, если в трейсе нет соответствующего Starting/Completed-события;
- суммирование вложенных длительностей как «чистого времени» процедуры: `total_ms` остаётся суммой elapsed duration и может включать время дочерних вызовов.

---

## Фаза T1. Общая модель фильтров событий

### Затрагиваемые файлы

- `internal/trc/store.go`
- `internal/trcsvc/types.go`
- `internal/trcsvc/runtime.go`
- `internal/mcp/registry.go`
- `cmd/trc.go`

### Изменения `TRCEventFilter`

Расширить структуру:

```go
type TRCEventFilter struct {
    SPID       int
    SPIDs      []int
    Procedure  string
    EventName  string
    EventNames []string
    TimeFrom   *time.Time
    TimeTo     *time.Time
    MinDurationMs *int64
}
```

Правила:

- одновременно задавать `SPID` и `SPIDs` нельзя;
- `Procedure` — точное совпадение без учёта регистра либо сохранить текущее case-sensitive поведение и явно зафиксировать его в контракте; предпочтительно `LOWER(procedure)=LOWER($N)`;
- `TimeFrom` включительно фильтрует по `start_time`;
- `TimeTo` исключительно фильтрует по `start_time`;
- `EventNames` формирует `event_name = ANY($N)`;
- пустой фильтр сохраняет текущее поведение.

В MCP добавить корректно валидируемые array helpers:

```go
optionalIntSlice(args, "spids")
optionalStringSlice(args, "event_names")
```

Ошибочные типы аргументов должны возвращать ошибку, а не игнорироваться.

---

## Фаза T2. Настоящая пагинация `codebase_trc_events`

### Проблема

`codebase_read_more` продолжает чтение сериализованного ответа, но исходный запрос уже ограничен первыми 1000 строками. Поэтому ответ вида `filtered_count=1000` не сообщает, существуют ли следующие события.

### Контракт

Добавить параметры:

| Параметр | Тип | Назначение |
|---|---|---|
| `after_id` | integer/int64 | keyset cursor; вернуть строки с `trc_events.id > after_id` |
| `spids` | integer[] | фильтр по нескольким SPID |
| `event_names` | string[] | фильтр по нескольким классам событий |
| `time_from` | RFC3339 string | начало интервала, включительно |
| `time_to` | RFC3339 string | конец интервала, исключительно |
| `min_duration_ms` | integer | минимальная длительность |
| `format` | `full`/`short` | исключить тяжёлые `columns`, `params`, полный `TextData` в short-режиме |

Максимальный размер одной страницы оставить 1000.

### Ответ

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

- `total_count` — все события сессии;
- `filtered_count` — полный `COUNT(*)` после фильтров, без `LIMIT` и cursor;
- `returned_count` — длина текущей страницы;
- `has_more` — существует хотя бы одна следующая строка;
- `next_after_id` — ID последней строки страницы, отсутствует при `has_more=false`.

Для определения `has_more` основной запрос выбирает `limit+1` строку. Отдельный `COUNT(*)` нужен только для точного `filtered_count`.

### Модель

Добавить в `TRCEvent` поле идентификатора сохранённой строки:

```go
StoreID int64 `json:"id,omitempty"`
```

`scanEventRow` должен получать `id`. Нельзя использовать `EventIndex` как DB-cursor: при streaming insert он является индексом исходного события, а `id` уже является стабильным ключом сохранённой строки.

### SQL

```sql
SELECT id, event_class, event_name, procedure, duration_ms,
       params, columns, parent_id, depth
  FROM trc_events
 WHERE session_id = $1
   AND ($2 = 0 OR id > $2)
   -- остальные фильтры
 ORDER BY id
 LIMIT $N + 1
```

### File-mode

Для `file_path` использовать `EventIndex` как логический cursor и возвращать его в `next_after_id`. В документации ответа отметить, что cursor непрозрачен и должен только передаваться в следующий вызов, а не интерпретироваться клиентом.

---

## Фаза T3. Корректная агрегация `codebase_trc_procedures`

### Новый тип параметров

```go
type ProceduresParams struct {
    Source     SessionSource
    SPID       int
    SPIDs      []int
    EventScope string
    EventNames []string
    Top        int
    SortBy     string
    GroupBySPID bool
}
```

### `event_scope`

Поддержать значения:

| Значение | Фильтр | Назначение |
|---|---|---|
| `sp_completed` | `event_name = 'SP:Completed'` | завершённые вызовы SQL-процедур, рекомендуемый и новый default |
| `rpc_completed` | `event_name = 'RPC:Completed'` | завершённые RPC |
| `completed` | `event_name IN ('SP:Completed','RPC:Completed','SQL:BatchCompleted')` | завершённые единицы верхнего уровня |
| `all` | без event-фильтра | legacy-поведение для диагностики парсера |
| `custom` | `event_names` обязателен | явный набор событий |

`SP:StmtCompleted` и `SQL:StmtCompleted` не должны входить в `sp_completed`; это предотвращает двойной учёт вызова.

Изменение default с `all` на `sp_completed` является намеренной корректировкой: текущие поля называются `call count` и `procedure duration`, но фактически считают также statement-события. Для совместимости оставить `event_scope=all` и явно указать изменение семантики в release notes.

### Метрики

Для каждой группы вернуть:

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

Единая формула для SQL и Go:

- `count = count(*)`;
- `total_ms = sum(duration_ms)`;
- `min_ms = min(duration_ms)`;
- `max_ms = max(duration_ms)`;
- `avg_ms = avg(duration_ms)`.

Нулевые duration являются валидными событиями короче разрешения измерения и участвуют во всех метриках. Это устраняет текущую разницу между SQL и in-memory реализациями.

### Сортировка и ограничение

`sort_by`: `total_ms` (default), `avg_ms`, `max_ms`, `count`.

`top`: default 100, max 1000. Ограничение применяется после агрегации. При `group_by_spid=true` результат группируется по `(spid, procedure)`, а `top` применяется глобально; для сравнительного Top-N использовать специализированный инструмент T4.

### SQL

```sql
SELECT spid,
       procedure,
       count(*) AS count,
       COALESCE(sum(duration_ms), 0) AS total_ms,
       COALESCE(min(duration_ms), 0) AS min_ms,
       COALESCE(max(duration_ms), 0) AS max_ms,
       COALESCE(avg(duration_ms), 0) AS avg_ms
  FROM trc_events
 WHERE session_id = $1
   AND event_name = 'SP:Completed'
   AND procedure IS NOT NULL
   AND procedure <> ''
   AND spid = ANY($2)
 GROUP BY spid, procedure
 ORDER BY total_ms DESC
 LIMIT $3
```

Если `group_by_spid=false`, поле `spid` не выбирается и группировка остаётся только по procedure.

---

## Фаза T4. Новый инструмент `codebase_trc_compare_procedures`

### Назначение

Один server-side запрос должен отвечать на вопрос: «какие Top-N процедуры самые дорогие в проблемном SPID и как те же процедуры работают в остальных потоках?» Без выгрузки тысяч сырых событий клиенту.

Добавить инструмент в `trcTools`, MCP registry и CLI-команду:

```text
codebase trc compare-procedures --session 2 \
  --focus-spid 728 \
  --compare-spids 700,179,741,748,751 \
  --top 20 \
  --event-scope sp_completed \
  --sort total_ms
```

### MCP-параметры

| Параметр | Обязательность | Назначение |
|---|---:|---|
| `session_id` / `file_path` | один из двух | источник |
| `focus_spid` | да | SPID, по которому строится Top-N |
| `compare_spids` | да | список сравниваемых SPID |
| `top` | нет | default 20, max 100 |
| `event_scope` | нет | default `sp_completed` |
| `event_names` | для custom | пользовательский scope |
| `sort_by` | нет | `total_ms`, `avg_ms`, `max_ms`, `count` |

`focus_spid` не должен дублироваться в `compare_spids`; дубликаты в массиве нормализуются.

### Алгоритм server-side

1. Отфильтровать события по scope.
2. Агрегировать focus SPID по procedure.
3. Выбрать Top-N focus-процедур по `sort_by`, с детерминированным secondary sort `procedure`.
4. Для выбранных имён агрегировать все compare SPID.
5. Вернуть нулевые `count/total` и `null` для `min/max/avg`, если процедура в конкретном потоке не завершалась.
6. Отдельно вычислить объединённый peer aggregate по всем compare SPID.
7. Рассчитать коэффициенты только при ненулевом знаменателе.

### Рекомендуемый SQL-каркас

```sql
WITH filtered AS (
    SELECT spid, procedure, duration_ms
      FROM trc_events
     WHERE session_id = $1
       AND event_name = 'SP:Completed'
       AND procedure IS NOT NULL
       AND procedure <> ''
       AND spid = ANY($2)
), focus_top AS (
    SELECT procedure,
           count(*) AS count,
           sum(duration_ms) AS total_ms,
           min(duration_ms) AS min_ms,
           max(duration_ms) AS max_ms,
           avg(duration_ms) AS avg_ms
      FROM filtered
     WHERE spid = $3
     GROUP BY procedure
     ORDER BY total_ms DESC, procedure
     LIMIT $4
), per_spid AS (
    SELECT f.spid, f.procedure,
           count(*) AS count,
           sum(f.duration_ms) AS total_ms,
           min(f.duration_ms) AS min_ms,
           max(f.duration_ms) AS max_ms,
           avg(f.duration_ms) AS avg_ms
      FROM filtered f
      JOIN focus_top t USING (procedure)
     GROUP BY f.spid, f.procedure
)
SELECT ...;
```

Список `$2` должен включать focus и compare SPID.

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
      "focus": {"count": 5, "total_ms": 1581162, "min_ms": 310274, "max_ms": 320758, "avg_ms": 316232.4},
      "peers": [
        {"spid": 700, "count": 8, "total_ms": 0, "min_ms": 0, "max_ms": 0, "avg_ms": 0}
      ],
      "peer_combined": {"count": 40, "total_ms": 0, "min_ms": 0, "max_ms": 0, "avg_ms": 0},
      "ratios": {"avg_vs_peers": null, "max_vs_peers": null}
    }
  ],
  "warnings": [
    "Only completed events are included; unfinished calls are absent",
    "Elapsed totals include child execution time and must not be summed across nesting levels as wall-clock time"
  ]
}
```

Числа в примере иллюстративны; тесты не должны фиксировать значения из пользовательского трейса.

`peer_combined.avg_ms` должен быть взвешенным средним по всем peer-вызовам (`sum(duration)/sum(count)`), а не средним от средних SPID.

`avg_vs_peers = focus.avg_ms / peer_combined.avg_ms`, `max_vs_peers = focus.max_ms / peer_combined.max_ms`. При отсутствии peer-вызовов или нулевом знаменателе — `null`.

### Незавершённые вызовы

В ответе обязательно предупреждать: `SP:Completed` отсутствует для процедуры, не завершившейся до конца трейса. Поэтому внешний `FCD_CORE_MassAccrual_Start` зависшего потока может отсутствовать в рейтинге, хотя его дочерние завершившиеся вызовы присутствуют.

Не следует искусственно оценивать длительность незавершённого вызова как `trace_end - start_time` в этом инструменте. Для этого нужен отдельный анализ открытых Starting-событий, если они присутствуют в исходном трейсе.

---

## Фаза T5. Новый инструмент `codebase_trc_spids`

### Назначение

Быстро получить список потоков и временные границы активности, не выгружая все события. Это позволяет найти кандидат на отсутствующий `BatchCompleted`.

### Параметры

- `session_id` / `file_path`;
- `spids` — необязательный фильтр;
- `time_from`, `time_to`;
- `sort_by`: `first_time`, `last_time`, `event_count`, `max_duration_ms`;
- `limit`.

### Ответ на каждый SPID

```json
{
  "spid": 728,
  "event_count": 7842,
  "first_time": "2026-09-14T13:56:50.107",
  "last_time": "2026-09-14T15:23:48.000",
  "sp_completed_count": 1234,
  "batch_completed_count": 0,
  "error_count": 0,
  "max_duration_ms": 1027443,
  "application_name": "dcoOwn-Administrator",
  "login_name": "itgev",
  "host_name": "WS10-2404"
}
```

Не называть поток «незавершённым» только по `batch_completed_count=0`: профиль трейса мог не включать `SQL:BatchCompleted`, а сессия могла выполнять RPC. Возвращать факты и предупреждение, а не недоказанный статус.

---

## Фаза T6. Индексы

### Затрагиваемый файл

- `internal/store/db_schema.go`

Добавить:

```sql
CREATE INDEX IF NOT EXISTS idx_trc_events_session_spid_id
    ON trc_events(session_id, spid, id);

CREATE INDEX IF NOT EXISTS idx_trc_events_spcompleted_spid_proc
    ON trc_events(session_id, spid, procedure)
    INCLUDE (duration_ms)
    WHERE event_name = 'SP:Completed'
      AND procedure IS NOT NULL
      AND procedure <> '';
```

Первый индекс покрывает keyset pagination по SPID. Второй покрывает основной сценарий Top-N/compare без чтения `columns JSONB`.

Перед окончательным включением обоих индексов проверить планы через `EXPLAIN (ANALYZE, BUFFERS)` на малой и большой TRC-сессиях. Если существующий `idx_trc_events_session_spid` полностью становится избыточным, удалить его отдельной миграцией только после подтверждения через `pg_stat_user_indexes`; не удалять автоматически в рамках этой доработки.

---

## Фаза T7. CLI и MCP

### MCP registry

- расширить schema `codebase_trc_events`;
- расширить schema `codebase_trc_procedures`;
- зарегистрировать `codebase_trc_compare_procedures`;
- зарегистрировать `codebase_trc_spids`;
- добавить оба имени в whitelist `trcTools`;
- обрабатывать ошибки всех `optional*`/`required*` helpers.

### CLI

Добавить/расширить флаги:

```text
trc events --spid --spids --event-name --event-names --after-id --time-from --time-to --format
trc procedures --spid --spids --event-scope --event-names --top --sort --group-by-spid
trc compare-procedures --focus-spid --compare-spids --top --event-scope --sort
trc spids --spids --time-from --time-to --sort --limit
```

CSV-списки CLI должны валидироваться: пустые элементы, нечисловой SPID и отрицательные значения возвращают ошибку.

---

## Фаза T8. Тесты

### `internal/trc/aggregate_test.go`

- `TestAggregateByProcedure_EventScopeSPCompleted` — statement-события не задваивают вызовы;
- `TestAggregateByProcedure_ZeroDurationConsistency` — count/min/max/avg/total одинаковы для Go и SQL semantics;
- `TestAggregateByProcedure_GroupBySPID`;
- `TestCompareProcedures_TopFromFocusOnly`;
- `TestCompareProcedures_MissingPeerProcedure`;
- `TestCompareProcedures_WeightedPeerAverage`;
- `TestCompareProcedures_ZeroDenominatorRatioIsNull`;
- `TestCompareProcedures_DeterministicTieOrder`.

### `internal/trc/store_integration_test.go`

- фильтрация одним SPID и массивом SPID;
- фильтрация `event_scope=sp_completed`;
- Top-N строится по focus SPID, затем те же имена выбираются для peers;
- агрегация не ограничивается первыми 1000 событиями;
- `filtered_count` считается до `LIMIT`;
- keyset pages не пересекаются, не теряют строки и сохраняют порядок;
- последняя страница возвращает `has_more=false`;
- параллельная вставка новых сессий не влияет на cursor внутри заданного `session_id`.

### `internal/trcsvc/runtime_test.go`

- saved-session и file-mode возвращают одинаковые агрегаты;
- неверные `event_scope`, `sort_by`, RFC3339 и массивы аргументов дают явную ошибку;
- `focus_spid` отсутствует — ошибка;
- `focus_spid` удаляется из `compare_spids` при дублировании;
- warning о completed-only присутствует в compare-ответе.

### `internal/mcp/server_test.go`

- новые инструменты присутствуют в профиле `trc`;
- input schemas содержат массивы SPID и параметры пагинации;
- обязательные параметры compare валидируются;
- `codebase_read_more` по-прежнему работает как транспортная пагинация и не подменяет `next_after_id`.

---

## Критерии приёмки

1. Один вызов `codebase_trc_compare_procedures` возвращает Top-20 focus SPID и метрики тех же процедур для пяти peer SPID.
2. В scope `sp_completed` один фактический вызов не задваивается через `SP:StmtCompleted`/`SQL:StmtCompleted`.
3. Результат агрегации использует все подходящие события, даже если их больше 1000.
4. `filtered_count` показывает полный размер выборки, `returned_count` — размер страницы.
5. Последовательное чтение по `next_after_id` возвращает каждую подходящую строку ровно один раз.
6. SQL и in-memory режимы одинаково трактуют `duration_ms=0`.
7. Отсутствующая `SP:Completed` не превращается в фиктивную длительность; ответ содержит предупреждение о незавершённых вызовах.
8. Существующие сценарии `session_id` и `file_path` продолжают работать.
9. Новые запросы используют server-side фильтрацию/агрегацию и не загружают всю сессию в Go для saved-session.
10. `go build ./...` и перечисленные тесты проходят.

## Проверка

```powershell
go build ./...
go test ./internal/trc/... ./internal/trcsvc/... ./internal/mcp/... ./cmd/... -count=1
```

Ручной сценарий:

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

- Top-N определяется только статистикой SPID 728;
- для каждой процедуры присутствуют все пять peer SPID;
- суммы совпадают с контрольным SQL `GROUP BY spid, procedure` по `event_name='SP:Completed'`;
- внешний незавершённый вызов не появляется среди completed-вызовов;
- объём ответа не требует выгрузки сырых `columns` всех событий.

## Затрагиваемые файлы

| Файл | Изменение |
|---|---|
| `internal/trc/model.go` | `StoreID`/cursor metadata |
| `internal/trc/aggregate.go` | scope-aware и per-SPID агрегация, единая формула метрик |
| `internal/trc/store.go` | фильтры, count, keyset pagination, aggregate/compare/SPID summary queries |
| `internal/trc/aggregate_test.go` | unit-тесты агрегации и сравнения |
| `internal/trc/store_integration_test.go` | интеграционные тесты SQL и пагинации |
| `internal/trcsvc/types.go` | новые params/results types |
| `internal/trcsvc/runtime.go` | orchestration saved-session/file-mode |
| `internal/trcsvc/runtime_test.go` | parity и validation tests |
| `internal/store/db_schema.go` | составные/partial индексы |
| `internal/mcp/registry.go` | схемы и handlers MCP |
| `internal/mcp/server_test.go` | регистрация и schemas новых tools |
| `cmd/trc.go` | CLI flags и новые подкоманды |

## Порядок реализации

1. T1 — общая модель и валидация фильтров.
2. T3 — корректная агрегация существующего инструмента.
3. T4 — compare-procedures как основной пользовательский сценарий.
4. T2 — DB-row pagination и корректные counters.
5. T5 — SPID summary.
6. T6 — индексы после получения реальных планов.
7. T7 — завершение CLI/MCP контрактов.
8. T8 — полный набор unit/integration/regression tests.

При необходимости минимального первого релиза достаточно T3 + T4 + соответствующих тестов: это непосредственно разблокирует сравнение проблемного SPID с параллельными потоками. T2 и T5 можно выпустить следующей итерацией.
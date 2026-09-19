# Design: trc-existing-tools-filters

## Context

Концепция — `Modifications/trc-spid-procedure-analysis-features.md` (часть 1: существующие инструменты). Ключевые факты текущего кода, определяющие подход:

- `TRCEventFilter` — скалярные `SPID/Procedure/EventName` (`internal/trc/store.go:534`); `ExecuteEvents` выполняет два COUNT-запроса на вызов (`internal/trcsvc/runtime.go:119-123`).
- `TRCEvent` не содержит поля SPID: file-mode берёт SPID из `Columns[12]`, saved-session не SELECT-ит выделенную колонку `spid` вообще (в `trc_events` она есть, `db_schema.go:534`).
- `ExecuteProcedures` не принимает параметров; saved-session агрегирует в SQL (`LoadProceduresAggregated`), file-mode — в памяти (`AggregateByProcedure`, жёсткий `SP:Completed`, сортировка по `TotalMs`).
- CLI `trc events` имеет `--spid`, `--proc`, `--limit`; флага `--event-name` никогда не было. MCP `codebase_trc_events` имеет скалярные `spid`/`event_name`.
- `InitSchema` уже содержит паттерн удаления legacy-индексов (`db_schema.go:894-898`).

## Goals / Non-Goals

**Goals:**

- Server-side фильтрация (spids[], event_names[], время, длительность), keyset-пагинация и Top-N/сортировка для saved-session; эквивалентное поведение file-mode.
- Short-формат без чтения `params`/`columns` JSONB для saved-session.
- Строгая валидация с именем аргумента и индексом элемента в ошибках.
- Полная обратная совместимость вызовов без новых параметров.

**Non-Goals:**

- Новые инструменты `codebase_trc_compare_procedures`, `codebase_trc_spids` — часть 2.
- Cursor-индекс `(session_id, id)` и partial-индекс `SP:Completed` — не включены (полная выгрузка без фильтров считается нетипичным сценарием; агрегацию обслуживают существующие индексы).
- Scope `all` / enum `event_scope` — исключены (см. решение D5).
- Изменение транспортной пагинации `codebase_read_more`, нормализация имён процедур, расширение `codebase_trc_slow`.

## Decisions

### D1. Единая модель фильтра с указателями

`TRCEventFilter` переходит на `SPIDs []int`, `EventNames []string`, `TimeFrom/TimeTo *time.Time`, `MinDurationMs *int64`, `AfterID *int64`. Указатели отличают отсутствующее значение от нулевого; `AfterID` в фильтре, а не отдельным параметром — фильтр единой сущностью проходит через store API. Внутренняя модель всегда массивная, включая одиночные значения. Альтернатива — параллельные scalar-поля — отклонена: именно её меняем.

### D2. Keyset-пагинация по положительному id

Saved-session: курсор — положительный `trc_events.id` (BIGSERIAL PK), запрос `WHERE session_id=$1 AND (id > $cursor) ORDER BY id LIMIT $limit+1`; `has_more` определяется лишней строкой, не включаемой в ответ. File-mode: логический ID = `EventIndex + 1`. Курсор непрозрачен для клиента. Альтернатива OFFSET отклонена: цена skip-scan растёт с глубиной, стабильность не гарантируется.

### D3. Счётчики: `filtered_count` всегда, `total_count` только на первой странице

`filtered_count` вычисляется COUNT-запросом с теми же условиями без курсора и limit — на каждой странице (контракт стабильности). `total_count` вычисляется только при `AfterID == nil` и опускается (`omitempty`) на страницах продолжения: экономит один COUNT на страницу; `0` в JSON не возвращается, чтобы не читаться как «пустая сессия».

### D4. `StoreID` и `TRCEventView`

`TRCEvent` получает `StoreID int64` (`json:"id,omitempty"`) — id строки в БД; file-mode заполняет его как `EventIndex + 1`, обеспечивая одинаковые ID в full/short. Для ответа вводится `TRCEventView` (в `trcsvc`): short-проекция SELECT-ит только выделенные колонки (`id, event_class, event_name, spid, procedure, start_time, end_time, duration_ms`) — без `params`/`columns` JSONB; SPID берётся из выделенной колонки таблицы, а не из `Columns[12]`. Full-формат возвращает доменные `TRCEvent` как сейчас (включая `params`/`columns`) плюс `id`.

### D5. Один параметр `event_names` вместо enum `event_scope`

Enum-пресеты (`sp_completed`, `completed`, …) исключены: явный список имён прозрачнее для MCP-модели (видно, какие классы событий агрегируются, включая риск пересекающихся длительностей уровней SP/RPC/Batch), консистентен с `events` и не требует таблицы «значение → набор». Отсутствие `event_names` = `SP:Completed` — текущее поведение. Следствие: агрегация «по всем событиям без event-фильтра» невыразима — ниша закрыта `events` с фильтром по процедуре.

### D6. `top=0` по умолчанию, детерминированная сортировка

Отсутствие `top` возвращает все агрегаты (совместимость: сегодня лимита нет); явный `top` — 1..1000, применяется после агрегации. `sort_by`: `total_ms` (default), `avg_ms`, `max_ms`, `count`; secondary — `procedure`, затем `spid`. Результат детерминирован при равных метриках.

### D7. `group_by_spid` и NULL-spid

`group_by_spid=true` — группы `(spid, procedure)`, SQL добавляет `WHERE spid IS NOT NULL` (события без SPID исключаются); Go-агрегатор симметрично их пропускает. `false` — поведение как сейчас, без SPID-измерения. `TRCProcAgg` получает `SPID int` с `omitempty`: при `group_by_spid=false` поле отсутствует в JSON, при `true` все значения положительны (NULL исключены), коллизии с `omitempty` нет.

### D8. Legacy-алиасы только на границе MCP у events

MCP `codebase_trc_events` сохраняет `spid`/`event_name`: handler нормализует их в массивы из одного элемента; одновременная передача scalar+array — ошибка с предложением использовать массив. `codebase_trc_procedures` массивных фильтров раньше не имел — получает только `spids`/`event_names`, без scalar-алиасов. CLI: `--spid` сохраняется, добавляются `--spids` и `--event-names` (CSV/повторяемые), новый singular-флаг `--event-name` не вводится (его в CLI никогда не было; scalar-форма для новых контрактов не добавляется). В сервисных типах скалярных полей не остаётся.

### D9. Array-helpers со строгой валидацией

`optionalIntSlice(args, "spids")` и `optionalStringSlice(args, "event_names")` принимают только массивы соответствующего типа; каждый элемент валидируется (для SPID — целое > 0 без дробных частей и переполнения); ошибка содержит имя аргумента и индекс элемента; пустые строки запрещены. Дедупликация с сохранением порядка первого появления выполняется до сервисного слоя. Время — только RFC3339, `time_from < time_to` при обеих границах.

### D10. Замена индекса через новое имя + legacy-drop

PostgreSQL не умеет добавлять колонку в существующий btree, а `CREATE INDEX IF NOT EXISTS` под старым именем не обновит определение на существующих БД. Поэтому: создаётся `idx_trc_events_session_spid_id (session_id, spid, id)`, устаревший `idx_trc_events_session_spid` удаляется в `InitSchema` по существующему паттерну legacy-drop. Альтернатива — миграция с проверкой определения через `pg_index` — отклонена как избыточная сложность при готовом паттерне.

### D11. SQL-форма фильтров

Общие условия строятся в `buildEventFilterWhere`: `spid = ANY($n)`, `event_name = ANY($n)`, `start_time >= $n`, `start_time < $n`, `duration_ms >= $n`, `procedure = $n`. События с NULL `start_time` естественным образом выпадают из временного диапазона. Агрегация процедур: `WHERE <фильтры> AND event_name = ANY(...) AND procedure <> ''` + `GROUP BY procedure | (spid, procedure)` + `ORDER BY <metric> DESC, procedure, spid` + `LIMIT top` (при top > 0).

## Risks / Trade-offs

- [Пагинация без SPID-фильтра идёт без профильного индекса `(session_id, id)`] → принято: полная выгрузка нетипична; план проверяется EXPLAIN, при необходимости индекс добавляется отдельным изменением.
- [`spid = ANY(...)` при 5–20 SPID: bitmap-скан не даёт порядок по `id`, вероятен sort] → принято для размеров страницы ≤ 1000; проверяется интеграционным тестом на большой сессии.
- [COUNT на каждой странице на 500K+ сессиях] → index-only scan по новому индексу для SPID-фильтров; время измеряется интеграционным тестом; при деградации — ослабление контракта отдельным изменением.
- [Расхождение file-mode/saved-session: SPID из `Columns[12]` против выделенной колонки] → parity-тесты в `runtime_test.go` на одинаковых данных.
- [`total_count` исчезает на страницах продолжения — неожиданность для клиентов] → задокументировано в schema-description инструмента.
- [DROP legacy-индекса при каждом запуске InitSchema] → идемпотентно и дёшево, соответствует существующему паттерну.

## Migration Plan

Миграция данных не требуется: замена индекса выполняется идемпотентным `InitSchema` при первом запуске новой версии. Откат — вернуть прежнюю версию кода: лишний индекс `idx_trc_events_session_spid_id` безвреден.

## Open Questions

Нет — все решения зафиксированы при проработке (В1–В7, У1–У4 в истории exploration; отражены в Decisions).

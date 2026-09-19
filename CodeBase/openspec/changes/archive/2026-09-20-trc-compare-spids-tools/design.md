# Design: trc-compare-spids-tools

## Context

Часть 1 (архив `2026-09-19-trc-existing-tools-filters`) реализовала единую модель фильтров (`TRCEventFilter` с массивами/указателями), keyset-пагинацию, серверную агрегацию `LoadProceduresAggregated(opts)` с группировкой `(spid, procedure)`, строгую валидацию в `trcsvc/params.go` и MCP-helpers (`optionalIntSlice`, `optionalStringSlice`, `optionalRFC3339`, `rejectScalarArrayConflict`). Часть 2 надстраивает два целевых инструмента концепции; все решения ниже зафиксированы при проработке (В1–В7) и проверены на реальных данных: nbki (547K событий, **без колонки Duration** — все `duration_ms=0`) и DIAPR-391 (с длительностями). Модель уже умеет собирать сравнение вручную (два вызова `procedures` + клиентский join) — часть 2 устраняет ручную сшивку и арифметику.

## Goals / Non-Goals

**Goals:**

- `codebase_trc_compare_procedures`: один вызов = Top-N focus + те же процедуры для каждого peer + взвешенный `peer_combined` + ratios, с гарантией покрытия и явной nullable-семантикой.
- `codebase_trc_spids`: обзор ландшафта трейса (потоки, счётчики, границы, ошибки, идентичность) без выгрузки сырых событий.
- Устойчивость к трейсам без Duration: предупреждение + рабочий путь через count-метрики.
- Эквивалентность saved-session (SQL) и file-mode (память).

**Non-Goals:**

- Изменение `codebase_trc_parse` — parse остаётся чистым, `spids` — единственная точка обзора (В6).
- Новые индексы без подтверждения планами; persist header трейса; транспортная пагинация; расширение `trc slow`; вычисление «чистого» времени процедуры без дочерних вызовов (всё — из концепции, вне объёма).

## Decisions

### D1. Event-фильтр: только `event_names[]` (В1)

Enum `event_scope` концепции мёртв по решению У1 части 1 — compare и spids-инструмент наследуют явный `event_names` (default `SP:Completed` для compare; сводка SPID не фильтруется по классам — её счётчики фиксированы). Консистентность трёх инструментов (`events`/`procedures`/`compare`) для MCP-модели важнее компактности enum.

### D2. Устойчивость к трейсам без Duration (В2 = b+c)

Тест nbki показал: трейс может быть снят без колонки Duration (547K событий, все `duration_ms=0`). Решение двухчастное:

- **Эмпирическое предупреждение**: `max(duration_ms) = 0` по всем событиям источника → `warnings` содержит «метрики длительности недоступны». Проверка — один SQL `SELECT max(duration_ms)` (или проход в file-mode); не требует persist header и работает для сохранённых сессий. Ложное срабатывание (колонка есть, но все нули) неразличимо и практически эквивалентно.
- **`count_vs_peers` ratio**: `focus.count / peer_combined.count` — третий ratio рядом с `avg_vs_peers` и `max_vs_peers`; на трейсах без Duration сравнение по числу вызовов остаётся рабочим (продемонстрировано на nbki: `sort_by=count`).

### D3. `top` в compare: default 20, max 100 (В3)

Ответ compare растёт как `процедуры × peers`: 20×5 = 100 peer-блоков — разумный дефолт; max 100 ограничивает деградацию при десятках peers. Отличается от `procedures` (default 0 = все, max 1000) осознанно: у compare нет сценария «верни всё», его смысл — диагностический топ.

### D4. Мода app/login/host с tie-break (В4)

`application_name`/`login_name`/`host_name` = самое частое непустое значение; при равной частоте — из самого раннего события (`id`/`EventIndex`). SQL: `mode() WITHIN GROUP` не даёт детерминированного tie-break по раннему id — используется паттерн `(value, count, min(id))` через `ORDER BY count DESC, min_id ASC LIMIT 1` в LATERAL/FILTER-агрегации по трём полям одним проходом; file-mode — map с аккумулятором `(value → (count, firstIndex))`. Семантика концепции сохранена: представительное и детерминированное значение.

### D5. `sort_by` сводки SPID: + `error_count` (В5)

К концепционному набору (`first_time`, `last_time`, `event_count`, `max_duration_ms`) добавлен `error_count` — «какой поток сыплет ошибками» — дешёвый `COUNT(*) FILTER (error IS NOT NULL AND error <> 0)` и частый первый диагностический вопрос. NULL-границы времени сортируются последними (факты без времени — в хвосте).

### D6. Алгоритм compare (наследование концепции + части 1)

1. Применить `event_names` и выбрать события focus + peers.
2. Агрегировать focus по procedure; Top-N по `sort_by` (tie-break: `procedure`, затем `spid` — как в части 1).
3. Для выбранных имён агрегировать каждый peer по отдельности — saved-session одним SQL с `GROUP BY (spid, procedure)` по фильтру `procedure = ANY($topNames)` (переиспользуются индексы `idx_trc_events_session_spid_id` и `idx_trc_events_session_proc`).
4. Отсутствующая пара → `count=0`, `total_ms=0`, `min/max/avg=null` (nullable-типы, отличимо от нуля).
5. `peer_combined` по сырым peer-событиям выбранных процедур: `sum(duration)/sum(count)` — взвешенный, не среднее средних (защита продемонстрирована сценарием 10×100 + 30×200 = 175).
6. Ratios: `avg_vs_peers`, `max_vs_peers`, `count_vs_peers`; `null` при нулевом знаменателе.
7. `rank` — позиция в Top-N focus.

Незавершённые вызовы (Starting без Completed) не получают синтетической длительности; Statement-события не задваивают вызов при default scope.

### D7. SQL-форма сводки SPID

Saved-session — один `GROUP BY spid` c `COUNT(*) FILTER` по `event_name` и ошибке, `min/max(start_time)` (ненулевые — NULL во времени у события уже означает отсутствие), `max(duration_ms)` и агрегация моды трёх identity-полей. Временной фильтр — по `start_time` (события без времени выпадают из отфильтрованной сводки, как в модели фильтров части 1). `spids`-фильтр — `spid = ANY`. File-mode — эквивалентный проход по `Columns[12]`/`Columns[31]`/`Columns[14]`/`Columns[10,11,8]`.

### D8. Контракты CLI/MCP — по паттернам части 1

MCP: два новых инструмента в `trcTools` и профиле `trc`; только array-параметры (`compare_spids`, `spids`, `event_names`), без scalar-алиасов — у них нет legacy; `focus_spid` — скаляр по природе. CLI: `trc compare-procedures --focus-spid N --compare-spids A,B,C --top --event-names --sort`, `trc spids --spids --time-from --time-to --sort --limit` (CSV-валидация `parseSPIDCSV`/`parseStringCSV`, RFC3339 — `parseRFC3339Flag`). Ошибки валидации — с именем параметра и индексом элемента.

### D9. Индексы: EXPLAIN-гейт без новых индексов по умолчанию

Целевые запросы сравнения (`GROUP BY (spid, procedure)` по `event_name = ANY` + `procedure = ANY`) и сводки (`GROUP BY spid`) обслуживаются существующими `idx_trc_events_session_spid_id (session_id, spid, id)` и `idx_trc_events_session_proc (session_id, procedure)`. Кандидат концепции — partial-индекс `SP:Completed` — переоценивается EXPLAIN'ом на реальных сессиях (nbki 547K); включается только при подтверждении планами, отдельным решением.

## Risks / Trade-offs

- [Compare на трейсах без Duration бесполезен для duration-метрик] → предупреждение D2 + `count_vs_peers` как рабочий путь; `sort_by=count` даёт осмысленный Top-N.
- [Ответ compare при top=100 × 20 peers = 2000 метрических блоков] → max top=100 и ограничение практически покрывают сценарий; транспортная пагинация MCP чанкует ответ при необходимости.
- [Мода identity-полей — три подзапроса на SPID] → один проход LATERAL по всем SPID; file-mode — O(n); на 547K событий приемлемо (проверяется интеграционным тестом на nbki-объёмах).
- [Расхождение saved/file] → parity-тесты на синтетике и DIAPR-391 (как в части 1).
- [False-positive предупреждения «длительности недоступны» при фактически нулевых длительностях] → принято: случаи неразличимы и семантически эквивалентны.

## Migration Plan

Миграция не требуется: новые инструменты и таблицы не затрагивают схему (индексы — только при EXPLAIN-подтверждении, отдельным решением). Откат — возврат кода; инструменты просто исчезают из реестра.

## Open Questions

Нет — В1–В7 зафиксированы при проработке; details алгоритма наследуются из концепции (F4/F5) и решений части 1.

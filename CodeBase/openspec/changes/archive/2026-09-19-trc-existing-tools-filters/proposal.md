# Proposal: trc-existing-tools-filters

## Why

Сценарий «сравнить проблемный SPID с параллельными потоками» (концепция `Modifications/trc-spid-procedure-analysis-features.md`, часть 1) требует server-side фильтрации и агрегации по списку SPID, набору классов событий, временному диапазону и последовательной выгрузки событий страницами. Текущие инструменты этого не дают: `trc events` фильтрует только скалярным SPID/имени события без курсора и читает `columns` JSONB даже для аналитических выгрузок; `trc procedures` жёстко агрегирует `SP:Completed` без фильтров, Top-N, сортировки и SPID-измерения. MCP-клиенты упираются в транспортную пагинацию и избыточный объём ответа.

## What Changes

- **Единая модель фильтров** `TRCEventFilter`: `SPIDs []int`, `EventNames []string`, `TimeFrom`/`TimeTo *time.Time`, `MinDurationMs *int64`, `AfterID *int64`. Валидация: положительные SPID, дедупликация с сохранением порядка, запрет пустых строк, полуинтервал `[time_from; time_to)`, `time_from < time_to`, только RFC3339, `MinDurationMs >= 0`.
- **`trc events` / `codebase_trc_events`**: новые параметры `spids[]`, `event_names[]`, `time_from`, `time_to`, `min_duration_ms`, `after_id`, `format=full|short`. Keyset-пагинация: ответ дополняется `has_more` и `next_after_id` (запрос `limit+1`); `filtered_count` возвращается на каждой странице, `total_count` — только на первой (`after_id` отсутствует), на страницах продолжения поле в JSON отсутствует. Short-формат исключает `params`/`columns`; short-запрос saved-session не читает JSONB-колонки (SPID/времена берутся из выделенных колонок таблицы). File-mode курсор — `EventIndex + 1`.
- **Legacy-алиасы**: MCP `spid` и `event_name` для `codebase_trc_events` сохраняются и нормализуются в массивы; одновременная передача scalar и array — ошибка. CLI `--spid` сохраняется, `--spids`/`--event-names` — массивы; новый singular-флаг `--event-name` в CLI не вводится.
- **`trc procedures` / `codebase_trc_procedures`**: новые параметры `spids[]`, `event_names[]` (отсутствие = `SP:Completed`, как сейчас), `top` (0 = все, default 0; при установке 1..1000), `sort_by` (`total_ms` default, `avg_ms`, `max_ms`, `count`; secondary sort `procedure`, затем `spid`), `group_by_spid` (default false; true — группы `(spid, procedure)`, события с `spid IS NULL` исключаются; при false — поведение как сейчас). Пустое имя процедуры исключается из агрегата, как сейчас.
- **Server-side**: saved-session выполняет фильтрацию, группировку, сортировку и top в PostgreSQL; file-mode применяет эквивалентные правила в памяти.
- **Индекс**: новый `idx_trc_events_session_spid_id (session_id, spid, id)` заменяет `idx_trc_events_session_spid` по существующему паттерну legacy-drop в `InitSchema`.
- **Вне объёма (часть 2)**: инструменты `codebase_trc_compare_procedures` и `codebase_trc_spids`; cursor-индекс `(session_id, id)`; partial-индекс `SP:Completed`; scope `all` (диагностика извлечения procedure закрывается `events` с фильтром по процедуре); расширение `codebase_trc_slow`; изменение транспортной пагинации `codebase_read_more`.

## Capabilities

### New Capabilities

(нет — все изменения в существующих возможностях)

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: расширенная модель фильтров и валидация; списку событий добавляются массивы SPID/событий, временной диапазон, минимальная длительность, keyset-пагинация и short-формат; агрегация по процедурам получает `event_names`, `spids`, `top`, `sort_by`, `group_by_spid`; серверная агрегация выполняет новые фильтры/сортировку/top в SQL.
- `mcp-server/mcp-transport-tools`: схемы `codebase_trc_events`/`codebase_trc_procedures` расширяются array/time/cursor/format-параметрами; строгие array-helpers; legacy scalar-алиасы только у events с ошибкой при конфликте с массивом; CLI-флаги по тем же контрактам.
- `infrastructure/database-schema`: индекс `idx_trc_events_session_spid` заменяется на `idx_trc_events_session_spid_id (session_id, spid, id)`.

## Impact

- `internal/trc/model.go` — `StoreID` в `TRCEvent`
- `internal/trc/store.go` — расширенный `TRCEventFilter`, keyset-запрос событий, short-проекция, агрегация с фильтрами/сортировкой/top в SQL
- `internal/trc/aggregate.go` — группировка `(spid, procedure)`, `event_names`, top/sort в file-mode
- `internal/trcsvc/types.go`, `internal/trcsvc/runtime.go` — параметры, `TRCEventView`, оркестрация saved-session/file-mode, валидация
- `internal/mcp/registry.go` — схемы, helpers `optionalIntSlice`/`optionalStringSlice`, нормализация legacy-алиасов
- `cmd/trc.go` — флаги `trc events`/`trc procedures`
- `internal/store/db_schema.go` — замена индекса
- Тесты: `aggregate_test.go`, `store_integration_test.go`, `runtime_test.go`, `server_test.go`, `cmd/trc_test.go`; README
- Совместимость: вызовы без новых параметров не меняют поведение (`top` default 0 = все процедуры, отсутствие `event_names` = `SP:Completed`, `total_count` на первой странице как раньше)

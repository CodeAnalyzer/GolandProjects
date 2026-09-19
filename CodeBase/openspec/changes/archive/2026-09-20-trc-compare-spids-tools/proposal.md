# Proposal: trc-compare-spids-tools

## Why

Целевой сценарий концепции `Modifications/trc-spid-procedure-analysis-features.md` (часть 2): «определить Top-N дорогих процедур проблемного SPID и сравнить их с теми же процедурами в параллельных потоках». Часть 1 (архив `2026-09-19-trc-existing-tools-filters`) дала фильтры и агрегаты, но сравнение сегодня модель собирает вручную: два вызова `procedures`, клиентский join до 100 строк, арифметика взвешенного среднего и ratios «в голове» — расход контекста и место для ошибок (классическая ловушка «среднее средних»). Сводка активности SPID («какие потоки есть, временные границы, ошибки, длительности») не получается вообще никак без выгрузки сырых событий: мы зондировали nbki-трейс выгрузкой 1000 событий с группировкой.

## What Changes

- **Новый инструмент `codebase_trc_compare_procedures` / `trc compare-procedures`**: один вызов отвечает «какие Top-N процедур самые дорогие в focus SPID и как те же процедуры выполнялись в peer SPID». Top-N определяется только статистикой focus; для каждой процедуры возвращаются метрики каждого peer; отсутствующая пара `(spid, procedure)` — `count=0`, `total_ms=0`, nullable `min/max/avg` (отличимо от валидного нуля); `peer_combined` — взвешенное среднее по всем peer-вызовам (`sum/sum`, не среднее средних); ratios `avg_vs_peers`, `max_vs_peers`, `count_vs_peers` — `null` при нулевом знаменателе. Параметры: `focus_spid`, `compare_spids[]` (дедупликация, focus удаляется, пусто после нормализации — ошибка), `event_names[]` (default `SP:Completed`, без enum `event_scope`), `top` (default 20, max 100), `sort_by` (`total_ms` default, `avg_ms`, `max_ms`, `count`; secondary `procedure`, затем `spid`).
- **Предупреждения в ответе compare**: completed-only (незавершённые вызовы отсутствуют), elapsed ≠ wall-clock (длительности включают дочерние вызовы), «метрики длительности недоступны» — эмпирическая проверка `max(duration_ms) = 0` по сессии (трейсы, снятые без колонки Duration, как nbki: count-метрики и `count_vs_peers` остаются рабочим путём).
- **Новый инструмент `codebase_trc_spids` / `trc spids`**: сводка активности по SPID без выгрузки сырых событий: `event_count`, `first_time`/`last_time` (по ненулевым `start_time`), счётчики `sp_completed_count`/`rpc_completed_count`/`batch_completed_count`, `error_count`, `max_duration_ms`, `application_name`/`login_name`/`host_name` — самое частое непустое значение с tie-break по самому раннему событию. Параметры: `spids[]` (опционально), `time_from`/`time_to`, `sort_by` (`first_time`, `last_time`, `event_count`, `max_duration_ms`, `error_count`), `limit` (default 100, max 1000). Инструмент возвращает наблюдаемые факты и предупреждения, но не вычисленный статус «незавершённости» потока.
- **Наследуется из части 1 без изменений**: массивная модель фильтров и валидация (положительные SPID, дедупликация, запрет пустых строк, RFC3339, ошибки с именем аргумента и индексом элемента), helpers `optionalIntSlice`/`optionalStringSlice`, nullable-семантика, запрет синтетической длительности незавершённых вызовов.
- **Saved-session** выполняет агрегации серверно в PostgreSQL; **file-mode** — эквивалентно в памяти (SPID из `Columns[12]`, ошибки из `Columns[31]`).
- **Вне объёма**: изменения `codebase_trc_parse` (parse остаётся чистым — `spids` единственная точка обзора), новые индексы без подтверждения планами (EXPLAIN-гейт как в части 1), транспортная пагинация, новые фильтры `codebase_trc_slow`, persist header трейса.

## Capabilities

### New Capabilities

(нет — новые инструменты относятся к существующей возможности агрегации TRC-анализа)

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: ADDED «Сравнение процедур focus/peer SPID» (Top-N по focus, nullable peer-метрики, взвешенный peer_combined, ratios включая count_vs_peers, предупреждения включая недоступность длительностей) и ADDED «Сводка активности SPID» (счётчики, временные границы, мода app/login/host, сортировка включая error_count, факты без статуса незавершённости).
- `mcp-server/mcp-transport-tools`: MODIFIED «TRC инструменты» — в перечень добавляются `codebase_trc_compare_procedures` и `codebase_trc_spids` с контрактами массивных параметров и предупреждений.

## Impact

- `internal/trc/aggregate.go` — compare-агрегация (Top-N focus, peer-метрики, peer_combined, ratios) и сводка SPID для file-mode
- `internal/trc/store.go` — серверные SQL-агрегации compare и SPID summary (GROUP BY + COUNT FILTER + mode с tie-break)
- `internal/trcsvc/types.go`, `params.go`, `runtime.go` — параметры, результаты, оркестрация saved/file, предупреждение о недоступности длительностей
- `internal/mcp/registry.go` — регистрация двух инструментов (схемы, handlers), профиль `trc`
- `cmd/trc.go` — подкоманды `trc compare-procedures` и `trc spids`
- Тесты: `aggregate_test.go`, `store_integration_test.go`, `runtime_test.go`, `server_test.go`, `cmd/trc_test.go`; README
- Совместимость: существующие инструменты не меняются; новых индексов нет до подтверждения EXPLAIN на реальных сессиях

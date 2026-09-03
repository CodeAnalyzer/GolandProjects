## Why

Построение дерева вызовов TRC опирается исключительно на пары событий Starting/Completed для восстановления вложенности. Когда трейс содержит только Completed-события (частая конфигурация SQL Profiler для снижения overhead), алгоритм выдаёт плоский список корневых узлов без вложенности — даже если временные интервалы событий явно показывают отношения родитель-потомок.

## What Changes

- Добавить fallback-алгоритм вложенности для трейсов без Starting-событий: восстановление parent-child по временным интервалам (событие B — потомок A, если интервал B полностью вложен в интервал A, и EventClass B «глубже» EventClass A)
- Применить fallback в трёх местах: `IncrementalParentTracker` (стриминг-парсинг), `ComputeParentIDs` (постфактум в памяти), `LoadEventsForTree` (серверный SQL CTE)
- Существующий алгоритм Starting/Completed остаётся основным путём; time-overlap fallback активируется только при нулевом количестве Starting-событий для данного SPID
- Иерархия глубины EventClass для fallback: `RPC:Completed` (11) > `SP:Completed` (43) > `SP:StmtCompleted` (45) > `SQL:BatchCompleted` (12) > `SQL:StmtCompleted` (41)

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: добавляется fallback-режим вложенности для Completed-only трейсов — по временным интервалам и иерархии EventClass

## Impact

- `internal/trc/tree.go` — `buildSPIDTree` и `ComputeParentIDs`: детект Completed-only режима per-SPID, переключение на interval-containment nesting
- `internal/trc/parent_tracker.go` — `IncrementalParentTracker`: детект Completed-only режима, переключение на interval-stack nesting при стриминг-парсинге
- `internal/trc/store.go` — `LoadEventsForTree`: альтернативный путь для Completed-only трейсов (SQL-level interval nesting или Go-level постобработка CTE-результатов)
- `internal/trc/tree_test.go` — новые тесты Completed-only вложенности
- `internal/trc/parent_tracker_test.go` — новые тесты стриминг Completed-only вложенности
- Без изменений схемы БД (колонки parent_id/depth те же)
- Без изменений CLI/MCP API (те же флаги, тот же формат вывода)

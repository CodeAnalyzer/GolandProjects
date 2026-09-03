## ADDED Requirements

### Requirement: Fallback-вложенность для Completed-only трейсов

Система SHALL предоставлять fallback-алгоритм восстановления вложенности вызовов для трейсов, содержащих только Completed-события (без парных Starting). Алгоритм SHALL применять временное вложение интервалов строго в рамках одного SPID: событие B считается потомком события A, только если A и B принадлежат одному SPID, интервал B полностью вложен в интервал A (start_B >= start_A AND end_B <= end_A), и EventClass B находится ниже A в иерархии глубины: `RPC:Completed` (11) > `SP:Completed` (43) > `SP:StmtCompleted` (45) > `SQL:BatchCompleted` (12) > `SQL:StmtCompleted` (41). События из разных SPID SHALL NEVER образовывать отношение родитель-потомок, даже если их временные интервалы перекрываются.

Fallback SHALL активироваться только для SPID, у которых отсутствуют Starting-события (zero `*Starting` events). Для SPID, содержащих хотя бы одно Starting-событие, SHALL применяться существующий алгоритм Starting/Completed пар без изменений.

Fallback SHALL применяться в трёх компонентах:
- `IncrementalParentTracker` (`parent_tracker.go`) — при стриминг-парсинге: отслеживание интервального стека вместо Starting/Completed стека
- `ComputeParentIDs` (`tree.go`) — при файловом режиме: постфактум вычисление ParentID/Depth по интервалам
- `LoadEventsForTree` (`store.go`) — при серверном режиме: загрузка событий с parent_id, вычисленными при парсинге, и построение дерева через recursive CTE без изменений (parent_id уже корректен)

#### Scenario: Completed-only трейс с временной вложенностью

- **GIVEN** .trc файл с SPID 122, содержащий только `SP:Completed` для `ProcA` (09:42:55–09:43:24) и `SP:StmtCompleted` для `MassDoc_Add` (09:42:55–09:43:24), без Starting-событий
- **WHEN** выполняется `codebase trc tree file.trc --spid 122`
- **THEN** `SP:StmtCompleted` для `MassDoc_Add` является дочерним узлом `SP:Completed` для `ProcA`
- **AND** `ProcA` — корневой узел с `Depth = 0`
- **AND** `MassDoc_Add` — дочерний узел с `Depth = 1`

#### Scenario: Fallback не активируется при наличии Starting-событий

- **GIVEN** .trc файл с SPID 55, содержащий как Starting, так и Completed события
- **WHEN** выполняется `codebase trc tree file.trc --spid 55`
- **THEN** применяется существующий алгоритм Starting/Completed пар
- **AND** fallback-алгоритм по интервалам не используется

#### Scenario: Несколько уровней вложенности в Completed-only трейсе

- **GIVEN** .trc файл с SPID 88, содержащий `RPC:Completed` (10:00–10:05), `SP:Completed` (10:01–10:04), `SP:StmtCompleted` (10:02–10:03), без Starting-событий
- **WHEN** выполняется `codebase trc tree file.trc --spid 88`
- **THEN** дерево имеет 3 уровня: `RPC:Completed` (root) → `SP:Completed` (child) → `SP:StmtCompleted` (grandchild)

#### Scenario: Перекрывающиеся, но не вложенные интервалы — siblings

- **GIVEN** .trc файл с SPID 77, содержащий `SP:Completed` для `ProcA` (10:00–10:03) и `SP:Completed` для `ProcB` (10:01–10:04), без Starting-событий
- **WHEN** выполняется `codebase trc tree file.trc --spid 77`
- **THEN** `ProcA` и `ProcB` — оба корневые узлы (siblings), т.к. ни один интервал не вложен в другой

#### Scenario: Fallback при серверном режиме (session_id > 0)

- **GIVEN** сохранённая TRC-сессия, SPID 122 содержит только Completed-события, parent_id вычислен через interval-nesting при парсинге
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `session_id` и `spid=122`
- **THEN** recursive CTE `LoadEventsForTree` строит дерево по parent_id из БД (без модификации CTE)
- **AND** вложенность восстановлена корректно

#### Scenario: Fallback с фильтром по процедуре

- **GIVEN** .trc файл с SPID 122, Completed-only, `ProcA` содержит вложенные `SP:StmtCompleted` для `MassDoc_Add`
- **WHEN** выполняется `codebase trc tree file.trc --proc ProcA --spid 122`
- **THEN** возвращено поддерево `ProcA` с дочерними `SP:StmtCompleted` узлами

#### Scenario: EventClass одинакового уровня — siblings

- **GIVEN** .trc файл с SPID 99, содержащий два `SP:Completed` события с одинаковым интервалом (10:00–10:05)
- **WHEN** выполняется `codebase trc tree file.trc --spid 99`
- **THEN** оба события — корневые узлы (siblings), т.к. EventClass одинаковый — нет отношения parent-child

#### Scenario: События из разных SPID не образуют parent-child

- **GIVEN** .trc файл с SPID 100 (`SP:Completed`, 10:00–10:05) и SPID 200 (`SP:StmtCompleted`, 10:01–10:04), без Starting-событий
- **WHEN** выполняется `codebase trc tree file.trc` (без фильтра по SPID)
- **THEN** `SP:Completed` в SPID 100 — корневой узел дерева SPID 100
- **AND** `SP:StmtCompleted` в SPID 200 — корневой узел дерева SPID 200
- **AND** между ними нет отношения родитель-потомок, несмотря на вложенность интервалов

## MODIFIED Requirements

### Requirement: Вычисление ParentID/Depth через IncrementalParentTracker

Система SHALL вычислять `ParentID` и `Depth` для каждого события через `ComputeParentIDs` (`tree.go`) с использованием `IncrementalParentTracker` (`parent_tracker.go`): для каждого SPID поддерживается стек Starting-событий, и при поступлении Completed-события со стека снимается соответствующее Starting. Это восстанавливает вложенность вызовов без полного перестроения дерева.

Для SPID, не содержащих Starting-событий, система SHALL использовать fallback-алгоритм интервального стека: события сортируются по start time, и для каждого события ищется ближайший открытый интервал с более высоким EventClass. ParentID и Depth вычисляются на основе этого интервального стека.

#### Scenario: Восстановление вложенности для пары Starting/Completed

- **GIVEN** .trc файл с `Starting` для `ProcA`, затем `Starting` для `ProcB`, затем `Completed` для `ProcB`, затем `Completed` для `ProcA` (один SPID)
- **WHEN** `IncrementalParentTracker` обрабатывает события последовательно
- **THEN** `ProcB` получает `ParentID` = ID `ProcA`, `Depth` = 1
- **AND** `ProcA` получает `ParentID` = 0 (root), `Depth` = 0

#### Scenario: Несбалансированные Starting без Completed

- **GIVEN** .trc файл с `Starting` для `ProcA` без парного `Completed` (потерянное событие)
- **WHEN** `IncrementalParentTracker` завершает обработку SPID
- **THEN** `ProcA` остаётся в стеке; её `ParentID`/`Depth` проставляются по текущему состоянию стека (восстановление частично)

#### Scenario: Fallback для Completed-only SPID при стриминг-парсинге

- **GIVEN** .trc файл с SPID 122, содержащим только `SP:Completed` и `SP:StmtCompleted` без Starting-событий
- **WHEN** `IncrementalParentTracker` обрабатывает события последовательно
- **THEN** `SP:StmtCompleted` получает `ParentID` = ID `SP:Completed` (если интервал вложен)
- **AND** `SP:Completed` получает `ParentID` = -1 (root), `Depth` = 0

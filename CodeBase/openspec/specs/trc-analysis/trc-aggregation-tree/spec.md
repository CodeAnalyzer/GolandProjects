# TRC Aggregation and Tree

## Purpose

Агрегация событий трейса по процедурам (count, min/max/avg/total duration), построение дерева вызовов по SPID с восстановлением вложенности через Starting/Completed пары, поиск ошибок и медленных событий.

## Requirements

### Requirement: Агрегация по процедурам

Система SHALL агрегировать события трейса по имени процедуры (извлечённому из exec-statements в TextData): count, min/max/avg/total duration, с enrichment из индекса (путь к файлу).

#### Scenario: Агрегация по процедуре

- **GIVEN** .trc файл с 100 вызовами `MyProc` с разной длительностью
- **WHEN** выполняется `codebase trc procedures file.trc`
- **THEN** возвращена агрегация: count=100, min/max/avg/total duration, путь к файлу процедуры

### Requirement: Дерево вызовов по SPID

Система SHALL строить дерево вызовов, сгруппированное по SPID, с восстановлением вложенности через Starting/Completed пары событий (RPC, SQL:Batch, SQL:Stmt, SP, SP:Stmt). Корневыми узлами дерева SHALL быть только события, чьи имена заканчиваются на `Starting` или `Completed`. События, не попадающие в эти два класса (diagnostic: SP:Recompile, SQL:StmtRecompile, Audit Login/Logout, ExistingConnection, Attention и др.), SHALL быть вложены как дети в текущий открытый фрейм, если он есть, и SHALL NOT становиться корневыми узлами, когда стек пуст.

При серверном режиме (session_id > 0) `parent_id` в таблице `trc_events` SHALL хранить реальный `id` родительской строки (не 1-based offset). Recursive CTE в `LoadEventsForTree` SHALL использовать прямой `JOIN tree t ON c.parent_id = t.id` по индексу `idx_trc_events_session_parent` без промежуточного `numbered` CTE. Anchor CTE выбирает только события с `event_name LIKE '%Starting' OR event_name LIKE '%Completed'` в качестве корней.

При файловом режиме (без БД) фильтрация SHALL выполняться в памяти: `buildSPIDTree` в ветке `default` (non-Starting/Completed события) не создаёт корневой узел, когда стек пуст — событие пропускается. Когда стек не пуст, событие прикрепляется как ребёнок текущего фрейма (существующее поведение).

#### Scenario: Дерево по SPID

- **GIVEN** .trc файл с событиями от SPID 55
- **WHEN** выполняется `codebase trc tree file.trc --spid 55`
- **THEN** возвращено дерево вызовов для SPID 55 с восстановленной вложенностью

#### Scenario: Дерево с ограничением глубины

- **GIVEN** .trc файл с глубокой вложенностью вызовов
- **WHEN** выполняется `codebase trc tree file.trc --max-depth 3`
- **THEN** дерево ограничено глубиной 3 уровня

#### Scenario: Diagnostic-события не становятся корневыми узлами

- **GIVEN** .trc файл с SPID 76, где первые события — SP:Recompile и SQL:StmtRecompile без открытого Starting-фрейма
- **WHEN** выполняется `codebase trc tree file.trc --spid 76`
- **THEN** SP:Recompile и SQL:StmtRecompile не появляются как корневые узлы дерева
- **AND** корневыми узлами являются только Starting/Completed события

#### Scenario: Diagnostic-события внутри вызова сохраняются

- **GIVEN** .trc файл с `SP:Starting exec ProcA`, затем `SP:Recompile`, затем `SP:Completed exec ProcA` (один SPID)
- **WHEN** выполняется `codebase trc tree file.trc --spid 55`
- **THEN** `SP:Recompile` присутствует как ребёнок узла `SP:Starting exec ProcA`
- **AND** `SP:Recompile` не является корневым узлом

#### Scenario: Фильтрация diagnostic-событий при загрузке из БД

- **GIVEN** сохранённая TRC-сессия с SPID 76, содержащая diagnostic-события с `parent_id IS NULL`
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `session_id` и `spid=76`
- **THEN** anchor CTE выбирает только Starting/Completed события как корни
- **AND** diagnostic-события с `parent_id IS NULL` исключены из дерева

#### Scenario: Дерево без diagnostic-событий уменьшает размер ответа

- **GIVEN** .trc файл с SPID 76, где 90% корневых событий — diagnostic (Recompile и др.)
- **WHEN** выполняется `codebase trc tree file.trc --spid 76`
- **THEN** размер ответа существенно уменьшен по сравнению с поведением до фильтрации

#### Scenario: Дерево из БД для большой сессии без таймаута

- **GIVEN** сохранённая TRC-сессия с 500K событий на 50 SPID, SPID 95 имеет 10K событий
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `session_id` и `spid=95`
- **THEN** дерево вызовов возвращается за время менее 5 секунд
- **AND** recursive CTE использует прямой JOIN по индексу `idx_trc_events_session_parent` без `numbered` CTE

### Requirement: Фильтрация дерева по имени процедуры

Система SHALL предоставлять фильтрацию дерева вызовов по имени процедуры через параметр `procedure` в CLI (`--proc`) и MCP (`procedure`). При заданном имени процедуры возвращаются только поддеревья, корневые узлы которых имеют `Start.Procedure` совпадающий с указанным именем.

При серверном режиме (session_id > 0) фильтрация SHALL выполняться в SQL внутри recursive CTE `LoadEventsForTree`: anchor ищет события с `procedure = $procedure` (вместо `parent_id IS NULL`), recursive part спускается только от них через прямой `JOIN tree t ON c.parent_id = t.id`. Это загружает из БД только поддерево нужной процедуры.

При файловом режиме (без БД) фильтрация SHALL выполняться в памяти: дерево строится по всем событиям, затем `FilterTreesByProcedure` находит узлы с совпадающей процедурой и возвращает их поддеревья.

#### Scenario: Дерево от конкретной процедуры через CLI

- **GIVEN** .trc файл с событиями процедур `ProcA`, `ProcB`, `ProcC` в одном SPID
- **WHEN** выполняется `codebase trc tree file.trc --proc ProcB`
- **THEN** возвращено только поддерево `ProcB` с её дочерними вызовами

#### Scenario: Дерево от конкретной процедуры через MCP

- **GIVEN** сохранённая TRC-сессия с событиями процедур `ProcA`, `ProcB`, `ProcC`
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `procedure = "ProcB"`
- **THEN** возвращено только поддерево `ProcB` (серверная фильтрация в CTE)

#### Scenario: Дерево от процедуры с фильтром по SPID

- **GIVEN** .trc файл с событиями процедуры `ProcB` в SPID 55 и SPID 66
- **WHEN** выполняется `codebase trc tree file.trc --proc ProcB --spid 55`
- **THEN** возвращено только поддерево `ProcB` для SPID 55

#### Scenario: Дерево от процедуры в большой сессии без таймаута

- **GIVEN** сохранённая TRC-сессия с 500K событий, процедура `ProcB` имеет 1000 событий на SPID 95
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `session_id` и `procedure = "ProcB"`
- **THEN** поддерево `ProcB` возвращается за время менее 5 секунд
- **AND** recursive CTE использует прямой JOIN по `parent_id` без `numbered` CTE

#### Scenario: Процедура не найдена

- **GIVEN** .trc файл без событий процедуры `NonExistentProc`
- **WHEN** выполняется `codebase trc tree file.trc --proc NonExistentProc`
- **THEN** возвращён пустой результат (нет деревьев)

#### Scenario: Дерево без фильтра по процедуре

- **GIVEN** .trc файл с событиями
- **WHEN** выполняется `codebase trc tree file.trc` без `--proc`
- **THEN** возвращён полный лес деревьев по всем SPID (существующее поведение не изменяется)

### Requirement: Поиск ошибок

Система SHALL предоставлять команду `trc errors` для поиска событий с ненулевой колонкой Error(31).

#### Scenario: События с ошибками

- **GIVEN** .trc файл с событиями, некоторые из которых имеют Error(31) ≠ 0
- **WHEN** выполняется `codebase trc errors file.trc`
- **THEN** возвращены только события с ошибками

### Requirement: Поиск медленных событий

Система SHALL предоставлять команду `trc slow` для поиска событий медленнее указанного порога (по умолчанию 100 мс), отсортированных по убыванию длительности.

#### Scenario: Медленные события

- **GIVEN** .trc файл с событиями разной длительности
- **WHEN** выполняется `codebase trc slow file.trc --slow-ms 500`
- **THEN** возвращены события с duration > 500 мс, отсортированные по убыванию

### Requirement: Список событий с фильтрами

Система SHALL предоставлять команду `trc events` для вывода декодированных событий с опциональной фильтрацией по SPID и имени процедуры.

#### Scenario: Фильтр по процедуре

- **GIVEN** .trc файл с событиями разных процедур
- **WHEN** выполняется `codebase trc events file.trc --proc MyProc`
- **THEN** возвращены только события для процедуры `MyProc`

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

### Requirement: Маппинг parent_id из offset в реальный id при сохранении в БД

Система SHALL вставлять события в `trc_events` через COPY IN с явным `id = baseID + EventIndex`,
где `baseID = COALESCE(MAX(id), 0) + 1 FROM trc_events` определяется перед вставкой,
а `EventIndex` — 0-based порядковый индекс события в потоке (вычисляется `IncrementalParentTracker`).
`parent_id` SHALL содержать `baseID + ParentID` если `ParentID >= 0`, иначе NULL —
это реальный id строки родителя сразу после вставки, без post-insert маппинга.

После вставки всех батчей система SHALL синхронизировать sequence:
`SELECT setval('trc_events_id_seq', (SELECT MAX(id) FROM trc_events))`.

`insertTRCEvents` SHALL NOT выполнять post-insert маппинг parent_id.
Корневые события (`ParentID < 0`) SHALL иметь `parent_id IS NULL`.

#### Scenario: Явный parent_id при COPY IN

- **GIVEN** 3 события: event[0] (root, ParentID=-1), event[1] (child of 0, ParentID=0), event[2] (child of 1, ParentID=1)
- **WHEN** `insertTRCEvents` выполняет COPY IN с `baseID=1000`
- **THEN** event[0] имеет `id=1000`, `parent_id IS NULL` в БД
- **AND** event[1] имеет `id=1001`, `parent_id=1000` (реальный id event[0]) в БД
- **AND** event[2] имеет `id=1002`, `parent_id=1001` (реальный id event[1]) в БД

#### Scenario: Маппинг parent_id после COPY IN

- **GIVEN** 3 события: event[0] (root, ParentID=-1), event[1] (child of 0, ParentID=0), event[2] (child of 1, ParentID=1)
- **WHEN** `insertTRCEvents` завершает COPY IN с явным id
- **THEN** event[0] имеет `parent_id IS NULL` в БД
- **AND** event[1] имеет `parent_id` = реальный `id` event[0] в БД
- **AND** event[2] имеет `parent_id` = реальный `id` event[1] в БД

#### Scenario: Маппинг для большой сессии без таймаута

- **GIVEN** TRC-сессия с 1.3M событий, которые вставлены через COPY IN батчами
- **WHEN** `insertTRCEvents` завершает COPY IN с явным id
- **THEN** post-insert маппинг (MapParentIDs) не выполняется
- **AND** `parent_id` содержит корректные реальные id сразу после вставки

#### Scenario: Стриминг-парсинг файла с >1M событий без post-insert маппинга

- **GIVEN** .trc файл с 1.3M событий, стриминг через `ParseFileToDB` батчами по 50K
- **WHEN** `ParseFileToDB` завершает все батчи COPY IN
- **THEN** `parent_id` в БД содержит корректные реальные id для всех 1.3M событий
- **AND** post-insert маппинг (MapParentIDs) не выполняется
- **AND** общее время вставки не превышает время COPY IN батчей (без дополнительного UPDATE)

#### Scenario: Синхронизация sequence после вставки

- **GIVEN** сессия с 100K событий вставлена с явными id от 5001 до 510000
- **WHEN** вставка завершена
- **THEN** `trc_events_id_seq` синхронизирован с `MAX(id)` через `setval`
- **AND** следующий `nextval('trc_events_id_seq')` возвращает 510001

### Requirement: Серверная агрегация LoadProceduresAggregated и LimitTrees

Система SHALL при работе из сохранённой сессии (БД) использовать серверную агрегацию `LoadProceduresAggregated` (`store.go:656-675`) вместо клиентской `AggregateByProcedure`, что переносит группировку по процедурам в PostgreSQL. Деревья ограничиваются через `LimitTrees` (root nodes + children per node) после построения.

#### Scenario: Серверная агрегация из сессии

- **GIVEN** сохранённая TRC-сессия с id 42 в БД
- **WHEN** `ExecuteProcedures` вызывает `trc.LoadProceduresAggregated(ctx, db, 42)`
- **THEN** агрегация по процедурам выполнена в PostgreSQL (GROUP BY procedure), а не в памяти клиента

#### Scenario: Ограничение дерева через LimitTrees

- **GIVEN** построенное дерево с 1000 root nodes и 100 children per node
- **WHEN** `ExecuteTree` вызывает `LimitTrees(trees, limit)` с `limit = 50`
- **THEN** оставлены первые 50 root nodes, у каждой — первые 50 children

### Requirement: EnrichAggregates для агрегаций

Система SHALL обогащать агрегации процедур через `EnrichAggregates` (`enrich.go`): передаёт map enrichment-данных (полученный из `EnrichEvents` для sample-событий) в агрегации, проставляя путь к файлу процедуры и номера строк. Используется как при серверной, так и при клиентской агрегации.

#### Scenario: Enrichment агрегации из sample-событий

- **GIVEN** агрегация по `MyProc` без enrichment и sample-события `MyProc` с enrichment (путь к файлу)
- **WHEN** `ExecuteProcedures` вызывает `EnrichAggregates(aggs, enrichMap)`
- **THEN** агрегация `MyProc` получает путь к файлу процедуры из sample-события

## Related code

- `internal/trc/aggregate.go` — `AggregateByProcedure` (клиентская агрегация)
- `internal/trc/tree.go` — `BuildTreesWithDepth`, `BuildTrees`, восстановление вложенности, `ComputeParentIDs`, `FilterTreesByProcedure`
- `internal/trc/parent_tracker.go` — `IncrementalParentTracker`, tracker вложенности событий для tree building
- `internal/trc/format.go` — `FormatTrees`, текстовое форматирование дерева
- `internal/trc/store.go` — `LoadProceduresAggregated` (серверная агрегация), `LoadEventsForTree` (с фильтром по procedure в CTE), `LoadEventsFiltered`, `LoadSlowEvents`, `LoadErrorEvents`, `LoadEventCount`
- `internal/trc/enrich.go` — `EnrichEvents`, `EnrichAggregates`
- `cmd/trc.go` — CLI commands `trc procedures`, `trc tree`, `trc errors`, `trc slow`, `trc events`

## Notes

- Агрегация сортируется по total duration по убыванию
- Дерево вызовов восстанавливает вложенность через пары Starting/Completed событий
- `--max-depth 0` означает без лимита глубины
- `--limit 0` для tree означает без лимита root nodes и children per node
- Execution-слой `internal/trcsvc/runtime.go` (`ExecuteProcedures`, `ExecuteTree`, `ExecuteEvents`, `ExecuteErrors`, `ExecuteSlow`) — общая точка входа для CLI (`cmd/trc.go`) и MCP-инструментов `codebase_trc_*`; устраняет дублирование оркестрации. Транспорт MCP — в `mcp-server/mcp-transport-tools`.
- `IncrementalParentTracker`/`ComputeParentIDs` восстанавливают `ParentID`/`Depth` инкрементально по потоку событий, без перестроения дерева; корректно работают с несбалансированными Starting/Completed (потерянные события)
- При работе из БД (`session_id > 0`) агрегация выполняется серверно через `LoadProceduresAggregated` (GROUP BY в PostgreSQL); из файла — клиентски через `AggregateByProcedure`
- `EnrichAggregates` переносит enrichment из sample-событий в агрегации — не делает lookup для каждой агрегации, что устраняет N+1
- `--proc` для `trc tree` фильтрует дерево по имени процедуры: при серверном режиме — в CTE (anchor по `procedure` вместо `parent_id IS NULL`), при файловом — через `FilterTreesByProcedure` в памяти
- `parent_id` в `trc_events` хранит реальный `id` родительской строки (после Go-маппинга в `insertTRCEvents`), не 1-based offset. `LoadEventsForTree` использует прямой `JOIN tree t ON c.parent_id = t.id` без промежуточного `numbered` CTE

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

Система SHALL строить дерево вызовов, сгруппированное по SPID, с восстановлением вложенности через Starting/Completed пары событий (RPC, SQL:Batch, SQL:Stmt, SP, SP:Stmt).

#### Scenario: Дерево по SPID

- **GIVEN** .trc файл с событиями от SPID 55
- **WHEN** выполняется `codebase trc tree file.trc --spid 55`
- **THEN** возвращено дерево вызовов для SPID 55 с восстановленной вложенностью

#### Scenario: Дерево с ограничением глубины

- **GIVEN** .trc файл с глубокой вложенностью вызовов
- **WHEN** выполняется `codebase trc tree file.trc --max-depth 3`
- **THEN** дерево ограничено глубиной 3 уровня

### Requirement: Фильтрация дерева по имени процедуры

Система SHALL предоставлять фильтрацию дерева вызовов по имени процедуры через параметр `procedure` в CLI (`--proc`) и MCP (`procedure`). При заданном имени процедуры возвращаются только поддеревья, корневые узлы которых имеют `Start.Procedure` совпадающий с указанным именем.

При серверном режиме (session_id > 0) фильтрация SHALL выполняться в SQL внутри recursive CTE `LoadEventsForTree`: anchor ищет события с `procedure = $procedure` (вместо `parent_id IS NULL`), recursive part спускается только от них. Это загружает из БД только поддерево нужной процедуры.

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

Система SHALL вычислять `ParentID` и `Depth` для каждого события через `ComputeParentIDs` (`tree.go:107-171`) с использованием `IncrementalParentTracker` (`parent_tracker.go`): для каждого SPID поддерживается стек Starting-событий, и при поступлении Completed-события со стека снимается соответствующее Starting. Это восстанавливает вложенность вызовов без полного перестроения дерева.

#### Scenario: Восстановление вложенности для пары Starting/Completed

- **GIVEN** .trc файл с `Starting` для `ProcA`, затем `Starting` для `ProcB`, затем `Completed` для `ProcB`, затем `Completed` для `ProcA` (один SPID)
- **WHEN** `IncrementalParentTracker` обрабатывает события последовательно
- **THEN** `ProcB` получает `ParentID` = ID `ProcA`, `Depth` = 1
- **AND** `ProcA` получает `ParentID` = 0 (root), `Depth` = 0

#### Scenario: Несбалансированные Starting без Completed

- **GIVEN** .trc файл с `Starting` для `ProcA` без парного `Completed` (потерянное событие)
- **WHEN** `IncrementalParentTracker` завершает обработку SPID
- **THEN** `ProcA` остаётся в стеке; её `ParentID`/`Depth` проставляются по текущему состоянию стека (восстановление частично)

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

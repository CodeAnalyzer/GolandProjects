## ADDED Requirements

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

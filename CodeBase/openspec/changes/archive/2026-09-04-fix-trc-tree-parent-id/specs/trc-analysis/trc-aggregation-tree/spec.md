## MODIFIED Requirements

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

#### Scenario: Процедура не найдена

- **GIVEN** .trc файл без событий процедуры `NonExistentProc`
- **WHEN** выполняется `codebase trc tree file.trc --proc NonExistentProc`
- **THEN** возвращён пустой результат (нет деревьев)

#### Scenario: Дерево без фильтра по процедуре

- **GIVEN** .trc файл с событиями
- **WHEN** выполняется `codebase trc tree file.trc` без `--proc`
- **THEN** возвращён полный лес деревьев по всем SPID (существующее поведение не изменяется)

#### Scenario: Дерево от процедуры в большой сессии без таймаута

- **GIVEN** сохранённая TRC-сессия с 500K событий, процедура `FCD_CCred_MassProcessByFOActID_MT` в SPID 95
- **WHEN** вызывается MCP-инструмент `codebase_trc_tree` с `session_id`, `spid=95`, `procedure="FCD_CCred_MassProcessByFOActID_MT"`
- **THEN** дерево вызовов возвращается за время менее 5 секунд
- **AND** recursive CTE не использует `numbered` CTE с `row_number()` материализацией всех событий сессии

## ADDED Requirements

### Requirement: Маппинг parent_id из offset в реальный id при сохранении в БД

Система SHALL после вставки событий в `trc_events` через `COPY IN` выполнять Go-маппинг `parent_id` из 1-based offset в реальный `id` строки. `ComputeParentIDs` вычисляет `ParentID` как 0-based индекс в срезе events; при сохранении добавляется +1 (1-based offset). После присвоения `id` базой (BIGSERIAL) система SHALL:
1. загрузить `id` через `SELECT id FROM trc_events WHERE session_id = $1 ORDER BY id`;
2. вычислить `realParentID = ids[ev.ParentID]` в Go для каждого события с `ParentID >= 0`;
3. выполнить `COPY IN` во временную таблицу `(id, parent_id)` и `UPDATE trc_events e SET parent_id = t.parent_id FROM temp t WHERE e.id = t.id` — JOIN по PK.

Этот маппинг SHALL выполняться один раз при парсинге (в `insertTRCEvents`), не при каждом запросе дерева. Корневые события (parent_id IS NULL) SHALL NOT затрагиваться этим маппингом.

#### Scenario: Маппинг parent_id после COPY IN

- **GIVEN** TRC-сессия с 1000 событий, вычисленными ParentID как индексы в срезе
- **WHEN** `insertTRCEvents` завершает COPY IN и выполняет UPDATE для маппинга
- **THEN** `parent_id` каждой строки содержит реальный `id` родительской строки в `trc_events`
- **AND** корневые события имеют `parent_id IS NULL`

#### Scenario: Дерево строится по реальным id после маппинга

- **GIVEN** сохранённая TRC-сессия после маппинга parent_id
- **WHEN** `LoadEventsForTree` выполняет recursive CTE с `JOIN tree t ON c.parent_id = t.id`
- **THEN** дерево вызовов корректно восстановлено
- **AND** каждый дочерний узел указывает на реальную строку родителя

#### Scenario: Маппинг не затрагивает корневые события

- **GIVEN** TRC-сессия с 5 корневыми событиями (ParentID = -1 в срезе)
- **WHEN** `insertTRCEvents` сохраняет события и выполняет UPDATE маппинга
- **THEN** 5 корневых событий имеют `parent_id IS NULL` в БД
- **AND** UPDATE не изменяет их `parent_id`

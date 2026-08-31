## MODIFIED Requirements

### Requirement: Дерево вызовов по SPID

Система SHALL строить дерево вызовов, сгруппированное по SPID, с восстановлением вложенности через Starting/Completed пары событий (RPC, SQL:Batch, SQL:Stmt, SP, SP:Stmt). Корневыми узлами дерева SHALL быть только события, чьи имена заканчиваются на `Starting` или `Completed`. События, не попадающие в эти два класса (diagnostic: SP:Recompile, SQL:StmtRecompile, Audit Login/Logout, ExistingConnection, Attention и др.), SHALL быть вложены как дети в текущий открытый фрейм, если он есть, и SHALL NOT становиться корневыми узлами, когда стек пуст.

При серверном режиме (session_id > 0) фильтрация SHALL выполняться в SQL: anchor CTE в `LoadEventsForTree` выбирает только события с `event_name LIKE '%Starting' OR event_name LIKE '%Completed'` в качестве корней. Recursive part подтягивает детей по parent_id без изменений — parent_id у Starting/Completed событий никогда не указывает на diagnostic-события.

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

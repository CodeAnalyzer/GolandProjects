# TRC Aggregation and Tree

## Purpose

Агрегация событий трейса по процедурам (count, min/max/avg/total duration), построение дерева вызовов по SPID с восстановлением вложенности через Starting/Completed пары, поиск ошибок и медленных событий.

## Requirements

### Requirement: Агрегация по процедурам

Система SHALL агрегировать вызовы процедур из событий, определяемых фильтром `event_names` (при отсутствии `event_names` — только `SP:Completed`, как раньше), с непустым именем процедуры: `count`, `min_ms`, `max_ms`, `avg_ms`, `total_ms` и enrichment из индекса. Пустое имя процедуры SHALL исключаться из агрегата. Нулевое значение `duration_ms` SHALL считаться валидной длительностью и участвовать во всех пяти метриках. Saved-session и file-mode SHALL возвращать одинаковые метрики для одного набора событий и фильтров.

Агрегация SHALL поддерживать фильтр `spids` (только события указанных SPID), ограничение `top` (0 или отсутствие — все процедуры; при явной установке — от 1 до 1000, применяется после агрегации) и параметр `sort_by`: `total_ms` (default), `avg_ms`, `max_ms`, `count`; сортировка SHALL быть детерминированной — secondary sort по `procedure`, затем по `spid`.

Параметр `group_by_spid` (default false) SHALL управлять группировкой: false — группы только по `procedure` (события с NULL `spid` участвуют, как раньше); true — группы по `(spid, procedure)`, при этом события с `spid IS NULL` SHALL исключаться из агрегата, а каждая группа содержит поле `spid`.

#### Scenario: Агрегация по процедуре

- **GIVEN** TRC-сессия содержит 100 событий `SP:Completed` для `MyProc` с разной длительностью
- **WHEN** выполняется `codebase trc procedures` или `codebase_trc_procedures` без новых параметров
- **THEN** возвращена агрегация `MyProc` с `count=100`, метриками длительности и путём к файлу процедуры
- **AND** число агрегатов не ограничено (top по умолчанию отсутствует)

#### Scenario: Statement-события не задваивают вызов

- **GIVEN** один вызов `MyProc` представлен событиями `SP:Completed`, `SP:StmtCompleted` и `SQL:StmtCompleted` с непустым именем процедуры
- **WHEN** выполняется агрегация процедур без `event_names`
- **THEN** в `count` и метриках `MyProc` учтено только событие `SP:Completed`

#### Scenario: Нулевая длительность участвует в метриках

- **GIVEN** `MyProc` имеет два события `SP:Completed` с `duration_ms` 0 и 100
- **WHEN** агрегация выполняется для сохранённой сессии и непосредственно для файла
- **THEN** оба режима возвращают `count=2`, `total_ms=100`, `min_ms=0`, `max_ms=100` и `avg_ms=50`

#### Scenario: Агрегация с явным набором событий

- **GIVEN** TRC-сессия содержит вызовы, представленные событиями `SP:Completed` и `RPC:Completed`
- **WHEN** агрегация запрашивается с `event_names=["RPC:Completed"]`
- **THEN** агрегированы только события `RPC:Completed` с непустым именем процедуры

#### Scenario: Top-N и сортировка

- **GIVEN** TRC-сессия содержит 150 процедур
- **WHEN** агрегация запрашивается с `top=20` и `sort_by=avg_ms`
- **THEN** возвращены 20 процедур с наибольшим `avg_ms`
- **AND** равные значения `avg_ms` упорядочены по имени процедуры

#### Scenario: Группировка по SPID

- **GIVEN** процедура `MyProc` вызывается в SPID 728 и SPID 700, а также существует событие `MyProc` с NULL `spid`
- **WHEN** агрегация запрашивается с `spids=[728, 700]` и `group_by_spid=true`
- **THEN** возвращены группы `(728, MyProc)` и `(700, MyProc)` с полем `spid`
- **AND** событие с NULL `spid` не участвует в агрегате

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

### Requirement: Единая модель расширенных фильтров TRC-событий

Система SHALL использовать единую внутреннюю модель фильтрации событий TRC: список SPID (`spids`), список имён событий (`event_names`), имя процедуры (точное case-sensitive совпадение, без нормализации), временной полуинтервал (`time_from`, `time_to`) и минимальная длительность (`min_duration_ms`). Правила: каждый SPID SHALL быть положительным; дубликаты в `spids` и `event_names` SHALL удаляться с сохранением порядка первого появления; пустые строки в списках SHALL запрещаться; `time_from` SHALL фильтровать `start_time >= time_from`, `time_to` SHALL фильтровать `start_time < time_to`, при одновременном наличии границ SHALL выполняться `time_from < time_to`; события с NULL `start_time` SHALL NOT попадать в заданный временной диапазон; `min_duration_ms` SHALL быть неотрицательным и задаёт `duration_ms >= min_duration_ms`; все фильтры, кроме курсора, SHALL участвовать в вычислении `filtered_count`. Времененные границы SHALL приниматься только в формате RFC3339. Скалярные формы `spid` и `event_name` существуют только как legacy-алиасы на границе CLI/MCP и SHALL нормализоваться в массивы из одного элемента до вызова сервисного слоя; одновременная передача legacy-алиаса и соответствующего массива SHALL возвращать ошибку с предложением использовать массив-параметр.

#### Scenario: Фильтр по нескольким SPID и классам событий

- **GIVEN** TRC-сессия содержит события SPID 728, 700 и 179 классов `SP:Completed` и `RPC:Completed`
- **WHEN** запрашивается список событий с `spids=[728, 700]` и `event_names=["SP:Completed", "RPC:Completed"]`
- **THEN** возвращены только события указанных SPID указанных классов
- **AND** `filtered_count` равен числу таких событий в сессии

#### Scenario: Временной полуинтервал

- **GIVEN** TRC-сессия содержит события с `start_time` до, внутри и после интервала
- **WHEN** запрашивается список событий с `time_from` и `time_to`
- **THEN** возвращены события с `start_time >= time_from` и `start_time < time_to`
- **AND** события без `start_time` в результат не входят

#### Scenario: Некорректные значения фильтров

- **GIVEN** запущен CLI или MCP-сервер
- **WHEN** передан `spids=[728, 0]`, пустая строка в `event_names`, `time_from >= time_to` или не-RFC3339 timestamp
- **THEN** возвращена ошибка с именем параметра и некорректным значением
- **AND** запрос к данным не выполняется

#### Scenario: Конфликт legacy-алиаса и массива

- **GIVEN** запущен MCP-сервер
- **WHEN** `codebase_trc_events` вызван одновременно с `spid=728` и `spids=[728]`
- **THEN** возвращена ошибка с предложением использовать параметр `spids`

#### Scenario: Дедупликация списков

- **GIVEN** запрос с `spids=[700, 728, 700]`
- **WHEN** выполняется фильтрация
- **THEN** фильтр эквивалентен `spids=[700, 728]` — порядок первого появления сохранён

### Requirement: Список событий с фильтрами

Система SHALL предоставлять команду `trc events` и MCP-инструмент `codebase_trc_events` для вывода декодированных событий с фильтрацией по списку SPID (`spids`), списку имён событий (`event_names`), имени процедуры, временному полуинтервалу и минимальной длительности по правилам единой модели расширенных фильтров. Система SHALL поддерживать постраничную выборку через keyset-курсор `after_id` и представление `format=full|short` (default `full`).

Результат SHALL содержать `filtered_count` — полный размер набора после фильтров без `limit` и курсора — на каждой странице; `returned_count` фактически возвращённых событий; применённый `limit`; `has_more` — признак наличия совпадающих событий после текущей страницы; `next_after_id` — курсор последнего возвращённого события, отсутствующий при `has_more=false` или пустой странице. `total_count` всех событий источника SHALL возвращаться только когда `after_id` не передан; на страницах продолжения поле SHALL отсутствовать в JSON. `has_more` SHALL определяться запросом `limit + 1` без включения дополнительной строки в ответ.

Saved-session SHALL выполнять фильтрацию и пагинацию серверно в PostgreSQL; file-mode SHALL применять эквивалентные правила в памяти, ключом курсора SHALL быть `EventIndex + 1` (первое событие имеет ID 1). Курсор SHALL быть непрозрачным для клиента: `next_after_id` передаётся в следующий вызов без арифметики; при изменении исходного файла между страницами консистентность последовательности SHALL NOT гарантироваться.

Short-формат SHALL возвращать идентификатор, класс, имя, SPID, процедуру, время и длительность каждого события, исключая `params` и `columns`; full-формат дополнительно возвращает `params` и `columns`. Full и short форматы SHALL возвращать одинаковые ID, порядок и число событий.

#### Scenario: Фильтр по процедуре

- **GIVEN** TRC-файл содержит события разных процедур
- **WHEN** выполняется `codebase trc events file.trc --proc MyProc`
- **THEN** возвращены только события для процедуры `MyProc`

#### Scenario: Количество совпадений превышает лимит

- **GIVEN** TRC-сессия содержит 1500 событий, удовлетворяющих фильтру, и установлен `limit=1000`
- **WHEN** запрашивается список событий без `after_id`
- **THEN** результат содержит `total_count` всех событий источника, `filtered_count=1500` и `returned_count=1000`
- **AND** массив `events` содержит 1000 элементов
- **AND** `has_more=true` и `next_after_id` равен курсору последнего возвращённого события

#### Scenario: Последовательное чтение страниц

- **GIVEN** фильтру соответствует 2500 событий и `limit=1000`
- **WHEN** выполняются три последовательных вызова, каждый с `after_id=next_after_id` предыдущей страницы
- **THEN** каждая страница возвращает `filtered_count=2500`
- **AND** страницы не пересекаются и не теряют события, порядок соответствует курсорному
- **AND** `total_count` присутствует только в ответе первого вызова
- **AND** последняя страница возвращает `has_more=false` и не содержит `next_after_id`

#### Scenario: Short-формат без params и columns

- **GIVEN** TRC-сессия содержит события с параметрами и декодированными колонками
- **WHEN** список событий запрашивается с `format=short`
- **THEN** каждое событие содержит идентификатор, класс, имя, SPID, процедуру, время и длительность
- **AND** `params` и `columns` отсутствуют
- **AND** ID, порядок и число событий совпадают с `format=full`

#### Scenario: File-mode курсор

- **GIVEN** TRC-файл без сохранённой сессии содержит 5 событий
- **WHEN** список событий запрашивается с `after_id=3`
- **THEN** возвращены события с логическим ID 4 и 5

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

Система SHALL при работе из сохранённой сессии (БД) использовать серверную агрегацию `LoadProceduresAggregated` вместо клиентской `AggregateByProcedure`: фильтры `spids` и `event_names`, группировка по `procedure` или `(spid, procedure)`, сортировка по `sort_by` с secondary-порядком и ограничение `top` SHALL выполняться в PostgreSQL. Деревья ограничиваются через `LimitTrees` (root nodes + children per node) после построения.

#### Scenario: Серверная агрегация из сессии

- **GIVEN** сохранённая TRC-сессия с id 42 в БД
- **WHEN** `ExecuteProcedures` вызывает `trc.LoadProceduresAggregated`
- **THEN** фильтрация, группировка, сортировка и top выполнены в PostgreSQL, а не в памяти клиента

#### Scenario: Серверная агрегация с фильтрами

- **GIVEN** сохранённая TRC-сессия содержит события SPID 728 и 700
- **WHEN** `codebase_trc_procedures` вызывается с `session_id`, `spids=[728]` и `top=10`
- **THEN** агрегация возвращает не более 10 процедур, рассчитанных только по SPID 728

#### Scenario: Ограничение дерева через LimitTrees

- **GIVEN** построенное дерево с 1000 root nodes и 100 children per node
- **WHEN** `ExecuteTree` вызывает `LimitTrees(trees, limit)` с `limit = 50`
- **THEN** оставлены первые 50 root nodes, у каждой — первые 50 children

### Requirement: Сравнение процедур focus/peer SPID

Система SHALL предоставлять команду `trc compare-procedures` и MCP-инструмент `codebase_trc_compare_procedures`, отвечающие одним вызовом на вопрос: какие Top-N процедур самые дорогие в focus SPID и как те же процедуры выполнялись в peer SPID.

Параметры: ровно один источник (`session_id` или `file_path`); `focus_spid` SHALL быть положительным; `compare_spids` — непустой список peer SPID (дедупликация с сохранением порядка первого появления, `focus_spid` SHALL удаляться из списка; пустой список после нормализации SHALL возвращать ошибку); `event_names` (при отсутствии — только `SP:Completed`); `top` default 20, max 100; `sort_by`: `total_ms` (default), `avg_ms`, `max_ms`, `count` с secondary-порядком `procedure`, затем `spid`.

Алгоритм SHALL: Top-N определяется только агрегатами focus SPID; для выбранных имён возвращаются метрики каждого peer SPID в исходном нормализованном порядке. Отсутствующая пара `(spid, procedure)` SHALL возвращать `count=0`, `total_ms=0` и nullable `min_ms`/`max_ms`/`avg_ms` — отсутствие вызовов отличимо от валидной нулевой длительности. `peer_combined` SHALL вычисляться по сырым peer-вызовам всех выбранных SPID: `avg_ms = sum(duration_ms) / count`, а не среднее от средних SPID. Ratios `avg_vs_peers`, `max_vs_peers`, `count_vs_peers` SHALL быть `null` при нулевом знаменателе или отсутствии peer-вызовов.

Ответ SHALL содержать `rank`, имя процедуры, focus-метрики, метрики каждого peer, `peer_combined` и ratios, а также предупреждения: в рейтинг входят только completed-события (незавершённые вызовы отсутствуют и не получают синтетическую длительность); суммы elapsed включают время дочерних вызовов и не являются wall-clock time. При `max(duration_ms) = 0` по всем событиям сессии ответ SHALL содержать предупреждение о недоступности метрик длительности — count-метрики и `count_vs_peers` остаются рабочим путём сравнения.

#### Scenario: Сравнение focus с peers

- **GIVEN** процедура `MassProtocolAccrual_Add` в focus SPID 728 имеет 5 вызовов `SP:Completed` с `avg_ms=316232`, а в peer SPID — 40 вызовов с `avg_ms=3000`
- **WHEN** выполняется `trc compare-procedures` с `focus_spid=728`, `compare_spids=[700,179]` и `top=20`
- **THEN** `MassProtocolAccrual_Add` занимает `rank=1` с focus-метриками
- **AND** для процедуры присутствуют метрики каждого peer SPID в переданном порядке
- **AND** `peer_combined.avg_ms` рассчитан по всем peer-вызовам вместе
- **AND** `ratios.avg_vs_peers` равен отношению focus avg к peer_combined avg

#### Scenario: Отсутствующая peer-процедура

- **GIVEN** процедура `DSXP_Log_FindOption` есть в focus SPID и отсутствует в peer SPID 867
- **WHEN** выполняется сравнение
- **THEN** peer 867 для этой процедуры возвращает `count=0`, `total_ms=0`
- **AND** `min_ms`, `max_ms`, `avg_ms` равны `null` — отсутствие отличимо от нулевой длительности

#### Scenario: Top-N определяется только focus

- **GIVEN** процедура `HeavyPeerProc` самая дорогая в peer SPID, но отсутствует в top focus SPID
- **WHEN** выполняется сравнение с `top=20`
- **THEN** `HeavyPeerProc` отсутствует в ответе

#### Scenario: peer_combined — взвешенное среднее

- **GIVEN** peer SPID 700 имеет 10 вызовов `MyProc` с `avg_ms=100`, peer SPID 179 имеет 30 вызовов с `avg_ms=200`
- **WHEN** выполняется сравнение
- **THEN** `peer_combined.avg_ms = (10*100 + 30*200) / 40 = 175`, а не среднее от `100` и `200`

#### Scenario: Нулевой знаменатель ratio

- **GIVEN** процедура отсутствует во всех peer SPID
- **WHEN** выполняется сравнение
- **THEN** `ratios.avg_vs_peers`, `ratios.max_vs_peers` и `ratios.count_vs_peers` равны `null`

#### Scenario: Трейс без данных длительности

- **GIVEN** трейс снят без колонки Duration — все `duration_ms = 0` (как nbki-трейс)
- **WHEN** выполняется сравнение с `sort_by=count`
- **THEN** ответ содержит предупреждение о недоступности метрик длительности
- **AND** Top-N по `count` и `ratios.count_vs_peers` содержат осмысленные значения

#### Scenario: Валидация параметров сравнения

- **GIVEN** запущен CLI или MCP-сервер
- **WHEN** передан неположительный `focus_spid`, пустой `compare_spids`, либо список peer, опустевший после удаления focus и дедупликации
- **THEN** возвращена ошибка с именем параметра
- **AND** запрос к данным не выполняется

### Requirement: Сводка активности SPID

Система SHALL предоставлять команду `trc spids` и MCP-инструмент `codebase_trc_spids`, возвращающие сводку активности по SPID без выгрузки сырых событий.

Параметры: ровно один источник; опциональные `spids` (фильтр подмножества) и `time_from`/`time_to` (полуинтервал `[from;to)` по `start_time` по правилам единой модели фильтров); `sort_by`: `first_time`, `last_time`, `event_count`, `max_duration_ms`, `error_count`; `limit` default 100, max 1000.

Для каждого SPID сводка SHALL содержать: `event_count` — все события SPID после временного фильтра; `first_time`/`last_time`, вычисленные по ненулевым `start_time` (null, если времени нет ни у одного события); счётчики `sp_completed_count`, `rpc_completed_count`, `batch_completed_count`; `error_count` — события с ненулевой ошибкой (`error IS NOT NULL AND error <> 0`; file-mode — колонка 31); `max_duration_ms`, включающий нулевые длительности; `application_name`, `login_name`, `host_name` — самое частое непустое значение, при равной частоте — значение из самого раннего события (по `id`/`EventIndex`); поля отсутствуют, если непустых значений нет. Отсутствие `SQL:BatchCompleted` у SPID SHALL NOT трактоваться как незавершённость: инструмент возвращает наблюдаемые факты и предупреждения, поле `unfinished` SHALL NOT вычисляться.

#### Scenario: Сводка по сессии

- **GIVEN** сохранённая TRC-сессия содержит события SPID 728 и 700 с разными классами и длительностями
- **WHEN** вызывается `codebase_trc_spids` с `session_id`
- **THEN** для каждого SPID возвращены `event_count`, `first_time`, `last_time`, счётчики completed-классов, `error_count` и `max_duration_ms`
- **AND** сырые события не выгружаются

#### Scenario: Мода app/login/host с tie-break

- **GIVEN** SPID 728 содержит 5 событий с `application_name="AppA"` и 5 событий с `"AppB"`, причём первое по `id` событие имеет `"AppA"`
- **WHEN** запрашивается сводка SPID
- **THEN** `application_name = "AppA"` — равная частота решается самым ранним событием

#### Scenario: Временной фильтр сужает сводку

- **GIVEN** SPID активен до и после `time_from`
- **WHEN** сводка запрашивается с `time_from`/`time_to`
- **THEN** `event_count` и счётчики учитывают только события с `start_time` в полуинтервале
- **AND** события без `start_time` не попадают в отфильтрованную сводку

#### Scenario: Сортировка по ошибкам

- **GIVEN** SPID 728 имеет 12 событий с ошибками, остальные SPID — ни одного
- **WHEN** сводка запрашивается с `sort_by=error_count`
- **THEN** SPID 728 первый в списке

#### Scenario: События без времени

- **GIVEN** все события SPID не содержат `start_time`
- **WHEN** запрашивается сводка SPID
- **THEN** `first_time` и `last_time` равны `null`
- **AND** `event_count` и остальные счётчики содержат фактические значения

### Requirement: EnrichAggregates для агрегаций

Система SHALL обогащать каждую процедуру итоговой агрегации доступными данными из индекса, включая путь к исходному файлу, независимо от позиции первого события процедуры в TRC-сессии. Saved-session и file-mode SHALL обеспечивать одинаковое покрытие enrichment для одинакового набора агрегированных процедур.

#### Scenario: Enrichment агрегации из sample-событий

- **GIVEN** итоговая агрегация содержит `MyProc`, а её первое событие находится после первой тысячи событий сессии
- **AND** индекс CodeBase содержит исходный файл `MyProc`
- **WHEN** выполняется агрегация процедур
- **THEN** агрегат `MyProc` содержит путь к исходному файлу

#### Scenario: Одинаковый enrichment в двух режимах

- **GIVEN** одна TRC-сессия доступна как сохранённая сессия и как исходный файл
- **WHEN** агрегация процедур выполняется в обоих режимах
- **THEN** одинаковые процедуры получают одинаковые доступные данные enrichment

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

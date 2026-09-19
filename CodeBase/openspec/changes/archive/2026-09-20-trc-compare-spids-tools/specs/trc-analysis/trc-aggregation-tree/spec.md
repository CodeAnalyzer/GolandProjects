# Delta: trc-aggregation-tree

## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: RTI инструменты

Система SHALL предоставлять MCP-инструменты для RTI-анализа: `codebase_rti_parse`, `codebase_rti_list`, `codebase_rti_summary`, `codebase_rti_tree`, `codebase_rti_errors`, `codebase_rti_slow`, `codebase_rti_details`, `codebase_rti_blog`, `codebase_rti_client_tree`, `codebase_rti_timeline`, `codebase_rti_delete`, `codebase_rti_prune`.

`codebase_rti_parse` SHALL принимать optional-аргумент `name` для задания `session_name`; при отсутствии `name` используется имя разобранного файла. `codebase_rti_delete` SHALL принимать `session_id` либо `name` (точное совпадение): аргументы взаимоисключающие, удаление по имени удаляет все совпадающие сессии и возвращает `deleted_count` и список `session_ids`. `codebase_rti_prune` SHALL принимать `keep_last` либо `keep_hours` (взаимоисключающие); `keep_hours` MUST быть больше нуля и удаляет сессии с `parsed_at` старше указанного числа часов; результат содержит `deleted_count`, `kept_last`, `kept_hours` и `cutoff`. Инструменты `codebase_rti_list` и `codebase_rti_summary` SHALL возвращать `session_name` в данных сессии.

#### Scenario: RTI summary через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая RTI-сессия с id 42
- **WHEN** вызывается `codebase_rti_summary` с `session_id = 42`
- **THEN** возвращена сводка сессии в формате доменных данных

#### Scenario: RTI parse с именем сессии

- **GIVEN** RTI-файл и запущенный MCP-сервер
- **WHEN** `codebase_rti_parse` вызывается с `file_path` и `name = "Проблемный расчёт"`
- **THEN** возвращён `session_id` сессии с `session_name = "Проблемный расчёт"`

#### Scenario: RTI delete по имени

- **GIVEN** сохранённые RTI-сессии с `session_name = "trace.rti"`
- **WHEN** вызывается `codebase_rti_delete` с `name = "trace.rti"`
- **THEN** все совпадающие сессии удалены
- **AND** ответ содержит `deleted_count` и `session_ids`

#### Scenario: Конфликт параметров RTI delete

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_rti_delete` вызывается одновременно с `session_id` и `name`
- **THEN** возвращена ошибка о взаимоисключающих параметрах

#### Scenario: RTI prune по часам

- **GIVEN** сохранённые RTI-сессии разного возраста
- **WHEN** вызывается `codebase_rti_prune` с `keep_hours = 24`
- **THEN** удалены сессии старше 24 часов по `parsed_at`
- **AND** ответ содержит `deleted_count`, `kept_hours = 24` и `cutoff`

#### Scenario: Некорректный keep_hours

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_rti_prune` вызывается с `keep_hours = 0` или одновременно с `keep_last`
- **THEN** возвращена ошибка валидации, сессии не удаляются

### Requirement: TRC инструменты

Система SHALL предоставлять MCP-инструменты для TRC-анализа: `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events`, `codebase_trc_procedures`, `codebase_trc_compare_procedures`, `codebase_trc_spids`, `codebase_trc_tree`, `codebase_trc_errors`, `codebase_trc_slow`, `codebase_trc_delete`, `codebase_trc_prune`. Каждый TRC-обработчик SHALL возвращать клиенту ошибку разбора переданного optional-аргумента вместо подстановки его нулевого значения.

`codebase_trc_parse` SHALL принимать optional-аргумент `name` для задания `session_name`; при отсутствии `name` используется имя разобранного файла. `codebase_trc_delete` SHALL принимать `session_id` либо `name` (точное совпадение): аргументы взаимоисключающие, удаление по имени удаляет все совпадающие сессии и возвращает `deleted_count` и список `session_ids`. `codebase_trc_prune` SHALL принимать `keep_last` либо `keep_hours` (взаимоисключающие); `keep_hours` MUST быть больше нуля и удаляет сессии с `parsed_at` старше указанного числа часов; результат содержит `deleted_count`, `kept_last`, `kept_hours` и `cutoff`. Инструменты `codebase_trc_list` и `codebase_trc_summary` SHALL возвращать `session_name` в данных сессии.

`codebase_trc_events` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `time_from`, `time_to` (RFC3339), `min_duration_ms`, `after_id`, `format=full|short` и SHALL возвращать полное `filtered_count` до `limit`, фактический `returned_count`, `has_more` и `next_after_id`. Существующие скалярные `spid` и `event_name` SHALL сохраняться у `codebase_trc_events` только как legacy-алиасы и нормализоваться в массивы из одного элемента; одновременная передача legacy-алиаса и соответствующего массива SHALL возвращать ошибку.

`codebase_trc_procedures` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `top`, `sort_by` и `group_by_spid`; скалярные alias-параметры для него SHALL NOT вводиться. `codebase_trc_procedures` SHALL возвращать enrichment и по умолчанию (без `event_names`) агрегировать только завершённые вызовы `SP:Completed`.

`codebase_trc_compare_procedures` SHALL принимать параметры `focus_spid`, `compare_spids` (integer[]), `event_names` (string[]), `top` (default 20, max 100) и `sort_by` по контракту сравнения focus/peer SPID; скалярные alias-параметры SHALL NOT вводиться. `codebase_trc_spids` SHALL принимать параметры `spids` (integer[]), `time_from`, `time_to` (RFC3339), `sort_by` и `limit`. Оба инструмента SHALL возвращать предупреждения `warnings` по своим контрактам, включая предупреждение о недоступности метрик длительности при `max(duration_ms) = 0` по сессии.

Array-параметры SHALL приниматься только массивами соответствующего типа со строгой валидацией каждого элемента: ошибка SHALL содержать имя аргумента и индекс некорректного элемента; пустые строки, дробные значения и переполнение SHALL NOT игнорироваться.

#### Scenario: TRC procedures через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** вызывается `codebase_trc_procedures` с `session_id = 42`
- **THEN** возвращена агрегация завершённых вызовов процедур с enrichment

#### Scenario: TRC parse с именем сессии

- **GIVEN** .trc файл и запущенный MCP-сервер
- **WHEN** `codebase_trc_parse` вызывается с `file_path` и `name = "Проблемный SPID"`
- **THEN** возвращён `session_id` сессии с `session_name = "Проблемный SPID"`

#### Scenario: TRC delete по имени

- **GIVEN** сохранённые TRC-сессии с `session_name = "trace.trc"`
- **WHEN** вызывается `codebase_trc_delete` с `name = "trace.trc"`
- **THEN** все совпадающие сессии удалены
- **AND** ответ содержит `deleted_count` и `session_ids`

#### Scenario: Конфликт параметров TRC delete

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_delete` вызывается одновременно с `session_id` и `name`
- **THEN** возвращена ошибка о взаимоисключающих параметрах

#### Scenario: TRC prune по часам

- **GIVEN** сохранённые TRC-сессии разного возраста
- **WHEN** вызывается `codebase_trc_prune` с `keep_hours = 24`
- **THEN** удалены сессии старше 24 часов по `parsed_at`
- **AND** ответ содержит `deleted_count`, `kept_hours = 24` и `cutoff`

#### Scenario: Некорректный keep_hours TRC

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_prune` вызывается с `keep_hours = 0` или одновременно с `keep_last`
- **THEN** возвращена ошибка валидации, сессии не удаляются

#### Scenario: Некорректный optional-аргумент TRC-инструмента

- **GIVEN** запущенный MCP-сервер
- **WHEN** TRC-инструмент вызывается с optional-аргументом неверного типа
- **THEN** инструмент возвращает ошибку с именем некорректного аргумента
- **AND** обработчик не выполняет запрос с нулевым значением вместо переданного аргумента

#### Scenario: Счётчики ограниченной выдачи TRC events

- **GIVEN** фильтру TRC events соответствуют 1500 событий
- **WHEN** вызывается `codebase_trc_events` с `limit=1000`
- **THEN** ответ содержит `filtered_count=1500` и `returned_count=1000`

#### Scenario: Нормализация legacy-алиасов events

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** `codebase_trc_events` вызывается с `spid=728`
- **THEN** фильтрация эквивалентна вызову с `spids=[728]`

#### Scenario: Конфликт scalar и array параметров

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается одновременно с `event_name` и `event_names`
- **THEN** возвращена ошибка с предложением использовать `event_names`

#### Scenario: Некорректный элемент массива SPID

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается с `spids=[728, -1]`
- **THEN** возвращена ошибка, содержащая имя аргумента `spids` и индекс некорректного элемента
- **AND** запрос к данным не выполняется

#### Scenario: Массивы и время в procedures через MCP

- **GIVEN** сохранённая TRC-сессия содержит события SPID 728 и 700
- **WHEN** вызывается `codebase_trc_procedures` с `session_id`, `spids=[728, 700]` и `group_by_spid=true`
- **THEN** возвращены агрегаты `(spid, procedure)` для указанных SPID

#### Scenario: Сравнение процедур через MCP

- **GIVEN** сохранённая TRC-сессия, focus SPID 728 и peers 700/179 содержат вызовы общих процедур
- **WHEN** вызывается `codebase_trc_compare_procedures` с `session_id`, `focus_spid=728` и `compare_spids=[700, 179]`
- **THEN** возвращён Top-N процедур по статистике focus с метриками каждого peer, `peer_combined` и ratios
- **AND** ответ содержит warnings о completed-only и elapsed ≠ wall-clock

#### Scenario: Сводка SPID через MCP

- **GIVEN** сохранённая TRC-сессия содержит события нескольких SPID
- **WHEN** вызывается `codebase_trc_spids` с `session_id` и `sort_by=event_count`
- **THEN** возвращена сводка по каждому SPID без выгрузки сырых событий
- **AND** порядок соответствует `sort_by`

#### Scenario: Валидация compare-параметров через MCP

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_compare_procedures` вызывается с `compare_spids=[728]` и `focus_spid=728`
- **THEN** возвращена ошибка: peer-список опустел после удаления focus
- **AND** запрос к данным не выполняется

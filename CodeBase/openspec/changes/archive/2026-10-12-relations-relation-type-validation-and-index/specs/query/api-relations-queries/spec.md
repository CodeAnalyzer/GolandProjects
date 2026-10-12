## ADDED Requirements

### Requirement: Строгая валидация фильтров графа связей

Система SHALL валидировать значения фильтров `relation_type`, `source_type` и `target_type` команды `query relations` / инструмента `codebase_query_relations` до обращения к БД. `relation_type` SHALL приниматься только из канонического словаря типов связей (calls_procedure, dispatches_to, dispatches_to_subscriber, selects_from, inserts_into, updates, deletes_from, references_table, executes_query, builds_query, implements_contract, executes_contract, publishes_event, subscribes_to_event, has_field, has_param, uses_param, change_modifies, depends_on_capability, references_code, usecase_involves). `source_type` и `target_type` SHALL приниматься только из словаря типов сущностей графа (sql_procedure, sql_table, query_fragment, pas_method, dfm_form, js_function, smf_instrument, api_contract, report_form, report_field, report_param, vb_function, spec_capability, spec_change, spec_requirement, spec_scenario, spec_usecase). Неизвестное значение SHALL приводить к ошибке со списком валидных значений и SHALL NOT приводить к запросу к таблице `relations`. Пустое значение SHALL означать отсутствие соответствующего фильтра.

#### Scenario: Неизвестный тип связи

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --relation-type __no_such_relation__`
- **THEN** возвращена ошибка валидации со списком валидных типов связи
- **AND** запрос к таблице `relations` не выполняется

#### Scenario: Неизвестный тип сущности

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-type __no_such_type__`
- **THEN** возвращена ошибка валидации со списком валидных типов сущностей
- **AND** запрос к таблице `relations` не выполняется

#### Scenario: Спецификационный тип связи принимается

- **GIVEN** проиндексированный проект со связями `depends_on_capability` между capability
- **WHEN** выполняется `query relations --relation-type depends_on_capability --limit 10`
- **THEN** возвращены связи `depends_on_capability`
- **AND** ошибка валидации не возникает

#### Scenario: Валидные фильтры работают без изменений

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-type sql_procedure --target-type sql_table --relation-type inserts_into --limit 10`
- **THEN** возвращены до 10 связей `inserts_into` от процедур к таблицам
- **AND** ошибка валидации не возникает

#### Scenario: Пустой фильтр означает отсутствие ограничения

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-name MyProc` без `relation_type`, `source_type`, `target_type`
- **THEN** фильтрация выполняется без ошибок валидации типа

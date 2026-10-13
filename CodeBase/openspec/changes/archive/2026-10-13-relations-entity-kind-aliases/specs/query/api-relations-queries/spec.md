## MODIFIED Requirements

### Requirement: Запрос к графу связей

Система SHALL предоставлять команду `query relations` для поиска связей в графе с фильтрацией по source_type, source_name, target_type, target_name и relation_type. Значения source_type и target_type SHALL нормализоваться: kind-типы API-контрактов (`service`, `event`, `callback_event`, `used_service`) SHALL приниматься как алиасы канонического типа `api_contract`, канонические типы словаря графа связей SHALL проходить без изменений. Для фильтра по имени без явного типа система SHALL разрешать имя в набор пар «тип сущности + идентификатор» через типизированные lookup'ы по индексированным именам и SHALL выбирать связи по этим парам. Разрешение имени SHALL покрывать все типы сущностей словаря графа связей (sql_procedure, sql_table, query_fragment, pas_method, dfm_form, js_function, smf_instrument, api_contract, report_form, report_field, report_param, vb_function, spec_capability, spec_change, spec_requirement, spec_scenario, spec_usecase), а не только их подмножество. Сопоставление имени SHALL быть exact-first: сначала точное совпадение без учёта регистра, и только при его отсутствии — подстрочное. При одновременном задании source_name и target_name результат SHALL содержать только связи, у которых source соответствует первому имени AND target — второму. Фильтрация по имени SHALL опираться на индексированный доступ и SHALL NOT приводить к полному сканированию таблицы связей.

#### Scenario: Фильтрация по source

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-type sql_procedure --source-name API_ --limit 100`
- **THEN** возвращены до 100 relations, исходящих от процедур с именем, начинающимся на `API_`

#### Scenario: Фильтрация по target и relation_type

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --target-type sql_table --target-name tContract --relation-type selects_from`
- **THEN** возвращены все relations `selects_from` к таблице `tContract`

#### Scenario: Точное совпадение имени приоритетно над подстрокой

- **GIVEN** проиндексированный проект с процедурами `MyProc` и `MyProcExtended`, где `MyProc` имеет связи
- **WHEN** выполняется `query relations --source-name MyProc`
- **THEN** возвращены связи только процедуры `MyProc`
- **AND** `MyProcExtended` не участвует в результате

#### Scenario: Подстрочный фолбэк при отсутствии точного совпадения

- **GIVEN** проиндексированный проект без сущности с точным именем `yPro`, но с процедурой `MyProc`
- **WHEN** выполняется `query relations --source-name yPro`
- **THEN** возвращены связи сущностей, чьё имя содержит `yPro`

#### Scenario: Имя без типа разрешается по всем типам сущностей

- **GIVEN** проиндексированный проект, где `pPortObject` — таблица
- **WHEN** выполняется `query relations --target-name pPortObject`
- **THEN** цель `pPortObject` найдена как таблица
- **AND** возвращены связи, указывающие на неё

#### Scenario: Пересечение при двух именах

- **GIVEN** проиндексированный проект со связью между процедурой `ADC_FillDataForFAO` и таблицей `pPortObject`
- **WHEN** выполняется `query relations --source-name ADC_FillDataForFAO --target-name pPortObject`
- **THEN** возвращены только связи, у которых source совпал с `ADC_FillDataForFAO` AND target — с `pPortObject`

#### Scenario: Kind-тип контракта нормализуется в api_contract

- **GIVEN** проиндексированный проект с service-контрактом `API_ObjectEvent_MassCreate`
- **WHEN** выполняется `query relations --source-type service --source-name API_ObjectEvent_MassCreate`
- **THEN** запрос выполняется без ошибки валидации
- **AND** результат эквивалентен `--source-type api_contract`

#### Scenario: Kind-тип target-контракта нормализуется в api_contract

- **GIVEN** проиндексированный проект с event-контрактом `OnAfterCCes_MassCreate`, публикуемым процедурой
- **WHEN** выполняется `query relations --target-type event --target-name OnAfterCCes_MassCreate --relation-type publishes_event`
- **THEN** возвращены связи `publishes_event`, указывающие на этот контракт
- **AND** ошибка валидации не возникает

#### Scenario: Разрешение имени для типа dfm_form

- **GIVEN** проиндексированный проект с DFM-формой `FormMain`, участвующей в графе связей
- **WHEN** выполняется `query relations --source-type dfm_form --source-name FormMain`
- **THEN** имя разрешено в пару «dfm_form + идентификатор» и возвращены соответствующие связи
- **AND** ошибка `unsupported relation entity type for name filter` не возникает

#### Scenario: Разрешение имени для спецификационного типа

- **GIVEN** проиндексированный проект с capability `query-symbol-search`, участвующей в связях `depends_on_capability`
- **WHEN** выполняется `query relations --source-type spec_capability --source-name query-symbol-search`
- **THEN** имя разрешено в пару «spec_capability + идентификатор» и возвращены соответствующие связи
- **AND** ошибка `unsupported relation entity type for name filter` не возникает

### Requirement: Строгая валидация фильтров графа связей

Система SHALL валидировать значения фильтров `relation_type`, `source_type` и `target_type` команды `query relations` / инструмента `codebase_query_relations` до обращения к БД. `relation_type` SHALL приниматься только из канонического словаря типов связей (calls_procedure, dispatches_to, dispatches_to_subscriber, selects_from, inserts_into, updates, deletes_from, references_table, executes_query, builds_query, implements_contract, executes_contract, publishes_event, subscribes_to_event, has_field, has_param, uses_param, change_modifies, depends_on_capability, references_code, usecase_involves). `source_type` и `target_type` SHALL приниматься из словаря типов сущностей графа (sql_procedure, sql_table, query_fragment, pas_method, dfm_form, js_function, smf_instrument, api_contract, report_form, report_field, report_param, vb_function, spec_capability, spec_change, spec_requirement, spec_scenario, spec_usecase). Дополнительно `source_type` и `target_type` SHALL принимать kind-типы API-контрактов (`service`, `event`, `callback_event`, `used_service`) как алиасы, нормализуемые в `api_contract` до проверки. Неизвестное значение SHALL приводить к ошибке со списком валидных значений и SHALL NOT приводить к запросу к таблице `relations`. Пустое значение SHALL означать отсутствие соответствующего фильтра.

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

#### Scenario: Kind-тип API-контракта принимается при валидации

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-type service --limit 10`
- **THEN** ошибка валидации не возникает
- **AND** фильтр применяется как `api_contract`

#### Scenario: Все канонические kind-типы принимаются

- **GIVEN** проиндексированный проект
- **WHEN** поочерёдно выполняются запросы с `--source-type` равным `service`, `event`, `callback_event`, `used_service`
- **THEN** каждый запрос проходит валидацию без ошибки
- **AND** результат эквивалентен фильтру `api_contract`

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

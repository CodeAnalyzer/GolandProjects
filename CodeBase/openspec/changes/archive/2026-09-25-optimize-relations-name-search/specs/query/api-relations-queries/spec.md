## MODIFIED Requirements

### Requirement: Запрос к графу связей

Система SHALL предоставлять команду `query relations` для поиска связей в графе с фильтрацией по source_type, source_name, target_type, target_name и relation_type. Для фильтра по имени без явного типа система SHALL разрешать имя в набор пар «тип сущности + идентификатор» через типизированные lookup'ы по индексированным именам и SHALL выбирать связи по этим парам. Сопоставление имени SHALL быть exact-first: сначала точное совпадение без учёта регистра, и только при его отсутствии — подстрочное. При одновременном задании source_name и target_name результат SHALL содержать только связи, у которых source соответствует первому имени AND target — второму. Фильтрация по имени SHALL опираться на индексированный доступ и SHALL NOT приводить к полному сканированию таблицы связей.

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

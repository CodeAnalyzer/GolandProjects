# Дельта: infrastructure/database-schema

## ADDED Requirements

### Requirement: Сброс таблиц кодовой базы (ResetCodebaseTables)

Store SHALL предоставлять метод `ResetCodebaseTables(ctx)`, очищающий одним statement `TRUNCATE TABLE ... CASCADE` все таблицы, заполняемые парсерами кодовой базы: файлы и символы (`files`, `symbols`, `ds_products`, `stats_snapshot`); SQL (`sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`); PAS/H/JS/SMF/DFM (`pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `h_files_defines`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`); отчёты (`report_forms`, `report_fields`, `report_params`, `vb_functions`); фрагменты/include (`query_fragments`, `include_directives`); API XML (`api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`); связи и retcode (`relations`, `ds_return_codes`); спеки (`spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`). Метод SHALL сохранять таблицы анализаторов и служебные: `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`, `trc_sessions`, `trc_events`, `schema_migrations`, `scan_runs`. Полный список усекаемых таблиц фиксируется тестом; если у сохраняемых таблиц появятся FK на усекаемые, тест SHALL это обнаружить. Метод идемпотентен и безопасен для пустых таблиц.

#### Scenario: После сброса таблицы кодовой базы пусты, анализаторы не тронуты

- **GIVEN** БД с заполненным индексом и сохранёнными RTI/TRC-сессиями
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** все таблицы кодовой базы из списка усечения пусты
- **AND** строки `rti_sessions`, `rti_calls`, `rti_client_events`, `trc_sessions`, `trc_events` остались на месте
- **AND** `schema_migrations` не изменилась

#### Scenario: История scan_runs сохраняется

- **GIVEN** БД с несколькими завершёнными прогонами в `scan_runs` и связанными записями в `files`
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** записи `files` удалены
- **AND** строки истории `scan_runs` остались на месте

#### Scenario: Идемпотентность на пустой БД

- **GIVEN** БД после `InitSchema` без данных
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** метод завершается без ошибок
- **AND** повторный вызов также завершается без ошибок

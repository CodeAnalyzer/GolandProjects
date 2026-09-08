## MODIFIED Requirements

### Requirement: Таблицы сущностей

Система SHALL создавать таблицы для всех типов сущностей: `files`, `scan_runs`, `ds_products`, `sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`, `pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`, `report_forms`, `report_fields`, `report_params`, `vb_functions`, `h_files_defines`, `api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`, `symbols`, `relations`, `query_fragments`, `include_directives`, а также таблицы спек: `spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`.

#### Scenario: Создание таблицы процедур
- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** таблица `sql_procedures` создана с колонками: id, file_id, procedure_name, body, start_line, end_line, ...

#### Scenario: Каталог продуктов и JS-константы
- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `ds_products` (каталог продуктов Diasoft для review `foreign*`) и `js_constants` (константы из JS, попадающие в `symbols` как `constant`)

#### Scenario: Создание таблиц спек
- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `spec_configs`, `spec_capabilities` (parent_id, title, purpose, notes, related_code, метрики покрытия), `spec_requirements`, `spec_scenarios` (given/when/then), `spec_usecases` (+ source_dir, usecase_kind, page_id), `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, staging `spec_code_mentions`, а также `spec_vocab` и `spec_embeddings` полнотекстового слоя

## ADDED Requirements

### Requirement: Сущности спек с привязкой к продукту

Система SHALL создавать таблицы спек по общей модели сущностей: каждая с FK `file_id` REFERENCES files ON DELETE CASCADE (повторная индексация файла каскадно пересоздаёт его сущности спек); `spec_configs` и `spec_capabilities` дополнительно несут `ds_product_id`. `spec_capabilities.parent_id` ссылается на `spec_capabilities` (ON DELETE SET NULL) для иерархии вложенных директорий; `spec_requirements.capability_id`, `spec_scenarios.requirement_id`, `spec_usecase_steps.usecase_id`, `spec_change_delta.change_id` — обязательные FK с CASCADE. Связи между сущностями спек и с кодом хранятся ТОЛЬКО в существующей полиморфной `relations` (новые relation_type: `depends_on_capability`, `usecase_involves`, `references_code`, `change_modifies`); иерархия, выражаемая FK, в relations НЕ дублируется.

#### Scenario: Каскадное обновление spec.md
- **GIVEN** ранее проиндексированный `spec.md` с capability, требованиями и сценариями
- **WHEN** файл переиндексируется (новый file_id)
- **THEN** старые записи spec_capabilities/spec_requirements/spec_scenarios удалены каскадно по file_id, новые вставлены

#### Scenario: Иерархия через parent_id
- **GIVEN** capability `FORMS/0409120/usage` внутри контейнера `FORMS/0409120`
- **WHEN** выполняется запрос иерархии
- **THEN** связь ребёнок→родитель читается из `parent_id` без обращения к relations

### Requirement: Полнотекстовые индексы спек

Система SHALL создавать для полнотекстового слоя спек: GIN-индексы на `search_vector` (tsvector, конфигурация 'russian') таблиц `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`; GIN-индекс pg_trgm на текстовых полях спек для поиска технических идентификаторов; btree-индексы на `spec_capabilities (LOWER(capability_name))`, `(spec_config_id)`, `(parent_id)`, `spec_requirements (capability_id)`, `spec_scenarios (requirement_id)`, `spec_usecases (spec_config_id)`, `spec_usecase_steps (usecase_id, flow_kind, step_order)`, `spec_code_mentions (LOWER(mention_name))`, `(source_type, source_id)`, `spec_change_delta (change_id)`, `spec_vocab (term)` (уникальный), `spec_embeddings (spec_id, embed_level)`. Новые relation_type покрываются существующими составными индексами `relations (source_type, source_id)` / `(target_type, target_id)` — отдельный индекс под relation_type не создаётся.

#### Scenario: GIN-индексы полнотекста
- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы GIN-индексы search_vector на четырёх таблицах спек и pg_trgm-индекс для технических идентификаторов

#### Scenario: Уникальность словаря
- **GIVEN** повторная вставка термина `CON_STP_MassAccrual` в `spec_vocab`
- **WHEN** выполняется batch insert словаря
- **THEN** дубликат не создаётся (уникальный индекс по term)

### Requirement: Batch insert для сущностей спек

Система SHALL загружать сущности спек batch insert (COPY IN, `pq.CopyIn`) по образцу существующих сущностей: `BatchInsertSpecCapabilities`, `BatchInsertSpecRequirements`, `BatchInsertSpecScenarios`, `BatchInsertSpecUsecases` (+steps), `BatchInsertSpecChanges` (+delta), `BatchInsertSpecCodeMentions`, `BatchInsertSpecVocab`, `BatchInsertSpecEmbeddings`. Размер batch — общий `batch_insert_size`. tsvector-колонки `search_vector` заполняются при вставке через `to_tsvector('russian', …)` от конкатенации весовых полей.

#### Scenario: Массовая загрузка требований
- **GIVEN** 60 000 распарсенных требований
- **WHEN** выполняется `BatchInsertSpecRequirements`
- **THEN** требования загружены через COPY IN батчами, search_vector заполнен для каждой записи

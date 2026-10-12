## MODIFIED Requirements

### Requirement: Индексы для производительности

Система SHALL создавать индексы для оптимизации запросов: GIN-индекс `pg_trgm` для `query_fragments.query_text`, составные индексы для часто используемых запросов, индексы на `session_id` для RTI/TRC таблиц, составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)` для JOIN-запросов, резолвящих сущности из `relations.target_id` через unified-индекс.

Для TRC-событий система SHALL создавать составной индекс `idx_trc_events_session_spid_id` на `trc_events(session_id, spid, id)`, поддерживающий keyset-пагинацию и фильтрацию событий по одному или нескольким SPID. `InitSchema` SHALL удалять устаревший индекс `idx_trc_events_session_spid (session_id, spid)` как префикс нового индекса по существующему паттерну legacy-drop.

Для графа связей система SHALL создавать два индекса на `relations`: `idx_relations_relation_type_id` на `(relation_type, id DESC)` для запросов с фильтром только по типу связи и `idx_relations_source_type_relation_id` на `(source_type, relation_type, id DESC)` для комбинированного фильтра `source_type + relation_type` с сортировкой `ORDER BY id DESC`. Существующие составные индексы `(source_type, source_id)`, `(target_type, target_id)`, `(source_type, relation_type, source_id)`, `(target_type, relation_type, target_id)` SHALL сохраняться. `InitSchema` SHALL сохранять легаси-`DROP INDEX IF EXISTS idx_relations_relation_type` как идемпотентную чистку старых БД; имя нового индекса отличается и этой чисткой не затрагивается.

#### Scenario: GIN-индекс для полнотекстового поиска

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан GIN-индекс `pg_trgm` на `query_fragments.query_text`

#### Scenario: Индекс на symbols для JOIN по entity_id

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)`
- **AND** JOIN вида `symbols s ON s.entity_id = r.target_id AND s.entity_type = $1` использует этот индекс

#### Scenario: Составной индекс SPID-пагинации TRC

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан индекс `idx_trc_events_session_spid_id` на `trc_events(session_id, spid, id)`
- **AND** устаревший индекс `idx_trc_events_session_spid` удалён

#### Scenario: Пагинация по SPID использует новый индекс

- **GIVEN** БД с сохранённой TRC-сессией
- **WHEN** выполняется постраничный запрос событий одного или нескольких SPID с курсором по `id`
- **THEN** план запроса использует `idx_trc_events_session_spid_id`

#### Scenario: Индексы графа связей по типу связи

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы индексы `idx_relations_relation_type_id` на `relations(relation_type, id DESC)` и `idx_relations_source_type_relation_id` на `relations(source_type, relation_type, id DESC)`
- **AND** легаси-индекс `idx_relations_relation_type` отсутствует

### Requirement: Полнотекстовые индексы спек

Система SHALL создавать для полнотекстового слоя спек: GIN-индексы на `search_vector` (tsvector, конфигурация 'russian') таблиц `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`; GIN-индекс pg_trgm на текстовых полях спек для поиска технических идентификаторов; btree-индексы на `spec_capabilities (LOWER(capability_name))`, `(spec_config_id)`, `(parent_id)`, `spec_requirements (capability_id)`, `spec_scenarios (requirement_id)`, `spec_usecases (spec_config_id)`, `spec_usecase_steps (usecase_id, flow_kind, step_order)`, `spec_code_mentions (LOWER(mention_name))`, `(source_type, source_id)`, `spec_change_delta (change_id)`, `spec_vocab (term)` (уникальный), `spec_embeddings (spec_id, embed_level)`. Новые relation_type (`depends_on_capability`, `usecase_involves`, `references_code`, `change_modifies`) не требуют отдельных индексов спек — доступ к графу связей обеспечивается индексами таблицы `relations` (см. требование «Индексы для производительности»).

#### Scenario: GIN-индексы полнотекста

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы GIN-индексы search_vector на четырёх таблицах спек и pg_trgm-индекс для технических идентификаторов

#### Scenario: Уникальность словаря

- **GIVEN** повторная вставка термина `CON_STP_MassAccrual` в `spec_vocab`
- **WHEN** выполняется batch insert словаря
- **THEN** дубликат не создаётся (уникальный индекс по term)

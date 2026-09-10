## MODIFIED Requirements

### Requirement: Индексы для производительности

Система SHALL создавать индексы для оптимизации запросов: GIN-индекс `pg_trgm` для `query_fragments.query_text`, составные индексы для часто используемых запросов, индексы на `session_id` для RTI/TRC таблиц, составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)` для JOIN-запросов, резолвящих сущности из `relations.target_id` через unified-индекс.

#### Scenario: GIN-индекс для полнотекстового поиска

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан GIN-индекс `pg_trgm` на `query_fragments.query_text`

#### Scenario: Индекс на symbols для JOIN по entity_id

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)`
- **AND** JOIN вида `symbols s ON s.entity_id = r.target_id AND s.entity_type = $1` использует этот индекс

# Delta: database-schema

## MODIFIED Requirements

### Requirement: Индексы для производительности

Система SHALL создавать индексы для оптимизации запросов: GIN-индекс `pg_trgm` для `query_fragments.query_text`, составные индексы для часто используемых запросов, индексы на `session_id` для RTI/TRC таблиц, составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)` для JOIN-запросов, резолвящих сущности из `relations.target_id` через unified-индекс.

Для TRC-событий система SHALL создавать составной индекс `idx_trc_events_session_spid_id` на `trc_events(session_id, spid, id)`, поддерживающий keyset-пагинацию и фильтрацию событий по одному или нескольким SPID. `InitSchema` SHALL удалять устаревший индекс `idx_trc_events_session_spid (session_id, spid)` как префикс нового индекса по существующему паттерну legacy-drop.

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

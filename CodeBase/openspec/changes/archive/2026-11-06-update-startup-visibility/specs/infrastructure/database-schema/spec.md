# Дельта: database-schema (индекс на files(path))

## MODIFIED Requirements

### Requirement: Индексы для производительности

Система SHALL создавать индексы для оптимизации запросов: GIN-индекс `pg_trgm` для `query_fragments.query_text`, составные индексы для часто используемых запросов, индексы на `session_id` для RTI/TRC таблиц, составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)` для JOIN-запросов, резолвящих сущности из `relations.target_id` через unified-индекс.

Для TRC-событий система SHALL создавать составной индекс `idx_trc_events_session_spid_id` на `trc_events(session_id, spid, id)`, поддерживающий keyset-пагинацию и фильтрацию событий по одному или нескольким SPID. `InitSchema` SHALL удалять устаревший индекс `idx_trc_events_session_spid (session_id, spid)` как префикс нового индекса по существующему паттерну legacy-drop.

Для графа связей система SHALL создавать два индекса на `relations`: `idx_relations_relation_type_id` на `(relation_type, id DESC)` для запросов с фильтром только по типу связи и `idx_relations_source_type_relation_id` на `(source_type, relation_type, id DESC)` для комбинированного фильтра `source_type + relation_type` с сортировкой `ORDER BY id DESC`. Существующие составные индексы `(source_type, source_id)`, `(target_type, target_id)`, `(source_type, relation_type, source_id)`, `(target_type, relation_type, target_id)` SHALL сохраняться. `InitSchema` SHALL сохранять легаси-`DROP INDEX IF EXISTS idx_relations_relation_type` как идемпотентную чистку старых БД; имя нового индекса отличается и этой чисткой не затрагивается.

Для таблицы `files` система SHALL создавать индекс `idx_files_path` на
`files(path)` с opclass `text_pattern_ops` (колонка объявлена `TEXT`);
предикаты pre-walk запросов инкрементального update (загрузка предыдущего
состояния, поиск дубликатов поколений, backfill привязок источников) MUST быть
sargable (экранированный LIKE-префикс, единый хелпер экранирования).
Узкопрефиксные запросы с план-константным (параметризованным) LIKE-паттерном —
`update --path <подпуть>` (загрузка предыдущего состояния и поиск дубликатов
поколений по подпути) — MUST использовать индекс вместо полного
последовательного скана таблицы. Полнокорневая загрузка предыдущего состояния
(LIKE-префикс покрывает все строки корня) и bulk-backfill привязок источников
(один UPDATE с VALUES-джойном: LIKE-паттерн вычисляется из колонки джойна и
не даёт индексного диапазона; префиксы источников покрывают всё дерево)
допускают Seq Scan. Добавление индекса SHALL сопровождаться bump версии схемы:
существующие БД получают индекс штатной миграцией при следующем `InitSchema`
(update); до первого `update` после замены бинарника read-only команды
возвращают ошибку необходимости обновления схемы.

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

#### Scenario: Индекс на files(path) создаётся InitSchema

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создан индекс `idx_files_path` на `files(path)` с opclass `text_pattern_ops`

#### Scenario: Миграция индекса на существующей БД

- **GIVEN** БД предыдущей версии схемы с заполненной таблицей `files`
- **WHEN** выполняется `codebase update`
- **THEN** индекс `idx_files_path` создан миграцией версии схемы
- **AND** узкопрефиксный параметризованный запрос `SELECT ... FROM files WHERE path LIKE '<подпуть update --path>/%'` использует индексный скан по `idx_files_path` (структурная проверка плана при отключённом seq scan)
- **AND** до выполнения этого `update` read-only команды возвращают ошибку необходимости обновления схемы

#### Scenario: Bulk-backfill привязок источников допускает Seq Scan

- **GIVEN** БД со схемой текущей версии и заполненной таблицей `files` под несколькими источниками
- **WHEN** выполняется bulk-backfill привязок (один UPDATE с VALUES-джойном по префиксам источников)
- **THEN** привязываются все строки под префиксами источников с границей разделителя (предикат sargable-вида)
- **AND** план запроса не обязан использовать `idx_files_path`: паттерн LIKE вычисляется из колонки джойна и не даёт индексного диапазона (фактическое поведение зафиксировано канарейка-тестом плана)

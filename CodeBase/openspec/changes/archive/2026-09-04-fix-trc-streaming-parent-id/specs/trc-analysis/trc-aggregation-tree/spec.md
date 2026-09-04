## MODIFIED Requirements

### Requirement: Маппинг parent_id из offset в реальный id при сохранении в БД

Система SHALL вставлять события в `trc_events` через COPY IN с явным `id = baseID + EventIndex`,
где `baseID = COALESCE(MAX(id), 0) + 1 FROM trc_events` определяется перед вставкой,
а `EventIndex` — 0-based порядковый индекс события в потоке (вычисляется `IncrementalParentTracker`).
`parent_id` SHALL содержать `baseID + ParentID` если `ParentID >= 0`, иначе NULL —
это реальный id строки родителя сразу после вставки, без post-insert маппинга.

После вставки всех батчей система SHALL синхронизировать sequence:
`SELECT setval('trc_events_id_seq', (SELECT MAX(id) FROM trc_events))`.

`insertTRCEvents` SHALL NOT выполнять post-insert маппинг parent_id.
Корневые события (`ParentID < 0`) SHALL иметь `parent_id IS NULL`.

#### Scenario: Явный parent_id при COPY IN

- **GIVEN** 3 события: event[0] (root, ParentID=-1), event[1] (child of 0, ParentID=0), event[2] (child of 1, ParentID=1)
- **WHEN** `insertTRCEvents` выполняет COPY IN с `baseID=1000`
- **THEN** event[0] имеет `id=1000`, `parent_id IS NULL` в БД
- **AND** event[1] имеет `id=1001`, `parent_id=1000` (реальный id event[0]) в БД
- **AND** event[2] имеет `id=1002`, `parent_id=1001` (реальный id event[1]) в БД

#### Scenario: Маппинг parent_id после COPY IN

- **GIVEN** 3 события: event[0] (root, ParentID=-1), event[1] (child of 0, ParentID=0), event[2] (child of 1, ParentID=1)
- **WHEN** `insertTRCEvents` завершает COPY IN с явным id
- **THEN** event[0] имеет `parent_id IS NULL` в БД
- **AND** event[1] имеет `parent_id` = реальный `id` event[0] в БД
- **AND** event[2] имеет `parent_id` = реальный `id` event[1] в БД

#### Scenario: Маппинг для большой сессии без таймаута

- **GIVEN** TRC-сессия с 1.3M событий, которые вставлены через COPY IN батчами
- **WHEN** `insertTRCEvents` завершает COPY IN с явным id
- **THEN** post-insert маппинг (MapParentIDs) не выполняется
- **AND** `parent_id` содержит корректные реальные id сразу после вставки

#### Scenario: Стриминг-парсинг файла с >1M событий без post-insert маппинга

- **GIVEN** .trc файл с 1.3M событий, стриминг через `ParseFileToDB` батчами по 50K
- **WHEN** `ParseFileToDB` завершает все батчи COPY IN
- **THEN** `parent_id` в БД содержит корректные реальные id для всех 1.3M событий
- **AND** post-insert маппинг (MapParentIDs) не выполняется
- **AND** общее время вставки не превышает время COPY IN батчей (без дополнительного UPDATE)

#### Scenario: Синхронизация sequence после вставки

- **GIVEN** сессия с 100K событий вставлена с явными id от 5001 до 510000
- **WHEN** вставка завершена
- **THEN** `trc_events_id_seq` синхронизирован с `MAX(id)` через `setval`
- **AND** следующий `nextval('trc_events_id_seq')` возвращает 510001

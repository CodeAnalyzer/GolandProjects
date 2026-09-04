## Why

`mapParentIDs` вызывается внутри `insertTRCEvents`, но в стриминг-режиме
(`ParseFileToDB`) `insertTRCEvents` вызывается многократно батчами по 50K
событий. `SELECT id` возвращает все строки сессии, а `events` — только текущий
батч, что вызывает `id count mismatch` для файлов с >50K событий. Рефакторинг
в `MapParentIDs(ctx, db, sessionID)` исправил логику, но для файла с 1.3M
событий post-insert маппинг (SELECT + COPY IN + UPDATE 1.3M строк) занял
52 минуты — неприемлемо.

## What Changes

- `insertTRCEvents` принимает `baseID` и вставляет явный `id = baseID + EventIndex`
  через COPY IN. `parent_id = baseID + ParentID` (если `ParentID >= 0`, иначе NULL) —
  реальный id строки родителя сразу, без post-insert маппинга
- Удалить `MapParentIDs` и все её вызовы из `SaveSession` и `ParseFileToDB`
- Перед вставкой: `baseID = COALESCE(MAX(id), 0) + 1 FROM trc_events`
- После вставки: `SELECT setval('trc_events_id_seq', (SELECT MAX(id) FROM trc_events))`
- `insertTRCEvents` выполняет только COPY IN с явным id, без Фазы 3 (маппинг)

## Capabilities

### New Capabilities

### Modified Capabilities
- `trc-analysis/trc-aggregation-tree`: MODIFIED requirement "Маппинг parent_id из offset в реальный id при сохранении в БД" — `parent_id` содержит реальный id строки родителя сразу при COPY IN через явный `id = baseID + EventIndex`, без post-insert маппинга

## Impact

- `internal/trc/store.go` — `insertTRCEvents` принимает `baseID`, добавляет `id` в COPY IN, вычисляет `parent_id = baseID + ParentID`. Удалить `MapParentIDs`. `SaveSession` — получить `baseID`, вызвать `setval` после вставки
- `internal/trc/parse_to_db.go` — получить `baseID` перед стримингом, передать в `insertTRCEvents`. Убрать `MapParentIDs`. Вызвать `setval` после
- `internal/trc/store_tree_integration_test.go` — обновить тесты: убрать `MapParentIDs`, проверить `parent_id` сразу после insert

## 1. Явный id в insertTRCEvents

- [x] 1.1 Добавить параметр `baseID int64` в `insertTRCEvents`. Добавить `id` в COPY IN: `id = baseID + int64(ev.EventIndex)`. `parent_id = baseID + int64(ev.ParentID)` если `ParentID >= 0`, иначе NULL. Убрать старую логику offset. Verify: go build.
- [x] 1.2 Удалить функцию `MapParentIDs` из `store.go`. Verify: go vet — нет unused functions, go build.
- [x] 1.3 Добавить helper `getBaseID(ctx, db)`: `SELECT COALESCE(MAX(id), 0) + 1 FROM trc_events`. Добавить helper `syncSequence(ctx, db)`: `SELECT setval('trc_events_id_seq', (SELECT MAX(id) FROM trc_events))`. Verify: go build.

## 2. Интеграция в SaveSession и ParseFileToDB

- [x] 2.1 В `SaveSession`: получить `baseID` перед `insertTRCEvents`, передать как параметр. Вызвать `syncSequence` после. Убрать вызов `MapParentIDs`. Verify: go build.
- [x] 2.2 В `ParseFileToDB`: получить `baseID` перед стримингом, передать в `insertTRCEvents` через `flushBatch`. Вызвать `syncSequence` после всех батчей. Убрать вызов `MapParentIDs`. Verify: go build.

## 3. Тесты

- [x] 3.1 Обновить `TestInsertTRCEvents_ParentIDMapping` — вставить события с `baseID`, проверить `parent_id` сразу после insert без `MapParentIDs`. Verify: go test ./internal/trc/... — PASS.
- [x] 3.2 Обновить `TestMapParentIDs_StreamingMultiBatch` → переименовать в `TestInsertTRCEvents_ExplicitID_MultiBatch` — вставить 2 батча через `insertTRCEvents` с одним `baseID`, проверить `parent_id` (включая cross-batch parent). Verify: go test ./internal/trc/... — PASS.

## 4. Регрессия

- [x] 4.1 Парсинг `Трейс после оптимизации покупки 5.trc` (1.3M событий) через CLI `codebase trc parse`. Verify: без ошибки, total_events > 1M, parent_id корректны (дерево строится).
- [x] 4.2 Парсинг `Трейс после оптимизации покупки 6.trc` (40K событий). Verify: без ошибки, дерево строится.
- [x] 4.3 go vet ./... и go build ./... — чисто. Verify: no errors.
- [x] 4.4 openspec validate --changes — чисто. Verify: no errors.

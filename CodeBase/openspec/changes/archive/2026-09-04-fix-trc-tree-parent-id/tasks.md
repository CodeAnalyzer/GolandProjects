## 1. Post-insert parent_id маппинг

- [x] 1.1 В `insertTRCEvents` (`internal/trc/store.go`) заменить SQL UPDATE с `row_number()` на Go-маппинг: после COPY IN выполнить `SELECT id FROM trc_events WHERE session_id = $1 ORDER BY id`, вычислить `realParentID = ids[ev.ParentID]` в Go, затем `COPY IN` во temp table `(id, parent_id)` + `UPDATE trc_events e SET parent_id = t.parent_id FROM temp t WHERE e.id = t.id`. Проверить: `go build ./...` чисто.
- [x] 1.2 Добавить integration-тест `TestInsertTRCEvents_ParentIDMapping` в `internal/trc/store_tree_integration_test.go`: вставить события с известными ParentID (offset), выполнить insert, проверить что `parent_id` в БД = реальный `id` родителя. Проверить что root-события имеют `parent_id IS NULL`.
- [x] 1.3 Проверить что `ComputeParentIDs` (`internal/trc/tree.go`) не требует изменений — ParentID = -1 для root, 0-based индекс для детей. Без изменений в Go-коде вычисления.

## 2. Упрощение LoadEventsForTree

- [x] 2.1 В `LoadEventsForTree` (`internal/trc/store.go:730`) убрать `numbered` CTE полностью. Заменить recursive CTE на прямой `JOIN tree t ON c.parent_id = t.id` (без `JOIN numbered nc` и `JOIN numbered np`). Anchor CTE оставить без изменений (фильтр по procedure или parent_id IS NULL + Starting/Completed). Проверить: `go build ./...` чисто.
- [x] 2.2 Добавить unit-тест `TestLoadEventsForTree_DirectJoinNoNumbered` в `internal/trc/store_test.go`: создать сессию с деревом из 3 уровней, вызвать `LoadEventsForTree`, проверить корректность дерева и что запрос не использует `numbered` CTE (по плану запроса или по структуре результата).
- [x] 2.3 Добавить тест `TestLoadEventsForTree_LargeSession_NoTimeout`: создать сессию с 10K+ событий на нескольких SPID, вызвать `LoadEventsForTree` с фильтром по SPID, убедиться что выполнение < 5 секунд.

## 3. Регрессионное тестирование

- [x] 3.1 Тест `TestLoadEventsForTree_ProcedureFilter`: создать сессию с процедурами ProcA, ProcB, ProcC, вызвать с `procedure="ProcB"`, проверить что возвращено только поддерево ProcB.
- [x] 3.2 Тест `TestLoadEventsForTree_ParentChildSameSPID`: вставить события с parent-child на разных SPID, убедиться что `LoadEventsForTree` с фильтром по SPID возвращает дерево только для указанного SPID (parent-child всегда на одном SPID).
- [x] 3.3 Тест `TestLoadEventsForTree_RootEventsHaveNullParent`: после маппинга проверить что root-события (ParentID = -1 в срезе) имеют `parent_id IS NULL` в БД и попадают в anchor CTE.
- [x] 3.4 Запустить `go test ./internal/trc/... -count=1` и убедиться что все тесты PASS (после замены SQL UPDATE на Go-маппинг).
- [x] 3.5 Запустить `go vet ./...` и `go build ./...` — чисто (после замены SQL UPDATE на Go-маппинг).

## 4. Валидация OpenSpec

- [x] 4.1 Запустить `openspec validate --change "fix-trc-tree-parent-id"` — без ошибок (после обновления артефактов).

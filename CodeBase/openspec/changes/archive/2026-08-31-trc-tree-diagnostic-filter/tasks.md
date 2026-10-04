## 1. In-memory path: buildSPIDTree

- [x] 1.1 Добавить guard в ветку `default` функции `buildSPIDTree` (`internal/trc/tree.go:201-203`): если `len(stack) == 0` — `continue` (пропустить diagnostic-событие без родителя). Проверить: `go build ./internal/trc/...`
- [x] 1.2 Добавить unit-тест `TestBuildSPIDTree_DiagnosticNoRoot` в `internal/trc/tree_test.go`: события SP:Recompile + SQL:StmtRecompile перед SP:Starting — не появляются в roots. Проверить: `go test ./internal/trc/... -run TestBuildSPIDTree_DiagnosticNoRoot -v`
- [x] 1.3 Добавить unit-тест `TestBuildSPIDTree_DiagnosticInsideCall` в `internal/trc/tree_test.go`: SP:Recompile между SP:Starting и SP:Completed — присутствует как ребёнок узла Starting. Проверить: `go test ./internal/trc/... -run TestBuildSPIDTree_DiagnosticInsideCall -v`

## 2. DB path: LoadEventsForTree

- [x] 2.1 Добавить условие `(e.event_name LIKE '%Starting' OR e.event_name LIKE '%Completed')` в `anchorWhere` в `LoadEventsForTree` (`internal/trc/store.go:723`), когда `procedure` не задан. Проверить: `go build ./internal/trc/...`
- [x] 2.2 Добавить unit-тест в `internal/trc/store_test.go` (или integration-тест в `store_integration_test.go`): diagnostic-события с `parent_id IS NULL` не попадают в дерево при загрузке из БД. Проверить: `go test ./internal/trc/... -run TestLoadEventsForTree -v`

## 3. Regression и сборка

- [x] 3.1 Запустить полный пакет тестов TRC: `go test ./internal/trc/... -count=1` — все тесты PASS
- [x] 3.2 Запустить `go build ./...` и `go vet ./...` — чисто
- [x] 3.3 Проверить на реальной сессии через CLI `trc tree --session 2 --spid 76` — diagnostic-события (SP:Recompile, SQL:StmtRecompile) отсутствуют как корневые узлы, дерево содержит только SP:Completed с процедурами

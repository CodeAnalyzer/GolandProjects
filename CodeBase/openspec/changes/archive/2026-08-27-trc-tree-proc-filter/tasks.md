## 1. Service-слой: TreeParams и ExecuteTree

- [x] 1.1 Добавить поле `Procedure string` в `TreeParams` (`internal/trcsvc/types.go`). Verify: `go build ./internal/trcsvc/...`
- [x] 1.2 В `ExecuteTree` (`internal/trcsvc/runtime.go`) при серверном режиме передавать `p.Procedure` в `LoadEventsForTree`. При файловом режиме — вызывать `FilterTreesByProcedure` после `BuildTreesWithDepth`. Verify: `go build ./internal/trcsvc/...`

## 2. Серверная фильтрация: LoadEventsForTree

- [x] 2.1 Добавить параметр `procedure string` в `LoadEventsForTree` (`internal/trc/store.go`). Если `procedure != ""` и `spid <= 0` — автовыбор SPID по процедуре (query: `SELECT spid ... WHERE procedure = $procedure ORDER BY count(*) DESC LIMIT 1`). Verify: `go build ./internal/trc/...`
- [x] 2.2 Модифицировать anchor CTE: при `procedure != ""` — `WHERE e.procedure = $procedure` вместо `e.parent_id IS NULL`. Recursive part не меняется. Verify: `go vet ./internal/trc/...`
- [x] 2.3 Обновить вызов `LoadEventsForTree` в `ExecuteTree` — передать `p.Procedure`. Verify: `go build ./...`

## 3. Файловая фильтрация: FilterTreesByProcedure

- [x] 3.1 Реализовать `FilterTreesByProcedure(trees map[int][]*TRCTreeNode, procedure string) map[int][]*TRCTreeNode` в `internal/trc/tree.go`. Обход каждого дерева, поиск узлов с `node.Start.Procedure == procedure`, возврат новых деревьев с найденными узлами как корнями (со всеми детьми). Verify: unit-тест `TestFilterTreesByProcedure` проходит
- [x] 3.2 Добавить unit-тест: дерево с ProcA→ProcB→ProcC, фильтр по ProcB → возвращено поддерево ProcB с ProcC. Verify: `go test ./internal/trc/... -run TestFilterTreesByProcedure`

## 4. CLI: флаг --proc для trc tree

- [x] 4.1 Добавить флаг `--proc` для `trcTreeCmd` в `cmd/trc.go` (привязка к `trcProcedure`). Verify: `codebase trc tree --help` показывает `--proc`
- [x] 4.2 Передать `trcProcedure` в `TreeParams.Procedure` в `runTRCTree`. Verify: `go build ./cmd/...`

## 5. MCP: параметр procedure в codebase_trc_tree

- [x] 5.1 Добавить `procedure` в `InputSchema` инструмента `codebase_trc_tree` в `internal/mcp/registry.go`. Verify: `go build ./internal/mcp/...`
- [x] 5.2 В handler-функции читать `procedure` через `optionalString` и передавать в `TreeParams.Procedure`. Verify: `go vet ./internal/mcp/...`

## 6. Тесты

- [x] 6.1 Unit-тест `FilterTreesByProcedure` — несколько процедур в одном SPID, фильтр возвращает только нужные поддеревья. Verify: `go test ./internal/trc/... -run TestFilterTreesByProcedure`
- [x] 6.2 Unit-тест `FilterTreesByProcedure` — процедура не найдена → пустой результат. Verify: `go test ./internal/trc/... -run TestFilterTreesByProcedure_NotFound`
- [x] 6.3 Интеграционный тест `ExecuteTree` с `Procedure` в файловом режиме (без БД). Verify: `go test ./internal/trcsvc/... -run TestExecuteTree_Procedure`
- [x] 6.4 Проверка сборки: `go build ./...` и `go vet ./...` — чисто. Verify: обе команды проходят без ошибок

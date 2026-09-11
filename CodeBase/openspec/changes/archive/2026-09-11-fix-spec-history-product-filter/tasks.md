## 1. MCP handler

- [x] 1.1 В `internal/mcp/registry.go` в handler `codebase_query_spec_history` (строки 1109–1119) добавить чтение опционального `product` через `optionalString(args, "product")` и передачу его в `ExecuteSpecHistory` третьим аргументом. Проверить: `go build ./internal/mcp/...` проходит без ошибок.
- [x] 1.2 Проверить, что MCP schema уже объявляет `product` как опциональный (строка 1105) — изменения schema не требуются. Если `oneOf` не пропускает `product`, убедиться что `product` вне `required` массивов.

## 2. Тесты

- [x] 2.1 Добавить MCP-тест: вызов `codebase_query_spec_history` с `name` и `product` возвращает только changes из указанного продукта. Проверить: `go test ./internal/specsvc/... -run SpecHistory` проходит.
- [x] 2.2 Добавить MCP-тест: вызов `codebase_query_spec_history` с `change` и `product` возвращает только capabilities из указанного продукта. Проверить: `go test ./internal/specsvc/... -run SpecHistory` проходит.
- [x] 2.3 Добавить MCP-тест: вызов без `product` возвращает changes из всех продуктов (регрессия). Проверить: `go test ./internal/specsvc/... -run SpecHistory` проходит.

## 3. Валидация

- [x] 3.1 Запустить `openspec validate fix-spec-history-product-filter` и убедиться, что change проходит валидацию без ошибок.
- [x] 3.2 Запустить `go build ./...` и `go test ./internal/... ./cmd/...` — убедиться, что весь проект собирается и тесты проходят.

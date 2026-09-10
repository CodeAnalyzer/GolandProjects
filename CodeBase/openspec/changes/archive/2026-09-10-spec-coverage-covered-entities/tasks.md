## 1. Структуры данных

- [x] 1.1 Переделать `SpecCoverageResult` в `internal/specsvc/specsvc.go`: поля `Product string`, `Capabilities []SpecCoverageCapability`. Убрать старые поля (`CapabilityID`, `CapabilityName`, `Title`, `ApiTotal`, `ApiCovered`, `CodeTotal`, `CodeListed`, `Gaps`). Проверить компиляцию.
- [x] 1.2 Добавить типы `SpecCoverageCapability` (`CapabilityName`, `Title`, `Covered []SpecCoverageEntity`) и `SpecCoverageEntity` (`Name`, `Kind`). Оставить `SpecCoverageGap` без изменений (для будущего `spec_gaps`). Проверить компиляцию.

## 2. Реализация ExecuteSpecCoverage

- [x] 2.1 Изменить сигнатуру `ExecuteSpecCoverage` в `internal/specsvc/specsvc.go`: `(ctx, db, product, name, kind string)`. `product` required (пустой → `ErrSpecSearchEmpty`), `name` optional, `kind` optional. Убрать `mode` и `normalizeCoverageMode`.
- [x] 2.2 Написать SQL-запрос: `target_capabilities` (фильтр по `ds_product_id` через `ds_products.product_name`, опционально по `LOWER(capability_name)`), `capability_sources` (union spec_capability/spec_requirement/spec_scenario), `covered` (DISTINCT по `cap_id, target_type, target_id` с фильтром `references_code` и опциональным `kind`), резолвинг имён через LATERAL UNION ALL по 8 таблицам. Проверить, что запрос возвращает `(cap_id, capability_name, title, entity_name, entity_kind)`.
- [x] 2.3 Реализовать сборку ответа: scan строк, дедупликация по `name|kind` внутри capability (`map[int64]*SpecCoverageCapability` + `map[string]bool`), сортировка `Covered` по `kind, name`. Вернуть `SpecCoverageResult` с пустым `Capabilities: []` (не `nil`) при отсутствии данных.

## 3. Удаление старой логики

- [x] 3.1 Удалить `computeSpecCoverage` (`specsvc.go:595-642`) — больше не нужна. Проверить, что нет других вызовов.
- [x] 3.2 Удалить `coverageMetric` (`specsvc.go:644-649`) — больше не нужна. Проверить, что нет других вызовов.
- [x] 3.3 Удалить `normalizeCoverageMode` (`specsvc.go:651-659`). Проверить, что нет других вызовов.
- [x] 3.4 Убрать gaps-режим из `ExecuteSpecCoverage` (блок `if mode == "gaps"`, `specsvc.go:517-590`). `SpecCoverageGap` оставить в файле.

## 4. CLI

- [x] 4.1 В `cmd/query_spec.go` изменить `querySpecCoverageCmd`: `--product` → `MarkFlagRequired`, `--name` → убрать `MarkFlagRequired`, добавить `--kind` (optional), убрать `--mode` и переменную `specCoverageMode`. Проверить `go build ./cmd/...`.
- [x] 4.2 Обновить вызов `specsvc.ExecuteSpecCoverage` в handler на новую сигнатуру `(ctx, db, specCoverageProduct, specCoverageName, specCoverageKind)`. Проверить компиляцию.

## 5. MCP registry

- [x] 5.1 В `internal/mcp/registry.go` обновить `codebase_query_spec_coverage`: input schema — `product` required, `name`/`kind` optional, `mode` убрать. Описание инструмента обновить. Проверить `go build ./internal/mcp/...`.
- [x] 5.2 Обновить handler: `product` через `requiredString`, `name`/`kind` через `optionalString`. Вызвать `specsvc.ExecuteSpecCoverage(ctx, db, product, name, kind)`. Проверить компиляцию.

## 6. Тесты

- [x] 6.1 Обновить `TestSpecCoverageAndHistoryValidateBeforeDBAccess` в `internal/specsvc/specsvc_test.go`: вызов `ExecuteSpecCoverage` с пустым `product` → `ErrSpecSearchEmpty`; убрать проверку invalid mode. Проверить `go test ./internal/specsvc/...`.
- [x] 6.2 Добавить тест на валидацию `kind` (если задан невалидный тип — ошибка или пустой результат, согласно решению в реализации). Проверить `go test ./internal/specsvc/...`.
- [x] 6.3 Проверить `go build ./...` и `go test ./...` — всё компилируется и проходит.

## 7. Валидация

- [x] 7.1 Запустить `openspec validate spec-coverage-covered-entities` — дельта-спека валидна.
- [x] 7.2 Запустить `go vet ./internal/specsvc/... ./cmd/... ./internal/mcp/...` — нет замечаний.

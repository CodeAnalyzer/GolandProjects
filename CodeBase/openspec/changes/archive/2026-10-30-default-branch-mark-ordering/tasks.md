# Tasks: default-branch-mark-ordering

## 1. Фикс discovery

- [x] 1.1 `internal/indexer/source_registry.go` (`discoverGitSources`): убрать раннюю маркировку `markedProducts`; в цикле накапливать productID; после цикла по всем продуктам вызвать `SetDefaultBranch(ctx, productID, defaultBranch)` по одному разу
- [x] 1.2 убедиться, что сброс `is_default = FALSE` в `UpsertDSProductBranch` (`ON CONFLICT (path)`) сохранён (смена ветки в каталоге снимает флаг с прежней линии)

## 2. Integration-тест

- [x] 2.1 `internal/indexer/source_discovery_integration_test.go`: сценарий «клиентский каталог сортируется раньше develop-каталога» — два git-каталога одного продукта (`fa-contracts-ext-check-save-pti` на `prj/CHECK_SAVE_PTI` и `fa-contracts-ext-develop` на `develop`), два прогона discovery (первый скан на пустой БД + установившийся), в обоих `develop is_default = true`, клиентская `false`; без фикса тест падает
- [x] 2.2 существующие discovery-тесты (`TestInit...SourceDiscovery`, смена ветки, stale-cleanup) проходят без правок ожиданий

## 3. Регресс и сборка

- [x] 3.1 `go vet ./...` чисто; `go test ./...` зелёный; `go test ./internal/indexer -tags integration` зелёный
- [x] 3.2 бамп buildNumber 1657 → 1658 в `cmd/root.go`; `go build -o CodeBase.exe .` из корня CodeBase; `codebase --version` показывает 1658

## 4. Верификация на рабочем ПК (C:\NT\FA#\7.2GIT)

- [x] 4.1 `codebase update` → контрольный `SELECT branch_name, is_default FROM ds_products_branch b JOIN ds_products p ON p.id = b.ds_product_id WHERE p.product_name = 'fa-contracts-ext'` → у `develop` (2838 файлов) `is_default = true`, у 11 клиентских `false`
- [x] 4.2 контрольная выборка с осью default: например `codebase query api-table --name pAPI_LoanExt_CredHistInfo --json` — сущность клиентской линии видна с корректным `branch_used`; общие сущности develop не потерялись

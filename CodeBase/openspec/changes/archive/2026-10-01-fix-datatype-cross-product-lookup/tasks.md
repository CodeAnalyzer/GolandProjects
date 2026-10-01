# Tasks: fix-datatype-cross-product-lookup

## 1. Store: контекстный резолвер типов

- [x] 1.1 Добавить параметры `fileID, productID int64` в `FindLatestSQLColumnDefinitionType` (internal/store/db_lookup_sql.go:173), добавить `JOIN files` и трёхуровневый `ORDER BY (scd.file_id = $3) DESC, (f.ds_product_id = $4) DESC, scd.id DESC` по design D1; проверить `go build ./...`
- [x] 1.2 Проделать то же для `BatchFindColumnDefinitionTypes` (db_lookup_sql.go:223): параметры контекста + приоритизация в ORDER BY; Go-дедупликация «первый выигрывает» не меняется (design D3); проверить `go build ./...`
- [x] 1.3 Добавить параметр `productID` в `FindAPIColumnDefinitionType` (db_lookup_sql.go:193): продуктовая приоритизация обеих веток через `api_contracts.file_id` / `api_business_object_tables.file_id` → `files.ds_product_id` (design D4); проверить `go build ./...`

## 2. Review Runner: проброс контекста запуска

- [x] 2.1 Расширить `reviewExecContext` полями `fileID`, `productID`, заполнить в `RunSQLFileCtx` из `indexedFile` (runner.go:113)
- [x] 2.2 `cachedFindColumnDefinitionType` (runner.go:392) и `prewarmColTypeCache` (runner.go:335) передают контекст в store-методы; убедиться, что все вызовы store-методов из review обновлены (`rg "FindLatestSQLColumnDefinitionType|BatchFindColumnDefinitionTypes|FindAPIColumnDefinitionType" internal/`) и `go build ./...` проходит
- [x] 2.3 Проверить, что при `fileID=0, productID=0` генерируемый SQL семантически совпадает со старым (регресс совместимости для тестов без контекста, design D2) — подтверждено тестом `TestFindLatestSQLColumnDefinitionType_ZeroContextLegacy` и батч-веткой `TestBatchFindColumnDefinitionTypes_ProductScope` (legacy-часть)

## 3. Тесты

Реализация: integration-тесты `internal/store/db_lookup_sql_integration_test.go`
(`//go:build integration`, testutil.Open) вместо sqlmock — sqlmock в проекте
отсутствует, testutil-паттерн тестирует реальный SQL (уточнение design D5).

- [x] 3.1 Integration-тест `TestFindLatestSQLColumnDefinitionType_ProductScope`: две записи `pConsQtyListRight.QtyType` — чужая (fa-reports, больший id) и своя (fa-contracts); резолвер с контекстом fa-contracts возвращает `DSIDENTIFIER` — PASS
- [x] 3.2 Integration-тест `TestFindLatestSQLColumnDefinitionType_SameFileTier`: определение из текущего файла побеждает same-product с большим id (сценарий #temp из дельта-спеки) — PASS
- [x] 3.3 Integration-тест `TestFindLatestSQLColumnDefinitionType_GlobalFallback`: пара не объявлена в продукте файла, побеждает глобальный latest (сценарий fallback из дельта-спеки) — PASS
- [x] 3.4 Integration-тест `TestFindLatestSQLColumnDefinitionType_ZeroContextLegacy`: нулевой контекст воспроизводит прежний глобальный порядок — PASS
- [x] 3.5 Прогнаны `go test ./internal/review/... ./internal/store/...` (unit) и `go test -tags integration ./internal/store/ -run "TestFindLatestSQLColumnDefinitionType|TestBatchFindColumnDefinitionTypes"` — все PASS (5/5)

## 4. Приёмка

- [x] 4.1 `go vet ./...` и `go test ./...` чисто (одиночный flake `internal/util` не воспроизвёлся при повторном прогоне, с диффом change не связан)
- [x] 4.2 Ручная проверка на локальном индексе: `review FA/fa-contracts/Consumer/SERVER/Accrual/CreditTurnoverLink_MassProcess.sql --rules datatype` — 0 findings `datatype` (базлайн: 6)
- [x] 4.3 Дифф review до/после на эталоне из 18 FA-файлов (писатели всех конфликтных пар из fa-contracts и fa-reports; baseline — сборка HEAD через git worktree): OLD=44, NEW=38, REMOVED=6 (ровно ложные `pConsQtyList*.QtyType` DSIDENTIFIER->DSINT_KEY из кейса), ADDED=0 — нового шума нет
- [x] 4.4 `openspec validate fix-datatype-cross-product-lookup` — без ошибок

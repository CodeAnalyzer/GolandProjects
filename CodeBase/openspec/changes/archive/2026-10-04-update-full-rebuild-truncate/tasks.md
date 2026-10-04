# Tasks: update-full-rebuild-truncate

## 1. Store: ResetCodebaseTables

- [x] 1.1 В `internal/store` (новый `db_reset.go` или `db_schema.go`) реализовать `ResetCodebaseTables(ctx)`: один `TRUNCATE TABLE <52 таблиц> CASCADE`; возвращать число усечённых таблиц. Проверка: `go build ./...`
- [x] 1.2 Тест `ResetCodebaseTables` (testutil): заполнить representative-таблицы из каждой группы (files, sql_tables, symbols, relations, spec_capabilities, stats_snapshot), вставить строки в rti_sessions/trc_sessions/scan_runs/schema_migrations; после сброса — таблицы усечения пусты, сохраняемые строки живы. Проверка: `go test ./internal/store/...`
- [x] 1.3 Тест-сторож по `information_schema`: у сохраняемых таблиц (rti_*, trc_*, schema_migrations) нет FK на усекаемые; список усечения в тесте соответствует 52 таблицам из спеки. Проверка: `go test ./internal/store/... -run Reset`
- [x] 1.4 Тест идемпотентности: `ResetCodebaseTables` на пустой БД и повторный вызов без ошибок. Проверка: `go test ./internal/store/... -run Reset`

## 2. Indexer: ветка пересборки в UpdateCtx

- [x] 2.1 Выделить внутренний пайплайн с меткой прогресса: `runInitPipeline(ctx, rootPath, parallel, progressLabel)`; `InitCtx` вызывает с `"init"`, поведение без изменений. Проверка: `go build ./... && go test ./internal/indexer/...`
- [x] 2.2 В `UpdateCtx` при `onlyModified == false`: ветка до `GetLatestFilesByRootPath` — `ResetCodebaseTables` → удаление LSA-sidecar (`spec_lsa_model.bin`, `spec_lsa_state.json`; пути как в `indexer_postprocess_spec_lsa.go`) → `runInitPipeline(..., "rebuild")`. Проверка: `go build ./... && go test ./internal/indexer/...`
- [x] 2.3 Интеграционный тест: после `UpdateCtx(onlyModified=false)` сущности в единственном экземпляре (без дублей), scan_runs-история сохранена, добавлен новый прогон. Проверка: `go test ./internal/indexer/... -run Rebuild`
- [x] 2.4 Тест возобновления: имитировать прерванную пересборку (строки необработанных файлов удалены, relations пуст), выполнить повторный `UpdateCtx(onlyModified=false)` — итог эквивалентен непрерывной пересборке, дублей нет; инкрементальный run relations не восстанавливает (зафиксировано отдельной проверкой). Проверка: `go test ./internal/indexer/... -run Rebuild`

## 3. CLI: справка флага

- [x] 3.1 `cmd/update.go`: уточнить справку `--modified`: `false = full rebuild of codebase index (TRUNCATE codebase tables; RTI/TRC sessions and scan history are kept)`. Проверка: `go run . help update`

## 4. Верификация

- [x] 4.1 `go build ./... && go test ./internal/store/... ./internal/indexer/...` — все тесты зелёные
- [x] 4.2 Прогон на FA: `codebase update --modified=false` — время сопоставимо с init (~1.5ч), сессии `rti_sessions`/`trc_sessions` на месте, `sql_tables`/`sql_columns` соответствуют свежему init, история `scan_runs` пополнилась, прогресс-строка с меткой `rebuild` *(выполняется пользователем вручную)*

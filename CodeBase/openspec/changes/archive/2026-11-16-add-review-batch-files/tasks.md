## 1. Runtime ядра и пакетный метод

- [x] 1.1 Выделить из `Runner.RunSQLFileCtx` ядро обработки одного файла (без изменения публичного поведения), сохранив все сбросы (`macroTypeCache`, `r.exec`, `r.scope` и остальные файловые кэши), и убедиться, что `go test ./internal/review` проходит.
- [x] 1.2 Добавить в `internal/review/types.go` типы `BatchFileResult`, `BatchError`, `BatchResult` (поля из design D3/D4) и проверить `go build ./...`.
- [x] 1.3 Ввести sentinel-ошибки `ErrFileNotIndexed` и `ErrProductNotSet` в `internal/review`, сохранив текущий текст сообщений; проверить существующие тесты на `file is not indexed`.
- [x] 1.4 Реализовать `Runner.RunSQLFilesCtx`: цикл по путям, накопление `BatchResult`, изоляция ошибок по файлам; покрыть тестом на список с одним «плохим» файлом в середине.
- [x] 1.5 Добавить scope-инвариантные кэши пакета (списки продуктов/веток, `ResolveAnalysisContext` по `dsinfoID`) и проверить тестом, что результаты batch совпадают с single для двух файлов из разных продуктов.

## 2. Сервисный слой и классификация ошибок

- [x] 2.1 Перенести `classifyReviewError` из `cmd/review.go` в `reviewsvc.ClassifyError`, добавить `errs.ErrInvalidArguments` и его маппинг в `invalid_arguments`; переиспользовать классификатор в одиночном режиме и проверить `go test ./cmd ./internal/reviewsvc`.
- [x] 2.2 Реализовать `reviewsvc.ExecuteBatchCtx`: один `store.DB`, один `CheckSchemaVersion`, вызов пакетного метода Runner; вернуть инфраструктурную ошибку только при отказе всего пакета.
- [x] 2.3 Замапить per-file sentinel-ошибки в коды `not_indexed` / `missing_product`, остальные — в `review_failed`; покрыть тестом таблицу кодов.

## 3. CLI: аргументы и список файлов

- [x] 3.1 Заменить `Args` команды на `cobra.MaximumNArgs(1)` и добавить валидацию режимов (`--files` vs позиционный путь); проверить `go test ./cmd`.
- [x] 3.2 Реализовать `readReviewFileList` (UTF-8, BOM/CRLF, `#`-комментарии, пустые строки, парные кавычки, дедуп с сохранением порядка) и покрыть unit-тестами все случаи из спеки.
- [x] 3.3 Обработать ошибки списка (отсутствует, нечитаем, пуст) и конфликт режимов через `errs.ErrInvalidArguments` → `invalid_arguments`; проверить JSON-envelope и ненулевой exit code.

## 4. Пакетный вывод и прогресс

- [x] 4.1 Собрать пакетный JSON-конверт (`mode`, `count`, `items`, `summary`, `files` с `total`/`succeeded`/`failed`, `meta`) в порядке списка, где `total` — после дедупа, а `results[].file` — нормализованный путь из списка; покрыть cmd-тестом успешного пакета.
- [x] 4.2 Реализовать частичный успех: `success=false`, findings успешных файлов в `items`, `files.results[]` с `error.code`; проверить тестом, что в stdout ровно один JSON-документ.
- [x] 4.3 Добавить `errs.ErrAlreadyReported` и guard в `cmd.Execute()` перед `writeReviewErrorResponse`; проверить ненулевой exit code без второго envelope.
- [x] 4.4 Реализовать потоковый человекочитаемый вывод (блок файла сразу после его завершения + общий итог) и прогресс `review [i/N] <file> rules k/m` в stderr; проверить, что `--json` не пишет прогресс в stdout.
- [x] 4.5 Убедиться, что одиночный `review <file> --json` не содержит `mode`/`files`, а MCP `codebase_review_sql` не изменился; проверить `go test ./cmd ./internal/mcp`.

## 5. Проверка и валидация

- [x] 5.1 Прогнать `gofmt -l` по изменённым файлам, `go vet ./cmd/... ./internal/review/... ./internal/reviewsvc/...`.
- [x] 5.2 Прогнать `go test ./cmd/... ./internal/review/... ./internal/reviewsvc/... ./internal/mcp/...` и убедиться в зелёном результате; после тестов поднять `buildNumber` с `1700` до `1703` в `cmd/root.go` и пересобрать `go build -o CodeBase.exe`.
- [x] 5.3 Проверить вручную на реальном FA: `CodeBase.exe review --files <list> --json` и `--rules truncTbl` — эквивалентность findings с одиночным запуском.
- [x] 5.4 Выполнить `openspec validate add-review-batch-files --strict` и, при необходимости, создать/обновить `Tests/TEST_CASES_REVIEW.md` (follow-up) с описанием пакетного режима.

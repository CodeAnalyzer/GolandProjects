# Code review: add-review-batch-files

- **Дата:** 2026-11-16
- **Объект:** реализация change `add-review-batch-files` (20/20 задач, `openspec validate --strict` — valid)
- **Объём:** 7 изменённых файлов (+538/−63), 5 новых файлов тестов, `buildNumber` 1700→1703, `CodeBase.exe` пересобран
- **Вердикт:** **APPROVED** — код соответствует спецификации change, регрессов не выявлено; 1_minor-расхождение формулировки design ↔ код и 6 незначительных замечаний (ни одно не блокирующее)

---

## 1. Состав изменений (по факту)

| Файл | Изменение |
|---|---|
| `cmd/review.go` | `--files`, `MaximumNArgs(1)`, валидация режимов, `readReviewFileList`/`trimPairedQuotes`, `executeReviewSingle/Batch`, пакетный конверт `reviewBatchSuccessResponse`, потоковый вывод, `reviewBatchProgress`, классификация → `reviewsvc.ClassifyError` |
| `cmd/root.go` | guard `errors.Is(err, errs.ErrAlreadyReported)` перед `writeReviewErrorResponse`; `buildNumber` 1703 |
| `internal/errs/errs.go` | `ErrInvalidArguments`, `ErrAlreadyReported` («review completed») |
| `internal/review/runner.go` | ядро `runSQLFileCtx` (без изменения публичного `RunSQLFileCtx`), `RunSQLFilesCtx`, `mergeSummary`, sentinel-обёртки `%w: %s`, флаг+кэши `scopeCacheEnabled` |
| `internal/review/scope.go` | `loadScopeLists`/`resolveAnalysisContext` — кэши пакетного прогона, гейт `scopeCacheEnabled` |
| `internal/review/types.go` | `BatchError`, `BatchFileResult`, `BatchResult`, `BatchHooks` |
| `internal/review/errors.go` (новый) | `ErrFileNotIndexed`, `ErrProductNotSet`, `BatchErrorCode` |
| `internal/reviewsvc/runtime.go` | `ExecuteBatchCtx`/`ExecuteBatchWithCtx`, `ClassifyError`, `ClassifyFileError` |
| Тесты (новые) | `cmd/review_batch_test.go`, `internal/review/batch_test.go`, `internal/review/review_batch_integration_test.go`, `internal/reviewsvc/runtime_test.go` |

## 2. Выполненные проверки (воспроизведено независимо)

| Проверка | Результат |
|---|---|
| `gofmt -l cmd internal` | изменённые файлы чисты (в выдаче только старые непересекающиеся файлы `dsinfosvc/encoding/query/rti/trc`, не относятся к change) |
| `go vet ./cmd/... ./internal/review/... ./internal/reviewsvc/... ./internal/errs/...` | clean |
| `go test -count=1 ./cmd/... ./internal/review/... ./internal/reviewsvc/... ./internal/mcp/...` | ok: cmd 1.2s, review 1.2s, reviewsvc 2.3s, mcp 6.1s |
| `go test -tags integration -run TestRunSQLFilesCtxMatchesSingleAndIsolatesErrors ./internal/review` (+ `TestExecuteCtx_SchemaNotInitialized` в reviewsvc) | ok |
| `openspec validate add-review-batch-files --strict` | valid |
| Smoke CLI (exe build 1703): `review --files missing.txt --json`, `review x.sql --files l.txt --json`, `review --json` | все — `invalid_arguments`, exit 1, ровно один error-envelope |
| Smoke FA (реальный индекс): список из 3 строк (дубль + кавычки + несуществующий файл), `--rules truncTbl,useSelectAll --json` | `mode=batch`, дедуп `total=2`, `succeeded=1/failed=1`, `success=false`, `not_indexed` с исходным текстом, `results[].file` нормализован, `items:[]` не null, exit 1, один JSON-документ |

## 3. Соответствие спецификации change

| Решение design | Реализация | Оценка |
|---|---|---|
| D1 флаг `--files` + `MaximumNArgs(1)` + валидация в `RunE` | `cmd/review.go` — режимы разведены, конфликт/0 аргументов → `ErrInvalidArguments` | Соответствует |
| D2 ядро `runSQLFileCtx` + `RunSQLFilesCtx`, один Runner | `runner.go:96-104` — публичная обёртка с валидацией; ядро сохраняет **все** сбросы (colType/macroType/indexCand/indexFields/memoryTable/viewName/procParams/procProductID/tableProductID/tableCreator, `r.exec=nil` defer, per-file `r.scope`) | Соответствует; замечание past-ревью про `macroTypeCache` учтено (задача 1.1) |
| D2 кэши scope-инвариантных lookup'ов | `scope.go` — `loadScopeLists`/`resolveAnalysisContext` с гейтом `scopeCacheEnabled`; включаются только в `RunSQLFilesCtx`, сбрасываются в `defer`; пишутся только в последовательном цикле (rules читают лишь `r.scope`) — гонок нет | Соответствует |
| D3 sentinel'ы + изоляция ошибок | `errors.go` — тексты сообщений сохранены идентично (`"%w: %s"`); per-file ошибки **не** оборачиваются `ErrReviewFailed` (обёрнута только инфраструктурная ошибка `RunSQLFilesCtx` в `ExecuteBatchWithCtx`) — контрольная точка past-ревью выполнена | Соответствует |
| D3/D4 `ErrInvalidArguments`, `ClassifyError` в reviewsvc, `succeeded`/`total`-после-дедупа, `results[].file` = normalizePath | Всё учтено (правки по замечаниям 1–4 change-ревью внесены в design/spec/tasks и код) | Соответствует |
| D5 `ErrAlreadyReported` + guard | `root.go` — guard внутри ветки `isReviewJSONMode`, до `writeReviewErrorResponse`; exit 1 без второго конверта (подтверждено smoke); sentinel-текст «review completed» нейтрален для stderr в не-JSON | Соответствует |
| D6 разбор списка | UTF-8, BOM первой строки, CRLF через TrimSpace, `#`-строки, парные кавычки, дедуп порядка первого вхождения; буфер до 1 МБ; отсутствующий/пустой список → `invalid_arguments` | Соответствует |
| D7 прогресс `review [i/N] <file> rules k/m`, потоковые блоки файлов, итог | `reviewBatchProgress` объединяет оба уровня (замечание 6 past-ревью закрыто); блок файла печатается в `OnFileDone` сразу (замечание 9 закрыто); `stop()` идемпотентен | Соответствует, см. замечание C1 |

Требования спеки `review/batch-review` (6 шт.) проверены по коду и smoke-прогону: порядок списка, изоляция ошибок, эквивалентность single (интеграционный тест на двух продуктах + ошибка в середине — ровно контрольная точка past-ревью), дедуп и семантика `total/succeeded/failed`, потоковый вывод, совместимость single/MCP. Сценарий «Дедупликация списка уменьшает total» подтверждён живым прогоном (3 строки → `total: 2`).

## 4. Анализ регресса

- **Одиночный режим**: `executeReviewSingle` — байт-в-байт прежняя логика (spinner, конверт, human-вывод); структура `reviewSuccessResponse` не менялась — поля `mode`/`files` не появляются.
- **MCP**: `codebase_review_sql` (`internal/mcp/registry.go:397`) по-прежнему вызывает single `ExecuteWithCtx`; слой не тронут; тесты `internal/mcp` зелёные.
- **Коды ошибок**: легаси-коды классификатора сохранены; новая ветка `errors.Is(ErrInvalidArguments)` стоит до строковых эвристик; расширение `containsAny` («unknown flag», «invalid argument») — улучшение, риск переклассификации низкий. Изменение пути ошибки «0 аргументов» (Cobra → RunE) сохраняет код `invalid_arguments` (проверено smoke).
- **Sentinel-обёртки**: тексты «file is not indexed: …»/«ds_product_id is not set for file: …» идентичны прежним; зависимая спека `review/review-scope` не нарушена.
- **Ядро `runSQLFileCtx`**: diff — только вынос валидации в обёртку; все сбросы и defer на месте; поведение single не изменилось (покрыто общими тестами `internal/review` — 59 правил, зелёные).

## 5. Замечания

### Незначительные

- **C1. Расхождение design D7 ↔ код: прогресс в `--json`-режиме.** Обновлённый D7: «В `--json` прогресс идёт только в stderr». Реализация в JSON-режиме не создаёт spinner/hooks вовсе — прогресса нет ни в stdout, ни в stderr. Спека это допускает (её требование — «stdout только конверт»), поведение совпадает с одиночным режимом и неразрушительно. Рекомендация: поправить формулировку D7 на «в `--json` прогресс не выводится; stdout содержит только конверт».
- **C2. `reviewsvc.ClassifyFileError` — мёртвый в проде код**: используется только тестом; прод-путь маппит коды через `review.BatchErrorCode` прямо в Runner. Оставить как публичный API-симметрию или удалить.
- **C3. Дедуп строго по строке** (case-sensitive, без нормализации сепараторов): «A.sql» и «a.sql» не схлопнутся, при этом lookup в БД case-insensitive → файл проверится дважды (две записи в `results[]`). Спеке не противоречит («повторяющиеся пути»), edge-кейс для Windows-списков; при желании — ключ дедупа `strings.ToLower(normalizePath(...))`.
- **C4. Строки `error` в `files.results[]` содержат `"count":0,"summary":{"total":0}`** — спека требует обязательные поля для ok-строк, для error — `error`; лишние нули не запрещены, но немного зашумляют. Косметика (`Count` нельзя `omitempty` — скрыл бы легитимные нули у ok-файлов).
- **C5. Опечатка в комментарии** `runner.go:107`: «преваринг» → «предпрогрев (prewarm)».
- **C6. `Use: "review <sql-file-path>"`** не упоминает `--files` (Short упоминает) — косметика help-строки.

### Инфо

- `Tests/TEST_CASES_REVIEW.md` не создан — задача 5.4 допускала «при необходимости», proposal позиционирует как follow-up. Ок.
- `gofmt`-замечания по `internal/dsinfosvc`, `internal/encoding`, `internal/query`, `internal/rti`, `internal/trc` — преждесуществующие, вне скоупа change.
- `ErrAlreadyReported` с текстом «review completed» в общем пакете `errs`: имя универсальное, текст привязан к review — при переиспользовании другими командами текст станет неточным (не актуально сейчас).

## 6. Вердикт

**APPROVED.** Реализация точно следует обновлённым артефактам change, все 4 важных замечания change-ревью закрыты (семантика `succeeded`, `total` после дедупа, sentinel `ErrInvalidArguments`, `results[].file`), эквивалентность batch/single подтверждена интеграционным тестом и ручным FA-прогоном, регрессов одиночного CLI/MCP не найдено. Рекомендуемые правки (C1–C6) можно внести попутно или оставить как есть — ни одна не влияет на контракт и стабильность.

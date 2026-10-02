## 1. Схема и признак is_generated

- [x] 1.1 `internal/store/db_schema.go`: миграция `ALTER TABLE files ADD COLUMN IF NOT EXISTS is_generated BOOLEAN NOT NULL DEFAULT FALSE` + идемпотентный backfill UPDATE (`rel_path ~* '/UPLOAD/'` или `extension = 't01'`); проверить запуск при `update` на существующей БД (копии помечены без переиндексации, повторный запуск ничего не меняет)
- [x] 1.2 `internal/fswalk`: поле `IsGenerated bool` в `FileInfo` + хелпер `isGeneratedFile(relPath, ext)` (сегмент `UPLOAD` без учёта регистра, расширение `t01`); unit-тест кейсов UPLOAD/upload, t01, канонический исходник
- [x] 1.3 `internal/indexer/indexer.go`: walker заполняет `IsGenerated`, `saveFileCtx` вставляет колонку; integration-проверка `files.is_generated` после индексации тестового дерева (UPLOAD-копия → true, `SERVER/...` → false)

## 2. Скоупированные lookup'ы store

- [x] 2.1 `internal/store/db_lookup_sql.go`: сигнатуры `FindLatestSQLProcedureIDsByNames` / `FindLatestSQLTableIDsByNames` с параметром `productID`; ORDER BY `(is_generated = FALSE) DESC, (f.ds_product_id = $p) DESC, id DESC` c JOIN `files`; нулевой productID валиден; проверить компиляцией всех вызывающих
- [x] 2.2 `internal/store/db_lookup_spec_mentions.go`: тот же контракт для `FindDFMFormIDsByNames`, `FindSMFInstrumentIDsByNames`, `FindPASMethodIDsByNames`, `FindAPIContractIDsByNames`, `FindReportFormIDsByNames`; `FindAPIContractIDsByTableNames` фильтрует контракты-копии при наличии не-копий; unit/integration-тест фильтра
- [x] 2.3 `internal/store/db_lookup_sql.go`: `BatchLookupProcedureProductIDs` и `BatchLookupProcedureParams` — порядок `(NOT is_generated) DESC, id DESC`; удалить мёртвые `FindLatestSQLProcedureIDByName` / `FindLatestSQLTableIDByName`; `go build ./...` и `go vet ./...` чисто

## 3. Spec-постпроцессор с продуктовым контекстом

- [x] 3.1 `internal/store/db_lookup_spec_mentions.go`: `LoadAllSpecCodeMentions` возвращает `ds_product_id` (JOIN `files` по `file_id`); модель `SpecCodeMention` дополнена полем
- [x] 3.2 `internal/indexer/indexer_postprocess_spec.go`: группировка имён упоминаний по продукту, per-product вызовы lookup'ов (продукт 0 — без продуктового предпочтения), сборка `lookup`-карт по группам; оба вызова процедурного lookup'а (первичный и second-chance unknown) скоупированы
- [x] 3.3 Integration-тест резолва (по образцу `db_lookup_sql_integration_test.go`): копия с большим id проигрывает источнику; кросс-продуктовая копия проигрывает продукту спеки; одно имя для спек разных продуктов резолвится раздельно; карта продуктов не берёт продукт от копии — все кейсы из дельты `indexing/openspec-parsing` и `infrastructure/database-schema`

## 4. Review preload и регресс

- [x] 4.1 `internal/review/review_lookup.go`: адаптация вызовов `BatchLookupProcedureParams` / `BatchLookupProcedureProductIDs` к новому поведению; прогон существующих review-тестов без регресса
- [x] 4.2 Полный прогон `go test ./...`, `go vet ./...` чисто; обновить существующие тесты, опирающиеся на старый глобальный latest-wins (если обнаружатся)

## 5. Верификация на FA

- [x] 5.1 Зафиксировать baseline ДО доработки: весь `root_path` (`D:/GITHUB/GolandProjects/FA`) уже проиндексирован текущим (старым) бинарем — сохранить его выводы в файлы: `spec-by-code BaseAlg_ConsMinRest`, покрытие capability `accrual-core/base-algorithms/rest/cons-min-rest`, выборку рёбер `references_code` с путями целевых файлов; после доработки переиндексировать fa-contracts новым бинарем и сравнить: target_id указывает на канонический источник
- [x] 5.2 Дифф выборки непокрытого кода fa-contracts: канонические файлы из репро (`BaseAlg_ConsMinRest`, `BaseAlgAmrtCostSinglePmnt`, ...) больше не присутствуют в выборке непокрытого кода (ложные пробелы закрыты — у канонов появились входящие рёбра `references_code`); доля рёбер `references_code` на `*/UPLOAD/*`/`.t01`-цели ~0. Baseline (замер до доработки, только sql_procedure-цели): 17/22362 рёбер на копии; после фикса они уходят на канон, остаток на копиях допустим только для кандидатов вида «копия — единственный одноимённый кандидат»; полный baseline — UNION по всем target-типам + дамп 17 рёбер с rel_path в baseline-файл (чек-лист закрываемых канонов)
- [x] 5.3 Зафиксировать результаты верификации (сравнение с baseline-файлами из 5.1); `openspec validate fix-spec-mention-resolver-scope` и `openspec validate --specs` чисто

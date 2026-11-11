## 1. ReviewScope: данные и разрешение ветки

- [x] 1.1 Добавить `DsProductBranchID int64` в `indexedFile` (`internal/review/types.go`) и выбор `f.ds_product_branch_id` в `getIndexedFile` (`internal/review/review_lookup.go`); проверить `go build ./...` и тест, что для файла `fa-contracts-11017/.../Cons_BlockLimitAuto_Check.sql` возвращается ветка `fa-contracts-11-01-7`.
- [x] 1.2 Добавить `branchID int64` в `reviewExecContext` и `scope ReviewScope` в `Runner`; заполнять scope в `RunSQLFileCtx` после `getIndexedFile`; проверить unit-тестом `review` на трёх значениях: файл develop, файл релизной линии, файл с `branch_id = NULL`.
- [x] 1.3 Реализовать построение `ProductBranch map[productID][]int64` (разрешённые области, см. design D2): свой продукт — из файла; продукты профиля — из `dsinfosvc.ResolveAnalysisContext(...).Branches`; остальные — `ds_products_branch.is_default`; проверить интеграционным тестом на двух продуктах (профиль покрывает один, второй уходит в default).
- [x] 1.4 Добавить `DsinfoID int64` в `review.Options` и проброс в `reviewsvc.ExecuteWithCtx`/`ExecuteCtx` → `Runner`; проверить, что несуществующий `DsinfoID` возвращает ошибку, а `0` использует цепочку env/active.
- [x] 1.5 Построить области EXT-продуктов (детект веток `ds_products_branch.branch_key LIKE 'prj/%'`): prj-файл → `{ветка файла, default}`; default-файл EXT → `{default} ∪ prj`; чужой EXT без профильной prj-линии → `default ∪ prj`, с профильной → `{default, профильная prj}`; проверить интеграционным тестом на `fa-contracts-ext` (develop + `prj/novikom-persform`).

## 2. Store: scope в batch-lookup'ах

- [x] 2.1 Добавить `LookupScope`/карту продуктов в `BatchLookupProcedureParams` и `BatchLookupProcedureProductIDs` (`internal/store/db_lookup_sql.go`); проверить интеграционным тестом: две ветки с разными `parameters` → выбирается сигнатура целевой ветки.
- [x] 2.2 Добавить scope в `BatchLookupTableProductIDs` и `BatchLookupTableCreators`; проверить тестом: создатель таблицы только в `develop` не подтверждает существование при scope `fa-contracts-11-01-7`.
- [x] 2.3 Синхронизировать value-приоритет batch- и ленивого путей с design D3/D4: `BatchFindColumnDefinitionTypes` — карта областей через unnest-пары (включая тир «разрешённая область чужого продукта»); `FindLatestSQLColumnDefinitionType` — `scope.BranchID` вместо 0 + область; `FindAPIColumnDefinitionType` — веточный приоритет через join `files`/`ds_products_branch` (сейчас только `product_match`); проверить интеграционным тестом на двух ветках с разными типами одной колонки.
- [x] 2.4 Проверить единство приоритета prewarm и ленивого пути (`BatchFindColumnDefinitionTypes` vs `FindLatestSQLColumnDefinitionType`) тестом на одном и том же наборе колонок; убедиться, что результат совпадает.

## 3. Review: lookup-слой

- [x] 3.1 Добавить scope в `batchLoadIndexCandidates`, `lookupTableIndexCandidates`, `lookupIndexExists`, `lookupIndexFieldsByName` (`internal/review/review_lookup.go`), включая фильтрацию API-индексов через `api_business_object_tables.file_id → files`; проверить интеграционным тестом: индекс только в чужой ветке не считается существующим.
- [x] 3.2 Добавить scope в `findAPITableNames`, `lookupProcedureParams`, `lookupAPIContractParams`, `lookupMacroSignature`; проверить тестом, что API-таблица/макрос из чужой ветки не подменяет целевое определение.
- [x] 3.3 Убрать first-wins дедуп `indexFieldsCache` между ветками (`batchLoadIndexCandidates`): при scope кандидаты одной области; проверить тестом с одинаковым именем индекса и разными полями в двух ветках — выбираются поля целевой ветки.
- [x] 3.4 Обновить все prewarm-функции (`prewarmColTypeCache`, `prewarmIndexCache`, `prewarmProcCaches`, `prewarmTableProductIDs`, `prewarmTableCreators`) на передачу scope; проверить, что review-тесты не делают лишних запросов и кэш заполняется (лог/счётчик запросов в тесте).

## 4. Правила: три режима

- [x] 4.1 `checkProcDuplicate`: перевести межфайловый поиск на область своего продукта (product+branch; для EXT — union) и добавить within-file детект по `parsed.Procedures`; проверить тестами: 3 ветки → нет finding, 2 файла одной ветки → finding, 2 `CREATE` в одном файле → finding, дубли в разных продуктах → нет finding, имя в prj и develop одного EXT-продукта → finding.
- [x] 4.2 Existence-правила (`checkExecNotExistsProcedures`, `checkTableExistsInMetadata`, `checkIndexExistsInDB`): применить strict-scope по областям; проверить тестами: сущность только в чужой ветке своего продукта → finding; сущность в целевой ветке чужого продукта → нет finding; prj-файл + сущность из ext-develop → нет finding; develop-файл EXT + сущность из prj-ветки → нет finding; сущность только в prj-ветке другого банка → finding.
- [x] 4.3 Value-правила (`datatype` и его подпроверки, `dateIntoString`, `emptyStringDate`, `isNullSameTypes`, `diffTypesComparison`, `floatToStringConvert`, `excessProcParams`, `duplicateOutputVariable`, `indexWrong`, `deferredUpdate`, `memoryUniqueIndexUpdate`, `useFuncInIndCol`): перевести на scoped lookup'ы; проверить тестами на дивергентных ветках (тип колонки, сигнатура процедуры, поле индекса) — findings считаются по целевой ветке.
- [x] 4.4 Identity-правила (`checkForeignTables`, `checkForeignPTables`, `checkForeignProcedures`): сохранить product-level объединение без фильтра по ветке и добавить для процедур проверку своего продукта раньше объявления чужим; проверить тестами: таблица своего продукта из другой ветки → не foreign; процедура своего продукта в другой ветке → не foreign.
- [x] 4.5 Проверить, что `isMemoryTable` (`memory_tables`) и `FindHDefineExistsByName` (`h_files_defines`, правило `statementsWithJoinsRequireAliases`) остаются глобальными по имени — спека `review/review-scope` объявляет их исключениями без веточного скоупинга; добавить/сохранить тест «декларация in-memory в другом продукте» из `deploy-stoppers` — он должен остаться зелёным.

## 5. Поверхности CLI и MCP

- [x] 5.1 Добавить `--dsinfo` в `cmd/review.go` через `reviewCmd.PersistentFlags().Int64Var(&analysisDsInfoID, ...)` и передать `DsinfoID` в `review.Options`; проверить CLI-тестом: `review file.sql --dsinfo 3` разрешает ветки чужих продуктов, `--dsinfo 999` даёт ошибку.
- [x] 5.2 Добавить `dsinfo_id` в схему `codebase_review_sql` и обработчик в `internal/mcp/registry.go`; проверить MCP-тестом: параметр принимается, передаётся в `reviewsvc.ExecuteWithCtx`, несуществующий id → ошибка.
- [x] 5.3 Проверить, что `codebase_review_sql` не добавляет `warnings` о ветках в ответ (по спеке) и что без `dsinfo_id` поведение идентично прежнему (активный профиль по цепочке).

## 6. Верификация и измерения

- [x] 6.1 Запустить `go build ./...`, `go vet ./...`, `gofmt -l` по изменённым пакетам; устранить замечания.
- [x] 6.2 Запустить unit-тесты `./internal/review/...` и интеграционные тесты `./internal/store/... ./internal/review/...`; все должны пройти.
- [x] 6.3 Прогнать review на реальных файлах: `FA/fa-contracts/.../Cons_BlockLimitAuto_Check.sql`, `FA/fa-contracts-11016/...`, `FA/fa-contracts-11017/...` — `procDuplicate` не должен появляться ни в одном; на файле с реальным дублем в одной ветке finding должен сохраниться.
- [x] 6.4 Замерить эффект на выборке: сравнить число findings `procDuplicate`, `datatype*`, `excessProcParams`, `indexWrong` до/после на N файлах; зафиксировать в `design.md`/notes, что межветочный шум ушёл и новые findings только по целевой ветке.
- [x] 6.5 Выполнить `openspec validate fix-review-branch-scoping --strict` и `openspec validate --all --strict`; оба должны быть валидны.
- [x] 6.6 Прогнать review на prj-файлах `FA/fa-contracts-ext-novikom-persform` (51 SQL-файл): 6 таблиц с создателем только в ext-develop (замер ревью) не дают `tableExistsInMetadata`; обращения prj-файлов к ext-develop-процедурам не дают `execNotExistsProc`; зафиксировать результат в `design.md`.

# Code Review: fix-review-branch-scoping

- **Дата**: 2026-11-11
- **Ревьюер**: opencode (L3/flash)
- **Предмет ревью**: реализация change `fix-review-branch-scoping` (uncommitted, рабочий каталог)
- **Вердикт**: ✅ **ГОТОВ К КОММИТУ И АРХИВАЦИИ.** Полное соответствие proposal/design/tasks и дельтам спек; все findings F1–F6 change-review применены и верифицированы. Блокирующих замечаний нет. Ниже — 3 Low-наблюдения (не блокеры) и Info-протокол.

## Объём проверки

1. Артефакты change: `proposal.md`, `design.md` (D1–D8, Measurements, Migration Plan, Implementation Notes), `tasks.md` (27/27 выполнены), дельты `specs/` (review-scope, deploy-stoppers, fine-code, dsinfo-resolution, mcp-transport-tools), файл change-review (F1–F6, Resolution).
2. Прочитан весь diff изменённых файлов и новые файлы:
   - `internal/review/scope.go` (новый) — `ReviewScope`, `buildReviewScope`, `buildProductBranchAreas`, `ownProductArea`, `foreignProductArea`, `isPrjBranchKey`;
   - `internal/review/runner.go` — `Runner.scope`, `reviewExecContext.branchID`, точка построения scope, `prewarmColTypeCache`, `cachedFindColumnDefinitionType`;
   - `internal/review/types.go` — `Options.DsinfoID`, `indexedFile.DsProductBranchID`;
   - `internal/review/review_lookup.go` — `getIndexedFile`, `cachedLookupProcedureScopeInfo`, `lookupProcedureCreateFiles`, `lookupIndexFieldsByName`, `batchLoadIndexCandidates`, `lookupProcedureParams`, `lookupAPIContractParams`, `lookupMacroSignature`, prewarm-функции;
   - `internal/review/review_rules.go` — `checkProcDuplicate` (переписан), `checkExecNotExistsProcedures`, `checkForeignProcedures`;
   - `internal/store/lookup_scope.go` — `ProductBranchPairs`, `NewProductBranchPairs`, `ListDSProductBranches`, `ListDSProductIDs`;
   - `internal/store/db_lookup_sql.go` — `FindLatestSQLColumnDefinitionType`, `FindAPIColumnDefinitionType`, `BatchFindColumnDefinitionTypes`, `BatchLookupProcedureParams`, `BatchLookupProcedureProductIDs` (+`ProcedureScopeInfo`, Go-агрегация), `BatchLookupTableCreators`;
   - `internal/indexer/indexer_sql_pas.go` — legacy-вызов с пустыми pairs;
   - `cmd/review.go`, `internal/mcp/registry.go` — поверхности `--dsinfo` / `dsinfo_id`;
   - тесты: `scope_test.go`, `review_scope_integration_test.go`, `db_lookup_branch_integration_test.go`, `review_dsinfo_integration_test.go` (cmd/mcp), правки 5 существующих тестов.
3. Проверена эквивалентность тиражей SQL-приоритетов batch/lazy (задача 2.4) вручную по текстам ORDER BY и тестом `TestPrewarmLazyEquivalence`.
4. Проверены guard'ы: `db == nil`, файл не проиндексирован, `ds_product_id = 0`, несуществующий dsinfo — до/в момент построения scope.

## Верификация (выполнена при ревью)

| Проверка | Результат |
|---|---|
| `go build ./...` | EXIT 0 |
| `go vet ./internal/{review,store,mcp,indexer}/... ./cmd/...` | EXIT 0 |
| `gofmt -l` по пакетам change (`review`, `store`, `cmd`, `mcp`, `indexer`) | чисто (gofmt-замечания есть только в файлах вне границ change: dsinfosvc/encoding/query/rti/trc — pre-existing, не трогались) |
| `go test ./internal/review ./internal/store ./cmd ./internal/mcp` (unit) | `ok` во всех |
| `go test -tags integration ./internal/review` (10 новых Scope/E2E-тестов) | PASS (25.6 s) |
| `go test -tags integration ./internal/store` (5 новых BranchArea + регресс) | PASS (36.3 s) |
| `go test -tags integration ./cmd ./internal/mcp` (TestReviewDsinfoFlagBehavior, TestReviewToolDsinfoIDBehavior) | PASS |
| `openspec validate fix-review-branch-scoping --strict` | valid |
| `openspec validate --all --strict` | 37/37 |

## Соответствие design-решениям

- **D1 (ReviewScope)**: структура точь-в-точь (`FileID/ProductID/BranchID/ProductBranch`); строится один раз в `RunSQLFileCtx` после `getIndexedFile` (runner.go:125) и **до** prewarm'ов и старта параллельных правил; после построения только читается — комментарий о гарантиях отсутствия data race на месте.
- **D2 (области)**: `buildProductBranchAreas` покрывает все сценарии — свой продукт (ветка файла авторитетна, профиль не переопределяет), чужие (профиль → default), EXT (prj-файл → `{prj, default}`; default-файл → `{default} ∪ prj`; чужой EXT → `default ∪ prj` или `default ∪ профильная prj`); детект `prj/%` с нормализацией lower/trim. Сверх design корректно закрыты краевые случаи из Implementation Notes: продукт без линий → `{0}`; свой продукт с удалённой строкой ветки → `{BranchID}` (unit-тесты есть).
- **D3 (три режима)**: existence — area-предикат в `BatchLookupTableCreators`, `lookupProcedureCreateFiles`, index-lookup'ах, `ExistsInArea` для `execNotExistsProc`; value — тираж `not_gen → area → product → default → id` в `BatchLookupProcedureParams`/`lookupProcedureParams`/`lookupMacroSignature`/типах колонок, синхронизирован между batch и lazy; identity — `BatchLookupTableProductIDs` не тронут (без scope), для процедур owner выбирается own-product-first (включая генерируемые копии) → `notGenerated` → `id DESC`, точь-в-точь по comment/спеке.
- **D4 (SQL)**: все area-предикаты через параметризованные `unnest($n::bigint[], $m::bigint[])`, строковой интерполяции нет; `pq.Array` корректен; нумерация параметров `$1..$7` сверена в каждом запросе (в т.ч. обязательное использование `$4/$5` в ORDER — ошибки pq нет); API-индексы скоупятся через `api_business_object_tables.file_id`, API-контракты — через `api_contracts.file_id`.
- **D5 (procDuplicate)**: переписан — within-file скан заголовков по expanded-контенту через `r.fileContent` (без БД), межфайловый через `lookupProcedureCreateFiles` в области своего продукта; дедуп `reported` исключает двойной finding; исключения `upload`/`.t01` сохранены; одна область с existence (EXT-union унаследован).
- **D6 (поверхности)**: `--dsinfo` на `PersistentFlags` с общим `analysisDsInfoID`; `Options.DsinfoID` проброшен через `reviewsvc` → `Runner` (reviewsvc/runtime.go:18); MCP `dsinfo_id` в схеме и handler'е; несуществующий id валит review (`buildReviewScope` → `ResolveAnalysisContext` → ошибка без fallback); guard `r.db == nil` отрабатывает раньше построения scope — «только когда есть БД» соблюдено. CLI/MCP-тесты на ошибочный id есть.
- **D7 (prewarm/lazy)**: все 5 prewarm-функций передают один `r.scope`; ленивые пути — тот же объект; first-wins дедуп `indexFieldsByName` устранён корректно (JOIN `files` + area-фильтр, детерминированный ORDER BY; `rows.Err()`/`rows2.Err()` проверяются).
- **D8 (branch_id NULL / EXT)**: `BranchID=0` → область `{0}` (any-branch), value-тиры `(branch=$5)` деградируют безопасно (`=0` false для всех реальных веток, схлопывание до тира «продукт» — как в Notes спеки); EXT-области не зависят от 0.

## Соответствие findings change-review (F1–F6)

| Finding | Статус |
|---|---|
| F1 EXT-модель develop ∪ prj | Реализована во всех слоях (D2/D3/D5/D8); спека review-scope содержит отдельное требование «Область видимости EXT-продуктов» с 4 сценариями; prj-детект — `branch_key LIKE 'prj/%'` с нормализацией |
| F2 исключения memory_tables/h_files_defines глобально | `isMemoryTable`/`FindHDefineExistsByName` не получили scope (проверено — не изменялись); асимметрия «существование глобально, сигнатура value» отражена в коде (`lookupMacroSignature` value-тираж) и в Notes спеки |
| F3 синхронизация batch/lazy/API (D3/D4) | unnest-пары во всех batch-функциях; `BranchID` для lazy; JOIN files/ds_products_branch для API; тест `TestPrewarmLazyEquivalence` PASS |
| F4 деградация value-тиров при BranchID=0 | Реализована и совпадает с Notes спеки |
| F5 масштаб новых existence-находок | Зафиксирован в proposal Impact + design Risks (28/63/53/0–16) |
| F6 сосуществование execNotExistsProc+foreignProcedureUsing | Реализовано: `checkForeignProcedures` не маскируется (оба правила независимы); сценарий в спеке «Режимы DB-lookup review» есть |

## Замечания

### R1 [Low] — `checkProcDuplicate` within-file: строки-комментарии и многострочные заголовки
`reProcDuplicateHeader` применяется построчно к expanded-контенту без вырезания комментариев: `-- create proc MyProc` в комментарии будет засчитан как определение (FP severity 1 при наличии живого `CREATE PROCEDURE MyProc`). Обратный случай — заголовок, разорванный переводом строки (`CREATE\nPROC`), не ловится (FN). Риск мал (в FA комментарии с полным заголовком CREATE PROC — редкость; дедуп требует двух вхождений одного имени), согласуется с квирком парсера из Implementation Notes. **Рекомендация**: при первых ложных срабатываниях — фильтровать строки, начинающиеся с `--`, отдельным точечным change; сейчас не блокер.

### R2 [Low] — `##temp`-процедуры не покрываются within-file детектом
Паттерн `(#?[a-z_][a-z0-9_#]*)` допускает один ведущий `#`: `CREATE PROC ##Tmp` не сматчится (FN для within-file). Паттерн консистентен с остальными regex review (`reUpdateTable`, `reInsertTable` и др. — та же конвенция), межфайловый детект через `parsed.Procedures` остаётся. Не блокер.

### R3 [Low] — build number не поднят
`cmd/root.go` не изменён: `buildNumber = "1684"` — совпадает с бинарями предыдущего change (`add-table-exists-in-metadata`). По прецеденту (1681→1684 в предыдущем change) bump упрощает контроль деплоя в `C:\NT\FA#\7.2GIT\Tools\CodeBase\`. **Рекомендация**: поднять build перед коммитом (например 1685).

### R4 [Info] — легаси-изменение поведения `BatchFindColumnDefinitionTypes` при нулевом контексте
В ORDER BY добавлены тиры `area` (вырожден при пустых pairs) и `default` (не вырожден): нулевой контекст (legacy-вызовы индексатора и старые тесты) теперь предпочитает default-ветку глобальному latest-wins. Это **гармонизация** с `FindLatestSQLColumnDefinitionType` (у которого тир default был и раньше) и с задачей 2.4; покрыто тестом `TestBatchFindColumnDefinitionTypes legacy`. Задокументировано в комментарии функции. Действий не требуется — отметить в notes к релизу.

### R5 [Info] — стиль/микро-производительность
`r.scope.Pairs()` пересобирает сортированные пары при каждом вызове (в `lookupProcedureCreateFiles` — дважды, в `lookupIndexFieldsByName`/`batchLoadIndexCandidates` — по разу); `ownerBetter`-замыкание создаётся на каждой итерации агрегации в `BatchLookupProcedureProductIDs`. Оверхед пренебрежим (карты на десятки записей, единицы вызовов на файл). Не требует правки.

### R6 [Info] — контракты пустых pairs соблюдены
`ProductBranchPairs{}` = «области не заданы → прежнее глобальное поведение» выполняется во всех ветках кода: пустой pairs в `BatchLookupTableCreators` идёт по отдельной ветке без JOIN, area-EXISTS при пустом unnесте вырождается в false, `ExistsInArea` при пустых pairs = false (задокументировано в комментарии; review всегда передаёт непустые пары, т.к. scope строится всегда при `db != nil`). Legacy-вызов `indexer_sql_pas.go` передаёт пустые pairs осознанно (вне границ change).

## Замечания вне блокеров — итог

Изменения R1–R3 не препятствуют коммиту и архивации: R1/R2 — редкие краевые случаи, зафиксированные как квирки; R3 — рекомендация перед коммитом. Повторный прогон замеров из design.md Measurements не выполнялся: замеры датированы сегодняшним днём и зафиксированы в design (40 файлов fa-contracts-11017: procDuplicate 40→0; EXT 51 prj-файл: до==после; реальный дубль fa-administrator сохраняется).

## Архитектурные наблюдения (не требуют действий)

- Полиморфизм area-предиката повторён в 10+ запросах текстово (`pb.bid = 0 OR pb.bid = f.ds_product_branch_id`) — сейчас это плюс (локальность SQL), при росте числа запросов можно вынести в SQL-фрагмент/функцию; отдельный change.
- `BatchLookupProcedureProductIDs` перешёл с `DISTINCT ON` на Go-агрегацию — детерминированность обеспечена уникальностью `p.id` в компараторе; при будущих новых тиражах владельца компаратор `ownerBetter` — единственная точка правки. Хорошая точка расширения.
- Дублирование BETWEEN batch (`BatchFindColumnDefinitionTypes`) и lazy (`FindLatestSQLColumnDefinitionType`) тиражей осталось текстовым (как и до change), но теперь реально синхронизировано и защищено тестом эквивалентности — приёмлемо.

## Resolution

Протокол ревью от 2026-11-11. Все findings R1–R6 разобраны: блокирующих нет; R1/R2 — приняты как известные квирки (зафиксированы, действия при появлении реальных FP/FN); R3 — рекомендация поднять build перед коммитом; R4–R6 — протокол без действий. Реализация соответствует proposal/design/tasks и дельтам спек; change готов к `/opsx-archive` после коммита.

Разбор и применение (2026-11-11, реализатор):
- **R1** — согласен, не править до появления реальных FP (зафиксировано в design.md Implementation Notes).
- **R2** — согласен, конвенция `#?` едина с остальными regex review; квирк зафиксирован в design.md Implementation Notes.
- **R3** — принято: `buildNumber` 1684 → 1689 (`cmd/root.go`; существенное изменение кода — владелец измеряет билды по существенности доработок), бинарь пересобран.
- **R4** — согласен, действий не требует; изменение tie-break при нулевом контексте зафиксировано в design.md Implementation Notes (к notes к релизу).
- **R5/R6** — протокол, действий не требует.

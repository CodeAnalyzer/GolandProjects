# Tasks: dsinfo-integration

## 1. Матчинг профиля на ветки

- [x] 1.1 Реализовать матчинг профиля на ветки: `store.LoadDsinfoProductMatches` (JOIN `dsinfo_products` × `ds_products` × `ds_products_branch`) + обёртка `dsinfosvc.ResolveProfile`; двухуровневое сопоставление — точный `branch_key`, затем fallback по числовым токенам хвоста в пределах продукта (`sup/11-01-7` ↔ `fa-contracts-11-01-7`, exact приоритетнее, при нескольких кандидатах — is_default/id); статусы `ok`/`product_not_indexed`/`branch_not_indexed`; без записи в БД; интеграционные тесты трёх статусов, нестандартного имени линии, приоритета exact и обновления статуса после индексации линии без повторного parse
- [x] 1.2 Таблица сопоставления в `dsinfo parse` и `dsinfo show` (CLI и MCP): продукт, ссылка манифеста, совпавшая ветка (`branch_key`), статус, итог «matched N/M; K branches missing»; отдельное JSON-поле `matching` (не менять `dsinfo.Product` и `ShowResult.Products`); тесты вывода на фикстуре
- [x] 1.3 `go build ./...` + тесты store/dsinfosvc зелёные

## 2. Контекст анализа и приоритет

- [x] 2.1 Реализовать `ResolveAnalysisContext(ctx, db, explicitID)`: `--dsinfo`/`dsinfo_id` > env `CODEBASE_DSINFO_ID` > активный профиль > пусто; ошибка на несуществующий id и на некорректный формат env; юнит-тесты цепочки приоритетов и ошибок
- [x] 2.2 Ввести `query.ProfileContext` + опцию `query.New(db, query.WithProfile(pc))`; ось `profile_match` в порядке `not_generated` → `profile_match` → `is_default` → id; точки применения: `GetProcedureResult`/`GetProcedureDetails`, `LookupRetCode`/`LookupRetCodeByMessage`, все вызовы `chosenBranchQuery` (20, через параметр `priorityTail`), `SearchSymbol`/`SearchInContent`, `SearchTable` (candidates-фаза), `SearchQueryFragment` (фаза по `content_key`); без профиля оси вырождаются — поведение среза B
- [x] 2.3 `branch_requested` в результатах (заполняется по продукту выбранной копии; пусто вне профиля); соблюсти границы — `query callers`/`relations`/`inspect` и spec-поверхности не меняются; интеграционные тесты: точная линия, fallback, продукт вне профиля, одноимённые внутри одной ветки, линия `sup/11-01-7`
- [x] 2.4 Ответные конверты: `warnings []string` в CLI `querySuccessResponse` и в MCP `runQueryOpt`; агрегация — одна строка на (продукт, запрошенная линия, использованная линия); тесты JSON-контракта и человекочитаемого/`--summary` вывода

## 3. CLI

- [x] 3.1 Persistent `--dsinfo <id>` на группах `query`/`rti`/`trc` (`cmd/query.go`, `cmd/rti.go`, `cmd/trc.go`), чтение в листовых командах; `init`/`update`/`dsinfo`/`health`/`stats` флаг не принимают; тест регистрации флагов
- [x] 3.2 Проброс `DsinfoID` через Params-структуры `rtisvc`/`trcsvc` (enrichment-команды) из CLI-хендлеров; разрешение контекста после открытия БД
- [x] 3.3 Предупреждение при fallback: агрегированная строка в `warnings` на продукт; тест человекочитаемого и JSON-вывода

## 4. RTI/TRC enrichment

- [x] 4.1 `internal/rti/enrich.go`: запросы с `query.WithProfile`; поля `Product`/`BranchUsed` (snake_case json) в `ProcedureEnrichment`; интерфейс `ProcedureLookup` не меняется; тесты: имя в двух продуктах — выбран профильный; без профиля — поведение среза B
- [x] 4.2 `internal/trc/enrich.go`: то же для событий и агрегаций (оба пути `enrichProcedureAggregates`); тест на мультиветочной фикстуре

## 5. MCP

- [x] 5.1 Параметр `dsinfo_id` (optional integer) на учитывающих профиль инструментах — entity/API/retcode/desc/symbol query и rti/trc enrichment; НЕ добавлять `codebase_query_callers`/`codebase_query_relations`/`codebase_query_inspect`, spec-инструментам, `dsinfo_*` и review; обновить unit-тесты схем
- [x] 5.2 Smoke MCP: JSON-RPC прогон с `dsinfo_id` на реальном профиле — `branch_requested`/`branch_used` и `warnings`; вызов без параметра — прежнее поведение

## 6. Финальная верификация

- [x] 6.1 `go test ./...` (unit+integration: dsinfosvc/store/query/rti/trc/mcp/cmd) зелёные
- [x] 6.2 Бамп buildNumber (1645; после ревью поднят до 1646 в 7.8) в `cmd/root.go` (константа `buildNumber`) и пересборка `CodeBase.exe` — до smoke, чтобы проверка шла на новом бинарнике
- [x] 6.3 Сквозной smoke на FA + реальном `Modifications/DiasoftSystemInfo.csv` собранным бинарником: parse → таблица статусов (в т.ч. fallback-линия); `query procedure --name <proc, различающаяся между ветками> --dsinfo <id>` → тело SP-линии с `branch_requested`; без флага → develop; fallback с warning; `query table`/`query symbol`/`query desc-search`/`query retcode` с профилем; `query callers`/`query spec search` с профилем — без изменений и без warnings; `rti details --dsinfo` → `product`/`branch_used`; несуществующий `--dsinfo 999` → ошибка; `openspec validate dsinfo-integration --strict`
- [x] 6.4 README: раздел профиля развёртывания (пример сценария инцидента), цепочка приоритета, порядок осей, правило матчинга (точное + fallback по версии), границы применения по поверхностям, семантика fallback, таблица параметров CLI/MCP; обновить счётчики/таблицы MCP

## 7. Фиксы по code review (Modifications/Code-review-dsinfo-integration.md)

- [x] 7.1 P1.1: предупреждения по фактическому ответу — `query.CollectBranchWarnings` (рефлексивный обход элементов, агрегация по (продукт, запрошенные линии, использованная линия)); профильные warnings из `ResolveAnalysisContext` удалены; `queryWarnings`-глобал и warnings в резолвере убраны; текст без утверждения «default-ветка», отдельный текст для копии без ветки; пустой ответ — без предупреждений; тесты (линия ok, сущности нет; коллектор; empty)
- [x] 7.2 P1.2: многострочные продукты — ok-линии приоритетнее fallback'а (default → старшая версия через `branchkey.CompareVersion` → id), `Requested` — список всех линий продукта, `branch_requested` через запятую, `used ∈ Requested` без предупреждения; тест на ok+missing и две ok
- [x] 7.3 P2.4: `NumericTokens` — только чисто числовые сегменты (цифры в `fa-ftfns440…` игнорируются); тест на `sup/11-01-11`
- [x] 7.4 P2.5: сводка parse/show — distinct-продукты (matched = ≥1 ok-строки), BranchesMissing = строки branch_not_indexed; тесты обновлены
- [x] 7.5 P2.3 (решение: ослабить спеку): идентификатор валидируется при применении контекста; на границах и без элементов enrichment не разрешается — требование цепочки переписано
- [x] 7.6 P3: наследование Product/BranchUsed/BranchRequested вложенным params/tables API (мёртвые вызовы удалены); ошибки `optionalInt` в rti-хендлерах MCP; warnings/product/branch в человекочитаемом RTI; контекст команды вместо `context.Background()`; desc-search — приоритет продукта профиля при коллизии (kind, name) + тест
- [x] 7.7 Артефакты приведены к фактической семантике (design D1/D2/D3/D4, дельты dsinfo-resolution/entity-queries/api-relations/rti/trc/mcp, proposal); `openspec validate --strict`
- [x] 7.8 Бамп buildNumber до 1646, пересборка `CodeBase.exe` и повторный smoke на FA (parse → multi-line fa-contracts-ext; mismatch warning; границы без warnings; MCP: ручной JSON-RPC через пайп не запустился, покрыт mcp integration + schema-тестами)

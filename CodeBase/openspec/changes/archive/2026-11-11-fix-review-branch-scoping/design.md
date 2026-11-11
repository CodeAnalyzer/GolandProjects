## Context

См. `proposal.md` — Why. Текущее состояние, важное для подхода:

- `Runner` (`internal/review/runner.go`) получает из индекса только
  `fileID` и `productID` (`reviewExecContext`), а `indexedFile`
  (`internal/review/types.go`) не содержит `ds_product_branch_id`.
- Все lookup'ы review живут в `internal/review/review_lookup.go`; часть из них
  делает прямые SQL-запросы, часть — batch-запросы в
  `internal/store/db_lookup_sql.go`.
- В query-слое уже есть `store.LookupScope{ProductID, BranchID}`
  (`internal/store/lookup_scope.go`), а `FindLatestSQLColumnDefinitionType`
  уже принимает `BranchID`; review передаёт `BranchID: 0`.
- Цепочка профиля уже реализована в `dsinfosvc.ResolveAnalysisContext`
  (`--dsinfo` / `CODEBASE_DSINFO_ID` / активный профиль / none) и даёт
  `AnalysisContext.Branches map[productID]branchID`.
- `cmd/review.go` не имеет `--dsinfo`; MCP `codebase_review_sql` не имеет
  `dsinfo_id`. `analysisDsInfoID` — общий пакетный var, на который уже вешают
  флаг `query`/`rti`/`trc`.
- В текущей БД 0 файлов с `branch_id/product_id = NULL`; все 15 продуктов
  имеют default-ветку. Индексы `files(ds_product_id)` и
  `files(ds_product_branch_id)` существуют.
- EXT-продукты (`fa-contracts-ext`, `fa-dbmaintenance-ext`) поставляются в банк
  как `develop ∪ prj/<банк>`: develop — общая логика (default-ветка),
  `prj/*` — уникальный функционал банка, пересечение имён между ветками — 0
  (замер по индексу). В prj-ветках 51 SQL-файл; 6 таблиц в DML-контексте
  prj-файлов имеют создателя только в ext-develop — по модели «одна разрешённая
  ветка на продукт» это ложные severity-1 findings уже сегодня.

## Goals / Non-Goals

**Goals:**
- Единая точка определения scope для всех DB-lookup'ов review.
- Убрать межветочные ложные срабатывания, сохранив настоящие находки внутри
  целевой ветки.
- Сохранить поведение для файлов без ветки (product-scope).
- Дать review те же поверхности профиля, что у query/rti/trc.

**Non-Goals:**
- Не менять набор правил, их severity и имена.
- Не менять поведение query/RTI/TRC поверхностей (кроме добавления review в
  общую цепочку разрешения профиля).
- Не вводить жёсткую фильтрацию для чужих продуктов: профиль остаётся
  приоритетом с fallback на default-ветку.
- Не менять схему БД.

## Decisions

### D1. `ReviewScope` — единый объект области видимости

```go
type ReviewScope struct {
    FileID        int64
    ProductID     int64              // продукт проверяемого файла
    BranchID      int64              // ветка файла; 0 = product-scope (branch_id NULL)
    ProductBranch map[int64][]int64  // productID -> разрешённые ветки (область продукта)
}
```

Значение карты — множество веток: у обычных продуктов одна ветка, у
EXT-продуктов — объединение (см. D2). В SQL карта разворачивается в плоские
пары `unnest`.

`Runner` строит scope один раз в `RunSQLFileCtx` после `getIndexedFile` и
хранит его на время запуска; prewarm и ленивые пути читают один и тот же
scope. Альтернатива — передавать product/branch в каждый lookup явно —
отклонена: слишком много сигнатур и риск рассинхронизации prewarm/lazy.

### D2. Разрешение области по продуктам

- Свой продукт: `files.ds_product_branch_id` (авторитетно, профиль не
  переопределяет).
- Чужие продукты: `AnalysisContext.Branches[productID]` из
  `ResolveAnalysisContext`; если продукта нет в профиле или линия не
  проиндексирована — `ds_products_branch.is_default`.
- `branch_id = NULL` у своего продукта: `BranchID = 0` → product-scope.
- **EXT-продукты** (есть ветка с ключом `prj/*`; поставка в банк =
  `develop ∪ prj/<банк>`, имена между ветками дизъюнктны):
  - файл в ветке `prj/*`: область своего продукта = `{ветка файла} ∪
    {default-ветка}` — отражает фактическую поставку банка;
  - файл в default-ветке EXT-продукта: область = `{default} ∪ {все prj/*}` —
    develop-файл в поставке банка видит prj-сущности;
  - чужой EXT-продукт: область = `{default} ∪ {все prj/*}`, а если профиль
    указывает `prj/*`-линию этого продукта — `{default} ∪ {профильная prj}`.
  Детект EXT-ветки — `ds_products_branch.branch_key LIKE 'prj/%'` (оба
  EXT-продукта следуют паттерну; без изменения схемы).

Карта `ProductBranch` строится один раз: свой продукт + все продукты профиля
+ default-ветки остальных продуктов; для EXT-продуктов значение — множество
веток (union). Полнота карты позволяет одному SQL-запросу сравнивать
кандидатов без N+1.

### D3. Три режима lookup'ов

| Режим | Предикат/порядок | Правила |
|---|---|---|
| existence | `f.ds_product_branch_id = ANY(область(f.ds_product_id))` (кроме `branch=0` — any; область = одна ветка, для EXT — union из D2) | `execNotExistsProc`, `tableExistsInMetadata`, `indexExistsInDB` |
| value | `ORDER BY file_id=$file DESC, (product,branch)=(own) DESC, branch IN область(product) DESC, product=own DESC, branch=default(product) DESC, id DESC` | `datatype`, `dateIntoString`, `emptyStringDate`, `isNullSameTypes`, `diffTypesComparison`, `floatToStringConvert`, `excessProcParams`, `duplicateOutputVariable`, `indexWrong`, `deferredUpdate`, `memoryUniqueIndexUpdate`, `useFuncInIndCol` |
| identity | объединение по продуктам без ветки; для процедур — свой продукт раньше чужого | `foreignTablesUsing`, `foreignPTablesUsing`, `foreignProcedureUsing` |

Исключения (проверяются глобально по имени, без веточного скоупинга):
декларации in-memory таблиц (`memory_tables`; задача 4.5 сохраняет текущее
поведение) — физическая сущность одна на БД; константы `#define`
(`h_files_defines`, `FindHDefineExistsByName`, правило
`statementsWithJoinsRequireAliases`) — объявления общие. Тип макроса
(`lookupMacroSignature`) при этом остаётся в value-режиме: асимметрия
«существование константы глобально, значение сигнатуры — с приоритетом
области» осознанна (факт объявления един, а сигнатура может меняться между
ветками).

Почему identity без ветки: «чужой продукт» — свойство продукта. Если таблица
создана в своём продукте в другой ветке, это не «чужая таблица», а
отсутствие в целевой ветке (его ловит existence-правило). Фильтрация identity
по ветке дала бы ложные `foreign*` findings.

Для EXT value-режима union не создаёт конфликтов: имена в `develop` и
`prj/*` дизъюнктны, один кандидат на имя попадает ровно в одну ступень.

### D4. SQL-реализация scope

- Передавать карту `ProductBranch` в запрос двумя массивами
  (`pq.Array`) и разворачивать через `unnest($n::bigint[], $m::bigint[])
  AS pb(product_id, branch_id)`; карта `map[int64][]int64` разворачивается
  в плоские пары — одна пара на разрешённую ветку продукта (для EXT
  продуктов пар несколько).
- Для existence: `JOIN pb ON pb.product_id = f.ds_product_id` и
  `WHERE pb.branch_id = 0 OR f.ds_product_branch_id = pb.branch_id`
  (эквивалент `= ANY(область)` из D3).
- Для value: `LEFT JOIN pb` и `ORDER BY` по ступеням из D3.
- Все запросы параметризованы; строковая интерполяция не используется.
- Batch-функции `db_lookup_sql.go` получают `LookupScope` (ProductID,
  BranchID) и карту; прямые запросы в `review_lookup.go`
  (`batchLoadIndexCandidates`, `lookupIndexExists`,
  `lookupIndexFieldsByName`, `findAPITableNames`, `lookupProcedureParams`,
  `lookupAPIContractParams`, `lookupMacroSignature`) получают те же параметры.
- API-индексы (`api_business_object_table_indexes`) скоупятся через
  `api_business_object_tables.file_id → files`, API-контракты — через
  `api_contracts.file_id`.

### D5. `procDuplicate`: область продукта + within-file

- Межфайловый поиск: `SELECT DISTINCT f.id ... WHERE proc_name=...` в
  пределах области своего продукта из D2 (для не-EXT файлов это продукт и
  ветка файла); `Distinct files > 1` → finding (прежние исключения
  `upload`/`.t01` сохраняются).
- Within-file: считать `parsed.Procedures` по имени > 1 (без БД) → finding.
  Это закрывает расхождение со спекой, которое `DISTINCT f.id` маскировал.
- Дубли вне области не учитываются. Семантика области едина с existence:
  имя в `prj/<банк>` и в develop одного EXT-продукта — дубль в поставке банка
  (текущие данные: пересечение 0, случай теоретический, но область одна на
  все режимы своего продукта).

### D6. Поверхности профиля

- `cmd/review.go`: `reviewCmd.PersistentFlags().Int64Var(&analysisDsInfoID,
  "dsinfo", 0, ...)`.
- `review.Options` получает `DsinfoID int64`; `reviewsvc.ExecuteCtx`/`ExecuteWithCtx`
  пробрасывают его в `Runner`.
- `internal/mcp/registry.go`: в схему `codebase_review_sql` добавляется
  `dsinfo_id`, handler вызывает `reviewsvc.ExecuteWithCtx` с этим id.
- `Runner` разрешает `AnalysisContext` только когда есть БД и есть lookup'и;
  несуществующий явный id → ошибка review (без молчаливого fallback).

### D7. Кэши и prewarm

- Один запуск review = один файл = один scope, поэтому кэши можно оставить
  keyed-by-name; меняется только запрос, которым они наполняются.
- `prewarmColTypeCache`, `prewarmIndexCache`, `prewarmProcCaches`,
  `prewarmTableProductIDs`, `prewarmTableCreators` получают scope и передают
  его в batch-функции. Ленивые пути используют тот же scope, чтобы не
  расходиться с prewarm.
- Убрать first-wins дедуп между ветками в `indexFieldsByName`: при scope
  кандидаты одной области не конфликтуют, а межветочные больше не попадают в
  кэш.

### D8. Совместимость с `branch_id = NULL` и EXT-продуктами

`BranchID = 0` означает «без фильтра по ветке» только для своего продукта.
Для чужих продуктов default-ветка всегда есть (проверено: 15/15), поэтому
`0` там не возникает. Это сохраняет прежнее поведение для legacy-файлов.

Деградация value-тиров при `BranchID = 0` (branch_id NULL у файла):
веточные тиры своего продукта («продукт+ветка», «разрешённая ветка»)
схлопываются — все кандидаты своего продукта получают приоритет тира
«продукт»; далее идут разрешённая ветка/область чужих продуктов, затем
default-ветки чужих, затем глобально самый свежий (`id DESC`) как финальный
tie-break. Existence при `BranchID = 0` — все ветки продукта (см. D3).

EXT-области (D2) не зависят от `BranchID = 0`: prj/default-файлы EXT-продукта
всегда имеют конкретную ветку; для чужих EXT-продуктов область включает
default, поэтому fallback-ветка существует всегда.

## Risks / Trade-offs

- **Появление новых findings там, где сущность отсутствует именно в целевой
  ветке** → это корректное поведение (деплой в ветку без зависимости опасен);
  смягчение: для чужих продуктов остаётся fallback на default-ветку, для
  `branch_id NULL` — product-scope, для EXT — union-область. Отразить в
  notes к релизу. Замер масштаба новых existence-находок на текущем индексе
  (имя существует в продукте, но отсутствует в ветке файла):
  `fa-contracts-11-01-7` — 28 имён, `fa-contracts-11-01-6` — 63,
  `fa-cards-11-01-9` — 53, develop-ветки прочих продуктов — 0–16; пул мал
  относительно 10 635 устраняемых ложных.
- **Детект EXT-веток по `branch_key LIKE 'prj/%'`** — эвристика без
  отдельного признака в схеме. У обоих EXT-продуктов паттерн соблюдается;
  если `prj/*`-ветка однажды появится у обычного продукта, его область
  ошибочно расширится (меньше findings, риск FN, не FP). При необходимости —
  явный признак отдельным change.
- **Расхождение prewarm и ленивого пути** → оба используют один `ReviewScope`;
  тест на эквивалентность.
- **Производительность**: добавляются JOIN к `files` и сортировка по
  product/branch. Индексы `files(ds_product_id)`,
  `files(ds_product_branch_id)` и `sql_*(file_id)` уже есть; batch-запросы
  остаются batch. Приемлемо.
- **Identity-режим без ветки может дать FN** (сущность есть в чужом продукте в
  неродственной ветке — не считается чужой) → принято: FP хуже FN для
  severity 3.
- **`procDuplicate` within-file по `parsed.Procedures`** может отличаться от
  строк `sql_procedures` (парсер видит expanded-контент) → используем parsed,
  это согласуется с остальными правилами review.
- **Несуществующий `--dsinfo` теперь валит review** → соответствует уже
  принятой семантике dsinfo (ошибка без fallback).

## Migration Plan

- Схема БД и индексация не меняются; переиндексация не требуется.
- Изменение поведенческое: после сборки бинаря review сразу использует scope.
- Откат — возврат бинаря; данные не затрагиваются.
- Проверка на реальной БД: прогнать review на
  `fa-contracts/.../Cons_BlockLimitAuto_Check.sql` (ожидается отсутствие
  `procDuplicate`) и на файле с реальным дублем в одной ветке (ожидается
  finding).
- Проверка EXT: prj-файлы `fa-contracts-ext-novikom-persform` (51 SQL-файл) —
  6 таблиц с создателем только в ext-develop (замер ревью) не дают
  `tableExistsInMetadata`; обращения prj-файлов к ext-develop-процедурам не
  дают `execNotExistsProc`.

## Measurements (2026-11-11, живой индекс localhost:5435/codebase)

Замер «до/после»: старый бинарь (build 1684, без scope) против нового
(данная сборка), правила `procDuplicate,datatype,excessProcParams,indexWrong`,
выборка — 40 SQL-файлов `FA/fa-contracts-11017/Consumer/SERVER` (SP-линия
`fa-contracts-11-01-7`):

| Правило | До | После |
|---|---|---|
| procDuplicate | 40 (по одному на файл — межветочный шум с develop) | 0 |
| datatype | 6 | 6 (состав находок идентичен, только-before/only-after пуст) |
| excessProcParams / indexWrong | 0 / 0 | 0 / 0 |

`tableExistsInMetadata` + `execNotExistsProc` на той же выборке: 0/0 до и
после (ложных severity-1 на SP-линии нет).

Реальный дубль сохраняется: `test_Log_FindOptionByCurrUser`
(fa-administrator, 11 файлов одной ветки) → `procDuplicate` finding на
каждом из файлов. Ветка файла `fa-contracts-11017/.../Cons_BlockLimitAuto_Check.sql`
в индексе — `fa-contracts-11-01-7` (задача 1.1 подтверждена на живых данных);
review этого файла (и `fa-contracts`, `fa-contracts-11016` аналогов) — 0 findings.

EXT-проверка (задача 6.6): 51 prj-файл `FA/fa-contracts-ext-novikom-persform`
(`fa-contracts-ext/prj/novikom-persform`) — до и после одинаково:
`execNotExistsProc` 13 (все — `M_PRINT_MES`, макрос с 120 объявлениями в
`h_files_defines` и 0 копий в `sql_procedures`; давний квирк правила,
вне границ этого change), `tableExistsInMetadata` 4
(`tCNE_PFBorrowerCreditHist_BAK` — создателя нет ни в одной ветке
глобально, истинная находка), `foreignProcedureUsing`/`foreignTablesUsing`/
`procDuplicate` — 0. Все 13 реальных процедур ext-develop
(`FCD_CNE_*`, `FCD_CONEXT_*`, `API_LnExt_*`), вызываемых из prj-файлов,
разрешаются через union-область `{prj, develop}` — новых ложных
`execNotExistsProc`/`tableExistsInMetadata` prj-обращения к ext-develop не
дают.

## Implementation Notes

- Продукты без единой строки `ds_products_branch` получают область `{0}`
  (product-scope); собственный продукт без строк веток при `BranchID != 0` —
  `{BranchID}` (строка ветки могла быть удалена, файлу доверяем).
- `BatchLookupTableProductIDs` остаётся без веточного scope: identity-режим
  возвращает множество продуктов, ветка не влияет (D3).
- `findAPITableNames` остаётся глобальным: identity-исключение API-таблиц
  для `foreignPTablesUsing` (ось продукта, ветка не влияет, D3).
- `indexer_sql_pas.go` вызывает `FindLatestSQLColumnDefinitionType` с пустыми
  pairs — legacy-глобальное поведение индексатора, вне границ change.
- Файлы с `ds_product_id IS NULL` не попадают ни в одну область
  (area-предикат `pb.product_id = f.ds_product_id` не матчит NULL) — спека
  review-scope объявляет их вне веточных областей.
- Парсер SQL дедуплицирует `parsed.Procedures` по имени (два `CREATE
  PROCEDURE` одного имени = одна запись), поэтому within-file детект
  `procDuplicate` (D5) реализован сканом заголовков
  `create\s+proc(?:edure)?\s+<name>` по контенту файла; finding — на втором
  и последующих вхождениях имени. Тот же квирк: `exec`-вызовы после
  select-блока не попадают в `parsed.Calls` (давнее ограничение парсера,
  не блокер).
- Квирки построчного скана заголовков (code-review R1/R2, 2026-11-11,
  решения — не править до появления реальных FP/FN): заголовок в комментарии
  (`-- create proc MyProc`) засчитывается как определение (потенциальный FP
  при живом дубле — в FA не встречается); заголовок, разорванный переводом
  строки (`CREATE\nPROC`), не ловится (FN); имена с двумя `#` (`##Tmp`)
  не покрываются паттерном (конвенция `#?` едина с остальными regex review;
  межфайловый детект через `parsed.Procedures` такие имена ловит).
- `BatchFindColumnDefinitionTypes` при нулевом контексте (пустые pairs,
  legacy-вызовы индексатора) изменил tie-break: добавленный тир
  `default-ветка` теперь предпочитается глобальному `id DESC` —
  гармонизация с ленивым `FindLatestSQLColumnDefinitionType` (у которого
  default-тир был всегда) по задаче 2.4; покрыто тестом legacy (code-review
  R4; отметить в notes к релизу).

## Open Questions

Нет: все развилки (strict vs fallback, product+branch для дублей, кросс-продуктовый
scope, `branch_id NULL`, EXT-union) закрыты решениями пользователя 2026-11-10.

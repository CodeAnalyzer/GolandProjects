# Tasks: branch-aware-lookup

## 1. LookupScope и ORDER BY

- [x] 1.1 Ввести `store.LookupScope{ProductID, BranchID}`; заменить позиционный productID в `FindLatestSQLProcedureIDsByNames`, `FindLatestSQLTableIDsByNames`, `FindLatestSQLColumnDefinitionType`, `FindLatestPASClassIDsByNames`, `FindLatestDFMFormIDsByClassNames`, `FindLatestAPIContractIDsByNamesAndKinds`, `FindLatestEventContractIDsByNames` и остальных `FindLatest*`; обновить все места вызова; проверить: `go build ./...` + существующие тесты
- [x] 1.2 Добавить оси `branch_match`/`default_branch` в ORDER BY всех `FindLatest*` (`db_lookup_sql.go`, `db_lookup_pas.go`, `db_lookup_dfm.go`, `db_lookup_api.go`) по приоритету `not_generated → branch_match → product_match → default_branch → id DESC`; проверить планами запросов, что индекс `files.ds_product_branch_id` используется (добавить при необходимости)

## 2. Детерминизм деталей процедуры

- [x] 2.1 Явный `ORDER BY (приоритет из 1.2) LIMIT 1` в `GetProcedureDetails`/`GetProcedureResult` (`internal/query/query_sql.go`); поля `branch_used`/`product` в результате; тест: две копии процедуры — стабильно выбирается одна, при повторных вызовах та же

## 3. branch_used в entity/API-запросах

- [x] 3.1 Пробросить `branch_used` (+`product`) в результаты entity queries (`internal/query/*`): tables, callers, fragments, retcodes, PAS, DFM, reports, JS/SMF; единый helper выборки ветки файла; дедупликация списковых запросов по (естественный ключ сущности, продукт) приоритетом default-ветки (методы — класс+имя; фрагменты — схлопывание побайтовых копий); тесты на мультиветочной фикстуре
- [x] 3.2 То же для API-запросов: контракты, таблицы/параметры/индексы, реализации, публикаторы, потребители, граф (`query_api.go`); отсутствие дублирующих рёбер из второй ветки — тест
- [x] 3.3 `SearchSymbol` (`internal/query/query.go`): дедуп строк, отличающихся только веткой одного продукта (ключ — тип, имя, продукт; одноимённые методы разных классов не схлопываются), `branch_used`/`product` в `SymbolResult`; тест: символ в 3 ветках → одна строка develop
- [x] 3.4 Exact-слой desc-search и гибридный результат (`internal/query/desc_query.go`): дедуп по (тип, имя, продукт) приоритетом default-ветки (зеркало правила LSA-корпуса среза A), `product`/`branch_used` в строке; тест: описание в двух ветках → одна строка
- [x] 3.5 Spec-поверхности: exact-слой spec-search, `spec_by_code`/`spec_coverage`/`spec_usecase`/`spec_deps`/`spec_history` — дедуп по (продукт, slug) приоритетом default-конфигурации с пометкой использованной конфигурации; `spec_config` перечисляет все конфигурации продукта с пометкой default; тесты на фикстуре develop + SP-worktree

## 4. Граф в scope источника

- [x] 4.1 В пост-обработке вызовов процедур (`indexer_postprocess_sql_calls.go`) передавать scope файла-источника вместо `0`; множество кандидатов ограничить ярусами (ветка источника → default-ветка продукта → другие продукты; остальные ветки того же продукта исключены — фильтрация в SQL, не только ORDER BY); группировка pending-пачек по `(product, branch)`, batch-запрос на группу; тесты: fragment в SP-ветке → ребро на SP-копию; имя только в чужой ветке того же продукта → ребро не создаётся; имя только в другом продукте → ребро создаётся
- [x] 4.2 То же для фрагментов/PAS/DFM/API-макросов (`indexer_postprocess_fragments.go`, `indexer_postprocess_pas.go`, `indexer_postprocess_callbacks.go`) и spec_code_mentions (`indexer_postprocess_spec.go` — сегодня productID ≠ 0, добавить ось ветки); тест изоляции веток: ни одно ребро не пересекает ветки одного продукта

## 5. Финальная верификация

- [x] 5.1 `go test ./...` (unit+integration: store/query/indexer) зелёные; регрессия на одно-веточном индексе — результаты идентичны прежним (сравнение до/после на тестовой БД)
- [x] 5.2 Бамп buildNumber до 1640 в `cmd/root.go` (константа `buildNumber`) и пересборка `CodeBase.exe` — до smoke, чтобы проверка шла на новом бинарнике
- [x] 5.3 Smoke на FA-дереве (develop + fa-contracts-11016/11017) собранным бинарником: `query procedure` по процедуре, различающейся между ветками, стабильно возвращает develop-копию с `branch_used`; callers из SP-каталога указывают на SP-копии; списки/поиск (`query tables`, symbol search, desc-search, spec_search, spec_coverage) без дублей веток с `branch_used`; `openspec validate branch-aware-lookup --strict`

## 6. Фиксы по code review

- [x] 6.1 Унифицировать `branch_used` на нормализованный `branch_key` во всех результатах query-слоя (было `branch_name` в query vs `branch_key` в specsvc)
- [x] 6.2 `FindLatestEventContractIDsByNames`: `module_key` после осей приоритета (default-ветка важнее алфавитного модуля)
- [x] 6.3 `apiChosenContractsCTE`: `contract_kind` в `DISTINCT ON` (одноимённые service/event не схлопываются)
- [x] 6.4 `SearchTable`: ключ (имя, продукт), `usages` в пределах выбранной ветки, enrichment (файлы/колонки/процедуры) с фильтром продукта и ветки; тест на двух продуктах
- [x] 6.5 Двухфазный дедуп (`chosenBranchQuery`: выбрать ветку → все строки выбранной ветки) во всех списковых запросах: symbols, tables/schema/index, PAS-методы, DFM/отчёты/JS/SMF, API, desc-search exact, spec-search (exact+trgm), spec_by_code, coverage, usecase, history, retcodes; одноимённые сущности одной ветки сохраняются
- [x] 6.6 `spec_by_code`: `line_number` исключён из ключа дедупа (сдвиг строк не порождает дубль); `spec_history` — `branch_used` сканируется в каждую запись
- [x] 6.7 API: вложенные параметры/таблицы наследуют `product`/`branch_used` контракта; убран мёртвый nil-check в `resolvePASClassTargets`
- [x] 6.8 Тесты на пробелы ревью: `branch_used` = `branch_key` (SP-копия), одноимённые строки одной ветки, изоляция enrichment по продукту, event-lookup по `owner_module`, API kind-одноимённики, spec_by_code со сдвигом строк, spec_history per-entry
- [x] 6.9 Артефакты согласованы с реализацией: дельты entity-queries/api-relations (query-слой без `branch_match`/`product_match`), relations-postprocessing (`not_generated` внутри ярусов), design.md (исключение review-фолбэка, двухфазный дедуп, `branch_key`)

# branch-aware-lookup: приоритет релизной ветки при выборе сущностей

## Why

После среза `branch-aware-indexing` в индексе законно сосуществуют копии
одной сущности из develop и релизных линий, но текущие lookup'и выбирают
«кто последний — тот и выиграл»: `GetProcedureDetails`/`GetProcedureResult`
вообще без `ORDER BY` (недетерминизм), `FindLatest*` — без оси ветки, а
пост-обработка графа резолвит имена глобально (`productID=0`), склеивая
рёбра между ветками. Любой запрос к мультиветочному индексу без этого
среза возвращает произвольную копию, а граф даёт ложные связи.

## What Changes

- **`LookupScope` вместо позиционного productID**: структура
  `{ProductID, BranchID}` в `FindLatest*`-функциях (~10 функций SQL/PAS/
  DFM/API-lookup'ов) и местах вызова.
- **Приоритет ветки в ORDER BY**: все lookup'и получают детерминированный
  порядок `not_generated → branch_match → product_match → default_branch →
  id DESC`; `GetProcedureDetails`/`GetProcedureResult` — явный
  `ORDER BY ... LIMIT 1` вместо неопределённого порядка.
- **Правило выбора без явной ветки**: копия из default-ветки (`is_default`)
  предпочтительнее остальных; результат сообщает фактическую ветку
  (`branch_used`).
- **Граф в scope источника**: пост-обработка (вызовы процедур, фрагменты,
  PAS↔DFM, API, spec_code_mentions) резолвит имена в ярусах кандидатов:
  ветка источника → default-ветка продукта → другие продукты; копии
  остальных веток того же продукта исключаются — ребро в чужую ветку не
  создаётся (остаётся неразрешённым).
- **Дедупликация веток в списках и поиске**: списковые entity queries,
  поиск символов, поиск описаний (exact-слой) и спек-поиск/spec-навигация
  возвращают одну строку на сущность/capability одного продукта (копии из
  других веток схлопываются по приоритету default-ветки, строка несёт
  `branch_used`); `spec_config` перечисляет все конфигурации продукта.
- **Не меняется**: CLI/MCP-контракты (новые поля `branch_used` — обратное
  совместимое расширение), выбор по профилю dsinfo — срез
  `dsinfo-integration`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/entity-queries`: новое требование — приоритет релизной ветки при
  выборе сущности (включая дедупликацию списковых запросов) и
  `branch_used` в результате; изменено требование «Детали SQL-процедуры» —
  детерминированный выбор копии.
- `query/api-relations-queries`: новое требование — приоритет ветки и
  `branch_used` в API-запросах и графе связей.
- `indexing/relations-postprocessing`: новое требование — резолв имён в
  scope ветки источника (ярусы кандидатов, включая spec_code_mentions)
  при пост-обработке графа.
- `query/symbol-search`, `query/description-search`,
  `query/spec-search`, `query/spec-queries`: новые требования —
  дедупликация копий из веток одного продукта в результатах поиска и
  spec-навигации (после среза A спеки релизных линий индексируются как
  отдельные конфигурации одного продукта).

## Impact

- **Код**: `internal/store/db_lookup_sql.go`, `db_lookup_pas.go`,
  `db_lookup_dfm.go`, `db_lookup_api.go` (`LookupScope`, `branch_match`),
  `internal/query/query_sql.go` (`ORDER BY/LIMIT 1`, поля `branch_used`),
  `internal/query/*` (проброс scope), `internal/query/query.go`
  (`SearchSymbol` — дедуп веток), `internal/query/desc_query.go` и
  spec-выборки (дедуп exact-слоёв), `internal/indexer/indexer_postprocess_sql_calls.go`,
  `indexer_postprocess_fragments.go`, `indexer_postprocess_spec.go`
  (scope источника вместо `0`), модель вывода (product/branch).
- **Схема БД**: без новых таблиц; возможны вспомогательные индексы для
  фильтра по `files.ds_product_branch_id` (проверяется по планам).
- **Совместимость**: на одно-веточном индексе поведение идентично (оси
  ветки нет — порядок вырождается в прежний); JSON-контракт расширяется
  полями `product`/`branch_used` (обратно совместимо, `format_version` не
  меняется).

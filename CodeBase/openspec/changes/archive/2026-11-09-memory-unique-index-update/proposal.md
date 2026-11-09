## Why

Memory-optimized таблицы MSSQL (переводимые в память через `ModuleTableInMemory.lst`,
см. capability `indexing/lst-parsing`) enforcement'ят уникальные индексы немедленно,
внутри оператора — классические приёмы вроде «обмена значениями уникального ключа
между строками через UPDATE», работающие на дисковых таблицах за счёт отложенного
применения индексных изменений, на них ломаются. Review не видит этот класс
ошибок: существующее правило `deferredUpdate` ловит только коллизию с
хинтованным индексом того же оператора и не знает про признак «таблица в памяти».

## What Changes

- Новое review-правило `memoryUniqueIndexUpdate` (severity 1, deploy stopper):
  для каждого `UPDATE`, чья целевая таблица объявлена в памяти (есть в
  `memory_tables`), изменяемые колонки секции `SET` не должны входить в состав
  полей уникального индекса этой таблицы.
- Уникальность определяется строго по признаку индекса, имя (конвенция
  `XPK*`/`XAK*`) уникальностью не является: `is_unique=TRUE` в
  `sql_index_definitions` (обычные `p*`-таблицы: `CREATE UNIQUE INDEX`,
  `M_CRT_INDEX('UNIQUE',...)`, dsxml `IndexType=2`) и `index_type IN (2,3)` в
  `api_business_object_table_indexes` (`pAPI_*`-таблицы; проверено по рабочей БД:
  оба значения дают уникальный индекс). Контрпример на конвенцию имён:
  `pObjectForRight` — `XPK`, объявлен `M_CRT_INDEX('', ...)` и неуникален.
- Покрытие по живому индексу FA (change-review 2026-11-09): признак имеют
  7 из 26 memory-таблиц (`pRuleDoc_QtyStack` + 6 API с XPK `index_type` 2/3);
  остальные 19 имеют только неуникальные индексы — правило для них неактивно
  по объективной причине (уникальных полей нет).
- Сопоставление таблицы с признаком in-memory — по имени, без product-scope
  (прецедент D3 из `memory-tables-flag`).
- Общий кэш индексов review (`indexCandCache`) обогащается признаком
  `IsUnique` у кандидата индекса.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: новое требование «Изменение полей уникального
  индекса таблицы в памяти» — обнаружение `UPDATE` секции `SET` которой
  затрагивает поля уникального индекса таблицы, объявленной в памяти;
  формирование finding `memoryUniqueIndexUpdate` severity 1.

## Impact

- **Код**: `internal/review/types.go` (константа `RuleMemoryUniqueIndexUpdate`,
  `IsUnique` в `tableIndexCandidate`), `internal/review/catalog.go` (запись
  каталога, severity 1), `internal/review/review_rules.go`
  (`checkMemoryUniqueIndexUpdate`), `internal/review/review_lookup.go`
  (`is_unique`/`index_type IN (2,3)` в двух SQL `batchLoadIndexCandidates`,
  lazy-загрузка множества имён `memory_tables` с флагом загруженности),
  `internal/review/runner.go` (поле кэша + сброс в `RunSQLFileCtx`),
  `internal/review/runner_test.go` (`TestEnabledRuleSet_*` по прецеденту).
- **CLI/MCP**: правило попадает в дефолтный набор review; новые ключи/инструменты
  не вводятся; отключается точечно через существующий `--rules`.
- **Схема БД**: без изменений — используются существующие `memory_tables` (v7),
  `sql_index_definitions`, `api_business_object_table_indexes`.
- **Документация**: README — новый пункт в секцию Deploy stoppers + перенос
  ошибочно размещённых в PostgreReq-секции `deferredUpdate`/`inSubQuery`/
  `varcharSize`/`columnInsert` (все severity 1 по каталогу).
- **Известные ограничения распознавания** (зафиксировано в design, вне scope):
  `M_CRT_INDEX('_UNIQUE_', ...)` не распознаётся парсером как уникальный
  (987 строк в индексе — на memory-таблицы не влияет, проверено), inline
  `UNIQUE`-constraint в `CREATE TABLE` в индекс не попадает вовсе; возможный
  парсер-фикс — отдельный change.

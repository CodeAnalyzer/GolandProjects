# Предложение: материализация `owner_key` в строку `symbols` на индексации

**Дата:** 2026-11-03
**Статус:** принято в разработку как OpenSpec change `symbols-owner-key-materialization` (2026-11-04, после ревизии кода: Н1 — третий потребитель выражения `legacyReportFormLookup`; Н2 — sanitizeUTF8String без TRIM + регистровая дельта ссылки vs декларации; Н3 — сужение backfill по типам + set-based UPDATE; Н4 — карта formClassByID без lookup, over-collapse касается только method). Актуальные решения — в design.md change; расхождения с этим документом трактуются в пользу change.
**Источник:** `BUG-query-search-performance-bottlenecks-20261103.md`, finding B4 (обсуждение от 2026-11-03; консенсус: долгосрочный фикс — материализация, LATERAL-костыль не делаем).

## Зачем

`symbolOwnerKeyExpr` (`internal/query/query.go:372-377`) — CASE с коррелированными
подзапросами к `pas_methods → pas_classes` (для `method`) и
`dfm_components → dfm_forms` (для `component`). Выражение включается в CTE `base`
в **двух** запросах:

- `SearchSymbol` (`internal/query/query.go:407`),
- `SearchInContent` (`internal/query/query.go:523`).

Оно вычисляется **для каждой** строки, попавшей в `base`, до дедупликации. Замер
bug-репорта: `--name DoAction --type method` → 4 278 строк × SubPlan (28 625 buffer
hits), 13,2 с холодный / 259–408 мс прогретый.

Отложить вычисление на «после `DISTINCT ON`/`LIMIT`» нельзя: `owner_key` — часть
ключа дедупа `DISTINCT ON (name_key, symbol_type, owner_key, product_key)`
(`query.go:444` и `:541`) — без него одноимённые методы разных классов схлопнутся.
Единственный способ убрать per-row коррелированные подзапросы — хранить владельца
в самой строке `symbols`.

## Что сделать

### 1. Схема (bump `CurrentSchemaVersion` v4 → v5)

```sql
ALTER TABLE symbols ADD COLUMN IF NOT EXISTS owner_name TEXT NOT NULL DEFAULT '';
```

- `DEFAULT ''` покрывает все типы, кроме `method`/`component` (сегодняшний CASE
  для них тоже возвращает `''`).
- **Обязателен bump** `CurrentSchemaVersion` (`internal/store/db_schema.go:13`,
  сейчас `codebase_schema_v4`) — иначе fast-path по маркеру версии молча не
  применит ALTER на существующих БД (правило readonly-schema-policy из AGENTS.md).

### 2. Backfill существующих данных — именованная миграция

По образцу `files_is_generated_backfill_v1` (`db_schema.go:1249-1257`,
`applyMigrationTx`): миграция `symbols_owner_name_backfill_v1` выполняет
однократный `UPDATE symbols SET owner_name = <выражение>` **ровно с текущим
SQL-выражением** `symbolOwnerKeyExpr` (те же два коррелированных подзапроса).
Это гарантирует побитовую семантическую эквивалентность для всех существующих
строк — миграция дорогая (одноразовый проход по symbols), но выполняется в
транзакции единожды на БД.

### 3. Индексатор

- `model.Symbol` (`internal/model/model.go:369-379`) — добавить поле `OwnerName`.
- `BatchInsertSymbols` (`internal/store/db_insert_j.go:107-108`) — включить
  колонку в CopyIn.
- Заполнение (данные уже есть в руках в момент создания symbols):
  - **method**: цикл после `FindPASMethodIDsByUnit`
    (`internal/indexer/indexer_sql_pas.go:920-939`) — `OwnerName = TRIM(method.ClassName)`
    (поле существует: `model.PASMethod.ClassName`, `model.go:188`).
  - **component**: цикл после `FindDFMComponentIDsByForm`
    (`internal/indexer/indexer.go:819-845`) — `OwnerName = FormClass` родительской
    формы (`model.DFMForm.FormClass`, `model.go:298`; доступен в том же
    parse-проходе `parseDFMFile`, при необходимости — formID→form_class одним
    lookup'ом по образцу `componentIDsCache`).
  - остальные типы (procedure, table, class, form, report_form, spec-символы
    `indexer_spec.go:268/331/438` и т.д.) — не заполнять (`''` по умолчанию).

### 4. Запросы

В `SearchSymbol` и `SearchInContent` заменить
`` LOWER(s.symbol_name) AS name_key, ` + symbolOwnerKeyExpr + ` AS owner_key ``
на
`` LOWER(s.symbol_name) AS name_key, COALESCE(s.owner_name, '') AS owner_key ``.
Константу `symbolOwnerKeyExpr` удалить. Других потребителей нет (grep: только
`:407` и `:523`).

## Осознанные семантические отличия (одно, и это улучшение)

Сегодня для method с **нерезолвнутым** классом (`class_id = 0`, pending-резолв не
состоялся) runtime-выражение возвращает `''` → одноимённые методы разных
неразрезолвленных классов **схлопываются в одну группу дедупа** (скрытый
over-collapse). После материализации `owner_name = ClassName` как записан в
коде → группы корректные. Значение для резолвнутых классов совпадает: строка
`ClassName` пишется тем же парсером, что и `pas_classes.class_name`.

## Оценка эффекта

- Из планов `SearchSymbol`/`SearchInContent` исчезают SubPlan-ы (4 278
  коррелированных lookup'ов на кейс `DoAction`); warm 259–408 мс → десятки мс,
  cold больше не выполняет тысячи случайных чтений сверх основного прохода.
- Выигрыш распространяется на **все** вызовы `symbol`/`content` (самый частый
  инструмент MCP: 35+151 вызовов в замере bug-репорта), а не только на
  патологические имена.

## Риски

- Schema-bump v5: все существующие БД при первом `update` получают ALTER +
  backfill (одна тяжёлая миграция; оценить время на FA-масштабе ~5,8 млн строк —
  ожидаемо единицы минут, вне критического пути последующих прогонов).
- Инкрементальный update: переиндексация файла пересоздаёт его symbols уже с
  `owner_name` — консистентно, reconcile не требуется.

## Верификация

1. Integration-тест схемы: колонка существует, backfill идемпотентен (повторный
   `InitSchemaCtx` не меняет заполненные значения — по образцу
   `db_lookup_spec_mentions_integration_test.go:239-288`).
2. Тест индексатора: после индексации PAS/DFM файлов `symbols.owner_name`
   заполнен только у `method`/`component`.
3. Эквивалентность: на копии боевой БД сравнить наборы строк `query symbol` для
   пачки имён до/после (совпадение, кроме задокументированного отличия выше).
4. `EXPLAIN (ANALYZE, BUFFERS)`: в планах `SearchSymbol`/`SearchInContent`
   отсутствуют `SubPlan`.
5. Замер до/после: `codebase query symbol --name DoAction --type method`
   (warm и cold).

## Связанные документы

- `Bug-reports/Realized/BUG-query-search-performance-bottlenecks-20261103.md` — B4.
- `Modifications/Realized/query-search-optimization-plan.md` — P5 (предшественник
  по symbol-поиску; там же оговорка о tool-level латентности).
- `Modifications/Realized/symbol-index-priority-entities-2ec932.md` — откуда
  symbols для `method`/`component` берутся.
# Design: fix-datatype-cross-product-lookup

## Context

Резолв типа колонки в review идёт по цепочке (все пути подтверждены кодом):

```
checkDatatype* (review_rules.go:2238 и др.)
  └─ cachedFindColumnDefinitionType (runner.go:392)      — кэш table|col
       ├─ hit  → значение из кэша
       └─ miss → FindLatestSQLColumnDefinitionType (db_lookup_sql.go:173)
                   WHERE table/col … ORDER BY id DESC LIMIT 1   ← глобально
                 sql.ErrNoRows → FindAPIColumnDefinitionType (db_lookup_sql.go:193)
                   API-контракты + BObjects, ORDER BY id DESC    ← глобально
prewarm: prewarmColTypeCache (runner.go:335) → BatchFindColumnDefinitionTypes
         (db_lookup_sql.go:223) — тот же глобальный latest-wins, батчем
```

Контекст запуска уже доступен: `RunSQLFileCtx` получает `indexedFile` с `ID`
и `DsProductID` (runner.go:85-94, обязательное условие `DsProductID != 0`);
кэш `colTypeCache` сбрасывается на каждый запуск файла (runner.go:66-68) —
контекст фиксирован в пределах запуска. Продуктовое скопирование уже применяется
в других правилах через `cachedLookupTableProductIDs` (review_lookup.go:107) —
precedent существует.

Данные (локальный индекс FA): 825 пар, где fa-contracts объявляет колонку, но
глобальный latest-wins выбирает чужой файл; 30 пар при этом меняют тип.
Источник дубликатов — ADP-адаптеры fa-reports (копии DDL с усечёнными типами),
разделяемые таблицы (dbo.spid в 7 продуктах), #temp-таблицы.

## Goals / Non-Goals

**Goals:**

- Детерминированное разрешение типа колонки относительно контекста
  анализируемого файла: same-file → same-product → global latest-wins.
- Одинаковая семантика в одиночном и батч-пути (prewarm), чтобы кэш не расходился
  с точечным lookup'ом.
- Продуктовая приоритизация API-fallback тем же правилом.

**Non-Goals:**

- Чистка парсерных артефактов в `sql_column_definitions` (`,name`,
  `numeric(15`, `DSMONEY)`) — отдельное изменение в `indexing/sql-parsing`.
- Fallback по графу зависимостей продуктов (fa-reports → fa-contracts):
  сохраняем global latest-wins как третий ярус.
- Изменение CLI/MCP-контрактов review — наблюдаемо меняется только набор findings.
- Продуктовое скопирование остальных lookup'ов (`FindSQLTableIDsByFileAndLine`
  и т.п.) — только те, что кормят `colTypeCache`.

## Decisions

### D1. Приоритет в SQL ORDER BY, а не каскад запросов в Go

Единый запрос с трёхуровневой сортировкой:

```sql
SELECT scd.data_type
FROM sql_column_definitions scd
JOIN files f ON f.id = scd.file_id
WHERE LOWER(scd.table_name) = LOWER($1)
  AND LOWER(scd.column_name) = LOWER($2)
  AND TRIM(COALESCE(scd.data_type, '')) <> ''
  AND scd.data_type <> 'DSUNKNOWN'
ORDER BY (scd.file_id = $3) DESC,
         (f.ds_product_id = $4) DESC,
         scd.id DESC
LIMIT 1
```

Альтернатива — три последовательных запроса (file → product → global) с early
return в Go. Отвергнуто: 3x round-trip на cache miss и расхождение с батч-путём.
SQL-вариант даёт fallback «бесплатно» и одинаково ложится на одиночный и батч
запросы.

`JOIN files` нужен только для яруса продукта; для строк текущего файла
`ds_product_id` совпадает по определению, конфликта ярусов нет.

### D2. Контекст передаётся параметрами `fileID`, `productID` (0 = без скопа)

`FindLatestSQLColumnDefinitionType(ctx, table, col, fileID, productID)` и
`BatchFindColumnDefinitionTypes(ctx, tables, fileID, productID)`. Нулевые
значения обоих параметров сохраняют текущее поведение (все `(x = 0)` в ORDER BY
дают `false` → остаётся `id DESC`) — это важно для существующих unit-тестов
без БД и для точек вызова вне review, если такие появятся.

Runner хранит контекст запуска (`reviewExecContext` расширяется полями
`fileID`, `productID`), `cachedFindColumnDefinitionType` берёт их оттуда —
сигнатуры правил (`checkDatatype*` и др.) не меняются, правки вызовов не нужны.

### D3. Батч-path: приоритет в ORDER BY батч-запроса

`BatchFindColumnDefinitionTypes` получает те же параметры; сортировка
`(scd.file_id = $ctx_file) DESC, (f.ds_product_id = $ctx_product) DESC, id DESC`
+ существующая дедупликация «первый выигрывает» в Go-цикле (db_lookup_sql.go:251)
остаётся без изменений. Ключ кэша `table|col` не меняется: контекст константен
в пределах запуска, кэш сбрасывается на каждый `RunSQLFileCtx`.

### D4. API-fallback: продуктовая приоритизация через join к files

`FindAPIColumnDefinitionType(ctx, table, col, productID)`:

- ветка контрактов: `api_contract_table_fields → api_contract_tables →
  api_contracts.file_id → files.ds_product_id`;
- ветка BObjects: `api_business_object_tables.file_id → files.ds_product_id`
  (file_id прямо в таблице, db_schema.go:366);
- `ORDER BY (ds_product_id = $productID) DESC, id DESC`.

Same-file ярус не применяется: API-определения не могут принадлежать
анализируемому SQL-файлу.

### D5. Тесты резолвера — integration через testutil (уточнение при реализации)

Проект не использует sqlmock: store-тесты выполняются как integration-тесты
(`//go:build integration`, `internal/store/testutil.Open(t)`) — изолированная
temp-БД с InitSchema на локальном Postgres, автоочистка через t.Cleanup. Тесты
резолвера (`db_lookup_sql_integration_test.go`) сеют точную модель бага — два
продукта, две DDL p-таблицы с разными типами, контролируемый порядок id — и
проверяют все ярусы приоритета плюс регрессию нулевого контекста. Это покрывает
реальный SQL (где и живёт фикс), чего sqlmock не даёт.

## Risks / Trade-offs

- [Новые findings, ранее замаскированные чужим типом] → ожидаемое следствие
  bug-fix'а; зафиксировать в changelog, при приёмке прогнать review на эталонном
  наборе FA-файлов и диффнуть результаты.
- [`(scd.file_id = $3)` без индекса по file_id уже покрыт существующим индексом
  `sql_column_definitions(file_id)`; join с files по PK] → деградации плана не
  ожидается; проверить EXPLAIN на батч-запросе (таблицы ~10⁶ строк).
- [Правило `datatype` молчит, когда тип неразрешим] → без изменений: текущий
  behavior (пустой тип → пропуск) сохранён.
- [Двусмысленность «same product» для файлов вне продуктов (`ds_product_id IS
  NULL`)] → `(f.ds_product_id = $4)` даёт `false` для NULL — такие строки
  участвуют только в глобальном ярусе, как и сейчас.

## Migration Plan

Изменение кода без миграции схемы/данных. Роллбэк — git revert; индекс
не требует переиндексации (используются существующие колонки).

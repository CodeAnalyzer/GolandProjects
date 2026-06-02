# Bug: foreignTablesUsing — ложное срабатывание для таблиц, созданных через SELECT INTO

## Правило
`foreignTablesUsing` (Severity 3)

## Файл с багом
`internal/review/review_lookup.go`, функция `lookupTableProductID`

## Описание проблемы

При проверке правила `foreignTablesUsing` CodeBase определяет продукт таблицы через функцию `lookupTableProductID`.
Эта функция ищет запись о создании таблицы в `sql_tables` с условием `context = 'create'`:

```go
// review_lookup.go:52-61
err := r.db.QueryRow(`
    SELECT f.ds_product_id
    FROM sql_tables t
    JOIN files f ON f.id = t.file_id
    WHERE LOWER(t.table_name) = LOWER($1)
      AND t.context = 'create'
      AND f.ds_product_id IS NOT NULL
    ORDER BY t.id DESC
    LIMIT 1
`, strings.TrimSpace(tableName)).Scan(&productID)
```

Если таблица не найдена по `context = 'create'`, делается второй запрос с `context = 'dfm_embedded'`.

**Проблема:** Sync-таблицы (например, `tConsInstitutionSync`) создаются в патч-файлах через конструкцию `SELECT ... INTO`, которая индексируется с `context = 'select_into'`, а не `context = 'create'`. В результате оба запроса возвращают `ErrNoRows`, функция возвращает `0`, и правило некорректно определяет принадлежность таблицы.

## Воспроизведение

Файл: `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetNumberByMask.sql`

Таблица `tConsInstitutionSync` используется в строке 171:

```sql
select @Portal = substring(rtrim(ltrim(i.Portal)), 1, 6)
  from tContract                 c M_NOLOCK_INDEX(XPKtContract)
 inner join tConsInstitutionSync i M_NOLOCK_INDEX(XPKtConsInstitutionSync)
         on c.InstitutionID = i.InstitutionID
 where c.ContractID = @ContractID
M_ISOLAT
M_FORCEORDER
```

Таблица `tConsInstitutionSync` создаётся в `fa-contracts/Consumer/SERVER/Patch/patch7_2_1056.sql`
через `SELECT ... INTO`.

Оба файла принадлежат одному продукту `fa-contracts`, однако CodeBase выдаёт:

```json
{
  "rule": "foreignTablesUsing",
  "severity": 3,
  "message": "Использование таблицы чужого продукта",
  "object": "tConsInstitutionSync",
  "current_product_id": 31059,
  "target_product_id": 52561
}
```

## Причина

`lookupTableProductID` ищет продукт таблицы только в `sql_tables` с условием `context = 'create'`
(и в качестве фоллбэка — `context = 'dfm_embedded'`).

Sync-таблицы типа `tConsInstitutionSync` создаются через `SELECT ... INTO` в патч-файлах —
такие таблицы индексируются с `context = 'select_into'` в `sql_tables` и с `definition_kind = 'select_into'`
в `sql_column_definitions`. Ни первый, ни второй запрос `lookupTableProductID` их не находит.

Это подтверждается данными `codebase_query_table_schema`:
```json
{
  "table_name": "tConsInstitutionSync",
  "column_name": "InstitutionID",
  "definition_kind": "select_into",
  "file": "fa-contracts/Consumer/SERVER/Patch/patch7_2_1056.sql"
}
```

`codebase_query_table_schema` использует таблицу `sql_column_definitions` (без фильтра по `definition_kind`)
и поэтому находит таблицу корректно. `lookupTableProductID` же использует только `sql_tables`,
из-за чего теряет таблицы с `context = 'select_into'`.

Итоговая цепочка в `lookupTableProductID`:
1. `sql_tables WHERE context = 'create'` → не найдено
2. `sql_tables WHERE context = 'dfm_embedded'` → не найдено
3. Возвращается `0` → но `target_product_id: 52561 ≠ 0` в выдаче, значит где-то
   (возможно в UPLOAD-копии патча) есть запись с `context = 'create'` и некорректным `ds_product_id`

## Решение

### Вариант 1: добавить `select_into` в первый запрос `sql_tables`

```go
err := r.db.QueryRow(`
    SELECT f.ds_product_id
    FROM sql_tables t
    JOIN files f ON f.id = t.file_id
    WHERE LOWER(t.table_name) = LOWER($1)
      AND t.context IN ('create', 'select_into')
      AND f.ds_product_id IS NOT NULL
    ORDER BY t.id DESC
    LIMIT 1
`, strings.TrimSpace(tableName)).Scan(&productID)
```

### Вариант 2 (надёжный): фоллбэк через `sql_column_definitions`

Если таблица не нашлась через `sql_tables` ни по одному из `context`,
искать продукт через `sql_column_definitions` — там хранятся колонки
из всех источников включая `select_into`, без фильтра по `definition_kind`:

```go
// фоллбэк: ищем через sql_column_definitions
err = r.db.QueryRow(`
    SELECT f.ds_product_id
    FROM sql_column_definitions scd
    JOIN files f ON f.id = scd.file_id
    WHERE LOWER(scd.table_name) = LOWER($1)
      AND f.ds_product_id IS NOT NULL
    ORDER BY scd.id DESC
    LIMIT 1
`, strings.TrimSpace(tableName)).Scan(&productID)
```

### Вариант 3 (дополнительно): исключить UPLOAD-файлы из всех запросов

Добавить фильтрацию по пути файла во все запросы, чтобы UPLOAD-копии
патч-файлов не давали некорректный `ds_product_id`:

```go
AND LOWER(f.path) NOT LIKE '%/upload/%'
AND LOWER(f.path) NOT LIKE '%\upload\%'
```

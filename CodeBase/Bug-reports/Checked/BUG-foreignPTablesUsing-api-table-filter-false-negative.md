# Bug: foreignPTablesUsing — ложное несрабатывание для внутренних p-таблиц, ошибочно зарегистрированных как API-таблицы

**Правило:** `foreignPTablesUsing`
**Severity:** 3
**Статус:** Ложное несрабатывание (false negative)
**Дата обнаружения:** 2026-06-24
**Файл:** `fa-contracts/Consumer/SERVER/MFBO/MB_BPCondition_insert_pp.sql`

## Описание проблемы

Правило `foreignPTablesUsing` не обнаруживает использование p-таблицы `pPortObject` чужого продукта в файле `MB_BPCondition_insert_pp.sql` (продукт `fa-contracts`). Таблица `pPortObject` создаётся в продуктах `fa-fmcore`, `fa-moneymarket`, `fa-stockmarket`, но не в `fa-contracts`. Ожидается замечание "Использование p-таблицы чужого продукта", но оно не выдаётся.

## Воспроизведение

```powershell
codebase review C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\MFBO\MB_BPCondition_insert_pp.sql
```

Результат:
```
Findings: 2
- [3] foreignTablesUsing  line=334 object=tCurrencyTrader  Использование таблицы чужого продукта
- [1] insertRowLock line=440 object=',
  Для INSERT необходимо использовать M_WITH_ROWLOCK для предотвращения эскалации блокировок
```

Ожидаемое, но отсутствующее замечание:
```
- [3] foreignPTablesUsing line=33 object=pPortObject Использование p-таблицы чужого продукта
```

Использование `pPortObject` в проверяемом файле:

```sql
-- строка 33-34: delete
delete pPortObject
  from pPortObject M_ROWLOCK_INDEX(XPKpPortObject)

-- строка 346-349: select from
select @NewBankProductID = New_ID
  from pPortObject M_NOLOCK_INDEX(XPKpPortObject)
 where SPID = @@spid
   and Type = 'BankProduct'

-- строка 459: insert into
insert into pPortObject M_WITH_ROWLOCK
```

## Причина

### Корневая причина: парсер DSArchitect XML не различает API-модули и внутренние модули

Парсер DSArchitect XML (`internal/parser/dsxml/parser.go`, функция `classifyPath`, строки 342-370) классифицирует **любой** XML-файл по пути `*/BObject/*/Table/*` как `api_table`, независимо от того, находится ли файл в API-модуле (например `fa-administrator\API_AccrualCore\`) или во внутреннем каталоге продукта (например `fa-administrator\Administrator\`).

```go
// parser.go:357
case "Table":
    return "api_table", businessObject
```

Функция не проверяет, содержит ли путь сегмент `API_` (признак API-модуля). В результате:

- **API-таблица** `pAccrual_Detail` из `fa-administrator\API_AccrualCore\DSArchitectData\BObject\API_AccrualCore_PTbl\Table\pAccrual_Detail.xml` → корректно классифицируется как `api_table`
- **Внутренняя p-таблица** `pPortObject` из `fa-administrator\Administrator\DSArchitectData\BObject\Administrator_PTbl\Table\pPortObject.xml` → **ошибочно** классифицируется как `api_table`

Оба файла попадают в таблицу `api_business_object_tables` без признака, отличающего API-контракты от внутренних деклараций.

### Как это влияет на проверку `foreignPTablesUsing`

Функция `checkForeignPTables` (`internal/review/review_rules.go:1405-1447`) перед проверкой чужого продукта фильтрует p-таблицы через `findAPITableNames`:

```go
// review_rules.go:1410-1418
apiNames, err := r.findAPITableNames(tableNames(tables))
// ...
for _, table := range tables {
    if _, exists := apiNames[strings.ToLower(table.Name)]; exists {
        continue  // pPortObject исключается здесь
    }
    filtered = append(filtered, table)
}
```

Функция `findAPITableNames` (`internal/review/review_lookup.go:347-388`) выполняет:

```sql
SELECT LOWER(table_name) FROM api_business_object_tables WHERE LOWER(table_name) = ANY($1)
```

Запрос возвращает `pPortObject`, так как таблица ошибочно индексирована в `api_business_object_tables`. В результате `pPortObject` исключается из проверки `foreignPTablesUsing` и замечание не генерируется.

### Структура путей DSArchitect XML

| Тип | Путь | Назначение |
|------|------|-----------|
| **API-модуль** | `fa-administrator\API_AccrualCore\DSArchitectData\BObject\...\Table\*.xml` | Контракты API для межпродуктового взаимодействия |
| **Внутренний модуль** | `fa-administrator\Administrator\DSArchitectData\BObject\...\Table\*.xml` | Внутренние декларации продукта, не предназначены для внешнего использования |

Ключевой признак API-модуля — сегмент пути `API_*` (например `API_AccrualCore`, `API_Credit`, `API_Payments`). Внутренние модули не содержат префикса `API_` (например `Administrator`, `Consumer`, `GAAP`).

## Подтверждение через CodeBase MCP

```json
{
  "count": 1,
  "items": [{
    "id": 13930,
    "business_object": "Administrator_PTbl",
    "table_name": "pPortObject",
    "file": "fa-administrator/Administrator/DSArchitectData/BObject/Administrator_PTbl/Table/pPortObject.xml",
    "source": "business_object"
  }]
}
```

`source: "business_object"` и путь `fa-administrator/Administrator/...` (не `fa-administrator/API_...`) подтверждают, что это внутренняя декларация, а не API-контракт.

## Ожидаемое поведение

1. Парсер DSArchitect XML должен различать API-модули и внутренние модули по наличию сегмента `API_` в пути файла
2. Таблицы из `*/Administrator/DSArchitectData/BObject/*/Table/*.xml` (и аналогичных внутренних путей без `API_`) не должны попадать в `api_business_object_tables` — они должны индексироваться как внутренние p-таблицы продукта
3. Функция `findAPITableNames` не должна возвращать внутренние p-таблицы, что позволит `checkForeignPTables` корректно проверять их на принадлежность чужому продукту

## Предложение по исправлению

### Вариант 1: Фильтрация в парсере DSArchitect XML (предпочтительный)

В функции `classifyPath` (`internal/parser/dsxml/parser.go:342-370`) добавить проверку, что путь содержит сегмент `API_` перед классификацией как `api_table`:

```go
case "Table":
    // Проверяем, что путь содержит API-модуль (сегмент начинается с "API_")
    if isAPIModulePath(parts[:i]) {
        return "api_table", businessObject
    }
    return "internal_table", businessObject
```

Где `isAPIModulePath` проверяет наличие сегмента пути, начинающегося с `API_`:

```go
func isAPIModulePath(parts []string) bool {
    for _, part := range parts {
        if strings.HasPrefix(part, "API_") {
            return true
        }
    }
    return false
}
```

В случае `internal_table` таблица не должна добавляться в `BusinessObjectTables` (или должна добавляться в отдельную категорию `InternalTables`), чтобы она не попадала в `api_business_object_tables`.

### Вариант 2: Фильтрация в `findAPITableNames` (минимальное изменение)

В функции `findAPITableNames` (`internal/review/review_lookup.go:347-388`) добавить JOIN с `files` и фильтрацию по пути:

```sql
SELECT LOWER(t.table_name)
FROM api_business_object_tables t
JOIN files f ON f.id = t.file_id
WHERE LOWER(t.table_name) = ANY($1)
  AND LOWER(f.rel_path) LIKE '%/api_%/dsarchitectdata/%'
```

Это исключит таблицы из внутренних модулей (пути вида `.../Administrator/DSArchitectData/...`).

**Недостаток варианта 2:** таблицы остаются в `api_business_object_tables` и могут влиять на другие правила и запросы (например `query api-table`).

### Вариант 3: Добавить колонку `is_api_module` в `api_business_object_tables` (компромиссный)

В схему БД добавить флаг `is_api_module BOOLEAN DEFAULT FALSE`, заполняемый при индексации на основе пути файла. В `findAPITableNames` добавить условие `AND is_api_module = TRUE`.

## Затронутые файлы

- `internal/parser/dsxml/parser.go` — функция `classifyPath` (вариант 1)
- `internal/review/review_lookup.go` — функция `findAPITableNames` (вариант 2)
- `internal/review/review_rules.go` — функция `checkForeignPTables` (потребитель результата `findAPITableNames`)
- `store/db_schema.go` — схема `api_business_object_tables` (вариант 3)
- `store/api_store.go` — вставка в `api_business_object_tables` (варианты 1, 3)

## Влияние

Без исправления правило `foreignPTablesUsing` не способно обнаружить использование чужих внутренних p-таблиц, декларированных в DSArchitect XML внутри не-API модулей. Это касается не только `pPortObject`, но и любых других p-таблиц, объявленных в `*/Administrator/DSArchitectData/BObject/*/Table/*.xml` и аналогичных путях во всех продуктах.

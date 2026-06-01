# Bug Report: Ложное срабатывание правила tableFullScan

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Medium
**Компонент:** SQL Review - Rule `tableFullScan`

## Описание проблемы

Правило `tableFullScan` ошибочно воспринимает строковые константы (даты) внутри функций `isnull()` как имена таблиц без условия фильтрации.

## Запросы, вызывающие ошибку

### Запрос 1 (строка 134 файла BaseAlg_PS_Subsidy.sql)
```sql
delete pConsPaySchAmountExt2 
  from pConsPaySchAmountExt2        se M_ROWLOCK_INDEX(XPKpConsPaySchAmountExt2)
 inner join pAPI_Accrual_Object    o  M_NOLOCK_INDEX(XPKpAPI_Accrual_Object)
         on o.SPID                 = se.SPID
        and o.ObjectID             = se.ContractID
 inner join tCtrCtrRelation       ctr M_NOLOCK_INDEX(XE1tCtrCtrRelation)
         on ctr.ParentContractID    = o.ObjectID
        and ctr.TypeLink            = CTRCTR_TYPE_CONTRACTEXTRAVERIFY
        and ctr.IsActive            = IS_ACTIVE
 inner join tConsPrepaymentDetail cpd M_NOLOCK_INDEX(XPKtConsPrepaymentDetail)
         on cpd.ContractID          = ctr.ContractID
        and cpd.Status              = 1
  left join pAPI_Accrual_Date      od M_NOLOCK_INDEX(XPKpAPI_Accrual_Date)
         on od.SPID                 = @@spid
        and od.TemplateNumber       = @TemplateNumber
        and od.ObjectID             = ctr.ParentContractID
 inner join tPaySchedule           ps M_NOLOCK_INDEX(XIE2tPaySchedule)
         on ps.ContractID           = cpd.ContractID
        and ps.ActionType           = @ActionType
        and ps.Version              = 0
        and ps.Flag & PAYSCHEDULE_PREPAYMENT > 0
        and ps.DatePayModified2    <= isnull(od.EndDate,'20501231')
 where se.SPID            = @@spid
M_FORCEORDER
```

**Ошибочное замечание:**
```
Rule: tableFullScan
Message: Таблица '20501231' не имеет условия фильтрации (полное сканирование)
```

### Запрос 2 (строка 179 файла BaseAlg_PS_Subsidy.sql)
```sql
select @@spid,
       o.ObjectID,
       min(cpd.PayDateFrom)
  from pAPI_Accrual_Object          o M_NOLOCK_INDEX(XPKpAPI_Accrual_Object)
  left join pAPI_Accrual_Date      od M_NOLOCK_INDEX(XPKpAPI_Accrual_Date)
         on od.SPID                 = @@spid
        and od.TemplateNumber       = @TemplateNumber
        and od.ObjectID             = o.ObjectID
 inner join tCtrCtrRelation       ctr M_NOLOCK_INDEX(XE1tCtrCtrRelation)
         on ctr.ParentContractID    = o.ObjectID
        and ctr.TypeLink            = CTRCTR_TYPE_CONTRACTEXTRAVERIFY
        and ctr.IsActive            = IS_ACTIVE
        and ctr.DateFrom           <= isNull(od.EndDate, '20501231')
 inner join tConsPrepaymentDetail cpd M_NOLOCK_INDEX(XPKtConsPrepaymentDetail)
         on ctr.ContractID          = cpd.ContractID
        and cpd.Status              = 1
 where o.SPID                       = @@spid
 group by o.ObjectID
M_FORCEORDER
```

**Ошибочное замечание:**
```
Rule: tableFullScan
Message: Таблица '20501231' не имеет условия фильтрации (полное сканирование)
```

## Причина ошибки

**Файл:** `Source/internal/review/review_rules.go`

**Проблемный код:** функции `parseTablesInFromClause` (строка 3268) и `parseTableWithAlias` (строка 3306)

### Анализ:

1. **Шаг 1 - Нормализация JOIN'ов** (строки 3281-3286):
   ```go
   normalized := strings.ReplaceAll(lower, " inner join ", ",")
   normalized = strings.ReplaceAll(normalized, " left join ", ",")
   // ... и т.д.
   ```
   Этот код заменяет все JOIN на запятые, чтобы разбить FROM clause на части.

2. **Шаг 2 - Разбивка по запятым** (строка 3289):
   ```go
   parts := strings.Split(normalized, ",")
   ```
   
   **Проблема:** Когда в SQL есть функция `isnull(od.EndDate, '20501231')`, она содержит запятую между аргументами. После замены JOIN на запятые, строка становится:
   
   ```
   ... from ... ctr on ... and ctr.datefrom <= isnulll(od.enddate, '20501231') ...
   ```
   
   При разбивке по запятым, часть `'20501231')` попадает в массив как отдельный элемент!

3. **Шаг 3 - Парсинг таблицы** (функция `parseTableWithAlias`, строка 3342):
   ```go
   tokens := strings.Fields(part)
   result.TableName = tokens[0]  // <-- Берёт первый токен как имя таблицы
   ```
   
   Для части `'20501231')` после удаления скобок и подсказок остается `'20501231'`, что воспринимается как имя таблицы.

### Корень проблемы:

Парсер не учитывает **вложенные скобки функций** при разбивке FROM clause. Запятая внутри `isnull(arg1, arg2)` не должна использоваться как разделитель таблиц.

## Рекомендации по исправлению

### Вариант 1: Учитывать скобки при нормализации
Перед заменой JOIN на запятые нужно временно заменить запятые внутри скобок на placeholder, а после разбивки восстановить:

```go
// Заменяем запятые внутри скобок на placeholder
func protectCommasInParens(sql string) string {
    // Реализовать защиту запятых внутри (...)
}
```

### Вариант 2: Улучшенный разбор FROM clause
Использовать более умный парсер, который отслеживает глубину скобок:

```go
// Отслеживать depth скобок при разбивке
for i, ch := range sql {
    if ch == '(' { depth++ }
    if ch == ')' { depth-- }
    if ch == ',' && depth == 0 { 
        // Только здесь разбивать
    }
}
```

### Вариант 3: Фильтрация результатов
В `parseTableWithAlias` добавить проверку на валидность имени таблицы:

```go
func parseTableWithAlias(part string) tableFromClause {
    // ...
    if !isValidTableName(tokens[0]) {
        return tableFromClause{}  // Пропустить невалидные
    }
    result.TableName = tokens[0]
    // ...
}

func isValidTableName(name string) bool {
    // Имя таблицы не должно начинаться с кавычки
    // и не должно быть числом
    if strings.HasPrefix(name, "'") || strings.HasPrefix(name, "\"") {
        return false
    }
    // Проверка на числовой формат даты
    if matched, _ := regexp.MatchString(`^\d{8}$`, name); matched {
        return false
    }
    return true
}
```

## Заключение

Проблема критична, так как ложные срабатывания снижают доверие к инструменту и заставляют разработчиков игнорировать замечания. Использование `isnull()` с константами-датами - распространённый паттерн в SQL-коде Diasoft 5NT.

**Рекомендуемый приоритет:** Medium-High  
**Сложность исправления:** Low (вариант 3 - фильтрация)  
**Сложность исправления:** Medium (варианты 1 и 2 - корректный парсинг)

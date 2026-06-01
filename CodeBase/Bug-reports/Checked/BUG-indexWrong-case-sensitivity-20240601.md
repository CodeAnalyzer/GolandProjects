# Bug Report: Ложное срабатывание indexWrong на различие в регистре имени индекса

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Low
**Компонент:** SQL Review - правило indexWrong

## Описание проблемы

Правило `indexWrong` ложно срабатывает, когда имя индекса в SQL-коде отличается от имени в базе данных только регистром символов. В MSSQL/Sybase имена объектов регистро-независимы (в зависимости от collation), поэтому `XIE0tConsInterest` и `xie0tconsinterest` — это один и тот же индекс.

## Файл с ошибкой

**Файл:** `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\CCred_FixPenaltyAmount.sql`

## Ложное срабатывание

| Строка | Правило | Таблица | Текущий индекс | Рекомендуемый |
|--------|---------|---------|---------------|---------------|
| 139 | `indexWrong` | `tconsinterest` | `xie0tconsinterest` | `XIE0tConsInterest` |

## Сообщение инструмента

```
Для таблицы tconsinterest указан индекс xie0tconsinterest, 
но по условиям лучше подходит XIE0tConsInterest
```

## Проблемный код

```sql
select @CountRates = count(1)
  from tConsInterest M_NOLOCK_INDEX(XIE0tConsInterest)
 where ObjType     = MARKTYPE_CONSUMER
   and ObjectID    = @ObjectID
M_ISOLAT
```

## Причина ошибки

Правило сравнивает имя индекса из SQL-кода с именем в БД без учёта регистро-независимости:

```go
// Псевдокод логики indexWrong
if indexFromSQL != indexFromDB {  // Сравнение с учётом регистра!
    // Генерируем замечание
}
```

**Проблема:** В SQL Server и Sybase имена объектов по умолчанию регистро-независимы (если collation не CS — case sensitive). Сравнение с учётом регистра приводит к ложным замечаниям.

## Почему это ложное срабатывание

| Индекс в SQL | Индекс в БД | Результат |
|--------------|-------------|-----------|
| `xie0tconsinterest` | `XIE0tConsInterest` | ✅ Один и тот же индекс |
| `XPKtContract` | `xpkttcontract` | ✅ Один и тот же индекс |
| `XIE1pAPI_Accrual_Date` | `xie1papi_accrual_date` | ✅ Один и тот же индекс |

Все примеры выше — это один индекс, просто написанный в разном регистре.

## Рекомендации по исправлению

### Вариант 1: Регистро-независимое сравнение (рекомендуется)

При сравнении имён индексов использовать `strings.EqualFold()` или приводить оба имени к одному регистру:

```go
// Было:
if indexFromSQL != indexFromDB {
    // Замечание
}

// Стало:
if !strings.EqualFold(indexFromSQL, indexFromDB) {
    // Замечание только если имена реально разные
}

// Или:
if strings.ToLower(indexFromSQL) != strings.ToLower(indexFromDB) {
    // Замечание
}
```

### Вариант 2: Учитывать collation БД

Если инструмент имеет доступ к настройкам БД, можно проверять `COLLATION` конкретной базы:
- `CI` (Case Insensitive) — регистро-независимое сравнение
- `CS` (Case Sensitive) — регистро-зависимое сравнение

```go
func shouldCompareCaseSensitive(dbCollation string) bool {
    return strings.Contains(dbCollation, "_CS_") // Case Sensitive
}
```

### Вариант 3: Сохранять исходный регистр в базе

При индексации таблиц сохранять имя индекса в том регистре, в котором оно создано в БД, и сравнивать без учёта регистра.

## Примеры других ложных срабатываний

```sql
-- Все эти варианты — один и тот же индекс:
M_NOLOCK_INDEX(XPKtContract)
M_NOLOCK_INDEX(xpkttcontract)
M_NOLOCK_INDEX(XpkTtContract)
m_nolock_index(xpkttcontract)

-- Аналогично для любого индекса:
XIE1pAPI_Accrual_Date = xie1papi_accrual_date = Xie1pApi_Accrual_Date
```

## Заключение

Сравнение имён индексов с учётом регистра является технически некорректным для SQL Server/Sybase, где имена по умолчанию регистро-независимы. Это приводит к шуму в отчётах и снижает доверие к инструменту.

**Рекомендуемый приоритет:** Low  
**Сложность исправления:** Very Low (однострочное изменение)  
**Затронутое правило:** `indexWrong`

**Примечание:** Исправление варианта 1 (использование `strings.EqualFold()`) решает проблему для 99% случаев без необходимости проверки collation.

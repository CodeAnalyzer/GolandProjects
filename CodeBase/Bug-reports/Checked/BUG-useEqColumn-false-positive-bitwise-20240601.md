# Bug Report: Ложное срабатывание правила useEqColumn на битовых операциях

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Medium
**Компонент:** SQL Review - Rule `useEqColumn`

## Описание проблемы

Правило `useEqColumn` ошибочно воспринимает проверку битовых флагов вида `column & mask = mask` как сравнение столбца с самим собой.

## Запрос, вызывающий ошибку

**Файл:** `fa-contracts\Consumer\SERVER\Accrual\BaseAlgAdditionCharge.sql` (строка 45)

```sql
insert into pCtrLawsuitRelation M_WITH_ROWLOCK
       (
       SPID,
       CtrLawsuitRelationID,
       ContractID,
       JuralDocType,
       Flag
       )
select @@spid,
       max(r.CtrLawsuitRelationID),
       r.ContractID,
       r.JuralDocType,
       r.Flag
  from pAPI_Accrual_Object      o M_NOLOCK_INDEX(XPKpAPI_Accrual_Object)
 inner join tCtrLawsuitRelation r M_NOLOCK_INDEX(XIEtCtrLawsuitRelation)
         on r.ContractID        = o.ObjectID
        and r.JuralDocType      = CONS_JDT_JUDGMENT
        and r.Flag & CONSUMER_CTRLAWSUIT_AUTO = CONSUMER_CTRLAWSUIT_AUTO
 where o.SPID                   = @@spid
   and o.NtfID                  = 0
 group by r.ContractID, r.JuralDocType, r.Flag
M_FORCEORDER
```

**Ошибочное замечание:**
```json
{
  "rule": "useEqColumn",
  "severity": 1,
  "message": "Нельзя сравнивать столбец с самим собой",
  "line": 45,
  "object": "consumer_ctrlawsuit_auto"
}
```

## Почему это ложное срабатывание

**Условие в SQL:**
```sql
r.Flag & CONSUMER_CTRLAWSUIT_AUTO = CONSUMER_CTRLAWSUIT_AUTO
```

**Что это на самом деле:**
- `r.Flag & CONSUMER_CTRLAWSUIT_AUTO` — побитовое И между полем `Flag` и константой-маской
- `= CONSUMER_CTRLAWSUIT_AUTO` — проверка, что результат равен константе (т.е. бит установлен)

**Это корректный SQL-идиом для T-SQL/Sybase для проверки битовых флагов:**

| Паттерн | Значение |
|---------|----------|
| `column & mask = mask` | "Бит установлен" (флаг включен) |
| `column & mask > 0` | "Хотя бы один бит из маски установлен" |
| `column & mask = 0` | "Бит не установлен" (флаг выключен) |

**Почему правило ошибается:**

Парсер видит паттерн `A = B` где `B` встречается в левой части выражения, и ошибочно считает это `column = column`. Он не учитывает, что:
1. Левая часть — это результат битовой операции (`&`), не сам столбец
2. `CONSUMER_CTRLAWSUIT_AUTO` — это константа (макрос), не столбец таблицы

## Причина в коде

**Файл:** `Source/internal/review/review_rules.go`

Функция `checkUseEqColumn` (поиск по имени правила `RuleUseEqColumn`) содержит логику проверки паттерна `column = column`, но не исключает битовые операции.

## Примеры других корректных паттернов, которые могут вызывать ложные срабатывания

```sql
-- Проверка бита (стандартный паттерн)
WHERE status & STATUS_ACTIVE = STATUS_ACTIVE

-- Проверка отсутствия бита
WHERE flags & FLAG_DELETED = 0

-- Проверка нескольких битов
WHERE mode & (MODE_READ | MODE_WRITE) > 0
```

## Рекомендации по исправлению

### Вариант 1: Исключить битовые операции из проверки

В функции проверки добавить условие: если выражение содержит битовые операторы (`&`, `|`, `^`), пропустить проверку:

```go
func checkUseEqColumn(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
    // ...
    for _, condition := range conditions {
        // Пропускаем битовые операции
        if containsBitwiseOperator(condition.Left) || containsBitwiseOperator(condition.Right) {
            continue
        }
        // ... остальная логика
    }
}

func containsBitwiseOperator(expr string) bool {
    // Проверяем наличие & | ^ вне строковых литералов
    // с учетом того что & может быть в BETWEEN или AND
    return regexp.MustCompile(`(?i)[^&]&[^&]`).MatchString(expr) ||
           strings.Contains(expr, "|") ||
           strings.Contains(expr, "^")
}
```

### Вариант 2: Улучшенный парсинг

Правильно разбирать выражение на AST и понимать, что:
- `(r.Flag & CONSUMER_CTRLAWSUIT_AUTO)` — это бинарное выражение
- Сравнение его с константой — не `column = column`

### Вариант 3: Белый список паттернов

Добавить распространённые паттерны битовых проверок как исключения:

```go
var bitwisePatterns = []*regexp.Regexp{
    regexp.MustCompile(`(?i)\w+\s*&\s*\w+\s*=\s*\w+`),  // column & mask = mask
    regexp.MustCompile(`(?i)\w+\s*&\s*\w+\s*>\s*0`),   // column & mask > 0
    regexp.MustCompile(`(?i)\w+\s*&\s*\w+\s*=\s*0`),   // column & mask = 0
}
```

## Заключение

Проверка битовых флагов — распространённый паттерн в кодовой базе Diasoft 5NT. Ложные срабатывания `useEqColumn` на таких конструкциях снижают доверие к инструменту и создают лишний шум в отчётах.

**Рекомендуемый приоритет:** Medium  
**Сложность исправления:** Low (вариант 1 или 3)  
**Частота:** Высокая (битовые флаги широко используются)

# Bug Report: Ложное срабатывание useEqColumn на присваивании в SELECT

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Medium
**Компонент:** SQL Review - правило useEqColumn

## Описание проблемы

Правило `useEqColumn` ложно срабатывает на операторах `SELECT` с присваиванием значения колонки в переменную. Паттерн `select @var = column` ошибочно интерпретируется как сравнение `column = column`.

## Файл с ошибками

**Файл:** `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\CancelCreditDemandBankrupt.sql`

## Ложные срабатывания (2 штуки)

| Строка | Правило | Объект | Сообщение |
|--------|---------|--------|-----------|
| 38 | `useEqColumn` | `protocolid` | Нельзя сравнивать столбец с самим собой |
| 38 | `useEqColumn` | `version` | Нельзя сравнивать столбец с самим собой |

## Проблемный код

Строка 38 в файле соответствует запросу:

```sql
select @Version   = Version
  from tPayScheduleVersion M_NOLOCK_INDEX(XPKtPayScheduleVersion)
 where ContractID = @ContractID
   and ProtocolID = @ProtocolID
   and Date       = @InDateTime
M_ISOLAT
```

Также другие похожие запросы в файле:

```sql
-- Строка 34
select @ProtocolID  = ProtocolID
  from tConsChngStateSync M_NOLOCK_INDEX(XIE0tConsChngStateSync)
 where ContractID   = @ContractID
   and FinOperID    = @FinOperID
   and StateType    = PROP_STATETYPE_CON_DEMANDED
   and InDateTime   = @InDateTime
M_ISOLAT

-- Строка 52
select @Version   = Version
  from tPayScheduleVersion M_NOLOCK_INDEX(XPKtPayScheduleVersion)
 where ContractID = @ContractID
   and ProtocolID = @ProtocolID
   and Date       = @InDateTime
M_ISOLAT
```

## Причина ошибки

**Файл:** `Source/internal/review/review_rules.go`

Функция `analyzeConditionForEqColumn` использует регулярное выражение:

```go
eqRe := regexp.MustCompile(`(?i)(\w+(?:\.\w+)?)\s*=\s*(\w+(?:\.\w+)?)`)
```

Этот regex находит `Version = @InDateTime` или подобные паттерны, но при обработке строки `select @Version = Version` происходит ошибка.

Проблема в том, что:
1. Парсер разбивает SQL на строки
2. При анализе строки `select @Version = Version` regex `\w+` захватывает `@Version` и `Version`
3. После нормализации (удаление `@` и префикса) получается `version = version`
4. Правило считает это сравнением колонки с самой собой

## Почему это присваивание, а не сравнение

В T-SQL/Sybase конструкция `select @var = column` — это **присваивание**:
- `@Version` — локальная переменная процедуры
- `Version` — колонка таблицы
- Результат: значение колонки `Version` записывается в переменную `@Version`

Это **не** сравнение в условии `WHERE`, а выражение в списке `SELECT`.

## Рекомендации по исправлению

### Вариант 1: Проверять контекст (рекомендуется)

Проверять, находится ли `=` в секции `SELECT` (присваивание) или `WHERE` (условие):

```go
func isInWhereClause(lines []string, lineIdx int, matchPos int) bool {
    // Определяем, в какой секции запроса находится строка
    inSelect := false
    inWhere := false
    
    for i := lineIdx; i >= 0; i-- {
        line := strings.ToLower(lines[i])
        if strings.Contains(line, "where") {
            inWhere = true
            inSelect = false
        } else if strings.Contains(line, "select") && !inWhere {
            inSelect = true
        }
    }
    
    return inWhere
}

// В analyzeConditionForEqColumn:
if !isInWhereClause(lines, startLine, matchPos) {
    continue  // Пропускаем, если не в WHERE
}
```

### Вариант 2: Проверять наличие @ в левой части

Если левая часть начинается с `@` — это переменная, не колонка:

```go
func analyzeConditionForEqColumn(lines []string, startLine int, file *indexedFile) []Finding {
    // ...
    for _, m := range matches {
        left := m[1]
        right := m[2]
        
        // Пропускаем присваивание в SELECT: @var = column
        if strings.HasPrefix(left, "@") {
            continue
        }
        
        // Проверяем сравнение колонок...
        leftNorm := normalizeIdentifier(left)
        rightNorm := normalizeIdentifier(right)
        
        if leftNorm == rightNorm && !strings.HasPrefix(left, "@") {
            findings = append(findings, Finding{...})
        }
    }
    // ...
}
```

### Вариант 3: Улучшить определение контекста

Использовать парсер SQL для точного определения:
- Если мы в `SELECT` clause → это присваивание
- Если мы в `WHERE` clause → это условие

```go
// Пример использования sqlparser для определения позиции
type ClauseType int
const (
    ClauseUnknown ClauseType = iota
    ClauseSelect
    ClauseWhere
    ClauseFrom
    // ...
)

func getClauseType(parsed *sqlparser.ParseResult, pos int) ClauseType {
    // Использовать информацию от парсера о позиции токена
}
```

## Воспроизведение

**Шаги:**
1. Создать SQL-файл с процедурой
2. Добавить запрос: `select @Var = Column from Table where ...`
3. Запустить review

**Ожидаемый результат:** Нет замечаний (валидное присваивание)

**Фактический результат:** Замечание `useEqColumn` — "Нельзя сравнивать столбец с самим собой"

## Примеры других паттернов

```sql
-- Все эти присваивания могут вызывать ложные срабатывания:
select @MaxID = max(ID) from Table
select @Sum = sum(Amount), @Count = count(*) from Table
select @Name = Name, @Date = Date from Table where ID = @ID

-- Агрегатные функции с присваиванием:
select @Total = sum(Amount) from Orders where Date > @Date
```

## Заключение

Правило `useEqColumn` не различает присваивание в `SELECT` и сравнение в `WHERE`. Это приводит к ложным срабатываниям на стандартных T-SQL конструкциях присваивания.

**Рекомендуемый приоритет:** Medium
**Сложность исправления:** Low-Medium
**Затронутое правило:** `useEqColumn`

**Примечание:** Исправление варианта 2 (проверка `@` в левой части) является минимальным и безопасным решением.

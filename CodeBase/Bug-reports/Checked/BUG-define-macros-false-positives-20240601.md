# Bug Report: Ложные срабатывания правил SQL review на макросы #define

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Medium-High
**Компонент:** SQL Review - все правила анализа SQL

## Описание проблемы

Парсер SQL review не распознаёт определения макросов `#define` и анализирует их тело как реальный SQL-код, что приводит к массовым ложным срабатываниям.

## Файл с ошибками

**Файл:** `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\BaseAlgAmountRepayChangeLimit.sql`

## Ложные срабатывания (8 штук)

### 1. Макрос `SELECT_TEMP(TBL)` (строки 16-17)

```sql
#define SELECT_TEMP(TBL)   \
select 'TBL'               \
select * from TBL M_NOLOCK \
M_ISOLAT

#define SELECT_TEMP(TBL)
```

**Ошибочные замечания:**
| Строка | Правило | Объект | Сообщение |
|--------|---------|--------|-----------|
| 16 | `forceOrder2Tbl` | `tbl, 'loan_debts_department_id'` | Запрос с 2 таблицами требует M_FORCEORDER |
| 16 | `indexExistsInDB` | `tbl.papi_confset_sysname` | Для таблицы tbl не найден индекс |
| 16 | `tableFullScan` | `tbl` | Таблица tbl не имеет условия фильтрации |
| 16 | `tableHintIsRight` | `tbl` | Таблица tbl имеет неправильный хинт m_delete_ptable_index |
| 17 | `tableHintExists` | `tbl` | Таблица tbl не имеет допустимого хинта индекса |
| 17 | `tableHintExists` | `'loan_debts_department_id'` | Таблица 'loan_debts_department_id' не имеет допустимого хинта |
| 17 | `useSelectAll` | — | Запрещено использование SELECT * |

**Проблема:** `TBL` — это параметр макроса, не реальная таблица. Тело макроса не должно анализироваться как SQL.

### 2. Макрос `M_FILL_CONFSET_SYSNAME` (строка 33)

```sql
#define M_FILL_CONFSET_SYSNAME                                        \
  M_DELETE_PTABLE_INDEX(pAPI_ConfSet_SysName,XPKpAPI_ConfSet_SysName) \
  insert pAPI_ConfSet_SysName  M_P_WITH_ROWLOCK                       \
         (                                                            \
         SPID,                                                        \
         SysName                                                      \
         )                                                            \
  select @@spid, 'LOAN_DEBTS_DEPARTMENT_ID'                           \
   union                                                              \
  select @@spid, 'OVERDUE_DEBTS_DEPARTMENT_ID'
   ...
```

**Ошибочное замечание:**
| Строка | Правило | Сообщение |
|--------|---------|-----------|
| 33 | `ansiInJoin` | Используйте ANSI-синтаксис для соединения таблиц |

**Проблема:** Строка 33 — это `select @@spid, 'LOAN_DEBTS_DEPARTMENT_ID'` внутри макроса, не реальный запрос с JOIN.

## Причина ошибки

**Файл:** `Source/internal/review/review_rules.go`

Парсер не учитывает `#define` директивы препроцессора. Когда он встречает:

```sql
#define SELECT_TEMP(TBL)   \
select * from TBL M_NOLOCK
```

Он воспринимает `select * from TBL M_NOLOCK` как реальный SQL-запрос и применяет к нему все правила.

Проблемные места в коде:

1. **`checkTableFullScan`** — анализирует `FROM` в теле макроса
2. **`checkAnsiInJoin`** — ищет JOIN в макросах
3. **`checkUseSelectAll`** — находит `SELECT *` в макросах
4. **`checkForceOrder2Tbl`** — считает таблицы в макросах

## Почему это критично

1. **Массовый шум:** Файлы с множеством макросов генерируют десятки ложных замечаний
2. **Сложность отладки:** Разработчик не может отличить реальные проблемы от ложных
3. **Игнорирование:** Постоянные ложные срабатывания приводят к игнорированию всех замечаний
4. **Распространённость:** Макросы `#define` широко используются в Diasoft 5NT

## Рекомендации по исправлению

### Вариант 1: Исключить тело макросов из анализа (рекомендуется)

При чтении файла пропускать строки, являющиеся продолжением `#define`:

```go
func shouldSkipLine(line string, inMacro bool) bool {
    // Проверяем начало макроса
    if strings.HasPrefix(strings.TrimSpace(line), "#define") {
        // Если строка заканчивается \ — макрос продолжается
        return strings.HasSuffix(line, "\\")
    }
    // Продолжение макроса (предыдущая строка заканчивалась на \)
    if inMacro {
        return strings.HasSuffix(line, "\\")
    }
    return false
}
```

### Вариант 2: Предобработка файла

Перед анализом удалить все определения макросов:

```go
func removeMacros(content string) string {
    // Удаляем #define ... (включая строки с \)
    macroRe := regexp.MustCompile(`(?m)^\s*#define\s+\w+.*(?:\\\r?\n.*)*`)
    return macroRe.ReplaceAllString(content, "")
}
```

### Вариант 3: Учитывать контекст строки

В каждом правиле проверять, что строка не является частью макроса:

```go
func isInMacro(lines []string, lineIdx int) bool {
    // Ищем назад до начала макроса или начала файла
    for i := lineIdx; i >= 0; i-- {
        line := lines[i]
        if strings.HasPrefix(strings.TrimSpace(line), "#define") {
            return true
        }
        // Если строка не заканчивается \ — макрос закончился
        if i < lineIdx && !strings.HasSuffix(lines[i], "\\") {
            return false
        }
    }
    return false
}
```

## Примеры других макросов, вызывающих проблемы

```sql
-- Макрос с псевдонимом таблицы
#define JOIN_WITH_ALIAS(TBL, ALIAS) \
  from TBL ALIAS M_NOLOCK

-- Макрос с DELETE
#define CLEAR_TABLE(TBL) \
  delete from TBL where SPID = @@spid

-- Макрос с INSERT
#define COPY_DATA(SRC, DST) \
  insert into DST select * from SRC
```

## Заключение

Проблема системная — парсер не учитывает препроцессор SQL-файлов. Все правила SQL review затронуты, так как они анализируют текст без понимания `#define` директив.

**Рекомендуемый приоритет:** Medium-High  
**Сложность исправления:** Medium  
**Затронутые правила:** `tableFullScan`, `ansiInJoin`, `useSelectAll`, `forceOrder2Tbl`, `tableHintExists`, `tableHintIsRight`, `indexExistsInDB`

**Примечание:** Исправление этого бага также решит проблему ложных срабатываний на `#define M_FILL_CONFSET_SYSNAME` и подобных макросах.

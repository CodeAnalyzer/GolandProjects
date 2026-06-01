# BUG: tableFullScan — ложные срабатывания из-за трёх багов парсера

**Правило:** `tableFullScan`
**Файл для воспроизведения:** `fa-contracts-ext-novikom-persform/API_Credit_Ext/Server/LoanExt/API_LnExt_MassSaveSubjectInfo.sql`
**Количество ложных срабатываний:** 6

---

## Баг 1: `hasStatementEnded` — `strings.Contains` без границ слова

### Описание

Функция `hasStatementEnded` использует `strings.Contains(lower, keyword)` для проверки служебных слов (`end`, `if`, `while`, `begin`, ...).
Так как проверка без границ слова, имена таблиц/столбцов, содержащие эти слова как **подстроку**, ложно завершают накопление оператора — раньше, чем строка с `WHERE` попадёт в буфер.

### Примеры кода (строки 569–577)

```sql
-- Таблица pCNE_LoanExt_DependantInfo содержит подстроку "end" в имени:
--   "dependantinfo" -> "dep-END-antinfo"
-- Строка 570 с FROM завершает накопление оператора SELECT через parenDepth или contains("end")
-- WHERE на строке 571 уже НЕ попадает в буфер → ложное срабатывание

select @MinID = min(AutoID)
  from pCNE_LoanExt_DependantInfo M_NOLOCK_INDEX(XPKpCNE_LoanExt_DependantInfo)  -- <- "end" в имени таблицы
 where SPID   = @@spid                                                             -- <- НЕ попадает в буфер
M_ISOLAT

select @MaxID = max(AutoID)
  from pCNE_LoanExt_DependantInfo M_NOLOCK_INDEX(XPKpCNE_LoanExt_DependantInfo)  -- <- "end" в имени таблицы
 where SPID   = @@spid                                                             -- <- НЕ попадает в буфер
M_ISOLAT
```

**Срабатывания:** строки 569 и 574 (объекты `pcne_loanext_dependantinfo`).

Аналогично ломаются любые таблицы/столбцы, содержащие подстроки `end`, `if`, `while`, `begin`, `return` и т.д.

### Правильное исправление

Заменить `strings.Contains` на проверку с границами слова (regex `\b`) или `HasPrefix`/`HasSuffix` для каждого токена после `strings.Fields`:

```go
// Вместо:
strings.Contains(lower, "end")

// Использовать:
reKeyword := regexp.MustCompile(`(?i)\b(end|begin|if|while|declare|return|exec|execute|go)\b`)
reKeyword.MatchString(lower)
```

---

## Баг 2: `parenDepth < 0` прерывает оператор при многострочном `NOT IN (...)`

### Описание

`checkTableFullScan` накапливает строки оператора и считает скобки через `countParens(line)`.
Когда `NOT IN (...)` разбит на **две строки** — открывающая `(` на одной строке, закрывающая `)` на другой — баланс остаётся ненулевым. Это само по себе не должно быть проблемой (накопление продолжается пока `parenDepth >= 0`).

Однако в реальности происходит следующее: строка с закрывающей `)` в конце `NOT IN` списка даёт `parenDepth < 0` из-за **несимметричного подсчёта вложенных скобок в хинтах** — хинты `M_UPDLOCK_INDEX(...)` и `M_NOLOCK_INDEX(...)` уже посчитаны в строках выше, и при возврате скобка `)` в `NOT IN` может опустить счётчик ниже нуля.

### Примеры кода (строки 209–218)

```sql
-- Строка 215-216: список NOT IN разбит на две строки.
-- На строке 216 закрывающая ')' опускает parenDepth < 0 → оператор завершается досрочно.
-- WHERE на строке 217 не попадает в буфер → ложное срабатывание.

update pCNE_LnExt_PersonInfo
   set NTFID = 760432
  from pCNE_LnExt_PersonInfo i M_UPDLOCK_INDEX(XPKpCNE_LnExt_PersonInfo)
 inner join pAPI_LoanExt_CredHistInfo c M_NOLOCK_INDEX(XPKpAPI_LoanExt_CredHistInfo)
         on c.SPID          = i.SPID
        and c.InstitutionID = i.InstitutionID
        and c.LoanType not in (1, 4, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 20, 21, 22, 24, 25,
                               34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 99)  -- <- ')' опускает parenDepth < 0
 where i.SPID               = @@spid   -- <- НЕ попадает в буфер
   and i.NTFID              = 0
M_FORCEORDER
```

**Срабатывание:** строка 209 (объект `ext_personinfo)`— имя со скобкой подтверждает досрочный выход).

Аналогичный паттерн на строках 258–266, 285–291.

### Правильное исправление

Начальный `parenDepth` должен учитывать только скобки **до стартовой позиции оператора** (уже реализовано для `depthBefore`), но логика остановки `parenDepth < 0` некорректна при прерывании внутри многострочного выражения.

Решение: не использовать `parenDepth < 0` как триггер завершения оператора — он слишком агрессивен. Завершение должно происходить **только** через `hasStatementEnded` (после исправления баг 1):

```go
// Вместо:
if hasStatementEnded(lower) || parenDepth < 0 {

// Использовать только:
if hasStatementEnded(lower) {
```

---

## Баг 3: целевая таблица `UPDATE` без алиаса не идентифицируется как отфильтрованная

### Описание

В конструкции `UPDATE t SET ... FROM t n WHERE n.col = ...` целевая таблица `t` добавляется в список через `extractTableAfterUpdate` **без алиаса** (алиас `""`).
При этом в `WHERE` фильтрация идёт через алиас `n`, а не через имя таблицы `t`.

В `isTableFiltered`:
- Алиас пустой → блок проверки по алиасу пропускается.
- Поиск по имени таблицы `"papi_loanext_msavesubjntf"` в `whereResult.Aliases` ищет `"n"` → не совпадает.
- Результат: `false` → ложное срабатывание.

### Пример кода (строки 865–871)

```sql
-- Целевая таблица pAPI_LoanExt_MSaveSubjNtf добавлена в список через extractTableAfterUpdate
-- без алиаса. В WHERE фильтрация через алиас 'n' из FROM-части.
-- isTableFiltered не может сопоставить имя таблицы с алиасом 'n' → false.

update pAPI_LoanExt_MSaveSubjNtf                                                          -- <- целевая таблица, alias=""
   set NTFMessage                     = m.Message
  from pAPI_LoanExt_MSaveSubjNtf      n M_UPDLOCK_INDEX(XPKpAPI_LoanExt_MSaveSubjNtf)    -- <- та же таблица с alias="n"
 inner join pAPI_Notification_Message m M_NOLOCK_INDEX(XPKpAPI_Notification_Message)
         on m.SPID                    = n.SPID
        and m.NotificationID          = n.NTFID
 where n.SPID                         = @@spid    -- <- фильтрация через алиас n, не через имя
M_FORCEORDER
```

**Срабатывание:** строка 865 (объект `papi_loanext_msavesubjntf`).

### Правильное исправление

Для `UPDATE` не добавлять целевую таблицу отдельно через `extractTableAfterUpdate`, если она уже присутствует в FROM-части (с алиасом). Либо в `extractTablesFromFromClause` для UPDATE искать целевую таблицу в FROM-части и брать её алиас оттуда:

```go
// В analyzeStatementForFullScan для UPDATE:
// Не добавлять отдельную запись для целевой таблицы через extractTableAfterUpdate.
// Вместо этого — использовать только таблицы из FROM-части (они уже содержат алиасы).
// Целевая таблица всегда присутствует в FROM с алиасом в корректно написанном UPDATE.
```

---

## Сводная таблица ложных срабатываний

| Строка | Объект (в Finding)              | Баг  | Реальная фильтрация                        |
|--------|---------------------------------|------|--------------------------------------------|
| 209    | `ext_personinfo)`               | #2   | `where i.SPID = @@spid`                    |
| 258    | `e_lnext_personinfo)`           | #2   | `where i.SPID = @@spid`                    |
| 285    | `pcne_lnext_personinfo`         | #2   | `where i.SPID = @@spid`                    |
| 569    | `pcne_loanext_dependantinfo`    | #1   | `where SPID = @@spid`                      |
| 574    | `pcne_loanext_dependantinfo`    | #1   | `where SPID = @@spid`                      |
| 865    | `papi_loanext_msavesubjntf`     | #3   | `where n.SPID = @@spid` (alias в FROM)     |

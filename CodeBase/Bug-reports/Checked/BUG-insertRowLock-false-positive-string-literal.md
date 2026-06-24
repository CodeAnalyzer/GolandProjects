# Bug: insertRowLock — ложное срабатывание на строковом литерале `'insert'` в SELECT-списке

**Правило:** `insertRowLock`  
**Severity:** 1 (Deploy Stopper)  
**Статус:** Ложное срабатывание (false positive)  
**Дата обнаружения:** 2026-06-24  
**Файл:** `fa-contracts/Consumer/SERVER/MFBO/MB_BPCondition_insert_pp.sql`

## Описание проблемы

Правило `insertRowLock` выдаёт ложное срабатывание на строке 440, содержащей строковый литерал `'insert'` в списке полей `SELECT`. Слово `insert` внутри одинарных кавычек распознаётся анализатором как SQL-оператор `INSERT`, после чего правило пытается найти имя таблицы и хинт `M_WITH_ROWLOCK` в последующих строках, не находит их и генерирует замечание.

## Воспроизведение

```powershell
codebase review C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\MFBO\MB_BPCondition_insert_pp.sql
```

Результат:
```
- [1] insertRowLock line=440 object=',
  Для INSERT необходимо использовать M_WITH_ROWLOCK для предотвращения эскалации блокировок
```

Реальный `INSERT` с `M_WITH_ROWLOCK` находится на строке 427 и не должен вызывать замечания:

```sql
-- строка 427: настоящий INSERT с M_WITH_ROWLOCK
    insert pAPI_Message_MassUpdStatus M_WITH_ROWLOCK
           (SPID,
            Number          ,
            NTFMessage      ,
            DObjCommandBrief,
            IsSync          ,
            DObjTypeBrief   ,
            NTFID           ,
            MessageID       ,
            ObjectID        )
    select @@spid,
           @ID,
           @RetMessage,
           'insert',           -- строка 440: строковый литерал, НЕ SQL-оператор
           @IsSync,
           'BPCondition',
           @RetVal,
           @MBBufferID,
           @newBankProductID

    exec FCD_CON_Message_MassUpdStatus   -- строка 447: начало нового оператора
```

## Причина

### Механизм ложного срабатывания

Функция `checkInsertRowLock` (`internal/review/review_rules.go:2249-2361`) построчно сканирует файл и ищет ключевое слово `insert` через `findKeywordPosition`:

```go
// review_rules.go:2280-2285
if !inInsert {
    insertIdx := findKeywordPosition(lower, "insert")
    if insertIdx >= 0 && !isInComment(line, insertIdx) {
        if !isInsertInSubquery(line) {
            inInsert = true
            insertStartLine = lineNum
            insertBuffer = []string{line}
```

На строке 440 (`           'insert',`) `findKeywordPosition` находит слово `insert`:

```go
// review_lookup.go:875-891
func findKeywordPosition(text, keyword string) int {
    // ...
    if lower[i:i+len(keyword)] == keyword {
        if i > 0 && isWordChar(lower[i-1]) {  // '\'' — не wordChar, проверка пройдена
            continue
        }
        if i+len(keyword) < len(lower) && isWordChar(lower[i+len(keyword)]) {
            continue  // '\'' — не wordChar, проверка пройдена
        }
        return i  // "insert" найдён!
    }
}
```

`isWordChar` (`review_lookup.go:404-406`) проверяет только `a-z`, `A-Z`, `0-9`, `_`. Одинарная кавычка `'` **не является** wordChar, поэтому `findKeywordPosition` считает, что `insert` в `'insert'` — standalone ключевое слово.

Далее проверяется `isInComment(line, insertIdx)` — но это не комментарий (`--`), а строковый литерал. **Проверки на нахождение внутри строкового литерала нет.**

Анализатор начинает собирать строки в `insertBuffer` со строки 440. На строке 447 (`exec FCD_CON_Message_MassUpdStatus`) `isNewSQLStatement` возвращает `true` (начинается с `exec`), и накопленный буфер передаётся в `analyzeInsertForRowLock`.

### Что происходит в `analyzeInsertForRowLock`

```go
// review_rules.go:2322-2361
func analyzeInsertForRowLock(lines []string, startLine int, file *indexedFile) *Finding {
    fullText := strings.Join(lines, " ")
    lower := strings.ToLower(fullText)

    if hasRowLock(lower) {   // M_WITH_ROWLOCK отсутствует в строках 440-446
        return nil            // → не возвращается
    }

    tableName := parseInsertTableName(lines[0])  // lines[0] = "           'insert',"
    // parseInsertTableName находит "insert", извлекает "','" как "имя таблицы"
    if tableName == "" {
        return nil
    }

    // tableName = "','" — не начинается с "#", не пустое
    return &Finding{
        Rule:     RuleInsertRowLock,
        Severity: SeverityDeployStopper,
        Message:  "Для INSERT необходимо использовать M_WITH_ROWLOCK ...",
        Object:   tableName,  // "','"
        Line:     startLine,  // 440
    }
}
```

`parseInsertTableName` (`review_lookup.go:1115-1156`) находит `insert` в строке `'insert',`, затем извлекает символы после него как имя таблицы: `'` и `,` (оба не являются пробелом, табуляцией, `(` или `;`). Результат: `tableName = "','"`, что отображается в finding как `object=','`.

### Корневая причина

`checkInsertRowLock` не маскирует содержимое одинарных кавычек перед поиском ключевого слова `insert`. В коде уже существует функция `maskSingleQuotedStringContent` (`review_parser.go:1042-1066`), которая заменяет символы внутри строковых литералов на `?`, но она **не используется** в `checkInsertRowLock`.

## Ожидаемое поведение

1. Слово `insert` внутри одинарных кавычек (`'insert'`) не должно распознаваться как SQL-оператор `INSERT`
2. `checkInsertRowLock` не должен начинать сбор строк в `insertBuffer` на строке 440
3. Замечание `insertRowLock` для данного файла не должно генерироваться (единственный настоящий `INSERT` на строке 427 содержит `M_WITH_ROWLOCK`)

## Предложение по исправлению

### Вариант 1: Маскирование строковых литералов перед поиском `insert` (предпочтительный)

В функции `checkInsertRowLock` (`internal/review/review_rules.go:2280`) применять `maskSingleQuotedStringContent` к строке перед поиском ключевого слова:

```go
// Было:
insertIdx := findKeywordPosition(lower, "insert")

// Стало:
masked := maskSingleQuotedStringContent(line)
insertIdx := findKeywordPosition(strings.ToLower(masked), "insert")
```

После маскирования строка `           'insert',` превращается в `           '??????',`, и `findKeywordPosition` не находит слово `insert`.

Функция `maskSingleQuotedStringContent` уже существует в `review_parser.go:1042-1066` и корректно обрабатывает экранированные кавычки (`''`).

### Вариант 2: Добавить проверку `isInStringLiteral`

По аналогии с `isInComment` добавить функцию `isInStringLiteral(line, pos)`, которая проверяет, находится ли позиция `pos` внутри одинарных кавычек:

```go
func isInStringLiteral(line string, pos int) bool {
    inString := false
    for i := 0; i < pos && i < len(line); i++ {
        if line[i] == '\'' {
            if inString && i+1 < len(line) && line[i+1] == '\'' {
                i++ // экранированная кавычка
                continue
            }
            inString = !inString
        }
    }
    return inString
}
```

И добавить проверку в `checkInsertRowLock`:

```go
if insertIdx >= 0 && !isInComment(line, insertIdx) && !isInStringLiteral(line, insertIdx) {
```

**Недостаток варианта 2:** не обрабатывает многострочные строковые литералы (хотя в T-SQL одинарные кавычки не могут переноситься на следующую строку без конкатенации, так что это допустимо).

## Затронутые файлы

- `internal/review/review_rules.go` — функция `checkInsertRowLock`, строка 2280 (вариант 1 или 2)
- `internal/review/review_parser.go` — функция `maskSingleQuotedStringContent` уже существует (вариант 1)
- `internal/review/review_lookup.go` — функция `findKeywordPosition` (потребитель замаскированной строки)

## Влияние

Без исправления правило `insertRowLock` генерирует ложные срабатывания (severity 1, deploy stopper) на любых SQL-файлах, где слово `insert` встречается внутри строковых литералов. Это может блокировать деплой файлов, не содержащих реальных нарушений. Примеры:

- `'insert'` в SELECT-списке (данный случай)
- `'insert into tTable'` в сообщениях об ошибках
- `'INSERT'` в логических проверках (`if @Action = 'insert'`)

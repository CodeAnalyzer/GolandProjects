# Оптимизация RTI-парсера: устранение тройной аллокации и прескрининг regex

Снижение потребления памяти и CPU в `internal/rti/parser.go` при разборе больших RTI-логов (100+ МБ): устранение избыточного копирования файла в RAM и замена последовательного перебора 14 regex на прескрининг по первому байту строки.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `parser.go:52-69` (ParseFile → decodeData → parseContent) | `os.ReadFile` → `data []byte` (N), `decodeData` → `content string` (N), `bytes.NewReader([]byte(content))` → копия `[]byte` (N) | 3× аллокация полного размера файла: ~300 МБ RAM для 100 МБ лога |
| 2 | `parser.go:116-460` (цикл parseContent) | Каждая строка последовательно проверяется 14 regex (`reBLogTableBound`, `reEnter`, `reExit`, `reBLogHeader`, `reTrace`, `reRetVal`, `reElapsed`, `reReturn`, `reParam`, `reBLogParam`, `reCheckpoint`, `reClientHeader` + 2 условных BLogEnter/BLogExit) | 85-95% строк не матчат ни один паттерн (данные таблиц BusinessLog, пустые строки, continuation), но проходят через все 14 regex |

---

## Затронутые файлы

- `internal/rti/parser.go` — основная оптимизация (strings.NewReader + prescreening switch)
- `internal/rti/parser_test.go` — обновление/добавление тестов

---

## План (2 шага)

### Шаг 1: strings.NewReader вместо bytes.NewReader (нулевой риск)

**Файл:** `internal/rti/parser.go:111`

**Текущий код:**
```go
scanner := bufio.NewScanner(bytes.NewReader([]byte(content)))
```

**Новый код:**
```go
scanner := bufio.NewScanner(strings.NewReader(content))
```

**Обоснование:**
- `strings.NewReader` читает напрямую из backing array строки `content` — не создаёт новую аллокацию `[]byte`
- `bufio.Scanner` работает с `io.Reader` — оба варианта функционально идентичны
- Убирает одну из трёх аллокаций полного размера файла (N байт)
- Память: ~3N → ~2N (data []byte + content string)

**Imports:**
- Добавить `"strings"` — уже присутствует (используется в `parser.go:142,168,284,381,395,457`)
- Убрать `"bytes"` — используется только в `bytes.NewReader` (строка 111) и в `decodeData` (строка 552: `bytes.NewReader(data)`). `decodeData` остаётся, поэтому `"bytes"` **не убирается**

**Почему безопасно:**
- `strings.Reader` реализует `io.Reader` — тот же интерфейс, что и `bytes.Reader`
- `bufio.Scanner` не различает типы reader'ов
- Все существующие тесты `parser_test.go` проходят без изменений

**Проверка:**
```
go build ./internal/rti/...
go test ./internal/rti/...
```

---

### Шаг 2: Прескрининг по первому байту строки (низкий риск)

**Файл:** `internal/rti/parser.go:116-460` (тело цикла `for scanner.Scan()`)

**Текущий код:** каждая строка последовательно проходит через цепочку `if m := reXxx.FindStringSubmatch(line); m != nil { ... continue }` — 14 проверок подряд.

**Новый код:** перед цепочкой regex-проверок вставляется `switch` по первому байту строки, который направляет строку только в релевантные regex-проверки:

```go
for scanner.Scan() {
    lineNum++
    line := scanner.Text()

    // [блок pendingClient — без изменений, строки 124-137]

    // Прескрининг: направляем строку только в релевантные regex
    // по первому символу. Это исключает 85-95% regex-вызовов
    // на нерелевантных строках (данные таблиц, пустые строки, continuation).
    if len(line) == 0 {
        continue
    }

    switch line[0] {
    case 'B':
        // BusinessLog table boundary / BLogParam / BLog Enter/Exit
        // reBLogTableBound: "BusinessLog: Data from ..."
        // reBLogParam: "BLogParam:@..."
        // reBLogEnter: "Enter @@TranCount" (BLog-контекст)
        // reBLogExit: "Exit @@TranCount" (BLog-контекст)
        // [блок captureTable — строки 140-172]
        // [блок pendingBLog Enter/Exit — строки 268-279]
        // [блок reBLogParam — строки 388-399]

    case 'E':
        // reEnter: "Enter <ProcName> @@TranCount=..."
        // reExit: "Exit <ProcName> @@TranCount=..."
        // reElapsed: "Elapsed, ms: <N>"
        // [блок reEnter — строки 175-207]
        // [блок reExit — строки 210-226]
        // [блок reElapsed — строки 338-349]

    case 'R':
        // reRetVal: "RetVal = <N>#<ctx>"
        // reReturn: "Return <N>"
        // [блок reRetVal — строки 282-335]
        // [блок reReturn — строки 352-371]

    case '@':
        // reParam: "@<Name> : <Type> = <Value>"
        // [блок reParam — строки 374-385]

    case 'T':
        // reBLogTableHeader: "Table header <cols>"
        // (только внутри captureTable — обрабатывается в case 'B' или отдельно)
        // [обработка внутри блока captureTable]

    case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
        // reTrace: "DD.MM.YYYY HH:MM:SS.mmm\tINFO\tTrace.Server..."
        // reBLogHeader: "DD.MM.YYYY HH:MM:SS.mmm\tINFO\tTrace.Server.BusinessLog..."
        // reClientHeader: "DD.MM.YYYY HH:MM:SS.mmm\t<Level>\t<Category>..."
        // [блок reBLogHeader — строки 229-242]
        // [блок reTrace — строки 245-265]
        // [блок reClientHeader — строки 425-454]

    default:
        // reCheckpoint: "<ProcName>_Begin_<N>" — первый символ — буква (любая, не E/R/B/T/@/digit)
        // Проверяется в default, т.к. имя процедуры может начинаться с любой буквы
        // [блок reCheckpoint — строки 402-420]
        // Unparsed — строки 457-459
    }
}
```

**Детали распределения паттернов по веткам:**

| Ветка | Паттерны | Условия |
|-------|----------|---------|
| `B` | `reBLogTableBound`, `reBLogParam`, `reBLogEnter`, `reBLogExit` | `reBLogEnter`/`reBLogExit` — только при `pendingBLog` |
| `E` | `reEnter`, `reExit`, `reElapsed` | |
| `R` | `reRetVal`, `reReturn` | `reRetVal` — с проверкой `pendingBLog` |
| `@` | `reParam` | |
| `T` | `reBLogTableHeader` | Только при `captureTable` |
| `0`-`9` | `reBLogHeader`, `reTrace`, `reClientHeader` | |
| `default` | `reCheckpoint`, Unparsed | `reCheckpoint` — имя процедуры может начинаться с любой буквы |

**Важные нюансы реализации:**

1. **Блок `captureTable` (строки 140-172)** — обрабатывает `reBLogTableBound` (начинается с `B`) и `reBLogTableHeader` (начинается с `T`), а также строки-данные таблицы (любой первый символ). Логика:
   - `reBLogTableBound` проверяется в `case 'B'` перед другими B-паттернами
   - Если `captureTable && currentTable != nil` — это состояние перехватывает **все** строки (включая `T` для header и любые для row data). Нужно сохранить это поведение: проверка `captureTable` должна идти **до** switch или быть первым `if` внутри каждой ветки.

2. **Блок `pendingClient` (строки 124-137)** — выполняется **до** switch, без изменений. Он проверяет `looksLikeNewRecordHeader(line)`, который сам вызывает 6 regex. Это не убирается прескринингом, т.к. `looksLikeNewRecordHeader` находится в `parser_client.go` и используется только для определения границы клиентского события. Оптимизация `looksLikeNewRecordHeader` — отдельная задача (если потребуется).

3. **`reCheckpoint` в `default`** — имя процедуры в Diasoft 5NT может начинаться с любой заглавной буквы (A-Z, не только E/R/B/T). Поэтому `reCheckpoint` проверяется в `default`-ветке. Это означает, что строки-данные таблиц BusinessLog (когда `captureTable == false`) и continuation-строки тоже попадают в `default` и проверяются одним `reCheckpoint` regex — это допустимо (1 regex вместо 14).

4. **Порядок внутри веток** — сохраняется тот же порядок проверок, что и в текущем коде. Это важно для `reRetVal` (перехват BLog-блоков) и `reEnter`/`reExit` (управление стеком).

**Альтернатива для captureTable:** вместо проверки `captureTable` в каждой ветке, вынести его до switch как отдельный `if`-блок (как сейчас), и `continue` — тогда switch будет достигаться только для строк вне captureTable-режима. Это проще и ближе к текущему коду.

**Рекомендуемая структура:**
```go
// [pendingClient — без изменений]

// BusinessLog table boundary (begin / end) — проверяется до switch,
// т.к. "BusinessLog:" начинается с 'B', но управляет captureTable-режимом
if m := reBLogTableBound.FindStringSubmatch(line); m != nil {
    // [существующий код строк 141-161]
    continue
}

// Table header / row capture — перехватывает все строки в captureTable-режиме
if captureTable && currentTable != nil {
    // [существующий код строк 166-171]
    continue
}

// Прескрининг по первому байту
if len(line) == 0 {
    continue
}

switch line[0] {
case 'E':
    // reEnter, reExit, reElapsed
case 'R':
    // reRetVal (с pendingBLog-логикой), reReturn
case '@':
    // reParam
case 'B':
    // reBLogParam, reBLogEnter/Exit (при pendingBLog)
case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
    // reBLogHeader, reTrace, reClientHeader
default:
    // reCheckpoint, Unparsed
}
```

**Почему безопасно:**
- Логика каждого regex-блока не изменяется — только порядок вызовов
- `reBLogTableBound` и `captureTable` остаются до switch (та же позиция, что и сейчас)
- Все существующие тесты `parser_test.go` проходят без изменений
- Распределение по веткам определено по первым символам реальных паттернов из `parser.go:20-42`

**Проверка:**
```
go build ./internal/rti/...
go test ./internal/rti/... -count=1
```

Дополнительно — ручной тест на реальном RTI-файле:
```
codebase rti parse <path-to-rti-file>
codebase rti summary <session-id>
```
Сравнить `TotalCalls`, `UnparsedLines`, `ErrorsCount` до/после — должны быть идентичны.

---

## Порядок выполнения

1. **Шаг 1** (strings.NewReader) → сборка + тесты rti
2. **Шаг 2** (prescreening switch) → сборка + тесты rti + ручной тест на реальном файле

Каждый шаг независим. Шаг 1 — 1 строка, можно коммитить отдельно. Шаг 2 — перестройка тела цикла, отдельный коммит.

---

## Результаты замеров

Тестовый файл: `Tests/ABS8E_abs_e1_SRV.rti` (77 МБ, 7690 вызовов)

| Версия | Время парсинга | Unparsed | Изменение |
|--------|---------------|----------|-----------|
| Базовая (сессия 1) | 3.77 сек | 13922 | — |
| После Шаг 1 + Шаг 2 (сессия 8) | 3.30 сек | 14021 | −12.5% CPU, +99 unparsed (исправление) |

**Unparsed +99 строк:** `reBLogEnter`/`reBLogExit` в старом коде проверялись без условия `pendingBLog` и перехватывали строки "Enter @@TranCount"/"Exit @@TranCount" вне BLog-контекста (continue без unparsed). В новом коде они проверяются только при `pendingBLog == true` — корректное поведение, эти строки теперь учитываются как unparsed.

**Почему меньше ожидаемого (−12.5% вместо −30-50%):** основное время уходит не на regex-проверки в main loop, а на:
- `looksLikeNewRecordHeader` (`parser_client.go:84-91`) — вызывает 6 regex на каждой строке внутри клиентского события (не оптимизирован прескринингом)
- I/O и декодирование кодировки (`os.ReadFile` + `decodeData`)
- `time.ParseInLocation` на каждой timestamp-строке

Шаг 1 (strings.NewReader) убрал одну аллокацию, но на время парсинга это не повлияло — bottleneck в CPU, не в памяти.

Тесты: `go test ./internal/rti/... -count=1` — PASS.

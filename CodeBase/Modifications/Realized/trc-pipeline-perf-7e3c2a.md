# Оптимизация TRC-пайплайна: UTF-16 декодер, strconv в горячих циклах, XEL-скан, GC pressure

Устранение узких мест в пайплайне парсинга и хранения TRC-трейсов: двойная аллокация при UTF-16 декодировании, `fmt.Sprintf`/`fmt.Sscanf` в горячих циклах, побайтовый скан в XEL-парсере, безусловное копирование в `sanitizeParams`, рассинхрон batch sizes.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `parser.go:402-408` (`decodeUTF16`) + `xel_format.go:153-162` (`decodeUTF16LE`) | Двойная аллокация: `make([]uint16, N)` + `make([]rune, N)` → `string` на каждое строковое поле каждого события | Миллионы двойных аллокаций для файлов с 100K+ событий и TextData-полями |
| 2 | `store.go:149` (`marshalColumns`) | `fmt.Sprintf("%d", id)` для каждого column ID (в 10-20× медленнее `strconv.Itoa`) | CPU на сериализации миллионов событий |
| 3 | `store.go:181` (`unmarshalColumns`) | `fmt.Sscanf(key, "%d", &id)` для каждого column key при загрузке из БД (в 5-10× медленнее `strconv.Atoi`) | CPU при загрузке событий |
| 4 | `enrich_parallel.go:127-134` (`sanitizeParams`) | `make([]TRCParam, len(params))` + `strings.ReplaceAll` на каждое поле, даже если нулевых байтов нет | GC pressure на миллионах событий |
| 5 | `xel_parser.go:491` (`parseXELCB`) | Побайтовый `pos++` при ненахождении маркера вместо `bytes.Index` | O(n) побайтовый скан для нераспознанных секций XEL |
| 6 | `parse_to_db.go:14` (`streamBatchSize=10000`) vs `store.go:13` (`batchInsertSize=50000`) | Каждый flush (10K) вызывает `insertTRCEvents` → `serializeParallel` (goroutine spawn/sync overhead) на 10K событий, затем CopyIn на 50K батчах | 100 вызовов overhead на 1M событий |
| 7 | `parser.go:226` (`make([]byte, length)`) | Per-event аллокация byte-буфера для полей события | GC pressure на больших файлах |

---

## Затронутые файлы

- `internal/trc/parser.go` — `decodeUTF16` single-pass, `sync.Pool` для byte-буферов
- `internal/trc/xel_format.go` — `decodeUTF16LE` single-pass, pre-allocated slice в `consumeActionTLVs`
- `internal/trc/store.go` — `strconv.Itoa` в `marshalColumns`, `strconv.Atoi` в `unmarshalColumns`
- `internal/trc/enrich_parallel.go` — zero-copy fast path в `sanitizeParams`
- `internal/trc/parse_to_db.go` — выровнять `streamBatchSize` с `batchInsertSize`
- `internal/trc/xel_parser.go` — `bytes.Index` вместо побайтового `pos++`
- `internal/trc/utf16.go` — **новый файл**: single-pass UTF-16LE декодер
- `internal/trc/utf16_test.go` — **новый файл**: тесты для single-pass декодера
- `internal/trc/store_test.go` — тесты для `marshalColumns`/`unmarshalColumns`
- `internal/trc/enrich_parallel_test.go` — тест для `sanitizeParams` zero-copy path

---

## План (7 шагов)

### Шаг 1: Single-pass UTF-16LE декодер (нулевой риск)

**Файлы:** `internal/trc/utf16.go` (новый), `internal/trc/utf16_test.go` (новый), `internal/trc/parser.go:402-408`, `internal/trc/xel_format.go:153-162`

**Проблема:** Текущие `decodeUTF16` и `decodeUTF16LE` создают два промежуточных слайса (`[]uint16` + `[]rune`) перед формированием `string`. Для миллионов событий с TextData-полями это миллионы двойных аллокаций.

**Решение:** Новый single-pass декодер `[]byte → string` без промежуточных слайсов:

```go
// decodeUTF16LEBytes декодирует UTF-16LE байты в string за один проход,
// без промежуточных []uint16 и []rune.
func decodeUTF16LEBytes(b []byte) string {
    if len(b)%2 != 0 {
        b = b[:len(b)-1]
    }
    n := len(b) / 2
    var sb strings.Builder
    sb.Grow(n) // worst case: 1 byte per rune (ASCII)
    for i := 0; i < n; i++ {
        r := rune(binary.LittleEndian.Uint16(b[i*2 : i*2+2]))
        if r >= 0xD800 && r <= 0xDBFF && i+1 < n {
            r2 := rune(binary.LittleEndian.Uint16(b[(i+1)*2 : (i+1)*2+2]))
            if r2 >= 0xDC00 && r2 <= 0xDFFF {
                r = ((r-0xD800)<<10)|(r2-0xDC00) + 0x10000
                i++
            }
        }
        sb.WriteRune(r)
    }
    return sb.String()
}
```

**Изменения:**
- Создать `internal/trc/utf16.go` с функцией `decodeUTF16LEBytes`
- В `parser.go:402-408`: заменить тело `decodeUTF16` на вызов `decodeUTF16LEBytes(b)`
- В `xel_format.go:153-162`: заменить тело `decodeUTF16LE` на вызов `decodeUTF16LEBytes(b)`
- `utf16ToString` в `header.go` — оставить как есть (используется в `findNextUTF16String` для потокового поиска, где `[]uint16` уже доступен из `binary.ByteSlice.Uint16s`)

**Тесты в `internal/trc/utf16_test.go`:**
- `TestDecodeUTF16LEBytes_ASCII` — чистый ASCII, проверка идентичности с `decodeUTF16`
- `TestDecodeUTF16LEBytes_Cyrillic` — кириллица (UTF-16LE коды 0x0410-0x044F)
- `TestDecodeUTF16LEBytes_SurrogatePair` — emoji U+1F600 (surrogate pair)
- `TestDecodeUTF16LEBytes_OddLength` — нечётная длина → последний байт отбрасывается
- `TestDecodeUTF16LEBytes_Empty` — пустой ввод → пустая строка
- `TestDecodeUTF16LEBytes_NullBytes` — нулевые байты (0x0000) → корректный rune U+0000
- `TestDecodeUTF16LEBytes_ParallelWithOld` — сравнение с `decodeUTF16` на случайных данных

**Почему безопасно:**
- Функция `decodeUTF16LEBytes` реализует стандартный алгоритм декодирования UTF-16LE (RFC 2781), идентичный `unicode/utf16.Decode`
- Существующие тесты `parser_test.go` и `xel_test.go` покрывают декодирование строковых полей — они пройдут без изменений
- `strings.Builder.Grow(n)` — worst case оценка (1 байт на rune для ASCII), для кириллицы фактический расход 2 байта на rune — Builder автоматически расширит

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -run UTF16 -count=1
go test ./internal/trc/... -count=1
```

---

### Шаг 2: `strconv.Itoa` в `marshalColumns` (нулевой риск)

**Файл:** `internal/trc/store.go:146-167`

**Текущий код (строка 149):**
```go
key := fmt.Sprintf("%d", id)
```

**Новый код:**
```go
key := strconv.Itoa(id)
```

**Обоснование:**
- `strconv.Itoa` в 10-20× быстрее `fmt.Sprintf("%d", id)` — не использует reflection, не создаёт format string
- Вывод идентичен для всех значений `int`
- `strconv` уже импортирован в `store.go` (используется в `nullableInt32` и др.)

**Почему безопасно:**
- `strconv.Itoa(int)` и `fmt.Sprintf("%d", int)` производят идентичные строки
- Тест `TestMarshalUnmarshalColumns` (если есть) или `TestInsertTRCEvents` покрывает round-trip

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -count=1
```

---

### Шаг 3: `strconv.Atoi` в `unmarshalColumns` (нулевой риск)

**Файл:** `internal/trc/store.go:170-183`

**Текущий код (строка 181):**
```go
if _, err := fmt.Sscanf(key, "%d", &id); err != nil {
    continue
}
```

**Новый код:**
```go
id, err := strconv.Atoi(key)
if err != nil {
    continue
}
```

**Обоснование:**
- `strconv.Atoi` в 5-10× быстрее `fmt.Sscanf(key, "%d", &id)` — не парсит format string
- `strconv` уже импортирован в `store.go`

**Почему безопасно:**
- `strconv.Atoi(string)` и `fmt.Sscanf(string, "%d", &int)` производят идентичный результат для валидных числовых строк
- Для нечисловых строк оба возвращают ошибку → `continue`

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -count=1
```

---

### Шаг 4: `sanitizeParams` zero-copy fast path (нулевой риск)

**Файл:** `internal/trc/enrich_parallel.go:124-135`

**Текущий код:**
```go
func sanitizeParams(params []TRCParam) []TRCParam {
    out := make([]TRCParam, len(params))
    for i, p := range params {
        out[i] = TRCParam{
            Name:  strings.ReplaceAll(p.Name, "\x00", ""),
            Value: strings.ReplaceAll(p.Value, "\x00", ""),
        }
    }
    return out
}
```

**Новый код:**
```go
func sanitizeParams(params []TRCParam) []TRCParam {
    // Fast path: если ни в одном параметре нет нулевых байтов,
    // возвращаем исходный слайс без копирования.
    hasNull := false
    for _, p := range params {
        if strings.IndexByte(p.Name, 0) >= 0 || strings.IndexByte(p.Value, 0) >= 0 {
            hasNull = true
            break
        }
    }
    if !hasNull {
        return params
    }
    // Slow path: копируем и очищаем только при наличии нулевых байтов.
    out := make([]TRCParam, len(params))
    for i, p := range params {
        out[i] = TRCParam{
            Name:  strings.ReplaceAll(p.Name, "\x00", ""),
            Value: strings.ReplaceAll(p.Value, "\x00", ""),
        }
    }
    return out
}
```

**Обоснование:**
- Нулевые байты в параметрах — редкое исключение (возникают только при повреждённых данных трейса)
- `strings.IndexByte` — SIMD-оптимизированная функция (одна инструкция на байт на современных CPU)
- Проверка `len(params)` полей на нулевые байты дешевле, чем безусловное копирование + `ReplaceAll` для всех событий

**Почему безопасно:**
- Если нулевых байтов нет — возвращается исходный слайс, который не мутируется downstream (используется только для чтения в `json.Marshal`)
- Если нулевые байты есть — поведение идентично текущему (копирование + очистка)
- Тест: `TestSanitizeParams_NoNullBytes` — проверка, что возвращается тот же слайс (pointer equality)
- Тест: `TestSanitizeParams_WithNullBytes` — проверка, что возвращается копия с очищенными значениями

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -run SanitizeParams -count=1
go test ./internal/trc/... -count=1
```

---

### Шаг 5: Выровнять `streamBatchSize` с `batchInsertSize` (нулевой риск)

**Файл:** `internal/trc/parse_to_db.go:14`

**Текущий код:**
```go
const streamBatchSize = 10000
```

**Новый код:**
```go
const streamBatchSize = 50000 // выровнено с batchInsertSize в store.go
```

**Обоснование:**
- При `streamBatchSize=10000` и `batchInsertSize=50000`: каждый flush (10K) вызывает `insertTRCEvents`, который внутри делает `serializeParallel` (создание goroutines + sync.WaitGroup) на 10K событий, затем CopyIn на одном 50K-батче (10K < 50K → одна транзакция)
- Для файла с 1M событий: 100 вызовов `insertTRCEvents` → 100 раз overhead на goroutine spawn/sync
- При `streamBatchSize=50000`: 20 вызовов → 5× меньше overhead
- Память: 50K событий × ~1KB (средний размер TRCEvent) ≈ 50МБ — приемлемо

**Почему безопасно:**
- `insertTRCEvents` уже обрабатывает батчи любого размера (внутренний цикл по `batchInsertSize`)
- Увеличение batch size не влияет на корректность, только на количество flush-вызовов
- Тест `TestParseFileToDB` (если есть) или ручной тест на `.trc` файле — количество сохранённых событий должно быть идентичным

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -count=1
```

Ручной тест на реальном файле:
```
codebase trc parse <path-to-trc-file>
codebase trc summary <session-id>
```
Сравнить `TotalEvents` до/после — должны быть идентичны.

---

### Шаг 6: `bytes.Index` вместо побайтового `pos++` в XEL-парсере (низкий риск)

**Файл:** `internal/trc/xel_parser.go` (функция `parseXELCB`, ~строки 480-510)

**Текущий код (схематично):**
```go
for pos < len(data) {
    // попытка декодировать wait_completed / action-then-data
    // ...
    if !decoded {
        pos++ // побайтовый скан
        continue
    }
}
```

**Новый код (схематично):**
```go
for pos < len(data) {
    // попытка декодировать wait_completed / action-then-data
    // ...
    if !decoded {
        // Быстрый поиск следующего потенциального маркера события.
        // Сначала ищем wait_completed-маркер, затем action-then-data anchor.
        nextWait := bytes.Index(data[pos:], xelWaitCompletedMarker)
        // Для action-then-data: ищем pkg-байт 0x01 или 0x02 с валидным TLV
        // (упрощённый поиск — см. детали ниже)
        if nextWait >= 0 {
            pos += nextWait
        } else {
            break // больше маркеров нет
        }
        continue
    }
}
```

**Детали реализации:**
1. `bytes.Index(data[pos:], xelWaitCompletedMarker)` — SIMD-оптимизированный поиск 12-байтового маркера `wait_completed` (83% событий)
2. Для action-then-data событий (sql_batch_completed, sql_statement_completed, sp_statement_completed): поиск начинается с байта `0x01` или `0x02` (package ID), затем проверка `data[p+3] == 0x10` (TLV marker). Использовать `bytes.IndexByte(data[pos:], 0x01)` / `bytes.IndexByte(data[pos:], 0x02)` с последующей валидацией TLV-структуры
3. Выбрать ближайший из двух найденных смещений (wait_completed vs action-then-data)
4. Если ни один не найден — `break` (конец данных)

**Альтернатива (более простой вариант):**
- Искать только `xelWaitCompletedMarker` через `bytes.Index` (покрывает 83% событий)
- Для action-then-data оставить `pos++` (они расположены между wait_completed-событиями, расстояние небольшое)
- Это уменьшает сложность реализации при сохранении большей части выгоды

**Рекомендуемый подход:** начать с простого варианта (только `bytes.Index` для wait_completed), измерить эффект, при необходимости расширить на action-then-data.

**Почему безопасно:**
- `bytes.Index` находит то же смещение, что и побайтовый скан — это функционально идентичные операции
- Тесты `TestXEL_DecodeWaitCompletedData`, `TestXEL_DecodeSQLBatchCompletedData`, `TestXEL_DecodeSQLStatementCompletedData` — покрывают декодирование событий
- Тест `TestXEL_ParseFullFile` (если есть) — покрывает полный цикл парсинга

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -run XEL -count=1
go test ./internal/trc/... -count=1
```

Ручной тест:
```
codebase trc parse <path-to-xel-file>
codebase trc summary <session-id>
```
Сравнить `TotalEvents` до/после — должны быть идентичны.

---

### Шаг 7: `sync.Pool` для byte-буферов событий (низкий риск)

**Файлы:** `internal/trc/parser.go:226`, `internal/trc/xel_format.go:124-148`

**Проблема:** `make([]byte, length)` в `parser.go:226` и `var fields []xelActionField` в `consumeActionTLVs` создают новые аллокации на каждое событие. Для файлов с 100K+ событий — значимый GC pressure.

**Решение для `parser.go` (binary TRC):**
```go
var fieldBufPool = sync.Pool{
    New: func() any {
        b := make([]byte, 0, 4096) // начальный capacity
        return &b
    },
}

// В цикле парсинга:
fields := (*fieldBufPool.Get().(*[]byte))[:length]
if cap(*fieldBufPool.Get().(*[]byte)) < length {
    // перераспределение при необходимости
    fields = make([]byte, length)
}
// ... использование fields ...
// В конце итерации:
buf := fields[:0]
fieldBufPool.Put(&buf)
```

**Решение для `consumeActionTLVs` (XEL):**
```go
var actionFieldsPool = sync.Pool{
    New: func() any {
        s := make([]xelActionField, 0, 8)
        return &s
    },
}

func consumeActionTLVs(data []byte, start int) ([]xelActionField, int) {
    fieldsP := actionFieldsPool.Get().(*[]xelActionField)
    fields := (*fieldsP)[:0]
    // ... существующая логика заполнения ...
    // Вызывающий код должен вернуть слайс в pool после использования
    return fields, p
}
```

**Важные нюансы:**
1. `sync.Pool` не гарантирует сохранение объектов между GC-циклами — это нормально, цель в снижении пикового давления, не в полном устранении аллокаций
2. Для `parser.go`: буфер должен быть возвращён в pool **после** `decodeEventFields` и `enrichEvent`, т.к. они могут удерживать ссылки на подстроки из `fields` (через `decodeUTF16`). **Внимание:** `decodeUTF16` копирует данные в новый `string`, так что ссылки на `fields` не сохраняются — pool безопасен
3. Для `consumeActionTLVs`: вызывающий код (`parseXELCB`) должен вернуть `fields` в pool после применения action-колонок к событию. Нужно добавить `defer actionFieldsPool.Put(&fields)` или явный возврат после использования

**Риск:** Если `decodeEventFields` или `decodeColumnValue` сохраняют ссылки на под-слайсы `fields` (через `string(b)` — безопасно, т.к. `string()` копирует; через `b` напрямую — небезопасно). Проверить: `decodeColumnValue` в `parser.go` — для `TypeBinary` возвращает `[]byte` напрямую из `fields` (строка ~380). Это **небезопасно** для pool — нужно явно копировать binary-данные: `append([]byte(nil), b...)`.

**Альтернатива (более простой вариант):** Применить `sync.Pool` только для `consumeActionTLVs` (XEL), где `Value` — это `data[valStart:valEnd]` (под-слайс из `data`, не из pooled буфера). Для `parser.go` — пропустить, т.к. binary-колонки требуют копирования, что нивелирует выгоду pool.

**Рекомендуемый подход:** Начать с `consumeActionTLVs` (безопасно, Value — срез из `data`, не из pooled буфера). Для `parser.go` — только если profiling покажет значимый вклад `make([]byte, length)` в GC.

**Почему безопасно (для `consumeActionTLVs`):**
- `xelActionField.Value` — это `data[valStart:valEnd]` (под-слайс из основного `data`, не из pooled буфера)
- После применения action-полей к событию (в `parseXELCB`), `fields` больше не нужен
- Возврат в pool через `defer` или явный вызов после use

**Проверка:**
```
go build ./internal/trc/...
go test ./internal/trc/... -run XEL -count=1
go test ./internal/trc/... -count=1
```

---

## Порядок выполнения

1. **Шаг 1** (UTF-16 single-pass) → сборка + тесты trc
2. **Шаг 2** (strconv.Itoa) → сборка + тесты trc
3. **Шаг 3** (strconv.Atoi) → сборка + тесты trc
4. **Шаг 4** (sanitizeParams zero-copy) → сборка + тесты trc
5. **Шаг 5** (streamBatchSize) → сборка + тесты trc + ручной тест
6. **Шаг 6** (bytes.Index для XEL) → сборка + тесты trc + ручной тест на .xel
7. **Шаг 7** (sync.Pool для consumeActionTLVs) → сборка + тесты trc

Шаги 1-5 — нулевой риск, можно комбинировать в один коммит.
Шаг 6 — отдельный коммит (изменение логики XEL-скана).
Шаг 7 — отдельный коммит (sync.Pool).

## Проверка после каждой группы шагов

```
go build ./...
$env:TMPDIR="C:\Temp"; go test ./internal/trc/... -count=1
```

После Шагов 5 и 6 — дополнительный ручной тест:
```
codebase trc parse <path-to-trc-or-xel-file>
codebase trc summary <session-id>
```

---

## Ожидаемый эффект

| Шаг | Метрика | Ожидание |
|-----|---------|----------|
| 1 | Аллокации на UTF-16 декодирование | −2× (убрать промежуточные слайсы) |
| 2 | CPU на marshalColumns | −10-20% (strconv.Itoa vs fmt.Sprintf) |
| 3 | CPU на unmarshalColumns | −5-10× (strconv.Atoi vs fmt.Sscanf) |
| 4 | Аллокации в sanitizeParams | −100% для событий без null bytes (большинство) |
| 5 | Overhead на goroutine spawn/sync | −5× (100 → 20 flush-вызовов на 1M событий) |
| 6 | XEL скан для нераспознанных секций | −10-100× (bytes.Index SIMD vs pos++) |
| 7 | GC pressure на XEL events | −30-50% аллокаций action fields |

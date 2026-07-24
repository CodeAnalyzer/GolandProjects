# Поддержка HRTI (анонимизированных RTI) в CodeBase

Интеграция декодера TDsHash в RTI-парсер CodeBase для автоматического декодирования зашифрованных строковых значений в HRTI-файлах.

## Контекст

HRTI — формат анонимизированных RTI-логов Diasoft 5NT. Все строковые поля (значения параметров, контексты ошибок, имена блоков BLog, ячейки таблиц) кодируются алгоритмом TDsHash: каждый символ заменяется по формуле `NewIndex = ((Ord(char) XOR M1) + M2) mod 128` → `CipherChar = C_LETTERS[NewIndex]`. Закодированные значения обёрнуты в маркеры `6D6...6D6` (декодируются как `#$#...#$#`).

Найдены 2 пары ключей (оба дают идентичный результат): M1=3/M2=102 и M1=67/M2=38. Используем M1=3, M2=102.

## План

### Phase 1: Декодер TDsHash (новый файл `internal/rti/hrti.go`)

Создать модуль декодирования HRTI-строк:

- **Константы**: `cLetters` — массив 128 Unicode-кодов (0-9, A-Z, a-z, Ё, А-Я, ё, а-я), `hrtiM1 = 3`, `hrtiM2 = 102`, `hrtiMarker = "6D6"`
- **Таблицы**: `ordToIndex` (map[rune]int для обратного поиска), `mod128ToRussian` (map[int][]rune — русские буквы по `ord % 128`)
- **`decodeHRTIString(s string) string`** — главная функция:
  1. Проверяет, что строка обёрнута в `6D6...6D6` (минимум 6 символов: 3 + 3)
  2. Извлекает содержимое между маркерами
  3. Для каждого rune: `idx = ordToIndex[rune]`, `v = ((idx - M2) mod 128) XOR M1`
  4. Разрешение коллизии ASCII↔Русский: prefer Russian для буквенных значений, ASCII для пробела (v=32) и знаков `%` `(` `)` `,` `.`
  5. Возвращает декодированную строку; если маркер не найден — возвращает оригинал
- **`isHRTIContent(content string) bool`** — авто-детект: проверяет наличие паттерна `6D6` в первых N параметрах/значениях лога
- **`decodeHRTIIfNeeded(value string) string`** — проверяет наличие маркера и декодирует, иначе возвращает как есть

### Phase 2: Интеграция в парсер (`internal/rti/parser.go`)

- В `ParseFile`: после `parseContent`, если обнаружен HRTI-контент — вызвать `DecodeHRTIResult(result)`
- **Альтернатива (предпочтительнее)**: декодировать на лету в `parseContent`, сразу при парсинге каждого поля. Но это усложняет логику. Проще — пост-обработка.
- Выбран подход: **пост-обработка** — после полного парсинга, перед возвратом результата

**Новая функция `DecodeHRTIResult(result *RTIParseResult)`**:
- Проход по всем `Calls`:
  - `RTIParam.Value` → `decodeHRTIIfNeeded`
  - `RetValContext` → `decodeHRTIIfNeeded`
  - `BLogBlock.BlockName` → `decodeHRTIIfNeeded`
  - `BLogTable.TableName` → `decodeHRTIIfNeeded`
  - `BLogTable.Columns` — каждый элемент → `decodeHRTIIfNeeded`
  - `BLogTable.Rows` — каждая строка: split по `_|_`, декодировать каждый элемент, join обратно
- Проход по всем `ClientEvents`:
  - `ErrorText` → `decodeHRTIIfNeeded`
  - `RawBody` → `decodeHRTIIfNeeded`
  - `SQL.Text` → `decodeHRTIIfNeeded`
  - `SQL.ExecProcedure` → `decodeHRTIIfNeeded`
  - `SQL.ExecParams[].Value` → `decodeHRTIIfNeeded`
  - `Connection` поля (Server, Database, User, AppName) → `decodeHRTIIfNeeded`
  - `BPL` поля (File, Title, Comment) → `decodeHRTIIfNeeded`

**Авто-детект**: в `parseContent`, после парсинга первых ~50 вызовов, проверить, есть ли в `RTIParam.Value` маркер `6D6`. Если да — установить флаг `isHRTI = true` и после парсинга вызвать `DecodeHRTIResult`.

### Phase 3: Тесты (`internal/rti/hrti_test.go`)

- `TestDecodeHRTIString_Basic` — декодирование `6D65Pe4PgъPUU6D6` → `МасНачЗалл`
- `TestDecodeHRTIString_NameWithSpaces` — `6D65PeeZNZS9aPgXeUSaXS9QPTZURSaaZedSW9999...6D6` → `Массовое начисление задолженностей`
- `TestDecodeHRTIString_NoMarker` — обычная строка без `6D6` возвращается как есть
- `TestDecodeHRTIString_NULL` — значение `NULL` возвращается как есть
- `TestDecodeHRTIString_Numeric` — числовое значение `20000009238` возвращается как есть
- `TestDecodeHRTIString_Empty` — пустая строка возвращается как есть
- `TestDecodeHRTIString_SysNames` — декодирование 20 системных имён из скрипта
- `TestIsHRTIContent` — авто-детект по наличию маркера
- `TestDecodeHRTIResult` — комплексный тест: парсинг HRTI-фрагмента + проверка декодированных полей
- `TestDecodeHRTIString_TableRow` — декодирование строки таблицы с разделителями `_|_`

### Phase 4: Интеграция в CLI/MCP

Изменений в CLI (`cmd/rti.go`) и MCP (`internal/mcp/registry.go`) не требуется — декодирование происходит прозрачно на уровне парсера. Все подкоманды (`parse`, `errors`, `slow`, `details`, `tree`, `blog`, `timeline`) автоматически получают декодированные значения.

## Файлы

| Файл | Действие |
|---|---|
| `internal/rti/hrti.go` | **Новый** — декодер TDsHash + DecodeHRTIResult |
| `internal/rti/hrti_test.go` | **Новый** — unit-тесты |
| `internal/rti/parser.go` | **Изменить** — вызов авто-детекта и DecodeHRTIResult в parseContent |

## Проверка

```bash
go build ./...
go test ./internal/rti/... -count=1 -run HRTI
go test ./internal/rti/... -count=1
```

Проверка на реальном файле:
```bash
codebase rti parse Modifications/cert_10002405568_20042026_21072026_транш_рамка_2.5.hrti
codebase rti errors Modifications/cert_10002405568_20042026_21072026_транш_рамка_2.5.hrti
codebase rti details --proc API_Sbcn_FindListByType Modifications/cert_10002405568_20042026_21072026_транш_рамка_2.5.hrti
```

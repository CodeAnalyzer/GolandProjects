# План реализации `codebase rti`

Детальный пошаговый план реализации команды `codebase rti` для анализа RTI-логов с обогащением из индекса CodeBase, на основе предложения `D:\GITHUB\GolandProjects\CodeBase\Modifications\rti-log-analyzer.md`.

## Корректировки к предложению (подтверждены пользователем)

1. **Триггер retcode-парсера**: не по имени файла `*retcode*.sql`, а по содержимому — regex-прескрининг в конце `parseSQLFile` на наличие паттернов: `ReturnCode_Insert`, `FCD_*_Notification_Save`, `_ADD_RETCODE_`, `__Notification_Save`.
2. **Третий паттерн ошибок**: `__Notification_Save(code, 'msg', 'proc', DSMODULE_*_ID)` → `FCD_CNE_Notification_Save` (пример: `DEVEXT_Notification.sql`). Структура аргументов: 4 параметра (добавляется `DsModuleID`).
3. **list/delete/prune**: добавляются в Фазу 3 (PostgreSQL store), не упомянуты в таблицах фаз предложения.
4. **slow**: Фаза 1 (без enrichment). **details**: Фаза 2 (с enriched-полями).

---

## Фаза 1: MVP — парсер + summary + tree + errors + slow

### Шаг 1.1: `DetectFromBytes` в `internal/encoding/encoding.go`

- Перенести эвристику из `internal/review/review_helpers.go:1939` (`detectFileEncoding`) в `internal/encoding/encoding.go` как `DetectFromBytes(data []byte) Encoding`.
- Логика: нет байт > 0x7F → ASCII; валидный UTF-8 → UTF8; иначе по маркерным диапазонам 0x80–0x9F = CP866, 0xC0–0xDF = CP1251.
- Обновить `internal/review/review_helpers.go`: `detectFileEncoding` делегирует в `encoding.DetectFromBytes`.
- **Тест**: `TestDetectFromBytes_ASCII`, `TestDetectFromBytes_UTF8`, `TestDetectFromBytes_CP1251`, `TestDetectFromBytes_CP866`.
- **Файлы**: `internal/encoding/encoding.go` (+~30 строк), `internal/review/review_helpers.go` (~5 строк изменено).

### Шаг 1.2: Схема `ds_return_codes` в `internal/store/db_schema.go`

- Добавить в `InitSchema()`:
  ```sql
  CREATE TABLE IF NOT EXISTS ds_return_codes (
      id BIGSERIAL PRIMARY KEY,
      file_id BIGINT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
      ret_code BIGINT NOT NULL,
      message TEXT NOT NULL,
      proc_name TEXT,
      module_id INTEGER
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_ds_return_codes_code ON ds_return_codes(ret_code);
  ```
- **Файлы**: `internal/store/db_schema.go` (+~10 строк).

### Шаг 1.3: Парсер `internal/parser/retcode/retcode.go`

- **Тип**: `RetCodeEntry { RetCode int64; Message string; ProcName string; ModuleID int }`.
- **Функция**: `Parse(content string) []RetCodeEntry`.
- **Три паттерна**:
  - **Паттерн 1 (Core/Admin)**: `ReturnCode_Insert <code>, <CONST|'msg'>, 'ProcName'`. Если arg2 — константа (без кавычек), пометить для последующего lookup в `h_files_defines`.
  - **Паттерн 2 (Applied products)**: `_ADD_RETCODE_(code, 'msg', 'proc')` и подобные product-макросы `M_*_RETCODE_INSERT(code, 'msg', 'proc')`. 3 аргумента: numeric, 'literal', 'proc'.
  - **Паттерн 2b (DEVEXT)**: `__Notification_Save(code, 'msg', 'proc', DSMODULE_*_ID)` — 4 аргумента. ModuleID извлекается из 4-го аргумента (либо числовое значение, либо `DSMODULE_*_ID` константа).
- **Regex-прескрининг**: функция `HasReturnCodes(content string) bool` — быстрая проверка на наличие `ReturnCode_Insert`, `FCD_*_Notification_Save`, `_ADD_RETCODE_`, `__Notification_Save`, `M_*_RETCODE_INSERT`.
- **ModuleID**: `ret_code / 10000` (вычисляется автоматически, если не указан явно в 4-м аргументе).
- **Тесты**: `TestParse_ReturnCodeInsert`, `TestParse_AddRetCodeMacro`, `TestParse_NotificationSave_4args`, `TestParse_HasReturnCodes_Positive`, `TestParse_HasReturnCodes_Negative`.
- **Файлы**: `internal/parser/retcode/retcode.go` (~120 строк), `internal/parser/retcode/retcode_test.go` (~80 строк).

### Шаг 1.4: Интеграция retcode-парсера в indexer

- В `internal/indexer/indexer_sql_pas.go` в конце `parseSQLLikeFile`:
  - Прессрининг содержимого через `retcode.HasReturnCodes(content)`.
  - Если true — вызвать `retcode.Parse(content)`, заполнить `[]model.RetCodeEntry`, batch-insert в `ds_return_codes`.
- Добавить `BatchInsertRetCodes` в `internal/store/db_insert_sql.go`.
- Для Паттерна 1 с константой: lookup в `h_files_defines` по `define_name` (уже в БД после индексации H-файлов). Если найдено — взять `define_value` как `message`.
- **Тест**: `TestParseSQLFile_RetCodeIntegration` (на тестовом SQL с `ReturnCode_Insert`).
- **Файлы**: `internal/indexer/indexer_sql_pas.go` (+~30 строк), `internal/store/db_insert_sql.go` (+~40 строк), `internal/model/` (новый тип `RetCodeEntry`).

### Шаг 1.5: Модель данных `internal/rti/model.go`

- Типы: `RTICall`, `RTIParam`, `RTICheckpoint`, `RTISession` (как в предложении, строки 99–136).
- Дополнительный тип: `RTISummary` для вывода `summary`.
- **Файлы**: `internal/rti/model.go` (~70 строк).

### Шаг 1.6: Module ID map `internal/rti/symbols.go`

- Хардкод `moduleIDMap` (44 записи из предложения, строки 168–257).
- Функция `ModuleNameByID(id int) string`.
- **Файлы**: `internal/rti/symbols.go` (~80 строк).

### Шаг 1.7: Парсер RTI-лога `internal/rti/parser.go`

- `func ParseFile(path string) (*RTIParseResult, error)`:
  - Чтение первых 8KB → `encoding.DetectFromBytes` для определения кодировки.
  - Потоковое чтение через `bufio.Scanner` (увеличенный buffer для длинных строк).
  - Один проход, стек вызовов по `NestLevel` + `SPID`.
  - Regex для каждого типа строки (предложение, строки 85–95).
  - Возвращает `RTIParseResult { Calls []RTICall; Summary RTISummary; UnparsedLines int }`.
- **Особенности парсинга**:
  - Стек: `map[int][]*RTICall` по SPID. При `Enter` — push, при `Exit` — pop по имени процедуры.
  - Дерево: `parent_id` вычисляется по `NestLevel` — родитель это последний call на вершине стека того же SPID с `NestLevel-1`.
  - `RetVal` может быть до `Exit` строки — заполнять в текущий call на вершине стека.
  - Параметры и checkpoints — к текущему call на вершине стека.
- **Тесты**: `TestParseFile_SimpleEnterExit`, `TestParseFile_NestedCalls`, `TestParseFile_Params`, `TestParseFile_RetVal`, `TestParseFile_Checkpoint`, `TestParseFile_MultipleSPID`, `TestParseFile_UnparsedLines`.
- **Файлы**: `internal/rti/parser.go` (~250 строк), `internal/rti/parser_test.go` (~150 строк).

### Шаг 1.8: Дерево вызовов `internal/rti/tree.go`

- `func BuildTree(calls []RTICall, rootProcedure string, maxDepth int) *RTITreeNode`.
- `func FormatTree(node *RTITreeNode, enriched map[string]*ProcedureEnrichment) string` — compact text format.
- `RTITreeNode { Call *RTICall; Children []*RTITreeNode }`.
- Формат: `procedure [elapsed, RetVal] ← module (module_id)` (предложение, строки 391–401).
- **Файлы**: `internal/rti/tree.go` (~120 строк).

### Шаг 1.9: CLI `cmd/rti.go` + `cmd/rti_commands.go`

- `cmd/rti.go`: Cobra-команда `rtiCmd` с подкомандами. Persistent flags: `--file`, `--procedure`, `--depth`, `--threshold`, `--session`, `--json`, `--ndjson`, `--limit`.
- `cmd/rti_commands.go`:
  - `rtiSummaryCmd`: парсинг файла → вывод `RTISummary` (5–10 строк).
  - `rtiTreeCmd`: парсинг файла → `BuildTree` → `FormatTree`.
  - `rtiErrorsCmd`: парсинг файла → фильтр `RetVal != 0` → таблица.
  - `rtiSlowCmd`: парсинг файла → сортировка по `ElapsedMs DESC` → таблица (без enrichment в фазе 1).
- Вывод: `--json` / `--ndjson` через существующие helpers (`writeJSON`, `writeNDJSON`).
- Регистрация в `cmd/root.go`: `rootCmd.AddCommand(rtiCmd)`.
- Добавить `isRTIMode(args)` в `isMachineReadableMode` (для suppression banner).
- **Тесты**: `TestRTISummary`, `TestRTITree`, `TestRTIErrors`, `TestRTISlow` (на тестовом RTI-файле).
- **Файлы**: `cmd/rti.go` (~40 строк), `cmd/rti_commands.go` (~200 строк), `cmd/root.go` (+~3 строк).

### Шаг 1.10: Lookup ошибок в `ds_return_codes` (фаза 1 enrichment)

- В `internal/rti/symbols.go` добавить `LookupRetCode(db *store.DB, code int) (*RetCodeMeaning, error)`.
- Прямой SQL: `SELECT ret_code, message, proc_name, module_id FROM ds_return_codes WHERE ret_code = $1`.
- Используется в `errors` и `tree` для вывода `meaning` (если найдено).
- В фазе 1 подключение к БД опционально — если БД недоступна, выводить `(unknown)`.
- **Файлы**: `internal/rti/symbols.go` (+~30 строк).

### Критерии готовности Фазы 1

- `codebase rti summary --file откат.rti` выводит корректную сводку для лога 40 MB.
- `codebase rti tree --file откат.rti --procedure ConsSale_PurchPortfolio` выводит дерево.
- `codebase rti errors --file откат.rti` выводит все RetVal ≠ 0 с meaning (если есть в `ds_return_codes`).
- `codebase rti slow --file откат.rti --threshold 10000` выводит топ медленных вызовов.
- Парсинг 40 MB файла занимает <3 секунд.
- `--json` и `--ndjson` работают.
- `go test ./internal/rti/... ./internal/encoding/... ./internal/parser/retcode/...` проходит.
- `go build ./...` успешен.

---

## Фаза 2: Enrichment из CodeBase

### Шаг 2.1: `internal/rti/enrich.go`

- `func EnrichCalls(q *query.Query, calls []RTICall) map[string]*ProcedureEnrichment`:
  - Для каждой уникальной процедуры — `q.SearchSymbol(name, "procedure", false, 1)`.
  - Кэширование: один lookup на уникальное имя.
  - Заполнение `SourceFile`, `LineNumber`.
- `func EnrichProcedure(q *query.Query, procName string) (*ProcedureEnrichment, error)`:
  - `SearchSymbol` → source file.
  - `GetProcedure` → параметры (имя, тип, direction, default).
  - `LookupRetCode` → meaning для RetVal.
- Подключение через `querysvc.Execute` / `querysvc.ExecuteWith` (существующая инфраструктура).
- **Тесты**: `TestEnrichCalls_FoundInIndex`, `TestEnrichCalls_NotFound`, `TestEnrichProcedure_WithParams`.
- **Файлы**: `internal/rti/enrich.go` (~100 строк), `internal/rti/enrich_test.go` (~60 строк).

### Шаг 2.2: Подкоманда `details` (фаза 2)

- `rtiDetailsCmd`: парсинг файла → найти call по `--procedure` → enrichment → вывод (предложение, строки 446–459).
- Вывод: `Procedure`, `Source` (file:line), `Module`, `Calls in log`, `Enter/Exit`, `Elapsed`, `RetVal + meaning`, `Params`, `Parent`.
- При `--json` — полный enriched JSON.
- **Файлы**: `cmd/rti_commands.go` (+~60 строк).

### Шаг 2.3: Enrichment в `tree` и `errors`

- `tree`: добавить `← module_name (module_id)` и `source_file` (если найдено).
- `errors`: добавить колонки `MEANING`, `SOURCE_FILE`.
- **Файлы**: `internal/rti/tree.go` (+~20 строк), `cmd/rti_commands.go` (+~20 строк).

### Критерии готовности Фазы 2

- Для каждой процедуры в `tree` выводится `← module_name (module_id)`.
- Для каждой процедуры в `details` выводится путь к `.sql` файлу и параметры.
- Для RetVal ≠ 0 выводится имя константы и описание (если найдено в индексе).
- Procedure не найдена → `source_file: "(not found)"`, не падает.
- `go test ./internal/rti/...` проходит.

---

## Фаза 3: PostgreSQL store + list/delete/prune

### Шаг 3.1: Схема `rti_*` таблиц в `internal/store/db_schema.go`

- Добавить 5 таблиц: `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_errors` (предложение, строки 527–597).
- Добавить индексы (предложение, строки 590–596).
- Все с `ON DELETE CASCADE` для автоматического удаления дочерних записей.
- **Файлы**: `internal/store/db_schema.go` (+~70 строк).

### Шаг 3.2: `internal/rti/store.go` — запись в PostgreSQL

- `func SaveSession(db *store.DB, result *RTIParseResult, filePath string) (int64, error)`:
  1. INSERT в `rti_sessions` → `session_id`.
  2. Batch INSERT `rti_calls` (через `BatchInsertRTICalls`).
  3. Batch INSERT `rti_params` (через `BatchInsertRTIParams`).
  4. Batch INSERT `rti_checkpoints` (через `BatchInsertRTICheckpoints`).
  5. INSERT в `rti_errors` для calls с `RetVal != 0`.
- Batch insert через `COPY FROM` или `pq.Array` (как существующие `BatchInsert*` в `store`).
- **Файлы**: `internal/rti/store.go` (~150 строк), `internal/store/db_insert_rti.go` (~120 строк).

### Шаг 3.3: Подкоманда `parse`

- `rtiParseCmd`: парсинг файла → `SaveSession` → вывод `session_id`.
- При `--json`: `{"session_id": 42, "file": "откат.rti", "calls": 1247}`.
- **Файлы**: `cmd/rti_commands.go` (+~40 строк).

### Шаг 3.4: Подкоманда `list`

- `rtiListCmd`: `SELECT id, file_path, parsed_at, total_calls, errors_count, file_size_bytes FROM rti_sessions ORDER BY parsed_at DESC LIMIT $1`.
- Вывод: таблица `ID FILE PARSED_AT CALLS ERRORS SIZE`.
- При `--json`: массив объектов.
- **Файлы**: `cmd/rti_commands.go` (+~40 строк), `internal/rti/store.go` (+~20 строк).

### Шаг 3.5: Подкоманда `delete`

- `rtiDeleteCmd --session N`: `DELETE FROM rti_sessions WHERE id = $1` (CASCADE удаляет дочерние).
- Вывод: `Deleted session N (file: откат.rti, 1247 calls)`.
- **Файлы**: `cmd/rti_commands.go` (+~30 строк), `internal/rti/store.go` (+~10 строк).

### Шаг 3.6: Подкоманда `prune`

- `rtiPruneCmd --keep-last N` или `--older-than N`:
  - `--keep-last`: `DELETE FROM rti_sessions WHERE id NOT IN (SELECT id FROM rti_sessions ORDER BY parsed_at DESC LIMIT $1)`.
  - `--older-than`: `DELETE FROM rti_sessions WHERE parsed_at < NOW() - ($1 || ' days')::interval`.
- Вывод: кол-во удалённых сессий + список (предложение, строки 493–498).
- **Файлы**: `cmd/rti_commands.go` (+~50 строк), `internal/rti/store.go` (+~20 строк).

### Шаг 3.7: Запросы к сохранённому логу (`--session`)

- `summary --session N`, `tree --session N`, `errors --session N`, `slow --session N`, `details --session N`:
  - Если указан `--session` — данные из БД вместо парсинга файла.
  - `--file` становится опциональным при наличии `--session`.
  - Если ни `--file`, ни `--session` — ошибка.
  - Если `--session` не указан — auto (последняя сессия).
- **Файлы**: `cmd/rti_commands.go` (+~60 строк), `internal/rti/store.go` (+~80 строк).

### Критерии готовности Фазы 3

- `codebase rti parse --file откат.rti` сохраняет данные в PostgreSQL, возвращает `session_id`.
- `codebase rti list` показывает сохранённые сессии.
- `codebase rti delete --session 2` удаляет сессию и все связанные данные.
- `codebase rti prune --keep-last 5` оставляет 5 последних сессий.
- `codebase rti summary --session 1` работает без `--file`.
- `go test ./internal/rti/...` проходит.

---

## Фаза 4: MCP-инструменты

### Шаг 4.1: Регистрация MCP-инструментов

- В `internal/mcp/registry.go` добавить:
  - `codebase_rti_parse` — парсинг лога, возврат JSON с `session_id`.
  - `codebase_rti_summary` — сводка по логу (из файла или сессии).
  - `codebase_rti_tree` — дерево вызовов.
  - `codebase_rti_errors` — ошибки из лога.
- MCP-инструменты возвращают чистые доменные данные (без CLI envelope), как в существующих `codebase_query_*` инструментах.
- **Файлы**: `internal/mcp/registry.go` (+~60 строк).

### Критерии готовности Фазы 4

- MCP-клиент может вызвать `codebase_rti_parse` с путём к файлу и получить `session_id`.
- `codebase_rti_summary` / `tree` / `errors` работают через MCP.

---

## Сводка по файлам

### Новые файлы

| Файл | Фаза | Объём |
|------|------|-------|
| `internal/rti/model.go` | 1 | ~70 строк |
| `internal/rti/parser.go` | 1 | ~250 строк |
| `internal/rti/parser_test.go` | 1 | ~150 строк |
| `internal/rti/tree.go` | 1 | ~120 строк |
| `internal/rti/symbols.go` | 1 | ~110 строк |
| `internal/parser/retcode/retcode.go` | 1 | ~120 строк |
| `internal/parser/retcode/retcode_test.go` | 1 | ~80 строк |
| `cmd/rti.go` | 1 | ~40 строк |
| `cmd/rti_commands.go` | 1–3 | ~400 строк |
| `internal/rti/enrich.go` | 2 | ~100 строк |
| `internal/rti/enrich_test.go` | 2 | ~60 строк |
| `internal/rti/store.go` | 3 | ~200 строк |
| `internal/store/db_insert_rti.go` | 3 | ~120 строк |

### Изменяемые файлы

| Файл | Фаза | Изменение |
|------|------|-----------|
| `internal/encoding/encoding.go` | 1 | +`DetectFromBytes` (~30 строк) |
| `internal/review/review_helpers.go` | 1 | `detectFileEncoding` → делегирование в `encoding.DetectFromBytes` |
| `internal/store/db_schema.go` | 1+3 | +`ds_return_codes` (~10 строк), +`rti_*` (~70 строк) |
| `internal/store/db_insert_sql.go` | 1 | +`BatchInsertRetCodes` (~40 строк) |
| `internal/indexer/indexer_sql_pas.go` | 1 | +retcode-прескрининг и парсинг (~30 строк) |
| `internal/model/` | 1 | +тип `RetCodeEntry` (~10 строк) |
| `cmd/root.go` | 1 | +`rootCmd.AddCommand(rtiCmd)`, +`isRTIMode` (~5 строк) |
| `internal/mcp/registry.go` | 4 | +MCP-инструменты (~60 строк) |

### Итого

- **Фаза 1**: ~1100 строк (включая тесты)
- **Фаза 2**: ~200 строк
- **Фаза 3**: ~500 строк
- **Фаза 4**: ~60 строк
- **Всего**: ~1860 строк

---

## Риски и контроль

| Риск | Контроль |
|------|---------|
| Парсер не распознаёт нестандартные строки лога | Нераспознанные строки пропускаются, счётчик `unparsed_lines` в summary |
| Кодировка лога не определяется | `DetectFromBytes` читает первые 8KB; fallback — CP1251 |
| Procedure не найдена в индексе CodeBase | Выводить `source_file: "(not found)"`, не падать |
| Константа ошибки не найдена в индексе | Выводить `meaning: "(unknown)"`, не падать |
| Лог содержит несколько SPID (параллельные транзакции) | Стек вызовов разделяется по SPID |
| RetCode-парсер ложные срабатывания | Regex-прескрининг + точные паттерны (3 аргумента для макросов, 4 для Notification_Save) |
| БД растёт от накопления RTI-сессий | `list`/`delete`/`prune` + `ON DELETE CASCADE` + AUTOVACUUM |

---

## Порядок реализации

1. **Фаза 1** (MVP): шаги 1.1 → 1.10. После каждого шага — `go build ./...` и `go test`. В конце фазы — тест на реальном RTI-логе (`откат.rti`).
2. **Фаза 2** (Enrichment): шаги 2.1 → 2.3. Требует проиндексированную БД CodeBase.
3. **Фаза 3** (PostgreSQL store): шаги 3.1 → 3.7. Требует работающий `parse`.
4. **Фаза 4** (MCP): шаг 4.1. После стабилизации фаз 1–3.

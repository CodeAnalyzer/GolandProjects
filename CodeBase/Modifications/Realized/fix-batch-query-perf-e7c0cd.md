# Fix: Регрессия производительности batch-запросов init/update пайплайна

Устранить 2× замедление `codebase update`: исправить несоответствие индексов в batch WHERE-выражениях и вынести DELETE из параллельной зоны пост-обработки.

---

## Причины

| # | Проблема | Эффект |
|---|----------|--------|
| 1 | Batch-запросы используют `LOWER(TRIM(col))`, индексы — на `LOWER(col)` → PostgreSQL делает SeqScan | Для `sql_tables` (10M+ строк) — полный проход на каждый файл с фрагментами |
| 2 | `DeleteSubscribesToEventRelations` конкурирует с двумя параллельными `COPY INTO relations` (5 индексов) | Contention на index page locks во время пост-обработки |

---

## Шаг 1 — Фикс индексов: убрать TRIM из SQL WHERE (5 файлов)

Заменить `LOWER(TRIM(col))` → `LOWER(col)` в subquery SELECT и WHERE всех batch-методов.
После замены PostgreSQL использует существующие `idx_*_lower` индексы через Bitmap Index Scan.

Значения в `$1` уже нормализованы в Go-коде (`strings.ToLower(strings.TrimSpace(...))`),
поэтому семантика запросов не меняется.

### `internal/store/db_lookup_sql.go`

- `FindLatestSQLProcedureIDsByNames` — `LOWER(TRIM(proc_name))` → `LOWER(proc_name)` (SELECT + WHERE)
- `FindLatestSQLTableIDsByNames` — `LOWER(TRIM(table_name))` → `LOWER(table_name)` (SELECT + WHERE)

Используемые индексы после фикса:
- `idx_sql_procedures_proc_name_lower ON sql_procedures(LOWER(proc_name))`
- `idx_sql_tables_table_name_lower ON sql_tables(LOWER(table_name))`

### `internal/store/db_lookup_pas.go`

- `FindLatestPASClassIDsByNames` — `LOWER(TRIM(class_name))` → `LOWER(class_name)` (SELECT + WHERE)

### `internal/store/db_lookup_dfm.go`

- `FindLatestDFMFormIDsByClassNames` — `LOWER(TRIM(form_class))` → `LOWER(form_class)` (SELECT + WHERE)
- `FindLatestDFMComponentIDsByFormAndNames` — `LOWER(TRIM(component_name))` → `LOWER(component_name)` (SELECT + WHERE)

### `internal/store/api_store.go`

- `FindLatestAPIContractIDsByNamesAndKinds` — `LOWER(TRIM(contract_name/contract_kind))` → `LOWER(contract_name/contract_kind)` (SELECT + WHERE)
- `FindLatestEventContractIDsByNames` — `LOWER(TRIM(contract_name))` → `LOWER(contract_name)` (SELECT + WHERE)

---

## Шаг 2 — Фикс параллелизма: DELETE до горутин (2 файла)

### `internal/indexer/indexer_postprocess_callbacks.go`

Убрать вызов `DeleteSubscribesToEventRelations` (и его error-handling блок) из начала `postProcessCallbackEventRelations`.
Функция теперь только: загрузка callbacks → batch-resolve → build relations → saveRelations.

### `internal/indexer/runner.go`

В `runPostProcessingParallel` вызвать `idx.db.DeleteSubscribesToEventRelations()` **до** `wg.Add(3)`.
При ошибке — логировать и добавить `stats.Errors++` в collector, затем продолжить (как сейчас).

Результат: `COPY INTO relations` из трёх горутин больше не конкурирует с DELETE.

---

## Шаг 3 — Проверка

```
go build ./...
go test ./internal/store/... ./internal/indexer/...
```

---

## Затронутые файлы

| Файл | Изменений |
|------|-----------|
| `internal/store/db_lookup_sql.go` | 2 функции, ~4 строки |
| `internal/store/db_lookup_pas.go` | 1 функция, ~2 строки |
| `internal/store/db_lookup_dfm.go` | 2 функции, ~4 строки |
| `internal/store/api_store.go` | 2 функции, ~6 строк |
| `internal/indexer/indexer_postprocess_callbacks.go` | удалить DELETE-блок (~8 строк) |
| `internal/indexer/runner.go` | добавить DELETE + error handling (~8 строк) |

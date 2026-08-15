# Устранение N+1 DB-запросов в циклах review и indexer

Замена поочерёдных DB-запросов в циклах на однократную batch-загрузку с переиспользованием map/слайса.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `review_rules.go:2012-2023` `checkDatatypeExecParams` | `lookupProcedureParams(call.Name)` — QueryRow на каждый exec-вызов | N roundtrip'ов, где N = кол-во уникальных exec-вызовов в файле (до 50–100) |
| 2 | `review_rules.go:4656-4663` `checkExcessProcParams` | `lookupProcedureParams(call.Name)` — QueryRow на каждый exec-вызов | То же — N roundtrip'ов на файл |
| 3 | `review_rules.go:4690-4701` `checkDuplicateOutputVariable` | `lookupProcedureParams(call.Name)` — QueryRow на каждый exec-вызов | То же — N roundtrip'ов на файл |
| 4 | `review_rules.go:1556-1580` `checkForeignProcedures` | `lookupProcedureProductID(call.Name)` — QueryRow на каждый exec-вызов | N roundtrip'ов |
| 5 | `review_rules.go:1583-1594` `checkExecNotExistsProcedures` | `lookupProcedureProductID(call.Name)` — QueryRow на каждый exec-вызов | N roundtrip'ов |
| 6 | `review_rules.go:54-87` `checkForeignTables` | `lookupTableProductIDs(table.Name)` — до 3 Query на каждое имя таблицы | 3×N roundtrip'ов |
| 7 | `review_rules.go:1525-1552` `checkForeignPTables` | `lookupTableProductIDs(table.Name)` — до 3 Query на каждое имя таблицы | 3×N roundtrip'ов |
| 8 | `indexer.go:692-712` `saveJSFile` | `FindJSFunctionIDByFileAndLine(fileID, query.LineNumber)` — QueryRow на каждый query-call | N roundtrip'ов |
| 9 | `indexer_relations.go:155-178` `buildJSProcedureCallRelations` | `FindJSFunctionIDByFileAndLine(fileID, call.LineNumber)` — QueryRow на каждый call | N roundtrip'ов |

**Ключевое наблюдение:** правила 1–5 работают с одним и тем же `dedupeProcedureCalls(parsed.Calls)` — один и тот же список уникальных имён процедур запрашивается из БД до 5 раз (по разу в каждом правиле). Правила 6–7 аналогично для таблиц.

---

## Затронутые файлы

| Файл | Изменений |
|------|-----------|
| `internal/store/db_lookup_sql.go` | +2 функции: `BatchLookupProcedureParams`, `BatchLookupProcedureProductIDs` |
| `internal/store/db_lookup_sql.go` | +1 функция: `BatchLookupTableProductIDs` |
| `internal/store/db_lookup_j.go` | +1 функция: `FindJSFunctionIDRangesByFile` |
| `internal/review/review_lookup.go` | +3 batch-метода-обёртки на Runner |
| `internal/review/runner.go` | prewarm-вызовы перед правилами |
| `internal/review/review_rules.go` | 7 правил: замена циклового DB-вызова на map-lookup |
| `internal/indexer/indexer.go` | `saveJSFile`: замена циклового DB-вызова на локальный резолв |
| `internal/indexer/indexer_relations.go` | `buildJSProcedureCallRelations`: то же |
| `internal/review/runner_test.go` | unit-тесты на batch-методы |
| `internal/indexer/indexer_test.go` | unit-тест на `FindJSFunctionIDRangesByFile` (если есть) |

---

## План (5 шагов)

### Шаг 1: `BatchLookupProcedureParams` в store (высокий приоритет)

**Файл:** `internal/store/db_lookup_sql.go`

**Новая функция:**
```go
// BatchLookupProcedureParams возвращает map: LOWER(proc_name) -> []SQLParam
// для всех указанных имён процедур одним запросом.
func (db *DB) BatchLookupProcedureParams(names []string) (map[string][]model.SQLParam, error) {
    if len(names) == 0 {
        return map[string][]model.SQLParam{}, nil
    }
    rows, err := db.Query(`
        SELECT LOWER(proc_name), parameters
        FROM sql_procedures
        WHERE LOWER(proc_name) = ANY($1)
        ORDER BY id DESC
    `, pq.Array(names))
    // ...
    // Для каждой строки: json.Unmarshal(parameters) -> []model.SQLParam
    // В map оставляем только первую (последнюю по id) запись для каждого имени
}
```

**Используемый индекс:** `idx_sql_procedures_proc_name_lower ON sql_procedures(LOWER(proc_name))`

**Runner-обёртка** в `review_lookup.go`:
```go
func (r *Runner) batchLookupProcedureParams(names []string) (map[string][]model.SQLParam, error) {
    // нормализовать names (lower, trim), вызвать db.BatchLookupProcedureParams
}
```

**Проверка:**
```
go build ./internal/store/... ./internal/review/...
go test ./internal/store/... ./internal/review/...
```

---

### Шаг 2: `BatchLookupProcedureProductIDs` в store (средний приоритет)

**Файл:** `internal/store/db_lookup_sql.go`

**Новая функция:**
```go
// BatchLookupProcedureProductIDs возвращает map: LOWER(proc_name) -> ds_product_id
// для всех указанных имён процедур одним запросом.
func (db *DB) BatchLookupProcedureProductIDs(names []string) (map[string]int64, error) {
    if len(names) == 0 {
        return map[string]int64{}, nil
    }
    rows, err := db.Query(`
        SELECT LOWER(p.proc_name), f.ds_product_id
        FROM sql_procedures p
        JOIN files f ON f.id = p.file_id
        WHERE LOWER(p.proc_name) = ANY($1)
          AND f.ds_product_id IS NOT NULL
        ORDER BY p.id DESC
    `, pq.Array(names))
    // ... первая строка на имя (последняя по id) -> map
}
```

**Используемый индекс:** `idx_sql_procedures_proc_name_lower`

**Runner-обёртка** в `review_lookup.go` — делегирует в store.

---

### Шаг 3: `BatchLookupTableProductIDs` в store (средний приоритет)

**Файл:** `internal/store/db_lookup_sql.go`

**Новая функция:**
```go
// BatchLookupTableProductIDs возвращает map: LOWER(table_name) -> set of ds_product_id
// для всех указанных таблиц одним запросом (3 источника: sql_tables create/select_into,
// sql_column_definitions, sql_tables dfm_embedded).
func (db *DB) BatchLookupTableProductIDs(names []string) (map[string]map[int64]struct{}, error) {
    if len(names) == 0 {
        return map[string]map[int64]struct{}{}, nil
    }
    // 3 запроса (как сейчас в lookupTableProductIDs), но каждый с ANY($1) вместо скаляра
    // Источник 1: sql_tables WHERE context IN ('create','select_into')
    // Источник 2: sql_column_definitions WHERE definition_kind IN ('create','select_into')
    // Источник 3: sql_tables WHERE context = 'dfm_embedded' (только если источники 1+2 пусты для имени)
    // Результат: map[LOWER(table_name)] -> map[productID]struct{}
}
```

**Runner-обёртка** в `review_lookup.go` — делегирует в store.

---

### Шаг 4: Prewarm + замена циклов в review_rules.go (все 7 правил)

**Файлы:** `internal/review/runner.go`, `internal/review/review_rules.go`

#### 4a. Prewarm в runner.go

В методе, запускающем правила (перед `buildRuleTasks` или в начале `Run`), добавить:

```go
// Собрать уникальные имена процедур из parsed.Calls
procNames := collectUniqueProcNames(parsed)
if len(procNames) > 0 {
    r.procParamsCache = batchLookupProcedureParams(procNames)     // Шаг 1
    r.procProductIDCache = batchLookupProcedureProductIDs(procNames)  // Шаг 2
}

// Собрать уникальные имена таблиц из parsed.Tables
tableNames := collectUniqueTableNames(parsed)
if len(tableNames) > 0 {
    r.tableProductIDCache = batchLookupTableProductIDs(tableNames)    // Шаг 3
}
```

Добавить поля в `Runner`:
```go
procParamsCache     map[string][]model.SQLParam   // lower(proc_name) -> params
procProductIDCache  map[string]int64               // lower(proc_name) -> productID
tableProductIDCache map[string]map[int64]struct{}  // lower(table_name) -> productIDs
```

#### 4b. Замена в 7 правилах

| Правило | Было | Стало |
|---------|------|-------|
| `checkDatatypeExecParams` | `r.lookupProcedureParams(call.Name)` в цикле | `r.procParamsCache[lower(call.Name)]` |
| `checkExcessProcParams` | `r.lookupProcedureParams(call.Name)` в цикле | `r.procParamsCache[lower(call.Name)]` |
| `checkDuplicateOutputVariable` | `r.lookupProcedureParams(call.Name)` в цикле | `r.procParamsCache[lower(call.Name)]` |
| `checkForeignProcedures` | `r.lookupProcedureProductID(call.Name)` в цикле | `r.procProductIDCache[lower(call.Name)]` |
| `checkExecNotExistsProcedures` | `r.lookupProcedureProductID(call.Name)` в цикле | `r.procProductIDCache[lower(call.Name)]` |
| `checkForeignTables` | `r.lookupTableProductIDs(table.Name)` в цикле | `r.tableProductIDCache[lower(table.Name)]` |
| `checkForeignPTables` | `r.lookupTableProductIDs(table.Name)` в цикле | `r.tableProductIDCache[lower(table.Name)]` |

**Совместимость:** если `r.procParamsCache == nil` (DB недоступен или prewarm не выполнен), правила должны fallback на старый путь (вызов `lookupProcedureParams`). Это сохраняет работоспособность в unit-тестах без БД.

**Проверка:**
```
go build ./...
go test ./internal/review/...
```

---

### Шаг 5: `FindJSFunctionIDRangesByFile` в store + замена в indexer (средний приоритет)

**Файл:** `internal/store/db_lookup_j.go`

**Новая функция:**
```go
type JSFuncRange struct {
    ID        int64
    LineStart int
    LineEnd   int
}

// FindJSFunctionIDRangesByFile возвращает все JS-функции файла с их line-диапазонами.
// Один запрос вместо N запросов FindJSFunctionIDByFileAndLine.
func (db *DB) FindJSFunctionIDRangesByFile(fileID int64) ([]JSFuncRange, error) {
    rows, err := db.Query(`
        SELECT id, line_start, line_end
        FROM js_functions
        WHERE file_id = $1
        ORDER BY line_start DESC, id DESC
    `, fileID)
    // ... scan в []JSFuncRange
}
```

**Локальный резолв** (вместо DB-запроса в цикле):
```go
func resolveJSFunctionByLine(ranges []JSFuncRange, lineNumber int) (int64, error) {
    for _, r := range ranges {
        if lineNumber >= r.LineStart && lineNumber <= r.LineEnd {
            return r.ID, nil
        }
    }
    return 0, dbsql.ErrNoRows
}
```

#### 5a. indexer.go:692-712 — saveJSFile

```go
// До цикла:
funcRanges, err := idx.db.FindJSFunctionIDRangesByFile(fileID)
if err != nil {
    return fmt.Errorf("failed to load JS function ranges: %w", err)
}

// В цикле:
functionID, err := resolveJSFunctionByLine(funcRanges, query.LineNumber)
```

#### 5b. indexer_relations.go:155-178 — buildJSProcedureCallRelations

```go
// До цикла:
funcRanges, err := idx.db.FindJSFunctionIDRangesByFile(fileID)
if err != nil {
    return nil, err
}

// В цикле:
sourceID, err := resolveJSFunctionByLine(funcRanges, call.LineNumber)
```

**Проверка:**
```
go build ./...
go test ./internal/indexer/... ./internal/store/...
```

---

## Порядок выполнения

1. [ ] Шаг 1: `BatchLookupProcedureParams` в store + Runner-обёртка
2. [ ] Шаг 2: `BatchLookupProcedureProductIDs` в store + Runner-обёртка
3. [ ] Шаг 3: `BatchLookupTableProductIDs` в store + Runner-обёртка
4. [ ] Шаг 4: Prewarm в runner.go + замена циклов в 7 правилах review_rules.go
5. [ ] Шаг 5: `FindJSFunctionIDRangesByFile` + замена в indexer.go и indexer_relations.go
6. [ ] Финальная сборка и тесты: `go build ./... && go test ./internal/...`

---

## Ожидаемый эффект

| Шаг | Участок | Roundtrip'ов до | Roundtrip'ов после | Ускорение |
|-----|---------|-----------------|-------------------|-----------|
| 1–4 | `lookupProcedureParams` × 3 правила | 3×N | 1 (prewarm) | ~3N→1 |
| 2–4 | `lookupProcedureProductID` × 2 правила | 2×N | 1 (prewarm) | ~2N→1 |
| 3–4 | `lookupTableProductIDs` × 2 правила | 6×N (3×2) | 3 (prewarm) | ~6N→3 |
| 5 | `FindJSFunctionIDByFileAndLine` × 2 места | 2×N | 2 (по 1 на файл) | ~2N→2 |

**Суммарно:** для файла с 30 уникальными exec-вызовами и 10 таблицами — ~270 DB-roundtrip'ов → ~7 (1+1+3+2), ускорение ~38×.

---

## Риски и обратная совместимость

- **Fallback:** если prewarm не выполнен (cache == nil), правила используют старый путь — unit-тесты без БД продолжают работать
- **Идемпотентность:** batch-запросы используют `ORDER BY id DESC` + first-wins — та же семантика, что и скалярные `ORDER BY id DESC LIMIT 1`
- **Индексы:** все batch-запросы используют существующие `idx_*_lower` индексы через `ANY($1)` — без новых индексов

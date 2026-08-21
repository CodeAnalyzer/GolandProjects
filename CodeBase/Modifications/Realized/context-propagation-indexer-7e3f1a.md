# Распространение context.Context через indexer pipeline

`processFileCtx` получает `ctx` и открывает транзакцию через `BeginTx(ctx, nil)`, но ~130 внутренних DB-вызовов выполняются с `context.Background()`. При отмене (Ctrl+C / SIGTERM) транзакция абортится, но индивидуальные запросы продолжают выполняться — секунды бесполезной работы на крупных файлах. Post-processing и `resolveDSProductID` также полностью отвязаны от `ctx`.

---

## Контекст проблемы

| # | Участок | Текущее поведение | Влияние |
|---|---------|-------------------|---------|
| 1 | `processFileInTx` → `parse*File` | `ctx` не передаётся; все `idx.db.*` вызовы используют `context.Background()` | При отмене запросы продолжают выполняться; транзакция абортится, но воркер блокируется на I/O |
| 2 | `resolveDSProductID` | `context.Background()` в `GetOrCreateDSProductIDByName` | INSERT/SELECT продолжается после Ctrl+C |
| 3 | `saveIncludeDirective` | `context.Background()` в `FindLatestFileIDByPaths` и `Exec` | То же |
| 4 | `saveJSConstantSymbols` | `context.Background()` в 4 DB-вызовах | То же |
| 5 | `buildQueryFragmentRelations` и др. | `context.Background()` в 7 вызовах | То же |
| 6 | `postProcess*` (5 файлов, 17 вызовов) | `context.Background()` во всех DB-запросах | Post-processing не отменяется по ctx (частично смягчено фиксом #1: пропуск при `ctx.Err() != nil`, но уже запущенные запросы не прерываются) |

**Текущий путь контекста:**
```
processFileCtx(ctx) → WithBatchTxCtx(ctx) → BeginTx(ctx)
  → processFileInTx(file, fileID, stats)        ← ctx НЕ передаётся
    → parseSQLFile(file, fileID, stats)          ← ctx НЕ передаётся
      → idx.db.BatchInsert*(context.Background()) ← ctx потерян
```

---

## Затронутые файлы

- `internal/indexer/indexer.go` — `processFileInTx`, все `parse*File`, `resolveDSProductID`, `saveIncludeDirective`, `saveJSConstantSymbols`, `saveFileCtx`
- `internal/indexer/indexer_sql_pas.go` — `parseSQLFile`, `parsePASFile`, `buildT01GeneratedSubscriberRelations`, `resolveColumnTypes`
- `internal/indexer/indexer_relations.go` — `buildQueryFragmentRelations`, `buildReportFieldUsageRelations`, `buildReportParamUsageRelations`, `buildVBFunctionQueryRelations`, `saveRelationsAndCollectJSCallRefs`
- `internal/indexer/indexer_postprocess_callbacks.go` — `postProcessCallbackEventRelations`
- `internal/indexer/indexer_postprocess_fragments.go` — `postProcessFragmentRelations`, `postProcessJSCallRelations`, `postProcessT01SubscriberRelations`, `postProcessAPIMacroRelations`, `postProcessAllFragmentRelations`
- `internal/indexer/indexer_postprocess_pas.go` — `postProcessPASPending`
- `internal/indexer/indexer_postprocess_sql_calls.go` — `postProcessSQLProcedureCallRelations`
- `internal/indexer/indexer_postprocess_retcode.go` — `postProcessRetCodeConstants`
- `internal/indexer/runner.go` — `runPostProcessingParallel` (передача ctx в postProcess*)

---

## План (4 шага)

### Шаг 1: processFileInTx + parse*File + вспомогательные (indexer.go)

**Принцип:** добавить `ctx context.Context` первым параметром во все внутренние функции, заменить `context.Background()` → `ctx`.

**Изменение сигнатур:**

| Функция | Было | Стало |
|---|---|---|
| `processFileInTx` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseHFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseDFMFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseJSFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseTPRFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseRPTFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseSMFFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseXMLFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `parseT01File` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `saveIncludeDirective` | `(fileID, path, includePath, lineNum)` | `(ctx, fileID, path, includePath, lineNum)` |
| `saveJSConstantSymbols` | `(fileID, constants)` | `(ctx, fileID, constants)` |
| `resolveDSProductID` | `(productName)` | `(ctx, productName)` |

**Точка входа** — `processFileCtx` (без изменений сигнатуры, уже принимает `ctx`):
```go
func (idx *Indexer) processFileCtx(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
    return idx.db.WithBatchTxCtx(ctx, func(txdb *store.DB) error {
        fileIdx := &Indexer{...}
        return fileIdx.processFileInTx(ctx, file, fileID, stats)  // +ctx
    })
}
```

**`processFileInTx`** — передаёт `ctx` во все `parse*File`:
```go
func (idx *Indexer) processFileInTx(ctx context.Context, file fswalk.FileInfo, fileID int64, stats *model.ScanStats) error {
    switch strings.ToUpper(file.Language) {
    case "SQL":
        if err := idx.parseSQLFile(ctx, file, fileID, stats); err != nil { ... }
    case "PAS":
        if err := idx.parsePASFile(ctx, file, fileID, stats); err != nil { ... }
    // ... все остальные
    }
}
```

**Внутри `parseHFile`** (и аналогично во всех остальных) — заменить:
```go
// Было:
idx.db.BatchInsertHDefines(context.Background(), ...)
idx.db.FindHDefineIDsByFile(context.Background(), ...)
idx.db.BatchInsertSymbols(context.Background(), ...)
idx.saveIncludeDirective(fileID, path, inc.IncludePath, inc.LineNumber)

// Стало:
idx.db.BatchInsertHDefines(ctx, ...)
idx.db.FindHDefineIDsByFile(ctx, ...)
idx.db.BatchInsertSymbols(ctx, ...)
idx.saveIncludeDirective(ctx, fileID, path, inc.IncludePath, inc.LineNumber)
```

**`resolveDSProductID`** — вызывается из `saveFileCtx` (которая уже принимает `ctx`):
```go
// Было:
func (idx *Indexer) resolveDSProductID(productName string) (int64, error) {
    ...
    productID, err := idx.db.GetOrCreateDSProductIDByName(context.Background(), productName)

// Стало:
func (idx *Indexer) resolveDSProductID(ctx context.Context, productName string) (int64, error) {
    ...
    productID, err := idx.db.GetOrCreateDSProductIDByName(ctx, productName)
```

**Количество замен в indexer.go:** ~75 `context.Background()` → `ctx`, 12 сигнатур.

**Проверка:**
```
go build ./internal/indexer/...
go test ./internal/indexer/... -count=1
```

---

### Шаг 2: parseSQLFile + parsePASFile + relations (indexer_sql_pas.go, indexer_relations.go)

**Изменение сигнатур:**

| Файл | Функция | Было | Стало |
|---|---|---|---|
| `indexer_sql_pas.go` | `parseSQLFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `indexer_sql_pas.go` | `parsePASFile` | `(file, fileID, stats)` | `(ctx, file, fileID, stats)` |
| `indexer_sql_pas.go` | `buildT01GeneratedSubscriberRelations` | `(content, fileID, procedures, calls)` | `(ctx, content, fileID, procedures, calls)` |
| `indexer_sql_pas.go` | `resolveColumnTypes` | `(fileID, fragments)` | `(ctx, fileID, fragments)` |
| `indexer_relations.go` | `buildQueryFragmentRelations` | `(fileID, fragments)` | `(ctx, fileID, fragments)` |
| `indexer_relations.go` | `buildReportFieldUsageRelations` | `(fileID, reportFormID, fields, params)` | `(ctx, fileID, reportFormID, fields, params)` |
| `indexer_relations.go` | `buildReportParamUsageRelations` | `(fileID, reportFormID, fragments, params)` | `(ctx, fileID, reportFormID, fragments, params)` |
| `indexer_relations.go` | `buildVBFunctionQueryRelations` | `(fileID, reportFormID, functions, fragments)` | `(ctx, fileID, reportFormID, functions, fragments)` |
| `indexer_relations.go` | `saveRelationsAndCollectJSCallRefs` | `(path, fileID, fragments, stats)` | `(ctx, path, fileID, fragments, stats)` |

**Количество замен:** ~30 в `indexer_sql_pas.go`, ~7 в `indexer_relations.go`.

**Проверка:**
```
go build ./internal/indexer/...
go test ./internal/indexer/... -count=1
```

---

### Шаг 3: Post-processing (5 файлов)

**Изменение сигнатур:**

| Файл | Функция | Было | Стало |
|---|---|---|---|
| `indexer_postprocess_callbacks.go` | `postProcessCallbackEventRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_fragments.go` | `postProcessFragmentRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_fragments.go` | `postProcessJSCallRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_fragments.go` | `postProcessT01SubscriberRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_fragments.go` | `postProcessAPIMacroRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_fragments.go` | `postProcessAllFragmentRelations` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_pas.go` | `postProcessPASPending` | `(collector)` | `(ctx, collector)` |
| `indexer_postprocess_sql_calls.go` | `postProcessSQLProcedureCallRelations` | `(collector, parallel)` | `(ctx, collector, parallel)` |
| `indexer_postprocess_retcode.go` | `postProcessRetCodeConstants` | `(collector)` | `(ctx, collector)` |

**Вызов в `runPostProcessingParallel`** (runner.go):
```go
// Было:
go func() { defer wg.Done(); idx.postProcessPASPending(collector) }()
go func() { defer wg.Done(); idx.postProcessSQLProcedureCallRelations(collector, parallel) }()
// ...

// Стало:
go func() { defer wg.Done(); idx.postProcessPASPending(ctx, collector) }()
go func() { defer wg.Done(); idx.postProcessSQLProcedureCallRelations(ctx, collector, parallel) }()
// ...
```

**Количество замен:** 17 `context.Background()` → `ctx`, 9 сигнатур.

**Проверка:**
```
go build ./...
go test ./internal/indexer/... -count=1
```

---

### Шаг 4: Сборка + полные тесты

```
go build ./...
go vet ./internal/indexer/...
go test ./... -tags=integration -count=1
```

---

## Порядок выполнения

1. **Шаг 1** (indexer.go) → сборка + тесты indexer
2. **Шаг 2** (indexer_sql_pas.go, indexer_relations.go) → сборка + тесты indexer
3. **Шаг 3** (5 post-process файлов + runner.go) → сборка + тесты indexer
4. **Шаг 4** (полная сборка + все тесты)

Каждый шаг компилируется independently. Шаги 1-3 можно коммитить раздельно.

---

## Ожидаемый результат

| Сценарий | Было | Стало |
|---|---|---|
| Ctrl+C при обработке SQL 20K строк | Транзакция абортится, но ~30 DB-запросов продолжаются до естественного завершения (секунды) | Все DB-запросы получают `ctx.Done()`, отменяются немедленно |
| Ctrl+C при post-processing | 17 запросов с `context.Background()` продолжаются | Все запросы отменяются по `ctx` (дополнительно к guard `ctx.Err() == nil` из фикса #1) |
| `resolveDSProductID` при отмене | INSERT продолжает выполняться | INSERT отменяется по `ctx` |
| Нормальная работа (без отмены) | Без изменений | Без изменений (`ctx` не отменён → запросы выполняются как раньше) |

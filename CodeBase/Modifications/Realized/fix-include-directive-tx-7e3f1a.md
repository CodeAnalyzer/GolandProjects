# Fix: include-директивы записываются вне транзакции файла

Исправить находку код-ревью: `saveIncludeDirective` вызывает `idx.db.Exec(...)` — унаследованный метод встроенного `*sql.DB`, минуя `boundTx`. INSERT выполняется вне транзакции файла.

---

## Контекст

| # | Проблема | Серьёзность | Файл | Строки |
|---|---------|-------------|------|--------|
| 1 | `saveIncludeDirective` пишет через `idx.db.Exec(...)` (встроенный `*sql.DB.Exec`) вместо `execCtx` — INSERT вне `boundTx` | Высокая | `internal/indexer/indexer.go` | 1632-1635 |
| 2 | Ошибка записи include логируется, но не возвращается — файл коммитится с частичными/битыми include-директивами | Средняя | `internal/indexer/indexer.go` | 499-502, 911-914, 1140-1143 |
| 3 | Аналогично в `indexer_sql_pas.go` | Средняя | `internal/indexer/indexer_sql_pas.go` | 396-399 |

### Корневая причина

`store.DB` (`internal/store/db.go:15-22`) встраивает `*sql.DB` и имеет поле `boundTx *sql.Tx`. Метод `execCtx` (`internal/store/db_tx.go:52-58`) корректно маршрутизирует запрос в `boundTx` если он задан. Однако `execCtx` — **unexported**, недоступен из пакета `indexer`.

`saveIncludeDirective` вызывает `idx.db.Exec(...)` — это разрешается во встроенный `*sql.DB.Exec`, который **не знает** про `boundTx` и выполняется в автокоммите (отдельное соединение из пула).

SELECT-часть той же функции (`FindLatestFileIDByPaths`, `FindLatestHFileIDByNameLike`) корректно идёт через `QueryRowContext` → `boundTx`.

### Последствия

1. **При rollback файла** — include-директива остаётся в БД (orphaned row с `file_id`, которого нет)
2. **При FK-проверке** — `file_id` ещё не закоммичен, INSERT вне tx может получить FK violation
3. **Несогласованность** — SELECT видит незафиксированные данные из tx, а INSERT пишет мимо неё

---

## Шаг 1 — Экспортировать `execCtx` как `ExecContext` (1 файл)

### `internal/store/db_tx.go`

Строки 52-58 — переименовать `execCtx` → `ExecContext` и обновить все вызовы внутри пакета `store`.

Текущий код:
```go
func (db *DB) execCtx(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.boundTx != nil {
		return db.boundTx.ExecContext(ctx, query, args...)
	}
	return db.DB.ExecContext(ctx, query, args...)
}
```

Новый код:
```go
// ExecContext выполняет запрос в boundTx, если она задана, иначе напрямую.
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.boundTx != nil {
		return db.boundTx.ExecContext(ctx, query, args...)
	}
	return db.DB.ExecContext(ctx, query, args...)
}
```

Обновить все вызовы `execCtx` → `ExecContext` внутри пакета `store` (15+ мест: `db_files.go`, `db_schema.go`, `db_lookup_pas.go`, `db_insert_retcode.go`, `db_resolve_retcode.go`, `db_scan_runs.go`, `api_store.go`).

---

## Шаг 2 — Заменить `idx.db.Exec` → `idx.db.ExecContext` (1 файл)

### `internal/indexer/indexer.go`

Строки 1632-1635 — заменить вызов:

Было:
```go
_, err := idx.db.Exec(`
	INSERT INTO include_directives (file_id, include_path, resolved_file_id, line_number)
	VALUES ($1, $2, $3, $4)
`, fileID, includePath, resolvedFileID, lineNum)
```

Стало:
```go
_, err := idx.db.ExecContext(ctx, `
	INSERT INTO include_directives (file_id, include_path, resolved_file_id, line_number)
	VALUES ($1, $2, $3, $4)
`, fileID, includePath, resolvedFileID, lineNum)
```

---

## Шаг 3 — Возвращать ошибку include вместо глушения (2 файла)

Сейчас на всех 4 call-сайтах ошибка логируется и инкрементирует `stats.Errors++`, но **не возвращается** — обработка файла продолжается, и транзакция коммитится с частичными данными.

### `internal/indexer/indexer.go` — 3 call-сайта

**Строка 498-503** (parseSQLFile):
```go
for _, inc := range result.Includes {
	if err := idx.saveIncludeDirective(ctx, fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
		return fmt.Errorf("failed to save include directive %s: %w", inc.IncludePath, err)
	}
}
```

**Строка 910-915** (parseTPRFile):
```go
for _, inc := range result.Includes {
	if err := idx.saveIncludeDirective(ctx, fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
		return fmt.Errorf("failed to save include directive %s: %w", inc.IncludePath, err)
	}
}
```

**Строка 1139-1144** (parseJSFile):
```go
for _, inc := range result.Includes {
	if err := idx.saveIncludeDirective(ctx, fileID, path, inc, 1); err != nil {
		return fmt.Errorf("failed to save include directive %s: %w", inc, err)
	}
}
```

### `internal/indexer/indexer_sql_pas.go` — 1 call-сайт

**Строка 395-400** (parsePASFile):
```go
for _, inc := range result.Includes {
	if err := idx.saveIncludeDirective(ctx, fileID, path, inc.IncludePath, inc.LineNumber); err != nil {
		return fmt.Errorf("failed to save include directive %s: %w", inc.IncludePath, err)
	}
}
```

---

## Шаг 4 — Тесты (1 файл)

### `internal/store/db_tx_test.go`

Добавить unit-тест для `ExecContext`:

- `TestExecContext_NoBoundTx` — без `boundTx` выполняется через `*sql.DB.ExecContext` (проверка что метод работает)
- `TestExecContext_WithBoundTx` — с `boundTx` выполняется в транзакции (вставка + rollback → данных нет)

### `internal/indexer/indexer_test.go` (если существует) или через существующие тесты

Убедиться, что существующие тесты индексатора проходят с новым поведением (ошибка include → rollback файла).

---

## Проверка

```
go build ./...
go vet ./internal/store/... ./internal/indexer/...
go test ./internal/store/... ./internal/indexer/... -count=1
```

---

## Затронутые файлы

| Файл | Изменений |
|------|-----------|
| `internal/store/db_tx.go` | `execCtx` → `ExecContext` (переименование, 1 строка) |
| `internal/store/*.go` | `execCtx` → `ExecContext` во всех вызовах (15+ мест, механический rename) |
| `internal/indexer/indexer.go` | `idx.db.Exec` → `idx.db.ExecContext` (1 строка) + 3 call-сайта: возврат ошибки вместо глушения (~6 строк) |
| `internal/indexer/indexer_sql_pas.go` | 1 call-сайт: возврат ошибки вместо глушения (~2 строки) |
| `internal/store/db_tx_test.go` | ~2 теста для `ExecContext` (~30 строк) |

---

## Откат

Все изменения изолированы и обратимы:
- Шаг 1: переименовать `ExecContext` обратно в `execCtx` (но это reintroduces баг — не рекомендуется)
- Шаг 2: вернуть `idx.db.Exec` (но это reintroduces запись вне tx — не рекомендуется)
- Шаг 3: вернуть `logError` + `stats.Errors++` вместо `return err` (но это reintroduces частичный коммит — не рекомендуется)

---

## Ожидаемый результат

| Проблема | Было | Стало |
|----------|------|-------|
| INSERT вне транзакции | `idx.db.Exec(...)` → автокоммит, минуя `boundTx` | `idx.db.ExecContext(ctx, ...)` → выполняется в `boundTx` |
| Ошибка include глушится | Логируется, файл коммитится с битыми данными | Ошибка возвращается → rollback файла |
| Orphaned rows при rollback | Include-директива остаётся в БД | Откатывается вместе с файлом |

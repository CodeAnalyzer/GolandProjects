# Fix: Гонка данных в review.Runner, утечка соединения в store.NewDB, DSN без экранирования

Исправить 3 находки код-ревью: 2 критичных (гонка данных, утечка соединения) и 1 существенную (DSN-инъекция).

---

## Контекст

| # | Проблема | Серьёзность | Файл | Строки |
|---|---------|-------------|------|--------|
| 1 | Гонка данных в `fileProcessedContent` — параллельная запись в `r.exec.macroResult` без синхронизации | Критичная | `internal/review/runner.go` | 671-685 |
| 2 | Утечка `*sql.DB` при ошибке `PingContext` в `NewDB` — `db.Close()` отсутствует на error-ветке | Критичная | `internal/store/db.go` | 105-115 |
| 3 | DSN собирается `fmt.Sprintf` без экранирования значений — пароль с пробелом/кавычкой ломает подключение, возможна инъекция параметров | Существенная | `internal/store/db.go` | 76-79, 100-103 |

---

## Шаг 1 — Фикс гонки в `fileProcessedContent` (1 файл)

### Проблема

`runRuleTasks` (`runner.go:470`) запускает до `maxWorkers` горутин. 12 из 13 правил вызывают `r.fileProcessedContent(path)` (`review_rules.go`, 12 вызовов). Метод обращается к `r.exec.macroResult` без блокировки. Если `len(r.exec.macroResult.SourceMap) == 0` (fallback-ветка, строки 674-676), несколько горутин одновременно вызывают `replaceMacros` и пишут результат в `r.exec.macroResult` — гонка read-modify-write.

### Решение

`macroResult` уже гарантированно вычислен в `RunSQLFile` (`runner.go:104`) и сохранён в `r.exec` (`runner.go:117`). Fallback-ветка — мёртвый код в нормальном пути исполнения. Убрать её, сделать метод read-only.

### `internal/review/runner.go`

Строки 671-685 — заменить тело `fileProcessedContent`:

```go
func (r *Runner) fileProcessedContent(path string) (macroReplaceResult, error) {
	if r.exec != nil && normalizePath(path) == r.exec.filePath {
		return r.exec.macroResult, nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return macroReplaceResult{}, err
	}
	return replaceMacros(string(content)), nil
}
```

Убираются строки 672-679 (многоуровневый if + fallback `replaceMacros`), заменяются на одно условие. Метод становится read-only — никаких гонок.

---

## Шаг 2 — Фикс утечки соединения в `NewDB` (1 файл)

### Проблема

`db.go:105-115`: `sql.Open` создаёт пул соединений. Если `PingContext` (строка 113) падает, `db` не закрывается. Для сравнения, `dbDefault` (строка 85) корректно закрыт через `defer dbDefault.Close()`.

### Решение

### `internal/store/db.go`

Строки 113-115 — добавить `db.Close()` перед `return` на error-ветке:

```go
if err := db.PingContext(pingCtx2); err != nil {
	db.Close()
	return nil, fmt.Errorf("failed to ping database: %w", err)
}
```

---

## Шаг 3 — Фикс DSN: экранирование значений (1 файл)

### Проблема

`db.go:76-79` и `:100-103`: DSN собирается через `fmt.Sprintf("host=%s ... password=%s ...", ...)`. Значения не экранируются. Формат libpq `key=value` уязвим:
- Пароль с пробелом: `password=my secret` → libpq разбирает как `password=my` + лишний токен `secret`
- Пароль с одинарной кавычкой: `password=it's` → обрыв значения
- Инъекция: `password=x sslmode=disable` → подмена параметра DSN

### Решение

В libpq формате `key=value` значения экранируются одинарными кавычками: `password='my secret'`. Одинарная кавычка внутри значения удваивается: `'it''s'`. Это документированное поведение libpq (https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING).

### `internal/store/db.go`

Добавить helper-функцию `quoteDSNValue` (перед `NewDB`):

```go
// quoteDSNValue экранирует значение для libpq keyword/value DSN.
// Значения, содержащие пробелы, кавычки или спецсимволы, оборачиваются
// в одинарные кавычки; одинарная кавычка внутри удваивается.
func quoteDSNValue(s string) string {
	if s == "" {
		return "''"
	}
	needsQuoting := false
	for _, r := range s {
		if r == ' ' || r == '\'' || r == '\\' || r == '\t' || r == '\n' {
			needsQuoting = true
			break
		}
	}
	if !needsQuoting {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
```

Заменить сборку DSN (строки 76-79):

```go
dsnDefault := fmt.Sprintf(
	"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s connect_timeout=%d",
	quoteDSNValue(cfg.Host), cfg.Port, quoteDSNValue(cfg.User),
	quoteDSNValue(cfg.Password), quoteDSNValue(cfg.SSLMode), cfg.ConnectTimeout,
)
```

Заменить сборку DSN (строки 100-103):

```go
dsn := fmt.Sprintf(
	"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
	quoteDSNValue(cfg.Host), cfg.Port, quoteDSNValue(cfg.User),
	quoteDSNValue(cfg.Password), quoteDSNValue(cfg.Database),
	quoteDSNValue(cfg.SSLMode), cfg.ConnectTimeout,
)
```

Добавить import `"strings"` (если отсутствует).

**Примечание:** `port` и `connect_timeout` — числовые поля, экранирование не требуется. `host` экранируется на случай IPv6-адреса с двоеточием или пути Unix-сокета с пробелами.

---

## Шаг 4 — Тесты (2 файла)

### `internal/store/db_test.go`

Добавить unit-тест для `quoteDSNValue`:

- `TestQuoteDSNValue_Simple` — `"localhost"` → `"localhost"` (без кавычек)
- `TestQuoteDSNValue_WithSpace` — `"my secret"` → `"'my secret'"`
- `TestQuoteDSNValue_WithQuote` — `"it's"` → `"'it''s'"`
- `TestQuoteDSNValue_Empty` — `""` → `"''"`
- `TestQuoteDSNValue_WithBackslash` — `"a\\b"` → `"'a\\b'"`
- `TestQuoteDSNValue_InjectionAttempt` — `"x sslmode=disable"` → `"'x sslmode=disable'"` (инъекция нейтрализована)

### `internal/review/runner_test.go`

Добавить тест на thread-safety `fileProcessedContent` (опционально, т.к. фикс убирает запись — гонка невозможна по построению):

- `TestFileProcessedContent_ReadOnly` — проверить, что метод не модифицирует `r.exec.macroResult` при пустом `SourceMap`.

---

## Проверка

```
go build ./...
go vet ./internal/store/... ./internal/review/...
go test ./internal/store/... ./internal/review/... -count=1
```

---

## Затронутые файлы

| Файл | Изменений |
|------|-----------|
| `internal/review/runner.go` | `fileProcessedContent`: убрать fallback-ветку (~4 строки удалено) |
| `internal/store/db.go` | `db.Close()` на error-ветке (1 строка) + `quoteDSNValue` (~15 строк) + замена 2 `fmt.Sprintf` (~6 строк) + import `strings` |
| `internal/store/db_test.go` | ~6 тестов для `quoteDSNValue` (~40 строк) |
| `internal/review/runner_test.go` | ~1 тест на read-only (~15 строк) |

---

## Откат

Все изменения изолированы и обратимы:
- Шаг 1: возврат fallback-ветки (но это reintroduces гонку — не рекомендуется)
- Шаг 2: убрать `db.Close()` (но это reintroduces утечку — не рекомендуется)
- Шаг 3: вернуть `fmt.Sprintf` без `quoteDSNValue` (но это reintroduces DSN-уязвимость — не рекомендуется)

---

## Ожидаемый результат

| Проблема | Было | Стало |
|----------|------|-------|
| Гонка в `fileProcessedContent` | Латентная гонка при пустом `SourceMap` | Метод read-only, гонка невозможна |
| Утечка `*sql.DB` | `db` не закрывается при ошибке ping | `db.Close()` на error-ветке |
| DSN-инъекция | Пароль с пробелом/кавычкой ломает подключение | Значения экранируются одинарными кавычками |

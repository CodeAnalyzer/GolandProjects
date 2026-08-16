# Рефакторинг обработки ошибок: sentinel errors + errors.Is

Замена string-based классификации ошибок на sentinel errors с `errors.Is`. Устраняет хрупкость `strings.Contains(err.Error(), ...)` — теперь классификация основана на типах ошибок, а не на тексте сообщений.

## Контекст проблемы

В CodeBase было два паттерна string-based обработки ошибок:

1. **Управление потоком** в `internal/querysvc/inspect.go` — `strings.Contains(err.Error(), "at least one relation filter must be provided")` для пропуска ошибки "нет фильтров". Ломается при любом изменении текста ошибки.

2. **Классификация для JSON-ответов CLI** — функции `classifyReviewError`, `classifyQueryError`, `classifyHealthError`, `classifyStatsError` в `cmd/` использовали `err.Error()` + `strings.Contains` / `==` для определения категории ошибки. Тот же риск: смена текста ошибки в service-слое ломает классификацию.

При этом часть ошибок — внешние (cobra: `"required flag(s)"`, `"unknown flag:"` и т.п.) — не имеют sentinels и остаются на string-matching.

## Решение

### Новый пакет `internal/errs`

Централизованный набор sentinel errors для всех внутренних ошибок CodeBase:

```go
var (
    ErrConfigNotLoaded   = errors.New("config not loaded")
    ErrDBConnect         = errors.New("failed to connect to database")
    ErrSchemaInit        = errors.New("failed to init schema")
    ErrNoRelationFilters = errors.New("at least one relation filter must be provided")
    ErrQueryFailed       = errors.New("query failed")
    ErrReviewFailed      = errors.New("review failed")
    ErrStatsFailed       = errors.New("failed to get stats")
    ErrHealthCheckFailed = errors.New("failed to inspect index readiness")
)
```

### Обёртывание ошибок в service-слое

В `runtime.go` файлах service-слоя `fmt.Errorf("...: %w", err)` заменены на `fmt.Errorf("%w: %w", sentinel, err)`. Go 1.25 поддерживает множественный `%w`, что позволяет `errors.Is` находить sentinel в цепочке обёрнутых ошибок.

Для ошибок без внутреннего `err` (например, `config not loaded`) sentinel возвращается напрямую.

### Классификация через `errors.Is`

Функции `classify*Error` переписаны: вместо `message == "config not loaded"` используется `errors.Is(err, errs.ErrConfigNotLoaded)`. String-matching (`containsAny`) сохранён только для внешних cobra-ошибок.

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/errs/errs.go` | **Новый** — 8 sentinel errors |
| `internal/query/query_relations.go` | **Изменить** — `fmt.Errorf(...)` → `errs.ErrNoRelationFilters` (2 места) |
| `internal/querysvc/inspect.go` | **Изменить** — `strings.Contains` → `errors.Is(err, errs.ErrNoRelationFilters)` (2 места) |
| `internal/querysvc/runtime.go` | **Изменить** — 3 ошибки обёрнуты через sentinel |
| `internal/reviewsvc/runtime.go` | **Изменить** — 3 ошибки обёрнуты через sentinel |
| `internal/systemsvc/runtime.go` | **Изменить** — 3 ошибки обёрнуты через sentinel |
| `cmd/health.go` | **Изменить** — 3 ошибки + `classifyHealthError` на `errors.Is` |
| `cmd/stats.go` | **Изменить** — 3 ошибки + `classifyStatsError` на `errors.Is` |
| `cmd/init.go` | **Изменить** — `fmt.Errorf("config not loaded")` → `errs.ErrConfigNotLoaded` |
| `cmd/review.go` | **Изменить** — `classifyReviewError` на `errors.Is` |
| `cmd/query_execution.go` | **Изменить** — `classifyQueryError` на `errors.Is` |
| `cmd/query_execution_test.go` | **Изменить** — тесты используют sentinels + `fmt.Errorf("%w: %w", ...)` |
| `cmd/stats_health_test.go` | **Изменить** — тесты используют sentinels + `fmt.Errorf("%w: %w", ...)` |

## Пример: было → стало

### inspect.go (управление потоком)

**Было:**
```go
if err != nil && !strings.Contains(err.Error(), "at least one relation filter must be provided") {
    return nil, err
}
```

**Стало:**
```go
if err != nil && !errors.Is(err, errs.ErrNoRelationFilters) {
    return nil, err
}
```

### classifyReviewError (классификация)

**Было:**
```go
func classifyReviewError(err error) string {
    message := err.Error()
    switch {
    case message == "config not loaded":
        return "config_error"
    case containsAny(message, "failed to connect to database", "connection refused", "dial tcp"):
        return "database_unavailable"
    case containsAny(message, "failed to init schema"):
        return "schema_init_failed"
    case containsAny(message, "review failed"):
        return "review_failed"
    case containsAny(message, "required flag", "accepts", "unknown review rule"):
        return "invalid_arguments"
    default:
        return "internal_error"
    }
}
```

**Стало:**
```go
func classifyReviewError(err error) string {
    switch {
    case errors.Is(err, errs.ErrConfigNotLoaded):
        return "config_error"
    case errors.Is(err, errs.ErrDBConnect):
        return "database_unavailable"
    case errors.Is(err, errs.ErrSchemaInit):
        return "schema_init_failed"
    case errors.Is(err, errs.ErrReviewFailed):
        return "review_failed"
    case containsAny(err.Error(), "required flag", "accepts", "unknown review rule"):
        return "invalid_arguments"
    default:
        return "internal_error"
    }
}
```

### service-слой (обёртывание)

**Было:**
```go
return nil, fmt.Errorf("failed to connect to database: %w", err)
```

**Стало:**
```go
return nil, fmt.Errorf("%w: %w", errs.ErrDBConnect, err)
```

## Что осталось на string-matching

Только внешние ошибки, не имеющие sentinels:
- **cobra**: `"required flag(s)"`, `"unknown flag:"`, `"accepts 0 arg(s)"`, `"unknown command"`, `"required flag"`, `"accepts"`, `"unknown review rule"`
- **network**: `"connection refused"`, `"dial tcp"` — из `net` package, sentinels отсутствуют
- **health.go**: `"failed to ping database"`, `"failed to ping default database"` — из `cmd/health.go` (локальный код, но пинг — обёртка над `db.Ping()` без своего sentinel)

## Проверка

```bash
go build ./...
go vet ./...
go test ./... -tags=integration -count=1
```

Все тесты PASS, включая:
- `TestClassifyQueryError` — 7 кейсов (sentinels + cobra + unknown)
- `TestClassifyStatsError` — 5 кейсов
- `TestClassifyHealthError` — 5 кейсов

## Откат

Изменения полностью обратимы. Удаление `internal/errs/` и возврат к `fmt.Errorf` в service-слое восстанавливает исходное поведение. Текст ошибок не изменился — `errs.ErrConfigNotLoaded.Error()` == `"config not loaded"`, что сохраняет совместимость с логами и пользовательским выводом.

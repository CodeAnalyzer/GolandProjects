# Оптимизация NewParser(): package-level vars для regexps

Перенос всех `regexp.MustCompile` вызовов в package-level `var` блоки во всех парсерах, чтобы `NewParser()` стал дешёвым (присваивание указателей на shared regexps вместо компиляции).

## Контекст

При `codebase init`/`update` каждый индексируемый файл создаёт новый парсер через `NewParser()`, который компилирует regexps с нуля. SQL-парсер — ~40 regexps, PAS — ~35, SMF — ~30. На тысячах файлов это десятки тысяч лишних компиляций. `*regexp.Regexp` безопасен для concurrent read-only использования, поэтому shared regexps можно переиспользовать между goroutines в worker pool.

## Парсеры к изменению (10 файлов)

| # | Файл | Regexps | Вызовы NewParser() |
|---|------|---------|---------------------|
| 1 | `internal/parser/apimacro/parser.go` | 6 | `indexer.go:1168` (per SQL file) |
| 2 | `internal/parser/sql/sql_parser.go` | ~40 | `indexer_sql_pas.go:23` (per SQL/T01), `indexer.go:274` (per H file), `indexer_relations.go:96` (per fragments batch), `review/runner.go:43` (per Runner) |
| 3 | `internal/parser/pas/pas_parser.go` | ~35 | `indexer_sql_pas.go:778` (per PAS file) |
| 4 | `internal/parser/js/js_parser.go` | 7 | `indexer.go:491` (per JS file), `smf_parser.go:188` (per SMF file) |
| 5 | `internal/parser/dfm/dfm_parser.go` | ~15 | `indexer.go:346` (per DFM file) |
| 6 | `internal/parser/h/h_parser.go` | 8 | `indexer.go:234` (per H file) |
| 7 | `internal/parser/smf/smf_parser.go` | ~30 + jsParser | `indexer.go:873` (per SMF file) |
| 8 | `internal/parser/tpr/tpr_parser.go` | 4 | `indexer.go:617` (per TPR file) |
| 9 | `internal/parser/rpt/rpt_parser.go` | 5 | `indexer.go:737` (per RPT file) |
| 10 | `internal/parser/dsxml/parser.go` | 0 | `indexer.go:974` (per XML file) — без regexps, пропускаем |

## Процесс выполнения

После каждого шага Cascade завершает редактирование и **ждёт явного акцепта пользователя**. Пользователь сам запускает сборку и тесты, сообщает результат, после чего Cascade переходит к следующему шагу.

## План реализации

### Шаг 1: apimacro (пилотный, самый простой — 6 regexps)

- В `internal/parser/apimacro/parser.go` добавить package-level `var` блок с 6 `regexp.MustCompile`
- `NewParser()` переписать на присваивание указателей
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/apimacro/... ./internal/indexer/...`

### Шаг 2: sql (самый большой, ~40 regexps)

- В `internal/parser/sql/sql_parser.go` добавить package-level `var` блок
- `NewParser()` переписать
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/sql/... ./internal/indexer/... ./internal/review/...`

### Шаг 3: pas (~35 regexps)

- В `internal/parser/pas/pas_parser.go` добавить package-level `var` блок
- `NewParser()` переписать
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/pas/... ./internal/indexer/...`

### Шаг 4: js (7 regexps)

- В `internal/parser/js/js_parser.go` добавить package-level `var` блок
- `NewParser()` переписать
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/js/... ./internal/indexer/...`

### Шаг 5: smf (~30 regexps + jsParser)

- В `internal/parser/smf/smf_parser.go` добавить package-level `var` блок
- `jsParser` заменить на package-level `var defaultJSParser = js.NewParser()` (после шага 4 это дёшево)
- `NewParser()` переписать
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/smf/... ./internal/indexer/...`

### Шаг 6: dfm, h, tpr, rpt (пакетно)

- Аналогично для каждого из 4 парсеров
- **Пауза** — пользователь проверяет: `go build ./... && go test ./internal/parser/... ./internal/indexer/...`

### Шаг 7: финальная проверка

- Пользователь запускает полный цикл: `go build ./... && go test ./...`
- Опционально: бенчмарк — замер времени `codebase init` до/после

## Шаблон изменения (на примере apimacro)

**Было:**
```go
func NewParser() *Parser {
    return &Parser{
        createProcRe: regexp.MustCompile(`...`),
        initEventRe:  regexp.MustCompile(`...`),
        // ...
    }
}
```

**Стало:**
```go
var (
    createProcRe = regexp.MustCompile(`...`)
    initEventRe  = regexp.MustCompile(`...`)
    // ...
)

func NewParser() *Parser {
    return &Parser{
        createProcRe: createProcRe,
        initEventRe:  initEventRe,
        // ...
    }
}
```

## Риски и митигация

- **Thread safety:** `*regexp.Regexp` safe для concurrent read (Match, FindStringSubmatch). Ни один парсер не мутирует regexps после создания. ✅
- **Тесты:** Тесты создают `NewParser()` напрямую — API не меняется, тесты работают без изменений. ✅
- **Инициализация:** Package-level `var` инициализируются при первом импорте пакета (один раз). ✅
- **Порядок инициализации:** SMF импортирует JS пакет — `js.NewParser()` в smf var-блоке безопасен, т.к. js package-level vars инициализируются раньше (Go гарантирует порядок по импорту). ✅

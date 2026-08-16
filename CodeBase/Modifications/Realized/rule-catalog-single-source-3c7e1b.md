# Устранение дублирования каталога правил SQL review

Каталог из 53 правил SQL review дублируется в 5 местах и уже разъехался — в CLI help и MCP description отсутствуют правила, которые при этом валидируются switch-блоками. Лечится единым `ruleCatalog` как единственным источником истины.

---

## Контекст проблемы

### 5 источников каталога правил

| # | Файл | Строки | Назначение | Проблема |
|---|------|--------|------------|----------|
| 1 | `internal/review/types.go` | 8-61 | `RuleID` константы (53 шт.) | Базовый источник — ок |
| 2 | `internal/review/review_helpers.go` | 212-267 | `enabledRuleSet` — map всех правил с `true` | Дублирует список; при добавлении правила нужно не забыть добавить сюда |
| 3 | `internal/review/runner.go` | 184-448 | `buildRuleTasks` — 53 if-блока с closures | Дублирует список + привязка rule→check-метод; при добавлении правила нужно не забыть добавить сюда |
| 4 | `cmd/review.go` | 123-124 + 257 | switch-валидация + help-строка `--rules` | **Help-строка отстаёт**: нет `varUseAfterCursor`, `excessProcParams`, `duplicateOutputVariable`, `useOnlyDeclaredCursors` (4 правила) |
| 5 | `internal/mcp/registry.go` | 144 (description) + 161-162 (switch) | Описание MCP-инструмента + валидация | **Description отстаёт**: нет `varUseAfterCursor` (1 правило); switch ок |

### Фактические расхождения

**CLI help-строка** (`cmd/review.go:257`) — отсутствуют 4 правила:
- `varUseAfterCursor`
- `excessProcParams`
- `duplicateOutputVariable`
- `useOnlyDeclaredCursors`

**MCP description** (`mcp/registry.go:144`) — отсутствует 1 правило:
- `varUseAfterCursor`

Switch-блоки в обоих местах (`cmd/review.go:123`, `mcp/registry.go:161`) содержат все 53 правила — расхождение только в текстовых описаниях.

### Следствия

- При добавлении нового правила нужно вручную обновить **5 мест** (types.go, enabledRuleSet, buildRuleTasks, CLI switch+help, MCP switch+description). Забыть хотя бы одно — правило не попадёт в валидацию или в help.
- Уже разъехалось: 4 правила отсутствуют в CLI help, 1 — в MCP description. Пользователь не может передать эти правила через `--rules`, хотя switch их принимает.
- Help-строка CLI — 492 символа в одну строку, нечитаемо, генерируется вручную.

---

## Принцип

- **Единый источник истины:** `ruleCatalog` — package-level `map[RuleID]ruleMeta` в `internal/review/catalog.go`.
- `ruleMeta` содержит: `Severity`, `Description`, `Build` (функция-обёртка над check-методом Runner).
- Все 5 потребителей читают из `ruleCatalog`; ручное перечисление правил удалено.
- Добавление нового правила = одна запись в `ruleCatalog` + одна константа в `types.go`.

---

## Затронутые файлы

| Файл | Изменение |
|------|-----------|
| `internal/review/catalog.go` | **Новый файл** — `ruleMeta`, `ruleCatalog`, `AllRuleIDs()`, `RuleListString()`, `ValidateRuleIDs()` |
| `internal/review/types.go` | Без изменений (константы RuleID остаются) |
| `internal/review/review_helpers.go` | `enabledRuleSet` — генерируется из `ruleCatalog` |
| `internal/review/runner.go` | `buildRuleTasks` — цикл по `ruleCatalog` вместо 53 if-блоков |
| `cmd/review.go` | `parseReviewRules` — валидация через `review.ValidateRuleIDs()`; help-строка через `review.RuleListString()` |
| `internal/mcp/registry.go` | description + handler — валидация через `review.ValidateRuleIDs()`; description через `review.RuleListString()` |

Оценка: 1 новый файл (~120 строк), 5 файлов изменено. Удаление ~280 строк дублирующего кода, добавление ~30 строк потребляющего.

---

## План (4 фазы)

### Фаза 1 — Создать `catalog.go` *(остановка — ждать акцепт)*

**Новый файл:** `internal/review/catalog.go`

```go
package review

import (
	"context"
	"fmt"
	"sort"
	"strings"

	sqlparser "github.com/codebase/internal/parser/sql"
)

// ruleMeta — метаданные правила: severity, описание и функция-обёртка.
type ruleMeta struct {
	Severity    int
	Description string
	Build       func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error)
}

// ruleCatalog — единственный источник истины для каталога правил.
// Порядок не имеет значения; AllRuleIDs() возвращает отсортированный список.
var ruleCatalog = map[RuleID]ruleMeta{
	RuleForeignTablesUsing: {
		Severity:    SeverityFineCode,
		Description: "Использование таблицы чужого продукта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForeignTables(ctx, parsed, file, "t")
		},
	},
	RuleForeignPTablesUsing: {
		Severity:    SeverityFineCode,
		Description: "Использование P-таблицы чужого продукта",
		Build: func(r *Runner, ctx context.Context, parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
			return r.checkForeignPTables(ctx, parsed, file)
		},
	},
	// ... все 53 правила ...
}
```

Публичные функции:

```go
// AllRuleIDs возвращает отсортированный список всех RuleID из каталога.
func AllRuleIDs() []RuleID {
	ids := make([]RuleID, 0, len(ruleCatalog))
	for id := range ruleCatalog {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return string(ids[i]) < string(ids[j]) })
	return ids
}

// RuleListString возвращает comma-separated список всех RuleID для help-строк и описаний.
func RuleListString() string {
	ids := AllRuleIDs()
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = string(id)
	}
	return strings.Join(parts, ",")
}

// ValidateRuleIDs проверяет, что все переданные rule IDs существуют в каталоге.
// Возвращает нормализованный список (без дубликатов) или ошибку.
func ValidateRuleIDs(raw []string) ([]RuleID, error) {
	seen := make(map[RuleID]struct{})
	result := make([]RuleID, 0, len(raw))
	for _, s := range raw {
		id := RuleID(strings.TrimSpace(s))
		if id == "" {
			continue
		}
		if _, exists := ruleCatalog[id]; !exists {
			return nil, fmt.Errorf("unknown review rule: %s", id)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

// DefaultEnabledRules возвращает map всех правил со значением true (для enabledRuleSet).
func DefaultEnabledRules() map[RuleID]bool {
	result := make(map[RuleID]bool, len(ruleCatalog))
	for id := range ruleCatalog {
		result[id] = true
	}
	return result
}
```

**Проверка:** `go build ./internal/review/...` + `go vet ./internal/review/...`

---

### Фаза 2 — Рефакторинг `enabledRuleSet` и `buildRuleTasks` *(остановка — ждать акцепт)*

**Файл:** `internal/review/review_helpers.go`

`enabledRuleSet` (строки 212-281) — заменить тело функции:

```go
func enabledRuleSet(rules []RuleID) map[RuleID]bool {
	result := DefaultEnabledRules()
	if len(rules) == 0 {
		return result
	}
	for key := range result {
		result[key] = false
	}
	for _, rule := range rules {
		rule = RuleID(strings.TrimSpace(string(rule)))
		if _, exists := result[rule]; exists {
			result[rule] = true
		}
	}
	return result
}
```

Удаляется 53-строчный map-литерал (строки 213-267), заменяется на вызов `DefaultEnabledRules()`.

**Файл:** `internal/review/runner.go`

`buildRuleTasks` (строки 181-450) — заменить 53 if-блока на цикл:

```go
func (r *Runner) buildRuleTasks(ctx context.Context, ruleSet map[RuleID]bool, parsed *sqlparser.ParseResult, file *indexedFile) []ruleTask {
	_ = ctx
	tasks := make([]ruleTask, 0, len(ruleSet))
	for ruleID, meta := range ruleCatalog {
		if !ruleSet[ruleID] {
			continue
		}
		// Захватываем переменные цикла явно
		rule := ruleID
		build := meta.Build
		tasks = append(tasks, ruleTask{
			rule: rule,
			run: func(ctx context.Context) ([]Finding, error) {
				return build(r, ctx, parsed, file)
			},
		})
	}
	return tasks
}
```

Удаляется ~265 строк (53 if-блока по ~5 строк), заменяется на ~15 строк.

**Проверка:** `go build ./internal/review/...` + `go vet` + `go test ./internal/review/... -count=1`

---

### Фаза 3 — Рефакторинг CLI `cmd/review.go` *(остановка — ждать акцепт)*

**Файл:** `cmd/review.go`

1. `parseReviewRules` (строки 109-136) — заменить switch-блок на `review.ValidateRuleIDs`:

```go
func parseReviewRules(raw string) ([]review.RuleID, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil, nil
	}
	parts := strings.Split(trimmed, ",")
	rawParts := make([]string, 0, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s != "" {
			rawParts = append(rawParts, s)
		}
	}
	rules, err := review.ValidateRuleIDs(rawParts)
	if err != nil {
		return nil, nil, err
	}
	return rules, rawParts, nil
}
```

Удаляется 53-case switch (строки 123-124).

2. Help-строка `--rules` (строка 257) — генерируется из каталога:

```go
func init() {
	reviewCmd.Flags().BoolVar(&reviewOutputJSON, "json", false, "output as JSON")
	reviewCmd.Flags().StringVar(&reviewRulesRaw, "rules", "",
		"comma-separated rules ("+review.RuleListString()+")")
	reviewCmd.Flags().IntVar(&reviewMinSeverity, "min-severity", review.SeverityFineCode, "minimum severity to output")
	rootCmd.AddCommand(reviewCmd)
}
```

Удаляется hardcoded-строка из 492 символов.

**Проверка:** `go build ./cmd/...` + `go vet` + `go test ./cmd/... -count=1` (если есть тесты на review CLI)

---

### Фаза 4 — Рефакторинг MCP `internal/mcp/registry.go` *(остановка — ждать акцепт)*

**Файл:** `internal/mcp/registry.go`

1. Description `codebase_review_sql` (строка 144) — генерируется из каталога:

```go
"codebase_review_sql": {
	Definition: toolDefinition{
		Name: "codebase_review_sql",
		Description: "Run static analysis (lint) checks on a single SQL file and return a list of findings with rule ID, severity, line number and message. Requires absolute file path. Available rule IDs: " + review.RuleListString() + ". Omit rules to run all enabled rules.",
		InputSchema: objectSchema(map[string]interface{}{
			"file_path":     stringProp("Full SQL file path"),
			"rules":         map[string]interface{}{"type": "array", "description": "Optional rule ids", "items": map[string]interface{}{"type": "string"}},
			"min_severity":  intProp("Minimum severity"),
		}),
	},
	Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
		filePath, err := requiredString(args, "file_path")
		if err != nil {
			return nil, err
		}
		rulesRaw, err := optionalStringSlice(args, "rules")
		if err != nil {
			return nil, err
		}
		minSeverity, err := optionalInt(args, "min_severity")
		if err != nil {
			return nil, err
		}
		rules, err := review.ValidateRuleIDs(rulesRaw)
		if err != nil {
			return nil, err
		}
		opts := review.Options{Rules: rules, MinSeverity: minSeverity}
		if db != nil {
			return reviewsvc.ExecuteWithCtx(ctx, db, filePath, opts, nil)
		}
		return reviewsvc.Execute(filePath, opts, nil)
	},
},
```

Удаляется 53-case switch (строки 161-166) и hardcoded description-строка.

**Проверка:** `go build ./internal/mcp/...` + `go vet` + `go test ./internal/mcp/... -count=1`

---

## Откат

Все изменения обратно совместимы:
- `ruleCatalog` — новый файл, не ломает существующий API.
- `enabledRuleSet`, `buildRuleTasks`, `parseReviewRules`, MCP handler — внутренние функции, сигнатуры не меняются (кроме `parseReviewRules` — упрощается, но вызывается только из `buildReviewOptions`).
- `RuleID` константы в `types.go` не меняются.
- Откат — revert конкретной фазы или всего изменения.

## Ожидаемый результат

| Метрика | Было | Стало |
|---------|------|-------|
| Источников каталога правил | 5 (types.go, enabledRuleSet, buildRuleTasks, CLI switch+help, MCP switch+desc) | 2 (types.go + ruleCatalog) |
| Мест для обновления при добавлении правила | 5 | 2 (константа + запись в catalog) |
| CLI help `--rules`: отсутствующих правил | 4 | 0 (генерируется из catalog) |
| MCP description: отсутствующих правил | 1 | 0 (генерируется из catalog) |
| Строк кода в `buildRuleTasks` | ~265 | ~15 |
| Строк кода в `enabledRuleSet` | ~70 | ~15 |
| Hardcoded-строк со списком правил | 3 (CLI help, MCP desc, CLI switch) | 0 |

## Риски

- **`buildRuleTasks` closure capture:** в Go ≤1.21 переменные цикла захватываются по ссылке — нужен явный shadow (`rule := ruleID`, `build := meta.Build`). Go 1.22+ исправляет это автоматически, но shadow — безопасная практика для всех версий.
- **Порядок правил в help/description:** `RuleListString()` возвращает sorted by string ID — алфавитный порядок. Текущий hardcoded порядок отличается (группировка по severity). Алфавитный порядок приемлем для machine-readable списка; для человекочитаемого help это улучшение (предсказуемый порядок).
- **Тесты:** существующие тесты `TestEnabledRuleSet_*` в `runner_test.go` проверяют наличие конкретных правил в `enabledRuleSet` — останутся зелёными, т.к. `DefaultEnabledRules()` возвращает тот же набор. Тесты `parseReviewRules` (если есть) — останутся зелёными, т.к. `ValidateRuleIDs` выполняет ту же валидацию.
- **MCP description длина:** `RuleListString()` для 53 правил ≈ 600 символов. Текущая hardcoded-строка ≈ 500 (без 5 отсутствующих правил). Рост на ~100 символов — некритично для MCP-протокола.

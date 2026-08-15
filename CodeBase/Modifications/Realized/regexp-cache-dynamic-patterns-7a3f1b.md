# Кэш динамических regexps и вынос статических в package-level vars

Вынос всех inline `regexp.MustCompile` с фиксированными паттернами в package-level `var`-блоки и введение глобального потокобезопасного кэша `util.CachedRegexp` для динамических паттернов (с `regexp.QuoteMeta` от runtime-аргументов).

## Контекст

В предыдущих итерациях (`optimize-regex-compilation-050860.md`, `optimize-newparser-package-level-vars-56ddea.md`, `perf-review-rules-2eaf1d.md`) статические `regexp.MustCompile` были вынесены в package-level vars в пакетах `review` и парсерах. Однако оставались inline-вызовы:

- В `internal/review` — 3 динамических (с `QuoteMeta`), исправлены в первой итерации (локальный `cachedRegexp` на `sync.Map`).
- В `internal/indexer/indexer_sql_pas.go` — 7 штук (6 статических + 1 динамический).
- В `internal/parser/js/js_parser.go` — 3 динамических.
- В `internal/parser/smf/smf_parser.go` — 1 динамический.
- В `internal/parser/smf/smf_parser_test.go` — 1 статический (в тесте).

Всего по проекту — 11 inline-вызовов вне пакета `review`.

## Изменения

### Новый файл: `internal/util/regexp_cache.go`

Глобальный потокобезопасный кэш для динамических regexps:

```go
package util

import (
    "regexp"
    "sync"
)

var regexpCache sync.Map

func CachedRegexp(pattern string) *regexp.Regexp {
    if v, ok := regexpCache.Load(pattern); ok {
        return v.(*regexp.Regexp)
    }
    re := regexp.MustCompile(pattern)
    regexpCache.Store(pattern, re)
    return re
}
```

Используется всеми пакетами, где есть динамические паттерны с `regexp.QuoteMeta`.

### `internal/review/review_helpers.go`

- Локальный `regexpCache sync.Map` + `cachedRegexp` заменён на делегирование в `util.CachedRegexp`.
- Убран import `"sync"`, добавлен import `"github.com/codebase/internal/util"`.
- `cachedRegexp(pattern)` сохранён как тонкая обёртка для обратной совместимости в пакете.

### `internal/indexer/indexer_sql_pas.go`

**6 статических → package-level vars** (новый блок `var (...)` после import):

| Переменная | Паттерн | Функция |
|---|---|---|
| `reIdxAliasFromJoin` | `\b(?:from\|join)\s+(\w+)\s+(?:as\s+)?(\w+)` | `parseSelectIntoFragmentInfo` |
| `reIdxAsAliasSuffix` | `\bas\s+(\w+)\s*$` | `extractAliasName` |
| `reIdxStripAsAlias` | `\s+as\s+\w+\s*$` | `extractSimpleSourceColumn` |
| `reIdxDirectColRef` | `^\s*(\w+)\.(\w+)\s*$` | `extractSimpleSourceColumn` |
| `reIdxIsnullCoalesce` | `^\s*(?:isnull\|coalesce)\s*\(` | `extractSimpleSourceColumn` |
| `reIdxQualifiedColRef` | `(\w+)\.(\w+)` | `extractSimpleSourceColumn` |

**1 динамический → `util.CachedRegexp`**:

| Функция | Паттерн |
|---|---|
| `findSelectIntoFragment` | `\binto\s+{tableName}\b` (через `QuoteMeta`) |

### `internal/parser/js/js_parser.go`

**3 динамических → `util.CachedRegexp`**:

| Функция | Паттерн |
|---|---|
| `isObjectUsedInFunction` | `\b{objectName}\.\w+\s*\(` |
| `isObjectUsedInFunction` | `\b{objectName}\.\w+\s*[^(\\s]` |
| `isObjectCreatedInFunction` | `\b(?:var\s+)?{objectName}\s*=\s*Sys\.CreateObject\(\s*"{objectType}"\s*\)` |

### `internal/parser/smf/smf_parser.go`

**1 динамический → `util.CachedRegexp`**:

| Функция | Паттерн |
|---|---|
| `findHelperFunctionBody` | `function\s+{helperName}\s*\([^)]*\)\s*\{` |

### `internal/parser/smf/smf_parser_test.go`

**1 статический → test-level var** `reTestWithBlock` (вынесен из функции `TestExtractWithBlock`).

### `internal/review/review_helpers_test.go`

Добавлены 3 unit-теста для `cachedRegexp`:

- `TestCachedRegexp_SamePatternReturnsSamePointer` — одинаковый паттерн возвращает тот же `*regexp.Regexp`.
- `TestCachedRegexp_DifferentPatternReturnsDifferentPointer` — разные паттерны дают разные указатели.
- `TestCachedRegexp_FunctionalMatch` — скомпилированный regexp корректно матчит строку.

## Проверка

- `go build ./...` — чисто.
- `go test ./... -tags=integration -count=1` — все тесты PASS (включая `internal/util`).
- Inline-вызов `regexp.MustCompile` вне package-level vars и `util.CachedRegexp` в проекте не осталось.

## Ожидаемый эффект

- Устранение повторной компиляции regexps для одинаковых `identifier`/`objectName`/`helperName`/`tableName` при многократных вызовах функций индексации и парсинга.
- 6 статических паттернов в `indexer_sql_pas.go` компилируются один раз при init пакета вместо каждого вызова функции.
- Глобальный кэш `util.CachedRegexp` доступен из всех пакетов, что позволяет избежать дублирования логики кэширования.

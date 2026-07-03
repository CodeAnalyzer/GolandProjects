# План оптимизации: вынести статические regex в пакетные var-блоки

Перенести все статические `regexp.MustCompile` из функций `internal/review` в пакетные `var`-блоки, сгруппированные по файлам и функциональным областям; динамические regex, зависящие от runtime-аргументов, оставить внутри функций.

## Принцип отбора

- **Выносить**: все `regexp.MustCompile` с **фиксированным паттерном** (без `regexp.QuoteMeta` от параметров).
- **Не выносить**: regex, которые собираются из runtime-значений:
  - `replaceIdentifierToken` (`review_parser.go:975`) — `QuoteMeta(token)`.
  - `extractCaseColumnRefs` (`review_helpers.go:553`) — `QuoteMeta(identifier)`.
  - `containsVarReference` (`review_helpers.go:2303`) — `QuoteMeta(v)`.

## Группировка по файлам

### `review_parser.go`

Создать несколько именованных `var`-блоков по темам:

- `cursorRegexes` — для `parseCursorDeclarations`, `parseDeallocateStatements`, `parseAllFetchIntoStatements`, `parseFetchIntoStatement`, `findFetchIntoTailBoundary`.
- `dmlParserRegexes` — для `parseUpdateSetStatement`, `parseInsertSelectStatement`, `parseSelectAssignStatement`.
- `aliasMapRegexes` — для `parseAliasMap`.
- `statementBoundaryRegexes` — для `hasStatementEnded`.
- `execArgRegexes` — для `parseExecArguments` (только `outRe`, один regex на функцию).
- `columnRefRegexes` — для `findAllUnqualifiedColumnRefs`, `extractColumnAliasName`, `extractOrderByColumns`.
- `caseAssignRegexes` — для `parseCasePartAssignments`.

Дублирующие паттерны (например, `(?i)^[a-z_][a-z0-9_]*$` в `extractColumnAliasName`) использовать один раз.

### `review_helpers.go`

Создать блоки:

- `joinConditionRegexes` — для `collectJoinColumnsFromOnPart`, `extractOnPartsForIndexWrong`.
- `caseConditionRegexes` — для `extractUnqualifiedConditionColumns`, `checkPTableSpid`.
- `typeParsingRegexes` — для `numericPrecisionScale`, `compareNumericPrecisionScale`, `varcharLength` (один общий `numericRe`, один `varcharRe`).
- `fromClauseRegexes` — для `extractFirstTableFromFromClause`.

### `review_lookup.go`

Создать блоки:

- `commentRegexes` — для `removeComments` (возможно, объединить с аналогичными в `findAllUnqualifiedColumnRefs`).
- `indexHintRegexes` — для `parseTableFromPart` (4 regex для хинтов).
- `conditionParseRegexes` — для `extractConditionInfo`, `extractJoinOnParts`.

## Порядок выполнения

1. Создать пакетные `var`-блоки в каждом из трёх файлов, снабдив их комментариями.
2. Заменить inline `regexp.MustCompile(...)` на соответствующие переменные.
3. Убедиться, что нет конфликтов имён с уже существующими пакетными regex (например, `reUpdateTable`, `reInsertTable`, `reFromJoinTable`, `reNumericLiteral` и т.д.). При необходимости дать новым переменным уникальные имена.
4. Запустить `go build ./...` и `go test ./... -count=1` для проверки.
5. Проверить, что динамические regex остались внутри функций и не сломаны.

## Ожидаемый результат

- Снижение нагрузки на GC и CPU при обработке больших файлов за счёт исключения повторных компиляций одних и тех же паттернов.
- Сохранение текущего поведения: все тесты PASS, ручная проверка на реальном файле не выявляет регрессий.

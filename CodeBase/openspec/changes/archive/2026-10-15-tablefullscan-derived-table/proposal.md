# Proposal: tableFullScan — поддержка derived tables и INSERT…SELECT

## Why

Правило `tableFullScan` (severity 1, deploy-stopper) даёт ложные срабатывания на распространённом паттерне `from (select …) alias`: парсер FROM-части принимает первый токен `(select` за имя таблицы, срезает скобки и выдаёт finding с бессмысленным `object=select` (репро: `CreditTurnoverLink_FastProcess.sql`, BUG-отчёт `BUG-tableFullScan-derived-table-false-positive-20261015.md`). Побочно INSERT…SELECT анализируется со строки `select`, и целевая таблица INSERT никогда не проверяется.

## What Changes

- `parseTablesInFromClause`/`parseTableWithAlias` распознают derived tables `(subquery) alias`: алиас берётся после парной закрывающей скобки, внутренние таблицы подзапроса анализируются рекурсивно со своим WHERE (багрепорт допускал и минимальный вариант «пропускать»; выбран полный — таблицы внутри подзапроса обязаны быть отфильтрованы так же).
- Защитный guard: `TableName` не может быть SQL-ключевым словом (`select` и т.п.) — исключает whole-class ложных объектов вида `object=select`.
- `findStatementStart` для tableFullScan знает `insert`: оператор ловится с ключевого слова `insert`, `line` указывает на начало оператора; анализируется только SELECT-часть (цель INSERT пишется, а не сканируется — на неё требование фильтра не распространяется).
- Тексты сообщений и severity не меняются; исчезают только ложные finding'и.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: требование «Full scan условия» расширяется — правило обязано корректно обрабатывать derived tables (не выдавать `object=select`, анализировать внутренние таблицы подзапроса) и INSERT…SELECT (finding привязан к началу оператора).

## Impact

- Код: `internal/review/review_lookup.go` (`parseTableWithAlias`, `parseTablesInFromClause`, `findStatementStart`), `internal/review/review_rules.go` (`analyzeStatementForFullScan` — контекст для рекурсии/insert).
- Тесты: `internal/review/review_rules_test.go`, `internal/…/runner_test.go` (новые кейсы: derived table с фильтром внутри — нет finding; без фильтра внутри — finding с именем внутренней таблицы; INSERT…SELECT — line=строка insert, object≠select).
- Регресс-риск: парсер FROM общий для нескольких правил (`tableHintExists`, `indexWrong` и др. используют `extractTablesFromFromClause`) — поведение для обычных таблиц не должно измениться; прогнать весь набор тестов review.
- Внешние интерфейсы (CLI/MCP-контракты) не меняются: меняется только содержимое findings (исчезают ложные, line/object становятся точнее).

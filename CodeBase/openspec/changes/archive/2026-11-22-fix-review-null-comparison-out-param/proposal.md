# Proposal: fix-review-null-comparison-out-param

## Why

Правило `nullComparison` выдаёт ложные срабатывания (severity 2) на объявлениях выходных параметров процедуры в короткой форме T-SQL `@Name DSTYPE = null out` / `= NULL out,`: регулярка-исключение `nullParamDefaultRe` (internal/review/types.go:142) распознаёт только ключевое слово `output`, но не его полный синоним `out`. В корпусе FA это 2359 ложных findings (2363 вхождения `= null out`, из них 4 — целиком в закомментированных строках, findings не дающих) в 823 файлах (fa-custody/SecSwift, fa-payments/UFEBS, fa-reports/RPT*, fa-cards и др.), что замусоривает отчёты review. Этот же класс FP уже чинился 2026-06-21 для длинной формы `= null output` (Bug-reports/Checked/BUG-nullComparison-output-param-false-positive-20260621.md) — короткая форма осталась непокрытой.

## What Changes

- Регулярка `nullParamDefaultRe` в `internal/review/types.go` распознаёт оба синонима направления выходного параметра: `output` и `out` (группа `(?:output\s*)?` → `(?:out(?:put)?\s*)?`).
- Строки объявлений `@Name DSTYPE = null out,` и `@Name DSTYPE = null out` (в любом регистре, с/без хвостовой запятой) пропускаются сканером `nullComparison` без формирования finding.
- Тест-кейсы короткой формы `out` — включая смешанный регистр (`Out`, `OUT`) и хвостовую часть строки с `--`-комментарием — добавляются в таблицу теста `TestCheckNullComparison_NoFalsePositive` (internal/review/review_rules_test.go) рядом с существующими кейсами `output`.
- Поведение для всех прочих входов (сравнения `= NULL` в коде, `IN (NULL)`, строки `SELECT @Var = null`) не меняется.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требование «Сравнение с NULL через =/<>» расширяется сценарием — объявления выходных параметров с дефолтом `= null out` (синоним `output`) не формируют finding `nullComparison`.

## Impact

- Код: `internal/review/types.go` (одна регулярка), `internal/review/review_rules_test.go` (тест-кейсы).
- CLI/MCP: `codebase review --rules nullComparison` и MCP-инструмент `codebase_review_sql` — на полном прогоне по FA ожидается минус ~2359 FP; реальное поведение (детекция истинных сравнений с NULL) не затрагивается.
- Совместимость: без breaking changes; finding-контракт не меняется.
- Спека: дельта `MODIFIED` в `review/postgre-requirements`.

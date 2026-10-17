## Why

SQL-парсер склеивает standalone-вызовы макросов Diasoft (`M_DELETE_PTABLE`, `M_BUSINESSLOG_PARAM`, `M_LOG_TABLE`, ...) с соседним оператором в один query-фрагмент (макро-строки не являются границей оператора; flush происходит на пустой строке или control-flow). На этом фоне build 1600 даёт два дефекта на `CreditTurnoverLink_MassInsert.sql`:

1. `diffTypesComparison` якорит finding на `fragment.LineNumber` — начало склеенного фрагмента (строка с `M_DELETE_PTABLE(...)`), а не на строку фактического сравнения в ON (сдвиг 228 → 242, 14 строк). Содержание finding'а корректно (DSINT_KEY vs DSIDENTIFIER), но якорь вводит в заблуждение.
2. `usageVarInSameSelect` даёт ложное срабатывание: `select @RowCount = @@rowcount, @RetVal = @@error` склеивается с последующими строками `M_BUSINESSLOG_PARAM(@RowCount, ...)`; dotall-регекс `reVarAssignInSelect` проглатывает макро-строки в выражение `@RetVal`, а `containsVarReference` (`@rowcount\b`) находит `@RowCount` внутри макроса → фиктивный «cross-usage».

Дополнительно обнаружена латентная мина: `containsVarReference` не различает системные переменные `@@` — строка `select @RetVal = @@error, @RowCount = @@rowcount` даст такой же ложный finding (подстрока `@rowcount` внутри `@@rowcount`).

## What Changes

- `diffTypesComparison`: якорь finding'а — строка фактического сравнения внутри фрагмента (offset совпадения в `fragment.QueryText`, по образцу `findAssignmentLineOffset`), а не `fragment.LineNumber`.
- `usageVarInSameSelect`: standalone-макросы `M_XXX(...)` (строка целиком — вызов макроса) вырезаются из текста фрагмента до разбора assignment'ов.
- `containsVarReference`: ссылка `@var` не матчится внутри системной переменной `@@var` (guard на предшествующий `@`).
- Регрессионные тесты на оба правила и на `@@`-guard; контрольный прогон на FA fa-contracts (Accrual).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требования «Сравнение выражений разных типов» и «Использование переменной в том же SELECT» получают уточнения якоря строки и иммунитета к макросам/`@@`-переменным.

## Impact

- Код: `internal/review/review_rules.go` (`checkDiffTypesComparison`, `checkUsageVarInSameSelect`), `internal/review/review_helpers.go` (`containsVarReference`, новый helper вырезания standalone-макросов), возможно `internal/review/review_parser.go`.
- Тесты: `internal/review/review_rules_test.go`.
- Совместимость: поведение CLI/MCP `review` не меняется (те же правила, severity, формат findings); меняются только line-якорь одного правила и исчезновение FP.
- Контроль: прогон по `FA/fa-contracts` — дифф findings относительно build 1600 должен содержать только line-shift `diffTypesComparison` и исчезновение FP `usageVarInSameSelect`; новых finding'ов быть не должно.

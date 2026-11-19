# Proposal: fix-fetch-into-trailing-comment

## Why

Хвостовой `--комментарий` на строке переменной многострочного `FETCH ... INTO` отбрасывает переменную при разборе: анкорный регэксп `reParserFetchIntoVar` (`^\s*@var\s*$`, review_parser.go:20) не допускает текста после переменной, а `splitTopLevelCSV`-часть «`@AccountsRequestID --используется внутри макроса...`» не матчится. На эталоне `FA/fa-contracts/Consumer/SERVER/Consumer/Cons_BankPartnerInstPrepare.sql` (FETCH 918–920, декларация 904–914) это даёт FP `cursorFetchArguments` («2 переменных, а в DECLARE 3 выражений» — реально переменных 3) и одновременно FN `datatype` (тип потерянной переменной не проверяется). Первопричина — асимметрия парсера: `parseCursorDeclarations` стриппит комментарии через `stripLineComments` (review_parser.go:338), а оба пути разбора FETCH (`parseFetchIntoStatement(fragment.QueryText)` в правилах и `parseAllFetchIntoStatements`) работают на сырых строках.

## What Changes

- `parseFetchIntoStatement` (review_parser.go) стриппит однострочные `--` и блочные `/* */` комментарии из входного текста до разбора — единая точка, покрывающая оба пути: fragment-level вызовы из `checkCursorFetchArguments` и `checkDatatypeFetchInto` и file-level сборку `parseAllFetchIntoStatements`.
- `parseAllFetchIntoStatements`: границы сбора оператора (boundary/stop-строки) определяются по стрипнутому тексту с переносом inBlock-состояния блочных комментариев; строки оператора хранятся сырыми — skip-спаны `varUseAfterCursor` не сжимаются, комментарий внутри FETCH не создаёт новый finding.
- `fetchIntoStatementLine` (строка FETCH в правилах) вычисляется по тому же стрипнутому тексту, что и разбор переменных.
- Эффект: `cursorFetchArguments` перестаёт терять переменную с хвостовым комментарием (FP устранён); `datatype` начинает проверять тип такой переменной (FN устранён); `varUseAfterCursor` не меняет наблюдаемого поведения.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требование «Несовпадение аргументов FETCH с DECLARE CURSOR» — подсчёт переменных FETCH не зависит от комментариев: переменная с хвостовым `--`-комментарием участвует в подсчёте наравне с остальными.
- `review/fine-code`: требование «Потенциальная потеря точности» — спаривание переменных FETCH с выражениями декларации включает переменные с хвостовыми комментариями.

## Impact

- Код: `internal/review/review_parser.go` (`parseFetchIntoStatement`, `parseAllFetchIntoStatements`, новый хелпер стриппинга текста), тесты `internal/review/review_rules_test.go`.
- Поведение: правила курсорного семейства иммунны к комментариям внутри FETCH-операторов; реальный детект (рассинхрон арности, потеря точности) сохраняется и срабатывает на корректных данных.
- Зависимости: нет новых внешних зависимостей; схема БД и CLI-контракты не меняются.

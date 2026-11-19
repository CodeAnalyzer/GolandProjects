# Proposal: fix-fetch-into-macro-tail

## Why

Standalone-вызов Diasoft-макроса (`M_BUSINESSLOG_BLOCK_END('...')`, `M_BUSINESSLOG_CHECKPOINT(...)`, `M_LOG_TABLE(...)`, ...), следующий за `FETCH ... INTO` и склеенный SQL-парсером в один фрагмент, не распознаётся хвостовыми фильтрами разбора FETCH как начало нового оператора: `reParserFetchIntoBoundary` (review_parser.go:21) и `isFetchStatementStopLine` (:591) знают только ключевые слова `select/exec/set/...`. Макро-строка попадает в CSV-часть переменных, анкорный `reParserFetchIntoVar` (`^\s*@var\s*$`) не матчит последнюю переменную с хвостом — и она отбрасывается. Батч-прогон по fa-contracts (1049 файлов с FETCH INTO, build 1707) дал 8 находок `cursorFetchArguments`, все с сигнатурой «N−1 переменных vs N выражений» — ~100% FP; эталон: `Cons_OMCV_End_Accrual.sql:217-218` (FETCH 2 переменных + `M_BUSINESSLOG_BLOCK_END('>>> Итерация курсора <<<')` → «1 переменных, а в DECLARE 2 выражений»). Дефект pre-existing: воспроизводится на билде до fix-fetch-into-trailing-comment (проверено git-stash пересборкой). Симметричный FN: последняя переменная не спаривается в `datatype` и не попадает в множества переменных `varUseAfterCursor`.

## What Changes

- `isFetchStatementStopLine` распознаёт standalone-макровызов (строка вида `M<IDENT>\s*\(` в начале строки) как начало нового оператора — сбор строк FETCH останавливается на нём.
- `reParserFetchIntoBoundary` получает ту же альтернативу — хвост `variablesPart` в `parseFetchIntoStatement` обрезается перед макро-строкой.
- Эффект: `cursorFetchArguments` перестаёт терять последнюю переменную перед макросом (FP устранён); `datatype` спаривает её (FN устранён); `varUseAfterCursor` получает полные множества переменных, а строки-аргументы макроса с переменными ниже DEALLOCATE перестают скрываться спаном FETCH и корректно детектируются как использования (устранение FN, отражено дельтой требования «Использование переменной после закрытия курсора»).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требование «Несовпадение аргументов FETCH с DECLARE CURSOR» — standalone-макровызов после списка переменных завершает список: переменные до него участвуют в подсчёте полностью; требование «Использование переменной после закрытия курсора» — standalone-макровызов после списка переменных FETCH не является строкой-продолжением FETCH: переменные в его аргументах анализируются как обычное использование.
- `review/fine-code`: требование «Потенциальная потеря точности» — последняя переменная перед макровызовом участвует в спаривании FETCH INTO.

## Impact

- Код: `internal/review/review_parser.go` (`reParserFetchIntoBoundary`, `isFetchStatementStopLine`), тесты `internal/review/review_rules_test.go`.
- Поведение: спаны FETCH перестают захватывать макро-строку за списком переменных — множества переменных и спаны `varUseAfterCursor` становятся полными; использования переменных в аргументах макросов ниже DEALLOCATE начинают находиться (сегодня скрыты спаном). Приёмка допускает задокументированный diff по `varUseAfterCursor`/`datatype` на эталонах с верификацией новых находок как реальных.
- Зависимости: нет новых; схема БД и CLI-контракты не меняются.

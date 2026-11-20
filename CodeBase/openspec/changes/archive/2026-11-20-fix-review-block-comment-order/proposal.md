## Why

В шапках SQL-файлов Diasoft блочный комментарий часто закрывается строкой из дефисов (`-------------------------------------------------------------------*/`). В 12 построчных сканерах `review` обработка строчного комментария (`--`) выполняется раньше закрытия блочного комментария (`*/`). Поэтому строка закрытия принимается за `--`-комментарий, `inBlockComment` не сбрасывается, и весь код после шапки перестаёт анализироваться. В 11 сканерах это префиксная проверка `--`, в `checkPostgreLabelGotoLevel` — усечение строки по первому `--`.

Пример: в `FA/fa-contracts/Consumer/SERVER/Consumer/BPConsCoolingOff_Transfer.sql` `checkIndexWrong` не видит `delete` на строке 40 и не сообщает, что для условий `SPID = @@spid and BankProductID = @BankProductID` индекс `XIE1pBPConsCoolingOff (SPID, BankProductID, AmountMin)` лучше указанного `XPKpBPConsCoolingOff (SPID, AutoID)`. `codebase review` возвращает `count: 0`, хотя правило должно дать finding.

## What Changes

- Выровнять порядок обработки комментариев в 12 построчных сканерах: сначала закрытие активного блочного комментария (`*/`), затем `TrimSpace` и проверка/усечение по `--`.
- Затронутые правила deploy-stoppers: `indexWrong`, `updateOnlyVar`, `pTableSpid`, `forceOrder2Tbl`, `saveTran`, `useDrop`, `mathOperations`, `existsWithAndInIf`, `indexExistsInDB`, `varcharSize`.
- Затронутые правила postgre-requirements: `useFuncInIndCol` и `postgreLabelGotoLevel`; в последнем сохраняется усечение строки по первому `--` после закрытия блока.
- Поведение незакрытого блочного комментария не меняется: код до конца файла по-прежнему игнорируется.
- Поведение строчных комментариев вне блока `/* */` не меняется.
- Добавить регрессионные тесты с шапкой, закрывающейся строкой на `--`, включая e2e-кейс `BPConsCoolingOff_Transfer.sql` (finding `indexWrong` на строке 40 с рекомендацией `XIE1`).
- Поднять `buildNumber` с 1710 до 1711.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: новое требование о порядке закрытия блочного комментария для 10 построчных сканеров deploy-stoppers.
- `review/postgre-requirements`: аналогичное требование для сканеров `useFuncInIndCol` и `postgreLabelGotoLevel`.

## Impact

- `internal/review/review_rules.go` — 12 циклов построчного сканирования.
- `internal/review/*_test.go` — регрессионные тесты.
- `cmd/root.go` — `buildNumber` 1710 → 1711.
- Схема БД, публичные команды и MCP-контракт не меняются.

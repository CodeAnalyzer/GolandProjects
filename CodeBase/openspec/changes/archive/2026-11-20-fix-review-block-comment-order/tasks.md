## 1. Deploy-stoppers scanners

- [x] 1.1 В `checkIndexWrong` и `checkUpdateOnlyVar` перенести обработку закрытия `inBlockComment` выше `TrimSpace`/проверки `--`, при этом `trimmed` вычислять после мутации строки; проверить `go test ./internal/review -run 'IndexWrong|UpdateOnlyVar'`
- [x] 1.2 В `checkPTableSpid` и `checkForceOrder2Tbl` выровнять порядок блок-комментарий → `--` с пересчётом `trimmed` после закрытия; проверить `go test ./internal/review -run 'PTableSpid|ForceOrder2Tbl'`
- [x] 1.3 В `checkSaveTran`, `checkUseDrop`, `checkMathOperations` выровнять порядок блок-комментарий → `--` с пересчётом `trimmed` после закрытия; проверить `go test ./internal/review -run 'SaveTran|UseDrop|MathOperations'`
- [x] 1.4 В `checkExistsWithAndInIf` выровнять порядок, сохранив мутацию `lines[i]` (а не локальной `line`); проверить `go test ./internal/review -run 'ExistsWithAndInIf'`
- [x] 1.5 В `checkIndexExistsInDB` и `checkVarcharSize` выровнять порядок блок-комментарий → `--` с пересчётом `trimmed` после закрытия; проверить `go test ./internal/review -run 'IndexExistsInDB|VarcharSize'`

## 2. Postgre-requirements scanners

- [x] 2.1 В `checkUseFuncInIndCol` перенести обработку `*/` выше проверки `--`; проверить `go test ./internal/review -run 'UseFuncInIndCol'`
- [x] 2.2 В `checkPostgreLabelGotoLevel` перенести закрытие `*/` выше усечения по первому `--`, сохранив усечение остатка после блока; проверить `go test ./internal/review -run 'PostgreLabelGotoLevel'`

## 3. Regression tests

- [x] 3.1 Добавить unit-регрессию `checkIndexWrong` с шапкой `/*---` … `---*/` и `delete ... M_ROWLOCK_INDEX(XPK...) where SPID = @@spid and BankProductID = @BankProductID`; моки `XPK (SPID, AutoID)`/`XIE1 (SPID, BankProductID, AmountMin)`; проверить finding на строке `delete` с `Object = pbpconscoolingoff.xpkpbpconscoolingoff` и упоминанием `XIE1`
- [x] 3.2 Добавить table-driven кейсы с dashed-header для остальных 11 сканеров (9 deploy + `useFuncInIndCol` + `postgreLabelGotoLevel`), используя существующий harness (`validateRuleCases`/`assertRuleFindings`) и mock-seam'ы; проверить `go test ./internal/review -run 'BlockCommentHeader'`
- [x] 3.3 Добавить негативные кейсы: незакрытый `/*` скрывает код, строка `-- ...` вне блока игнорируется (включая `-- goto MyLabel` при активной метке `MyLabel:`), одиночный `/* ... */` в строке не ломает анализ; проверить те же тесты
- [x] 3.4 Прогнать `go test ./internal/review` и убедиться, что все существующие тесты правил зелёные

## 4. Build and acceptance

- [x] 4.1 Поднять `buildNumber` в `cmd/root.go` с `1710` до `1711`; проверить `go run . 2>&1 | Select-String 'build 1711'`
- [x] 4.2 Выполнить `go vet ./...` и `go build ./...`
- [x] 4.3 Пересобрать `CodeBase.exe`, выполнить `CodeBase.exe review "D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\Consumer\BPConsCoolingOff_Transfer.sql" --rules indexWrong --json` и зафиксировать finding `indexWrong` на строке 40 с рекомендацией `XIE1pBPConsCoolingOff`
- [x] 4.4 Выполнить `openspec validate "fix-review-block-comment-order" --strict` и зафиксировать успешную валидацию

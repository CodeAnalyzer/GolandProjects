## 1. Препроцессор: token-paste параметров

- [x] 1.1 В `applyMacroArguments`/`replaceIdentifierToken` (`internal/review/review_parser.go`) заменить подстановку параметра на `(?i)(?:##)?\b<regexp.QuoteMeta(param)>\b(?:##)?` с `ReplaceAllLiteralString`; проверить, что `ch##mx##`+`mx=1` → `ch1`, `sb##mx##.SPID` → `sb1.SPID`, а обычный `mx` → `1`
- [x] 1.2 Поглощать `##` только как часть совпадения с **объявленным параметром**; проверить, что `##GlobalRest`, `##OUTTABLE`, `##CURRENT_MODULEID##`, `@val##PPVARCNT##PPLINE`, `##_SHARED_SEED_##t##OBJECT##Seed` остаются без изменений
- [x] 1.3 Убедиться, что paste перед именем макроса (`sp.##M_LOGINTIME` → `sp.loggedindatetime`) продолжает работать; прогнать существующий `TestReplaceMacros_TokenPasteOperator`

## 2. Тесты препроцессора

- [x] 2.1 Unit-тест в `internal/review/review_parser_test.go`: `ch##mx##` → `ch1`, `sb##mx##.SPID` → `sb1.SPID`; тест зелёный
- [x] 2.2 Unit-тест: обычный параметр без `##` (`mx` → `1`) — базовая подстановка не регрессировала
- [x] 2.3 Unit-тест: подстрочный guard — параметр `id`, текст `LinkID` остаётся `LinkID`
- [x] 2.4 Unit-тест: односторонний paste `##PREFVAR##ObjectID` при `PREFVAR=m.` → `m.ObjectID`
- [x] 2.5 Unit-тест: пустой аргумент `__InterestValue__()` → `ObjectID` (без `##ObjectID`/`ObjectID##`)
- [x] 2.6 Unit-тест: `@##OBJECT##SeedID` при `OBJECT=User` → `@UserSeedID`
- [x] 2.7 Тест-гард: `##GlobalRest`, `##OUTTABLE`, `##CURRENT_MODULEID##`, `##PPVARCNT##`, `##_SHARED_SEED_##` не схлопываются
- [x] 2.8 Подтвердить red→green: на коде до правки новый тест из 2.1 падает, после правки — проходит (зафиксировать в описании теста/PR)

## 3. Тест правила и якоря

- [x] 3.1 Интеграционный тест в `internal/review/review_rules_test.go`: `INSERT…SELECT` из 12 `union all` через `#define` с `##mx##` не даёт `tableFullScan` по `pCON_COver_CheckPayStrategy`; тест зелёный
- [x] 3.2 Проверка якоря через `Runner` (`mapExpandedLineToOriginal`): finding внутри раскрытого оператора маппится на исходную строку `insert` (а не на строку вызова макроса)
- [x] 3.3 Проверить поведение смежного правила (`checkIndexWrong`) на `##`-алиасах: добавить один кейс/baseline-сравнение и убедиться, что ложных изменений нет

## 4. Приёмка

- [x] 4.1 Прогнать `go test ./internal/review/...` — все тесты проходят
- [x] 4.2 Поднять `buildNumber` в `cmd/root.go` с `"1697"` до `"1699"`, затем пересобрать бинарник (`go build ./...` или `go run .`) перед smoke, чтобы не использовать устаревший `CodeBase.exe`
- [x] 4.3 Smoke на **индексированном** файле: `codebase review D:/GITHUB/GolandProjects/FA/fa-contracts/API_Credit/Server/Callback/API_CON_RuleDoc_MassGetLimitAcc.sql --rules tableFullScan --json` — finding по `pcon_cover_checkpaystrategy` на строке 2092 отсутствует
- [x] 4.4 Baseline-диф до/после на `fa-contracts/Underwriting/USR_GET_*.sql` и `fa-warranty/*WarrantyInterest*.sql`: новые findings не появились, ожидаемые ложные исчезли
- [x] 4.5 Прогнать `go vet ./internal/review/...` (или принятый в проекте линтер) — без новых замечаний

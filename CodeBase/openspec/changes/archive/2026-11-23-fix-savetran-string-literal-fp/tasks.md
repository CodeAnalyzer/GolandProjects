## 1. Правка checkSaveTran

- [x] 1.1 В `checkSaveTran` (`internal/review/review_rules.go`, ~996) заменить `lower := strings.ToLower(line)` на `lower := strings.ToLower(stripQuotedSegments(line))` и убедиться, что `go build ./...` проходит
- [x] 1.2 Проверить `go vet ./internal/review/` и `gofmt -l internal/review/` (пусто)

## 2. Тесты

- [x] 2.1 Добавить кейс в `TestCheckSaveTran_Coverage` (`internal/review/review_rule_coverage_test.go`): контент со строкой `PROFILE_TIME_EX('BEGIN tran and SAVE tran trnCardAccRelation_MassLoad...')` и `__BEGIN_TRAN__(trnCardAccRelation_MassLoad)` → `wantCount: 0` (сценарий «SAVE TRAN в строковом литерале»)
- [x] 2.2 Добавить кейс туда же: строка `select @s = 'save tran ''quoted'' text'` плюс отдельной строкой `SAVE TRAN MySavePoint` → `wantCount: 1`, `wantLine` на строке настоящего оператора (сценарий «Экранированный апостроф в литерале и настоящий SAVE TRAN»)
- [x] 2.3 Прогнать `go test ./internal/review/` — все существующие тесты (включая блочно-комментарийные кейсы saveTran из review_block_comment_test.go) зелёные

## 3. Верификация на реальных данных

- [x] 3.1 Поднять buildNumber в `cmd/root.go` со значения "1715" до "1716" и пересобрать CodeBase.exe
- [x] 3.2 Прогнать `codebase review D:\GITHUB\GolandProjects\FA\fa-cards\Cards\SERVER\Card\CardAccRelation_MassLoad.sql --rules saveTran` → 0 findings (было 1 на строке 130)
- [x] 3.3 Контроль полноты: найти в корпусе FA файлы с настоящим `SAVE TRAN` вне литералов (grep) и убедиться, что они по-прежнему флагуются; файлы с `save tran` только в литералах — не флагуются

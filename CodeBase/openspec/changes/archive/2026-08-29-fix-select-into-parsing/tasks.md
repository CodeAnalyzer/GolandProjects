## 1. Фикс парсера — блочные комментарии

- [x] 1.1 Добавить `continue` после `inBlockComment = true` в `sql_parser.go` (~строка 766). Verify: строка `/* create table ...` не матчит `createTableRe`, `inCreateTableDefinition` не устанавливается.
- [x] 1.2 Добавить регрессионный тест: SQL с `/* create table tFoo ... */` затем `SELECT ... INTO tFoo` — убедиться что колонки имеют `definition_kind = select_into`, а не `create_table`. Verify: `go test ./internal/parser/sql/... -run TestBlockCommentCreateTable -v`.

## 2. Фикс парсера — ELSE в isKeyword

- [x] 2.1 Добавить `"ELSE": true` в карту ключевых слов `isKeyword` в `sql_parser.go` (~строка 1856). Verify: `isKeyword("else")` возвращает `true`.
- [x] 2.2 Добавить регрессионный тест: SQL с CASE-выражением, содержащим `else ""` — убедиться что `else` не появляется как columnName. Verify: `go test ./internal/parser/sql/... -run TestCaseElseKeyword -v`.

## 3. Фикс парсера — -- комментарии в splitSQLByTopLevelComma

- [x] 3.1 Модифицировать `splitSQLByTopLevelComma` в `sql_parser.go` (~строка 157): при обнаружении `--` вне `inSingleQuote` обрезать текст до конца строки (завершить текущий сегмент). Verify: запятая в `--- comment, text` не создаёт лишний сегмент.
- [x] 3.2 Добавить регрессионный тест: проекция с `tal.NodeID, --- comment with, comma` — убедиться что `splitSQLByTopLevelComma` возвращает `tal.NodeID` как один сегмент. Verify: `go test ./internal/parser/sql/... -run TestSplitSQLByTopLevelComma_DashComment -v`.

## 4. Интеграционный тест на patch7_2_1056.sql

- [x] 4.1 Добавить интеграционный тест, парсящий фрагмент SQL из `patch7_2_1056.sql` (строки 373-455) и проверяющий: все колонки `tConsRuleAccSync` имеют `definition_kind = select_into`, ни одна не имеет `definition_kind = create_table`, `else` не среди columnName, `lat.Brief` не среди columnName (вместо него `TypeRule`). Verify: `go test ./internal/parser/sql/... -run TestSelectIntoPatch7_2_1056 -v`.

## 5. Валидация

- [x] 5.1 Запустить `go build ./...` и `go vet ./...`. Verify: без ошибок.
- [x] 5.2 Запустить `go test ./internal/parser/sql/... -v`. Verify: все тесты PASS, включая новые.

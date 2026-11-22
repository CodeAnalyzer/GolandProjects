## 1. Хелпер маскировки

- [x] 1.1 Реализовать `maskSQLComments(text string) string` в `internal/review/review_parser.go` (рядом с `sanitizeSQLForMacroScan`, при возможности — с общим ядром переходов): один проход по `[]byte`, string-aware (`''`-экранирование, состояние литерала переносится через `\n`), `/* ... */` и `-- ...` до конца строки; каждый байт комментария → `' '`, `\n` и `\r` сохраняются, `len(результат) == len(вход)` в байтах. Юнит-тесты: многострочный блок; `*//*` подряд; вложенный `/*` внутри блока; `--` и `/*` внутри литерала (`'a--b'`, `'/*x*/'`); литерал без закрывающей кавычки, открытый на одной строке (состояние через `\n`); незакрытый блок; CRLF; кириллический комментарий перед триггером — байтовая длина сохранена и `Line` совпадает с исходной; документирование принятого ограничения `[a--b]`/`"a--b"` (bracket-quoted идентификаторы) в комментарии хелпера
- [x] 1.2 Сверить полный список вызовов `splitStatementsWithOffsets` (grep) и зафиксировать, что все потребители — правила review (нет внешних зависимостей от текста комментариев); результат — комментарий в коде хелпера

## 2. Операторное семейство (splitStatementsWithOffsets)

- [x] 2.1 Применить `maskSQLComments` на входе `splitStatementsWithOffsets` (review_parser.go:1682); проверить юнит-тестами: фантомный оператор из `/* update ... */` и `-- select ...` не создаётся, `offset` и `lineNo` совпадают с домаскировочными для кода вне комментариев
- [x] 2.2 Добавить table-driven регрессии по образцу `review_block_comment_test.go` для `deferredUpdate`, `memoryUniqueIndexUpdate`, `inSubQuery`, `tooManyJoins`: триггер внутри `/* */` → 0 findings; тот же триггер вне комментария → finding сохраняется (wantLine/wantObject)

## 3. Построчные сканеры

- [x] 3.1 `checkInsertRowLock` (review_rules.go:3122): маскировать контент после `fileContent` до `strings.Split`; тесты: INSERT внутри `/* */` → 0 findings (кейс TmpTbl_GLReports), `;` и `union all` внутри комментария не завершают буфер, реальный INSERT с комментарием между `insert` и `select` → finding с корректным Line/Object
- [x] 3.2 `checkUseSelectAll` (review_rules.go:2493): маскировать `fileProcessedContent`; тесты: `select *` внутри `/* */` → 0 findings, хвостовой `-- ... select * from X` на строке кода → 0 findings, реальный `select *` → finding
- [x] 3.3 `checkTruncTbl` (review_rules.go:2528): маскировать `fileContent`; тесты: `truncate table` внутри `/* */` → 0 findings, реальный → finding

## 4. Курсорные парсеры (varUseAfterCursor)

- [x] 4.1 Применить `maskSQLComments` на входе `text` в `checkVarUseAfterCursor` (review_rules.go:5181) — пробел подтверждён: внешний цикл `parseAllFetchIntoStatements` (review_parser.go:465-473), `parseDeallocateStatements` (:435-443) и `parseCursorDeclarations` (:328-352) матчат сырые строки без состояния блока. Регрессии: закомментированные `fetch`/`deallocate`/`declare cursor` внутри `/* */` не считаются операторами; реальный `fetch` с комментарием в спане по-прежнему распознаётся; отметить в комментарии к правке, что расширение skip-спана `fetchVariablesLineEnd` по упоминанию переменной в комментарии после маскировки неактуально, но непротиворечиво

## 5. E2E и приёмка

- [x] 5.1 E2E: `codebase review FA/fa-generalledger/GeneralLedger/SERVER/Reporting/Operational/TmpTbl_GLReports.sql --min-severity 1` → 0 findings (до фикса: insertRowLock line=68)
- [x] 5.2 Приёмка на корпусе: регенерировать список fa-contracts файлов с `insert` внутри `/* */` (247 файлов), прогон 12 raw-правил `--files <list> --json --min-severity 1`; сверка с базой до фикса: findings внутри `/* */`-спанов (87) → 0; остальные findings идентичны построчно по `rule:line`, `Object` — с нормализацией хвостовых комментариев (например первая строка оператора у `tooManyJoins`); ожидаемые новые истинные находки (устранение FN вида `truncate /* c */ table t`) перечислить явно до прогона и сверить со списком после
- [x] 5.3 Прогнать полный пакет тестов `go test ./internal/review/... ./cmd/...` без регрессий

## 6. Сборка и финализация

- [x] 6.1 Поднять `buildNumber` 1712 → 1713 в `cmd/root.go`, пересобрать `CodeBase.exe` и подтвердить баннер `0.9.5 build 1713`
- [x] 6.2 `openspec validate fix-review-block-comment-masking --strict` — без ошибок; задачи 1–6.1 отмечены выполненными

## 1. Helper срезания неявного алиаса

- [x] 1.1 Добавить в `internal/review/review_parser.go` helper `stripImplicitSelectAlias(expr string) string`: снять ведущие модификаторы `DISTINCT`/`ALL`/`TOP n [PERCENT] [WITH TIES]`, затем на верхнем уровне (с учётом скобок и строковых литералов) найти хвостовой идентификатор-алиас при выполнении guard'а по символу перед пробелом (`[A-Za-z0-9_)\]]` и закрывающая кавычка); проверить `go build ./internal/review/...`
- [x] 1.2 Unit-тесты helper: `t.c alias` и `isnull(a,0) total` и `1 flag` и `case ... end x` срезаются; `a + b` и `a * b` не срезаются; `TOP 10 col` → `col`, `TOP (@n) PERCENT WITH TIES col` → `col`, `DISTINCT col` → `col`; проверить `go test ./internal/review/ -run TestStripImplicitSelectAlias -count=1`

## 2. Интеграция в правило и смежные FP-классы

- [x] 2.1 В `checkStatementsWithJoinsRequireAliases` (`internal/review/review_rules.go`, select-ветка при `!isUpdate`) применять `stripImplicitSelectAlias` после срезания явного `AS`-алиаса и перед `findAllUnqualifiedColumnRefs`; `lineOffset` для finding вычислять от исходной (до среза) части, так как `findColLineOffsetInPart` ищет подстроку в `queryText`; проверить, что существующие тесты правила `go test ./internal/review/ -run StatementsWithJoinsRequireAliases -count=1` проходят
- [x] 2.2 Убедиться, что WHERE/ON и UPDATE SET не затронуты (helper не вызывается для неявного алиаса): `TestCheckStatementsWithJoinsRequireAliases_UnqualifiedInWhere_Finding`, `_UnqualifiedInOn_Finding`, `_Update*` остаются зелёными
- [x] 2.3 Assignment-style алиас T-SQL (`alias = expression`) в SELECT: анализировать только RHS (`reParserSelectAssignAlias`), LHS не считать ссылкой
- [x] 2.4 Не считать ссылками строки препроцессора `#ifdef/#ifndef/#else/#elif/#endif/#define/#undef/#include` (с аргументами) и аргументы `M_DELETE_PTABLE[_INDEX/_SPID_INDEX](...)`
- [x] 2.5 Квалифицированная ссылка с пробелами вокруг точки (`alias. column`) — расширить `reParserQualifiedRef` (`[ \t]*` вокруг точки)
- [x] 2.6 Не сканировать ORDER BY (может ссылаться на выходные алиасы): обрезать `wherePart` по top-level `order`
- [x] 2.7 Сделать `truncateAtStatementBoundary` CASE-aware (отслеживать `caseDepth`, как `findTopLevelKeywordPosition`), чтобы top-level `CASE…END` не обрезался до `from`
- [x] 2.8 Второй потребитель `truncateAtStatementBoundary` — `checkUsageVarInSameSelect`: срезать комментарии (`removeComments`) до разбора assignment'ов, чтобы `--@var` в конце выражения не считался чтением переменной

## 3. Тесты правила

- [x] 3.1 Добавить регресс-тест по эталону: `SELECT dt.FundID as flFund, u.Brief meUser FROM hDealTransact dt, pAPI_User_FindLst u WHERE dt.UserID = u.UserID` → 0 findings по `meUser`
- [x] 3.2 Добавить негативные сценарии из спеки: неявный алиас функции/литерала (`isnull(t.b,0) total`, `1 flag`) и `DISTINCT tA.a myAlias` не дают findings
- [x] 3.3 Добавить позитивные/guard-сценарии: `SELECT col alias FROM ... JOIN` → finding только `col`; `SELECT a + b FROM ... JOIN` → findings `a` и `b`; `SELECT TOP 10 col ...` и `SELECT DISTINCT col ...` → finding `col`
- [x] 3.4 Добавить регресс-тесты `CASE…END` (без алиаса и с неявным алиасом): `case when a then b else c end [myAlias]` → findings по `a`, `b`, `c`
- [x] 3.5 Добавить регресс-тесты смежных FP-классов: assignment-style алиас, препроцессорные директивы, пробел вокруг точки, аргумент `M_DELETE_PTABLE(...)`, ORDER BY-алиас
- [x] 3.6 Прогнать `go test ./internal/review/... -count=1` — всё зелёное
- [x] 3.7 Добавить регресс-тест `usageVarInSameSelect`: `@var` внутри `--`-комментария в assignment'е не даёт finding (`TestCheckUsageVarInSameSelect_VarInComment_NoFinding`)

## 4. Приёмка на реальных данных

- [x] 4.1 Пересобрать `CodeBase.exe`; проверить успешную сборку
- [x] 4.2 `codebase review ...\NonBalVal_SelectHistory.sql` с `min-severity` по умолчанию (severity правила = 2; **не** `--min-severity 1`, который его скрывает) → остаётся только finding `ansiInJoin`, объект `meUser` отсутствует
- [x] 4.3 Замер по корпусу `D:\GITHUB\GolandProjects\FA\fa-payments\Payments\SERVER` (4472 файла), `min-severity` по умолчанию: `statementsWithJoinsRequireAliases` before=138 → after=87 (−51), новых 0; `usageVarInSameSelect` before=0 → after=0, новых 0; целевые файлы (`PlatDocumSelectGet`, `RKO_CalcPercent`, `ProcessSW101`, `MT940_MassReadAll86`, `CBAContractInfoDoc_Update`) → 0 findings (артефакты в `Tests/Temp/joins_before.json`, `joins_after2.json`, `cross_before.json`, `cross_after2.json`)
- [x] 4.4 `gofmt -l` по изменённым файлам пусто; `go test ./...` без FAIL

## 5. Финализация

- [x] 5.1 Bump `buildNumber` в `cmd/root.go` с `"1717"` до `"1718"` и пересобрать `CodeBase.exe`
- [x] 5.2 `openspec validate fix-statements-with-joins-implicit-alias-fp --strict` проходит без ошибок

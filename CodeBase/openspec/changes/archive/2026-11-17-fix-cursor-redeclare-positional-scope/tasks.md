# Tasks: fix-cursor-redeclare-positional-scope

## 1. Парсер: позиционные структуры курсоров

- [x] 1.1 Добавить `Line int` в `cursorDeclaration` (types.go) и новый тип `fetchStatement{CursorName string; Variables []string; LineStart, LineEnd int}`; перевести `parseCursorDeclarations` (review_parser.go) на возврат `[]cursorDeclaration` в порядке следования; `Line` — номер строки DECLARE/`__DECLARE_CURSOR__` (`i+1` до мутации `i = j - 1` в цикле продолжений, иначе уедет на строку перед границей); проверить `go build ./...`
- [x] 1.2 Унифицировать разбор FETCH в `parseAllFetchIntoStatements`: собирать строки оператора до `isCursorDeclarationBoundary` и прогонять через `parseFetchIntoStatement` (поддержка `into` на следующей строке, формы `FETCH c` / `__FETCH_NEXT__ c`); вернуть `[]fetchStatement`, где `LineEnd` — номер последней строки, реально давшей переменную (не индекс выхода цикла сбора); покрыть unit-тестами спаны: однострочный FETCH, многострочный FETCH, `into` на новой строке, FETCH перед `if`, FETCH в конце файла; проверить `go build ./...`
- [x] 1.3 Добавить обёртку name-set (map по имени из списка) для `isTableExistsNoiseName` и адаптировать вызовы `parseCursorDeclarations` в `checkTableExistsInMetadata`, `checkViewUsageInServerCode` (поведение name-set не меняется); обновить `TestParseCursorDeclarations_ExplicitAndMacro` (review_rules_test.go:1251) на новый тип; проверить `go test ./internal/review/`
- [x] 1.4 Добавить хелпер `nearestCursorDeclaration(decls []cursorDeclaration, name string, line int) (cursorDeclaration, bool)` — последняя декларация имени с `Line <= line`; покрыть unit-тестом (нет декларации выше / одна / несколько / декларация на той же строке)

## 2. Правила: позиционное сопоставление

- [x] 2.1 `checkCursorFetchArguments`: заменить lookup в map на `nearestCursorDeclaration`; строку FETCH вычислять по смещению префикса в `fragment.QueryText` (`fetchLine = fragment.LineNumber + strings.Count(queryText[:offset], "\n")`, offset из `reParserFetchIntoPrefix.FindStringSubmatchIndex` — `fragment.LineNumber` может указывать на склеенный `open`); FETCH без декларации выше — skip; покрыть тестами сценарии дельты (неск. жизней без рассинхрона, рассинхрон внутри жизни, FETCH выше декларации) и склеенный `open`+`fetch`
- [x] 2.2 `checkDatatypeFetchInto`: то же сопоставление (включая вычисление строки FETCH) для спаривания `Variables[i] ↔ SelectExpressions[i]` и `FromClause`; покрыть тестом сценарий mispairing из дельты fine-code
- [x] 2.3 `checkVarUseAfterCursor`: добавить `decls := parseCursorDeclarations(text)` на том же expanded-тексте, что `parseDeallocateStatements`/`parseAllFetchIntoStatements` (`rawFileContent` не использовать); окно после DEALLOCATE ограничить следующей декларацией того же имени ниже (`Line > dealloc.Line`; скан при 0-based `i` — пока `i+1 < nextDecl.Line`); множество переменных — из FETCH'ей текущей жизни (между предыдущей декларацией и dealloc); строки в спанах FETCH'ей этого курсора не сканировать; добавить дедупликацию `seen` с ключом `line|normalizedVar|cursor`; покрыть тестами (переобъявление, многострочный FETCH с `into` на новой строке после DEALLOCATE, базовый сценарий CLOSE+SET сохраняется; существующие тесты review_rules_test.go:3153–3293 остаются зелёными)

## 3. Приёмка

- [x] 3.1 Прогнать `go test ./...` (весь пакет review зелёный, включая существующие тесты правил курсоров)
- [x] 3.2 Приёмка по эталону: review на `FA/fa-administrator/Administrator/Server/Core/InstrumentSO_LoadStates.sql` (при запуске через `go run` обязательно `--config`, иначе `config not loaded`; либо установленный binary `codebase`) — с 56 находок остаются ровно 11 с разбивкой по правилам: 4 `inSubQuery` (214 ×2, 251, 263), 2 `useFuncInIndCol` (434, 451), 1 `execNotExistsProc` (591), 3 `datatype` вне курсоров (350 ×2, 369), 1 `varUseAfterCursor` `@ChangedBrief` (652 — известный substring-дефект `@ChangedBriefs`, вне скоупа); 45 курсорных FP (2+41+2) отсутствуют
- [x] 3.3 `openspec validate fix-cursor-redeclare-positional-scope` — без ошибок

## 4. Ревью-фиксы (code review)

- [x] 4.1 (B1) Сбор строк FETCH останавливается на строке нового SQL-оператора (`isFetchStatementStopLine`: select/exec/execute/print/set/insert/update/delete/goto), строки соединяются через `\n`, `reParserFetchIntoBoundary` расширен теми же ключевыми словами — следующий оператор больше не попадает в `Variables`/`LineEnd`; тесты: `fetch c into @a` + `select @a` (спан без захвата), `fetch c into @a` + `select @x, @y` (чужие переменные не попадают), то же с `into` на новой строке
- [x] 4.2 (B2) `gofmt -w` по всем изменённым файлам; `gofmt -l internal/review/` чист
- [x] 4.3 (M1) Тест окна DEALLOCATE усилен повторным использованием старой переменной `@a` в новой жизни — без ограничения окна тест падает
- [x] 4.4 (M2) Тест спанов/дедупа перестроен на валидной декларации следующей жизни: `MultilineFetchIntoNextLife_NoFinding` (0 находок), `OverlappingDeallocateWindows_Dedup` (ровно 1), `FetchAfterDeallocate_SpansSkipped` (валидный сценарий пользы спанов — FETCH ниже финального DEALLOCATE)
- [x] 4.5 (M3) Декларации с неразобранным SELECT (`select 1` без FROM) остаются граничными записями с пустыми `SelectExpressions`; `checkCursorFetchArguments` пропускает их (арность неизвестна); тесты `UnparseableSelect_KeepsBoundaryRecord` и `UnparseableDeclaration_NoFinding`; решение зафиксировано в design.md (Decision 6)
- [x] 4.6 Регресс после ревью-фиксов: `go test ./...` зелёный, приёмка на эталоне — ровно 11 находок с прежней разбивкой, `openspec validate --strict` без ошибок

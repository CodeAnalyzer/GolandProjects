# Code review: fix-statements-with-joins-implicit-alias-fp

- **Объект ревью:** реализация change `fix-statements-with-joins-implicit-alias-fp` (рабочее дерево, не закоммичено)
- **Изменённые файлы:** `cmd/root.go` (build 1718), `internal/review/review_parser.go` (+172), `internal/review/review_parser_test.go` (+32), `internal/review/review_rules.go` (+19/−7), `internal/review/review_rules_test.go` (+200), `CodeBase.exe`
- **Дата:** 2026-11-24
- **Статус:** APPROVED WITH ONE FOLLOW-UP — код соответствует плану, приёмка воспроизводится; найдена **одна кросс-правильная регрессия (FP в `usageVarInSameSelect`, 0 → 1 на корпусе)**, вскрытая CASE-aware-правкой и не покрытая заявленной приёмкой. Требует решения: точечный фикс здесь или отдельный change (см. C1).

## Методика проверки

- Полное чтение diff всех изменённых файлов; трассировка всех потребителей изменённых shared-сущностей.
- `openspec validate --strict` → valid; `go test ./...` → все пакеты ok; `gofmt -l` по изменённым файлам → пусто; баннер `0.9.5 build 1718`.
- Независимое воспроизведение приёмки: собран «до»-бинарь из HEAD (worktree, `GOWORK=off`), прогнаны оба затронутых правила по `server_sql_files.txt` (4472 файла) «до» и «после» (артефакты: `Tests/Temp/cross_before.json`, `cross_after.json`).
- Диффы множеств findings по ключу (rule, file, line, object); сверка с артефактами пользователя (`joins_before.json`, `joins_after.json` — промежуточный, `joins_after2.json`).

## Соответствие плану (design D1–D7 → код)

| План | Реализация | Оценка |
|---|---|---|
| D1 guard по символу перед пробелом | `stripImplicitSelectAlias` (`review_parser.go:2419`): хвостовой идентификатор, пробельный разделитель, `isValueEndByte` (идент-символ/`)`/`]`/кавычки), top-level + вне строк (`isTopLevelAt`), исключение keyword/function/type у кандидата | соответствует |
| D3 модификаторы `DISTINCT/ALL/TOP n (@n) [PERCENT] [WITH TIES]` | `stripSelectModifiers` + `reParserSelectTopPrefix`; снятие в цикле, строгие границы слова | соответствует |
| D4 scope — только select-ветка при `!isUpdate` | `review_rules.go:6270-6281`; WHERE/ON/UPDATE SET не задеты (тесты 2.2 зелёные) | соответствует |
| R6 (lineOffset от исходной части) | `offsetPart` сохраняется до среза (`review_rules.go:6268`), `findColLineOffsetInPart(queryText, offsetPart, …)` | учтено |
| D6 CASE-aware `truncateAtStatementBoundary` | `caseDepth` как в `findTopLevelKeywordPosition` (`review_parser.go:2552-2580`) | соответствует |
| D7 assignment-алиас | `reParserSelectAssignAlias` (LHS плейн-идентификатор, `@var`/`case when a = b` не попадают), анализируется RHS (`review_rules.go:6272`) | соответствует |
| D7 препроцессор / M_DELETE_PTABLE | `reParserPreprocessorDirective`, `reParserDeletePTableMacro` в `findAllUnqualifiedColumnRefs`; `m_delete_ptable*` как границы оператора | соответствует |
| D7 пробел вокруг точки | `reParserQualifiedRef` расширен `[ \t]*` вокруг точки; регэксп используется только `findAllUnqualifiedColumnRefs` (проверено) | соответствует |
| D7 ORDER BY | `wherePart` обрезается по top-level `order` (`review_rules.go:6225-6229`) | соответствует |

Тесты: `TestStripImplicitSelectAlias` (15 кейсов, включая негативные `a + b`, `dt.Confirmed + 1`, `case…end` без алиаса) и `TestCheckStatementsWithJoinsRequireAliases_ImplicitAliasScenarios` (15 сценариев — покрывают все 14 сценариев дельты + CASE-регресс). Существующие тесты правила остались зелёными.

Замечаний к качеству кода нет: границы в `truncateAtStatementBoundary` матчятся с word-boundary-суффиксом (`m_delete_ptable_index(…)` не ложно срезается более коротким `m_delete_ptable`); `stripSelectModifiers` строго укорачивает строку (зацикливание невозможно); нераспознанный `TOP`-префикс консервативно не срезает алиас (остаточный FP, не FN).

## Независимая верификация приёмки

| Метрика | Заявлено | Воспроизведено |
|---|---|---|
| Эталон `NonBalVal_SelectHistory.sql` | только `ansiInJoin`, `meUser` нет | ✅ `count=1`, `ansiInJoin` line 73 |
| Корпус SERVER, `statementsWithJoinsRequireAliases` | before=138 → after=87, новых 0 | ✅ до-бинарь (HEAD): 138; текущий: 87; дифф: **new=0, gone=51** |
| Целевые файлы → 0 | ✅ (PlatDocumSelectGet 25→0; RKO_CalcPercent/ProcessSW101/MT940_MassReadAll86 в final 0) | ✅ |
| `go test ./...`, gofmt, validate | green/чисто/valid | ✅ |

Исчезнувшие 51 finding распределены по 11 файлам (включая эталонный `meUser`). Промежуточный артефакт `joins_after.json` (153) корректно документирует траекторию «CASE-aware вскрыл замаскированные FP (4 целевых файла: 37+12+1+1=51) → D7 закрыл». Артефакты согласованы между собой; `min-severity` в замерах дефолтный (severity правила = 2).

## Замечания

### C1 (High, принять) — кросс-правильная регрессия: FP в `usageVarInSameSelect` (0 → 1 на корпусе)

Независимый прогон обоих потребителей `truncateAtStatementBoundary`:

- до (HEAD/1717): `statementsWithJoinsRequireAliases=138`, `usageVarInSameSelect=0`;
- после (1718): `statementsWithJoinsRequireAliases=87`, **`usageVarInSameSelect=1`**.

Finding: `CBAContractInfoDoc_Update.sql:646`, объект `@CashClientID`, сообщение «Переменная flagcash используется в выражении для @CashClientID…».

Механизм (подтверждён по коду и исходнику):

1. До правки `truncateAtStatementBoundary` резал SELECT на первом top-level `end` (строка 637, конец CASE у `@CBATariffPlanID`) — assignment'ы на строках 639 (`@FlagCash`) и 646 (`@CashClientID`) не анализировались.
2. CASE-aware (D6) перестал резать → оба assignment'ы видны.
3. В выражении для `@CashClientID` переменная `@FlagCash` встречается **только внутри `--`-комментария** (`… --@FlagCash`), а `checkUsageVarInSameSelect` не срезает комментарии ни в одном из путей разбора (`parseSelectAssignStatement`, ручной split, `parseCasePartAssignments`), и `containsVarReference` (`review_helpers.go:2378`) матчит `@FlagCash\b` прямо по сырому тексту. Реально используется `@FFlagCash` — другой идентификатор, подстрочного пересечения нет.

Это не «новый реальный finding», а **ложное срабатывание**, вскрытое расширением анализируемого множества — ровно тот класс эффекта, который D6 предсказывал («затрагивает обоих потребителей»), но приёмка в tasks 4.3 замеряла только `statementsWithJoinsRequireAliases`, поэтому заявленное «новых findings = 0» верно лишь в рамках одного правила.

**Решение (бинарное):**

- Вариант 1 (рекомендую): точечный фикс в этом же change — срезать комментарии в `checkUsageVarInSameSelect` до разбора assignment'ов (например, `queryText = removeComments(queryText)` после `truncateAtStatementBoundary`, либо на уровне expression). `removeComments` (`review_lookup.go:856`) заменяет комментарии пробелами с сохранением `\n`/`\r` — нумерация строк и `findAssignmentLineOffset` не пострадают. Плюс регресс-тест на кейс «`--@Var` в комментарии внутри SELECT с CASE» и допроход корпуса по `usageVarInSameSelect` (ожидание: 1 → 0, новых 0). Затрагивает один потребитель, правка аддитивная — соответствует стилю change.
- Вариант 2: вынести в отдельный change (латентный дефект «comments not stripped» существовал и раньше). Тогда в proposal/tasks 4.3 формулировку «новых findings = 0» необходимо уточнить как «по правилу statementsWithJoinsRequireAliases» и зафиксировать кандидата-последователя.

В любом из вариантов текущая формулировка приёмки в артефактах неточна и должна быть скорректирована.

### C2 (Low, принять) — формулировка приёмки

`proposal.md` (Impact: «замер … новых findings 0») и `tasks.md` 4.3 («новых findings = 0») не указывают область действия. Привязать к правилу и (после C1) добавить `usageVarInSameSelect` в замер.

### C3 (Low, принять как документацию) — консервативное исключение кандидата-алиаса

`stripImplicitSelectAlias` не срезает алиас, если его имя совпадает с keyword/function/type (например, реальный алиас `left`). Направление безопасное (остаточный FP, не FN), но это поведение не отражено в Risks design. Добавить одну строку в Risks.

### C4 (Info) — наблюдения без действий

- ORDER BY-обрезка применяется только к `wherePart`; `GROUP BY` по-прежнему сканируется (квалифицированные колонки — без finding), семантика не изменилась и в дельте зафиксирована корректно.
- `reParserSelectAssignAlias` корректно исключает `@var = …` и `case when a = b …` (LHS-charset и префикс `case`), `(?is)` + `$` без `m` не дают ложных межстрочных матчей.
- `isTopLevelAt` не отслеживает CASE — для позиции алиаса это несущественно (CASE не меняет paren-depth).
- `reParserPreprocessorDirective` применяется до удаления комментариев и строковых литералов; пересечения с ними на корпусе не выявлены (new=0).

## Вердикт

Реализация добротная, соответствует уточнённым артефактам (все замечания предыдущего change-ревью R1–R6 учтены: min-severity зафиксирован, обоснование нового helper'а добавлено в D1, `UPDATE SET` в требовании и сценарии, пометка об историческом заголовке, CASE-риски, offsetPart). Приёмка воспроизводится независимо. Единственное обязательное действие — закрыть C1 (кросс-правильный FP `usageVarInSameSelect`): рекомендованный путь — точечный фикс в этом change с регресс-тестом и допроходом корпуса, либо явное оформление последователем с корректировкой формулировок приёмки.

## Артефакты ревью

- `Tests/Temp/cross_before.json` — прогон HEAD (1717) по 4472 файлам, правила `statementsWithJoinsRequireAliases,usageVarInSameSelect`.
- `Tests/Temp/cross_after.json` — то же для build 1718.
- Worktree для «до»-бинарья удалён после замеров.

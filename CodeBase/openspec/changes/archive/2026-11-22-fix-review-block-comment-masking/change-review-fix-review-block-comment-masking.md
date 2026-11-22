# Ревью change `fix-review-block-comment-masking`

Дата: 2026-11-22.
Объект ревью: артефакты `proposal.md`, `design.md`, `tasks.md`, дельта-спеки
`specs/review/deploy-stoppers/spec.md`, `specs/review/postgre-requirements/spec.md`,
`.openspec.yaml`; код, который планируется изменить (`internal/review/review_rules.go`,
`internal/review/review_parser.go`, `internal/review/review_helpers.go`, тесты пакета
`internal/review`, `cmd/root.go`); реальный кейс
`FA/fa-generalledger/GeneralLedger/SERVER/Reporting/Operational/TmpTbl_GLReports.sql`.

## Итог

Change **готов к реализации после двух существенных уточнений**; остальное —
улучшения формулировок и тестов, реализацию не блокируют.

- **F1 (существенное).** `maskSQLComments` обязан сохранять **байтовую** длину, а не
  только количество `\n`. Иначе `offset`-арифметика `splitStatementsWithOffsets` и
  вычисление `Line` по исходному тексту разъедутся на кириллице (файлы review
  декодируются CP866→UTF-8, `runner.go:163`).
- **F2 (существенное).** Пробел в `checkVarUseAfterCursor` **подтверждён кодом**, а не
  «возможен»: `parseAllFetchIntoStatements` (`review_parser.go:465-473`) и
  `parseDeallocateStatements` (`:435-443`) матчат сырые строки без учёта состояния
  блока. Значит, задача 4.1 — это реализация маскировки, а не verify-only; иначе
  добавленное требование дельты `postgre-requirements` не будет выполнено.

`openspec validate fix-review-block-comment-masking --strict` — **valid**
(перепроверено в ходе ревью). Ссылки на номера строк в proposal/design для build 1712
точны (см. §3).

## 1. Объём ревью

- Артефакты change (proposal/design/tasks/две дельты/`.openspec.yaml`).
- Код-цели: `checkInsertRowLock`, `checkUseSelectAll`, `checkTruncTbl`,
  `splitStatementsWithOffsets` и его 4 потребителя, `checkVarUseAfterCursor` и его
  парсеры, хелперы `maskBlockCommentsKeepLines`/`stripLineComments`/
  `sanitizeSQLForMacroScan`, `findInsertWithoutColumns`.
- Данные: реальный файл-триггер FA, существующие тесты пакета (в т.ч. негативные
  кейсы `insertRowLock`/`truncTbl`/`useSelectAll`), `buildNumber`.
- Валидатор OpenSpec (`--strict`).

## 2. Валидация и согласованность артефактов

- `openspec validate --strict` — valid; `openspec show --deltas-only` распознаёт
  ровно 2 дельты, обе `ADDED`.
- Дельты: заголовки `## ADDED Requirements` (уровень 2), `### Requirement` (уровень 3),
  сценарии GIVEN/WHEN/THEN, английские SHALL/MUST в теле — соответствует
  `openspec/AGENTS.md`. Пути дельт зеркалят основные спеки.
- proposal ↔ design ↔ tasks согласованы: один и тот же набор из 7 сканеров
  (`insertRowLock`, `useSelectAll`, `truncTbl`, `deferredUpdate`,
  `memoryUniqueIndexUpdate`, `inSubQuery`, `tooManyJoins`) + verify для
  `varUseAfterCursor`; build 1712→1713 одинаково в proposal и tasks 6.1; точки
  вставки (`splitStatementsWithOffsets`, построчные сканеры) совпадают.
- Категории правил в дельтах соответствуют severity: `tooManyJoins` —
  `SeverityPostgreReq`, остальные шесть — `SeverityDeployStopper` (проверено по
  сигнатурам `Finding` в `review_rules.go`).
- Небольшое расхождение трактовки `varUseAfterCursor`: дельта `postgre-requirements`
  формулирует поведение как обязательное, а design/tasks — как условное
  («маскировка добавляется только при подтверждённом пробеле»). Поскольку пробел
  подтверждён (§5, F2), расхождение нужно снять в пользу реализации.

## 3. Проверка заявлений артефактов по коду

| Заявление артефакта | Проверка по коду | Статус |
|---|---|---|
| `checkInsertRowLock` (review_rules.go:3122) пропускает только `--`-строки, `isInComment` — только `--` в той же строке | `:3142-3144` префиксный `--`; `:3150-3152` `findKeywordPosition` + `!isInComment && !isInStringLiteral`; `isInComment` (`review_helpers.go:776-784`) проверяет только `--` | ✔ |
| `checkUseSelectAll` (2493), `checkTruncTbl` (2528) — regex по сырой строке, хвостовой `--` не маскируется | `:2501-2513` (`fileProcessedContent`, `selectAllRe`), `:2536-2548` (`fileContent`, `truncateTblRe`); `selectAllRe`/`truncateTblRe` — `types.go:132,135` | ✔ |
| `splitStatementsWithOffsets` (review_parser.go:1682) отслеживает строки и скобки, но не комментарии | `:1683-1722` — есть `inString`/`depth`, нет состояния `/* */`/`--`; `offset` — байтовый индекс (`text[startIdx:i]`) | ✔ |
| 4 потребителя сплиттера — только правила review | `review_rules.go:4232` (tooManyJoins), `:4432` (deferredUpdate), `:4552` (memoryUniqueIndexUpdate), `:4628` (inSubQuery); иных вызовов grep не находит | ✔ |
| `checkDeferredUpdate` 4424, `checkMemoryUniqueIndexUpdate` 4544, `checkInSubQuery` 4620, `checkTooManyJoins` 4225 | Все строки совпадают; `Line` считается по исходному `text[:offset]` (`:4241`, `:4440-4444`, `:4560-4564`, `:4631-4635`) | ✔ |
| `checkVarUseAfterCursor` 5181 | Совпадает; внутри `fileProcessedContent` (`:5184-5189`) | ✔ |
| Эталон `findInsertWithoutColumns` (review_parser.go:1965) — char-level FSM с литералами/комментариями | `:1967` `toLowerASCIIPreservingLen` (байтовый), `:1976-2002` блок/`--`/литералы | ✔ |
| `sanitizeSQLForMacroScan` — уже существующая маскировка лексером | `review_rules.go:1599-1655`: `[]byte`, состояния normal/string/line/block, `\n`/`\r` сохраняются; **но** литералы заменяются на `?` (`:1635`) | ✔ (см. F3) |
| `maskBlockCommentsKeepLines` — «построчная маскировка» | `review_parser.go:1183-1223`: работает в `[]rune`, **не** сохраняет байтовую длину (см. F1) | ✔, но не эталон для offset-арифметики |
| `stripLineComments` (review_helpers.go:1112) — построчная с переносом состояния | `:1112-1138`: переносит `inBlock`, но **не** string-aware | ✔ |
| `fileContent`/`fileProcessedContent` — CP866→UTF-8 | `runner.go:163` `encoding.ReadFile(path, encoding.CP866)`; `content = []byte(macroResult.Content)` (`:183`) | ✔ (обосновывает F1) |
| `buildNumber` 1712 | `cmd/root.go:29` `buildNumber = "1712"` | ✔ |
| `Object` в `tooManyJoins` — первая строка оператора | `review_rules.go:4243-4247` | ✔ (влияет на приёмку, F4) |
| Тест-каркас `ruleCase`/`newRuleTestFile`/`newRuleTestExec` существует | `review_rule_coverage_test.go:16,49,66`; `review_block_comment_test.go` существует | ✔ |
| Реальный кейс `TmpTbl_GLReports.sql:68` воспроизводим | Файл существует (79 строк); см. §4 | ✔ |

## 4. Проверка на реальном кейсе

`FA/fa-generalledger/.../TmpTbl_GLReports.sql`:

- блок `/*` открывается на строке 67, закрывается на 79; INSERT начинается на 68;
  внутри блока есть кириллица (66, 71, 74) и хвостовой `--union all` (78);
- строки 7, 20, 32-33, 44-45, 56 содержат слипшиеся `*//*` — это прямой тест
  перехода между соседними блоками;
- текущий `checkInsertRowLock`: строка 68 распознаётся как INSERT (нет `--` перед
  позицией), буфер собирается, но на строке 69 (`select ...`) `isNewSQLStatement`
  завершает оператор → finding с `Line = 68`. Это ровно тот FP, что описан;
- после байтовой маскировки блоки 67-79 станут пробелами, INSERT не будет найден →
  0 findings. Кириллица в файле — дополнительный аргумент за байтовую маскировку
  (F1): rune-based реализация уменьшит байтовую длину текста после строки 66 и
  сместит `offset`-координаты.

Путь в задаче 5.1 (`TmpTbl_GLReports.sql`) — корректный, файл на месте.

## 5. Находки

### F1. `maskSQLComments` обязан сохранять байтовую длину (существенное)

Design D1 говорит «length-preserving маскировка (длина текста и количество `\n` не
меняются)», но не фиксирует, что длина — **байтовая**. В пакете уже есть близкий по
имени хелпер `maskBlockCommentsKeepLines` (`review_parser.go:1183`), который работает
через `[]rune` и **не** сохраняет байтовую длину. Для `splitStatementsWithOffsets`
это критично:

- `splitStatementsWithOffsets` возвращает `offset` как байтовый индекс
  (`review_parser.go:1689-1727`);
- все 4 потребителя считают `Line` по **исходному** `text[:offset]`
  (`review_rules.go:4241`, `:4440-4444`, `:4560-4564`, `:4631-4635`);
- review декодирует файл CP866→UTF-8 (`runner.go:163`), поэтому кириллица в
  комментарии — 2 байта на символ; rune-маскировка уменьшит байтовую длину и
  сместит `offset` относительно исходного текста → неверные `Line` (и возможные
  ложные/пропущенные операторы на границах).

**Рекомендация.** В D1 и task 1.1 явно записать: реализация на `[]byte` (по образцу
`sanitizeSQLForMacroScan`, `review_rules.go:1600-1654` или
`findInsertWithoutColumns`, `review_parser.go:1967`), каждый байт комментария →
`' '`, `\n` **и `\r`** сохраняются; `len(результат) == len(вход)` в байтах. Тест
должен содержать кириллический комментарий **перед** триггером и проверять, что
`Line` после маскировки совпадает с исходным. Альтернатива, снимающая риск: маскировать
текст в каждом правиле и там же считать `Line` по уже маскированному тексту (тогда
координаты не смешиваются), но централизованный вариант в сплиттере требует строгой
байтовой инвариантности.

### F2. Пробел `varUseAfterCursor` подтверждён — task 4.1 должна быть реализацией (существенное)

Design (Context, `:15`) и tasks 4.1 трактуют `varUseAfterCursor` как «верификация:
маскировка только при подтверждённом пробеле». Проверка кода показывает, что пробел
есть:

- `parseAllFetchIntoStatements` (`review_parser.go:465-473`) во внешнем цикле матчит
  `reParserFetchCursorStart` по сырой `strings.TrimSpace(lines[i])` **без** состояния
  `inBlock`; строка `fetch cur into @Var` внутри многострочного `/* */` будет
  распознана как реальный FETCH;
- `parseDeallocateStatements` (`:435-443`) так же матчит `deallocate ...` по сырым
  строкам;
- `parseCursorDeclarations` (`:328-352`) матчит `declare ... cursor` по сырым
  строкам (стрип применяется только к продолжениям, `:369-371`).

Значит, дельта `postgre-requirements` («комментированные FETCH/DEALLOCATE/DECLARE
CURSOR не считаются операторами») требует именно маскировки входа
`checkVarUseAfterCursor`.

**Рекомендация.** Переформулировать 4.1 как «подтверждено: маскировать вход `text`».
Безопасность подтверждается: `Line` берётся как `i+1` по тем же строкам
(`review_rules.go:5304`), `Object` — имя переменной (`:5305`), длина строк
сохраняется. Отдельно задокументировать, что сознательное расширение skip-спана по
упоминанию переменной в комментарии (`fetchVariablesLineEnd`,
`review_parser.go:522-536`) после маскировки становится неактуальным, но
непротиворечивым: и парсер, и скан использования работают по одному маскированному
тексту, поэтому FP не возвращается.

### F3. Отношение к `sanitizeSQLForMacroScan` стоит зафиксировать (среднее)

В пакете уже есть байтовый length-preserving string-aware лексер
`sanitizeSQLForMacroScan` (`review_rules.go:1599-1655`), решающий почти ту же задачу.
Переиспользовать его напрямую нельзя: он заменяет содержимое строковых литералов на
`?` (`:1635`), что испортит правила, читающие литералы/идентификаторы из `stmt`
(`extractSetColumns`, `extractTablesFromFromClause`, `findInSubqueryPositions`).
Но дублировать state machine целиком нежелательно.

**Рекомендация.** В D1 явно указать: новый хелпер = тот же байтовый автомат, но
маскирует **только комментарии**, литералы оставляет как есть; по возможности вынести
общее ядро автомата или разместить `maskSQLComments` рядом с санитайзером, чтобы две
реализации не разошлись. Это снимет будущий вопрос «почему не переиспользовали».

### F4. Приёмка `rule:line:object` даст «безобидные» расхождения (среднее)

D3/5.2 требуют построчной идентичности остальных findings по `rule:line:object`.
После маскировки:

- `tooManyJoins` формирует `Object` из первой строки `entry.stmt`
  (`review_rules.go:4243-4247`). Если в baseline у первой строки был хвостовой
  `-- комментарий`, `Object` изменится (комментарий заменится пробелами и срежется
  `TrimSpace`), хотя `rule`/`line` те же;
- маскировка может **добавить** истинные findings там, где раньше комментарий стоял
  внутри оператора (например, `truncate /* c */ table t` не матчился
  `truncateTblRe`, а после маскировки сматчится) — это исправление FN, но в
  построчном diff оно будет выглядеть как «новая находка».

**Рекомендация.** В приёмке сравнивать `rule:line:object` с нормализацией хвостовых
комментариев в `Object` либо заранее перечислить ожидаемые «новые истинные» findings.
Иначе приёмка может дать ложную тревогу.

### F5. Полнота scope стоит сформулировать как эмпирическую (среднее)

Список вызовов сплиттера подтверждён (4, все — review-правила). Но в
`review_rules.go` много и других raw-сканеров на `fileContent`/`fileProcessedContent`
(например, `checkEmptyStringDate:5317`, а также `:4917`, `:5899`, `:6464`, `:6805`,
`:6972`), часть из которых не имеет общей маскировки. Scope change опирается на
измерение 247 файлов fa-contracts (87 FP только в 4 правилах) — это разумное
обоснование, но формулировка «в raw-сканерах review» звучит как исчерпывающая.

**Рекомендация.** В proposal/design явно сказать: «scope определён эмпирически по
корпусу; прочие raw-сканеры вне измеренного набора — отдельные кандидаты». Также
полезно перечислить уже comment-aware исключения (`checkAnsiInJoin:2999`,
`checkModifyOutProc:4294` — `maskBlockCommentsKeepLines`; `checkUseEqColumn:3239`,
`checkVarcharSize:4678` — собственные блоки; `checkEmptyReturn`/`checkRawTransactionControl`
— `stripLineComments`), чтобы не создавалось впечатления пропуска.

### F6. Формулировка «по-прежнему» в дельте неточна (мелкое)

`deploy-stoppers` (строка 5): «Блочный комментарий без закрывающего `*/` до конца
файла MUST **по-прежнему** скрывать весь последующий код». Для
`insertRowLock`/`useSelectAll`/`truncTbl` это **новое** поведение: сейчас они
пропускают только строки с префиксом `--`, а строки после незакрытого `/*` сканируют
как код. Слово «по-прежнему» вводит в заблуждение.

**Рекомендация.** Заменить на «SHALL скрывать» и добавить отдельный сценарий
«незакрытый `/*` до конца файла → триггер после него не даёт finding» (task 1.1
тестирует это на уровне хелпера, но в спеке сценария нет).

### F7. В дельте не все перечисленные сканеры имеют свой сценарий (мелкое)

Требование `deploy-stoppers` называет 6 правил, а сценарии покрывают
`insertRowLock`, `deferredUpdate`, `inSubQuery`, `useSelectAll` (хвостовой `--`) и
косвенно `truncTbl` (код после `*/`). Для `truncTbl` внутри `/* */` и для
`memoryUniqueIndexUpdate` внутри `/* */` явных negative-сценариев нет.

**Рекомендация.** Добавить по одному сценарию на `truncTbl` и
`memoryUniqueIndexUpdate` (это же закроет tasks 2.2/3.3 трассируемо).

### F8. Краевые случаи хелпера и мелочи tasks (мелкое)

- Сохранять не только `\n`, но и `\r` (CRLF/CP866-файлы; `sanitizeSQLForMacroScan`
  сохраняет оба).
- Строковое состояние должно переноситься через `\n` (многострочный литерал), как в
  `splitStatementsWithOffsets`; иначе `/*`/`--` внутри литерала на следующей строке
  будет принят за комментарий. В task 1.1 этого кейса нет.
- `*//*` подряд и `--` внутри блока уже перечислены в task 1.1 — хорошо; реальный
  `TmpTbl_GLReports.sql` (7, 20, 32-33, 44-45, 56, 78) это подтверждает.
- String-awareness по `''` достаточна для большинства случаев, но `[a--b]` /
  `"a--b"` (bracket/quoted identifiers) будут ошибочно приняты за комментарий.
  Вероятность низкая; стоит либо задокументировать как принятое ограничение, либо
  учесть `[...]`/`"..."` в автомате.
- Task 6.2 говорит «задачи 1–5 отмечены выполненными» — включить и 6.1.

### F9. Прочие уже-маскирующие правила (info)

Помимо перечисленных в proposal «12 сканеров из 1711», уже маскируют комментарии
`checkAnsiInJoin` (`review_rules.go:2999`, `maskBlockCommentsKeepLines`) и
`checkModifyOutProc` (`:4294`, тот же хелпер). Они корректно вне scope, но их стоит
упомянуть в списке «не затрагиваем», чтобы инвентаризация была полной.

## 6. Наблюдения вне scope (для памяти)

1. **Литералы в line-regex сканерах.** `checkUseSelectAll`/`checkTruncTbl` не
   маскируют содержимое строковых литералов, поэтому `set @sql = 'select * from t'`
   или `'truncate table x'` по-прежнему дают finding. Это pre-existing класс, не
   входит в текущий FP-класс (`/* */`) и не должен решаться этим change; при желании
   — отдельный кандидат (с оговоркой, что динамический SQL может быть намеренной
   целью правил).
2. **~27 правил на `parsed *sqlparser.ParseResult`** — другой источник контента;
   эмпирических FP не зафиксировано, вне scope (корректно отражено в Non-Goals).
3. **Унификация обработки комментариев.** После change останутся минимум четыре
   идиомы: `stripLineComments`, `maskBlockCommentsKeepLines`,
   `sanitizeSQLForMacroScan`, новый `maskSQLComments`. Отдельный рефакторинг-change
   позже (F3 — первый шаг к сближению).

## 7. Вердикт

- Диагностика (FP по тексту внутри `/* */`), механизм (единый length-preserving
  маскер + точки применения), build 1712→1713 и приёмка на реальном
  `TmpTbl_GLReports.sql` — корректны и подтверждены кодом; реальный кейс
  воспроизводится.
- Перед apply устранить **F1** (зафиксировать байтовую length-preserving семантику и
  тест с кириллицей/`Line`) и **F2** (перевести `varUseAfterCursor` из
  verify-only в реализацию с маскировкой входа). **F3–F5** рекомендуется учесть в
  design/приёмке; **F6–F9** — по усмотрению автора.
- После правок change можно переводить в реализацию.

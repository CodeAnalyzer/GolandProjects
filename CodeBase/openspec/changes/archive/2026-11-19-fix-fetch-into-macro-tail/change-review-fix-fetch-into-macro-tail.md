# Change review: fix-fetch-into-macro-tail

Дата: 2026-11-19
Объём ревью: `proposal.md`, `design.md`, `tasks.md`, дельты `review/postgre-requirements` и `review/fine-code`, код `internal/review` (parser/helpers/rules/tests), эталонные файлы FA и CLI-прогоны `review`.

## Вердикт

Подход верный, корневая причина и точка фикса определены правильно, дельты двух заявленных требований формально корректны (`openspec validate` — OK). Реализовывать **можно после правки F1 и F2**:

- **F1 (Major)** — change меняет наблюдаемое поведение **третьего** правила, `varUseAfterCursor` (сжатие спанов → аргументы макроса ниже DEALLOCATE становятся находками), но дельты к его требованию нет; в main spec остаётся широкая формулировка про строки-продолжения FETCH. Это блокирует архивацию, но не саму реализацию.
- **F2 (Medium)** — вводится второй регэксп «standalone-макроса» рядом с уже существующим `reStandaloneMacroLine`/`stripStandaloneMacroLines`, причём предложенный паттерн шире спецификации (`M_*`) и матчит обычные функции `max(`/`min(`/`mod(`/`month(` в начале строки.

F3–F7 — приёмочные baseline, непокрытые кейсы и редакционные уточнения.

## Проверено фактически

| Проверка | Результат |
|---|---|
| `openspec validate fix-fetch-into-macro-tail` | `Change ... is valid` |
| Заголовки/полнота MODIFIED-дельт | Названия совпадают с main spec, полный текст воспроизведён, старые сценарии сохранены (postgre — 6 базовых + 1 новый, fine-code — 8 базовых + 1 новый) |
| FP-эталон `Cons_OMCV_End_Accrual.sql`, `--rules cursorFetchArguments` | 1 finding, line 217: «1 переменных, а в DECLARE 2 выражений» — заявленный FP воспроизведён |
| Тот же эталон, `--rules varUseAfterCursor` | 0 findings (baseline) |
| Тот же эталон, `--rules datatype` | 1 finding: line 92 `API_Loan_FindListIDForPrd` (`varchar -> dsoperday`, exec-параметр) — в tasks 3.2 не зафиксирован |
| `Cons_BankPartnerInstPrepare.sql`: cursorFetchArguments / datatype / varUseAfterCursor | 0 / 3 (208 `@OpenAccBP`, 273 `@BankAlgMode`, 522 `@CntFundID`) / 0 — совпадает с tasks 3.4 |
| `InstrumentSO_LoadStates.sql`, все правила | 11 findings, разбивка прежняя: 4 inSubQuery, 3 datatype, 2 useFuncInIndCol, 1 execNotExistsProc, 1 varUseAfterCursor (line 652 `@ChangedBrief`) — совпадает с tasks 3.4 |
| Батч по fa-contracts (1062 файла с FETCH INTO), `--files ... --rules cursorFetchArguments --json` | `success=false` (4 файла с не-UTF8 именами), агрегированный `items` пуст, у **всех** файлов (включая `Cons_OMCV_End_Accrual.sql`) `count=0`; заявленные «8 находок» этим прогоном **не воспроизводятся** |
| Сканирование fa-contracts по паттернам FETCH → строка-макрос | Найдены макро-хвосты с `M_*(...)` (в т.ч. через пустую строку); не найдено макросов без скобок, макросов на одной строке со списком переменных и строк вида `max(`/`min(` сразу после FETCH |
| Регрессионный инвариант `isCursorDeclarationBoundary` | В `InstrumentSO_LoadStates.sql` и других проверенных файлах макросы `M_ISOLAT`/`M_NOLOCK_INDEX` остаются внутри SELECT декларации; change их не трогает |

## Анализ кода

- Потребители фильтров подтверждены и замкнуты на FETCH-пути:
  - `reParserFetchIntoBoundary` (review_parser.go:21) → `findFetchIntoTailBoundary` (:291) → `parseFetchIntoStatement` (:245) → `checkCursorFetchArguments` (review_rules.go:5803) и `checkDatatypeFetchInto` (:112);
  - `isFetchStatementStopLine` (review_parser.go:591) → сбор продолжений в `parseAllFetchIntoStatements` (:445) → `checkVarUseAfterCursor` (review_rules.go:5079).
- Диагноз из proposal точен: `reParserFetchIntoPrefix` находит FETCH, `variablesPart` режется только по ключевым словам `select/exec/set/...`; макро-строка остаётся в CSV, и анкорный `reParserFetchIntoVar` (`^\s*@var\s*$`) отбрасывает последнюю переменную. FP на строке 217 `Cons_OMCV_End_Accrual.sql` воспроизведён на текущем бинаре.
- Fragment-путь (правила `cursorFetchArguments`/`datatype`) чинится именно расширением `reParserFetchIntoBoundary`: после обрезки `variablesPart` перед макро-строкой `splitTopLevelCSV` даёт полный список.
- File-путь (`varUseAfterCursor`) требует именно `isFetchStatementStopLine`: без остановки сбора макро-строка попадает в `statementLines`, а `fetchVariablesLineEnd` (review_parser.go:516) по сырым строкам продлевает `LineEnd` до строки, где упомянута переменная (например, `M_BUSINESSLOG_PARAM(@TradeID, ...)`), сохраняя skip-спан.
- Сжатие спана семантически меняет `varUseAfterCursor`: строка-макрос ниже DEALLOCATE перестаёт быть «продолжением FETCH» и её аргументы-переменные матчатся как использование. Это действительно устранение FN, но оно затрагивает требование `review/postgre-requirements` «Использование переменной после закрытия курсора» (openspec/specs/review/postgre-requirements/spec.md:109–129), где сейчас сказано: «Строки-продолжения многострочного `FETCH ... INTO` … MUST NOT классифицироваться как использование переменных». Формулировка не уточняет, что макро-строка — уже новый оператор, поэтому без дельты поведение и спека расходятся.
- В `review_helpers.go` уже есть `reStandaloneMacroLine` (:2355) и `stripStandaloneMacroLines` (:2361), используемые в `checkUsageVarInSameSelect` (review_rules.go:5859). Предложенный `reParserStandaloneMacroCall` — третье по счёту определение «standalone-макроса» (плюс `reParserMacro`, `reDeletePtableMacro`), что противоречит формулировке Decision 4 «одна точка определения паттерна».
- Предложенный паттерн `(?i)^m[a-z0-9_]+\s*\(` без обязательного `_` матчит `max(`, `min(`, `mod(`, `month(`, `merge(` и т.п. В зоне действия фильтров это, как правило, безвредно (продолжение списка переменных обязано начинаться с `@`), но заявление в Risks «функцию без `M_` не заденет» неверно, и поведение не покрыто negative-тестом.
- Регэксп `\s*` после `\n` и после имени макроса включает перевод строки, поэтому альтернатива может матчить `m_foo` и `(` на разных строках. Для префиксного распознавания это допустимо, но стоит зафиксировать осознанно (`[ \t]*` либо тест).

## Findings

### F1 (Major, блокирует архивацию) — изменение `varUseAfterCursor` не отражено дельтой спецификации

Change декларирует третий эффект: «строки-аргументы макроса с переменными ниже DEALLOCATE перестают скрываться спаном FETCH и корректно детектируются как использования» (proposal, строка 11; design Decision 3; tasks 2.3). Это изменение наблюдаемого поведения правила `varUseAfterCursor`:

- меняется `LineEnd` FETCH-спана (`parseAllFetchIntoStatements` → `fetchVariablesLineEnd`);
- меняется множество находок правила для кода вида `DEALLOCATE c; ... FETCH c ...; M_INSERT_ELDOC(@ContractCursID)`.

При этом:
- дельты есть только к требованиям `cursorFetchArguments` и `datatype`;
- требование main spec «Использование переменной после закрытия курсора» (postgre-requirements/spec.md:109) содержит широкую фразу про строки-продолжения FETCH, под которую макро-строка формально попадает;
- в change нет ни MODIFIED-дельты этого требования, ни сценария «макровызов ниже DEALLOCATE: аргумент-переменная из курсора → finding» / «макровызов без переменных → не finding».

После `openspec archive` спеки будут описывать старое поведение (строки-продолжения не считаются использованием), а код — новое. Это нарушает принцип «спеки — источник истины».

Рекомендация: добавить в `specs/review/postgre-requirements/spec.md` MODIFIED-дельту требования «Использование переменной после закрытия курсора» с уточнением: standalone-вызов макроса после списка переменных FETCH не является строкой-продолжением FETCH; переменные в его аргументах анализируются как обычное использование. Добавить оба сценария (с переменной → finding; без переменной → нет finding) и отразить это в `proposal.md` (Modified Capabilities) и `tasks.md` группой приёмочных тестов.

### F2 (Medium) — дублирование паттерна и расхождение с `M_*` из спецификации

1. В `review_helpers.go` уже есть `reStandaloneMacroLine` (`(?i)^[\t ]*M_[A-Z0-9_]+[\t ]*\(.*\)[\t ]*\r?$`) и `stripStandaloneMacroLines`. Design Decision 4 обещает «одну точку определения паттерна», но добавляет ещё один регэксп в `review_parser.go`. Минимум нужно явно описать отношение к существующему хелперу (почему не переиспользуется), лучше — вынести общий предикат/константу и использовать её в обоих местах.
2. Формулировки spec/design/proposal говорят про макрос Diasoft `M_*`, а паттерн `m[a-z0-9_]+` без `_` захватывает обычные функции (`max(`, `min(`, `mod(`, `month(`). На текущих данных FA таких строк после FETCH не найдено, поэтому реального FP пока нет, но:
   - Risk в design («функцию без `M_` не заденет») неверен;
   - unit-тесты tasks 1.1 не содержат `max(x)`/`min(x)` как negative-кейс.
3. Рекомендация: сузить до `(?i)^m_[a-z0-9_]+\s*\(` (или переиспользовать `reStandaloneMacroLine` для `isFetchStatementStopLine`), добавить negative-тесты `max(x)` → false, `min(@a)` → false, `month(@d)` → false. Для `reParserFetchIntoBoundary` альтернативу вывести из того же определения, чтобы паттерн не разъезжался.

### F3 (Medium) — приёмочные baseline неполны, батч-критерий 3.3 невоспроизводим

- Tasks 3.2 требует «зафиксировать вывод» `datatype`/`varUseAfterCursor` на `Cons_OMCV_End_Accrual.sql`, но baseline не записан. Фактически сейчас: `datatype` = 1 finding (line 92, exec-параметр), `varUseAfterCursor` = 0. Без зафиксированного baseline нельзя отличить новую находку от старой.
- Tasks 3.3 опирается на «повторный прогон … ожидаемо 0» и заявленные 8 находок. Прогон на текущем билде 1707 по 1062 файлам вернул `success=false` (4 файла с не-UTF8 именами), агрегированный `items` пуст, у всех файлов (включая эталонный FP) `count=0`; одиночный прогон того же файла даёт 1 finding. То есть batch-инвокация в текущем виде не воспроизводит ни baseline, ни ожидаемый результат.

Рекомендация: до реализации зафиксировать точные baseline (per-file) и сделать 3.3 воспроизводимым: исключить/перекодировать 4 проблемных пути, проверить батч на подмножестве с известным FP, при необходимости — починить агрегацию/`success` batch-режима. Иначе критерий «ожидаемо 0» ничего не доказывает.

### F4 (Minor) — макросы без скобок не покрыты

Паттерн требует `(`. Standalone-макросы без скобок (`M_FORCEORDER`, `M_ISOLAT`, `M_WITH_ROWLOCK`) после FETCH останутся нераспознанной границей и сохранят дефект N−1. В fa-contracts таких случаев после FETCH не найдено, поэтому поведение можно принять, но его нужно **явно** зафиксировать в Non-Goals и покрыть unit-тестом (`M_FORCEORDER` → false, чтобы поведение было осознанным, а не случайным).

### F5 (Minor) — многострочный standalone-макрос: покрытие и совместимость с существующим хелпером

Предложенный префиксный паттерн корректно сработает и на многострочном вызове (`M_X(@a,` / `DSINT_KEY)`), а существующий `reStandaloneMacroLine` — нет (он требует закрывающую скобку на той же строке). Это аргумент в пользу префиксного предиката, но его нужно:
- отразить в design (почему не переиспользуется full-line regex);
- покрыть тестом «макровызов, разнесённый на две строки, завершает список переменных» либо явно объявить Non-Goal.

### F6 (Nit) — редакционные ошибки

- design.md:22 — «список переменныхFETCH INTO» (слипшиеся слова).
- proposal.md:5 — «FETCH INTО» содержит кириллическую `О` (ломает поиск/копирование).
- design.md:22 — фраза «список переменныхFETCH INTO продолжением … не расширяется» сформулирована нечитаемо; переписать.

### F7 (Nit) — `\s*` в паттерне пересекает границы строк

`(?i)^m[a-z0-9_]+\s*\(` и `\n\s*m[a-z0-9_]+\s*\(` допускают `m_foo` на одной строке и `(` на следующей (и несколько пустых строк между `\n` и именем). Для распознавания границы это, скорее всего, допустимо, но лучше либо ограничить `[ \t]*`, либо зафиксировать поведение тестом.

## Что в артефактах сделано хорошо

- Причина (FP + FN), механизм и эталон описаны точно, с координатами и воспроизведением на билде 1707 (`Cons_OMCV_End_Accrual.sql:217`).
- Выбор двух хвостовых фильтров и отказ от правки `isCursorDeclarationBoundary` обоснованы: `M_ISOLAT`/`M_NOLOCK_INDEX` — легитимные части SELECT декларации, существующие тесты остаются регрессионным фиксатором.
- Дельты `cursorFetchArguments` и `datatype` полные: названия совпадают, старые сценарии сохранены, новые сценарии behavior-oriented и покрывают именно макро-хвост.
- Разделение эффектов по путям осознано: fragment-путь → `cursorFetchArguments`/`datatype`, file-путь → `varUseAfterCursor`; это подтверждено grep-ом потребителей.
- Регрессионные baseline по `Cons_BankPartnerInstPrepare.sql` и `InstrumentSO_LoadStates.sql` (11 findings) воспроизведены и корректны.

## Рекомендуемые правки артефактов перед реализацией

1. **F1 (обязательно):** добавить MODIFIED-дельту к требованию «Использование переменной после закрытия курсора» (`review/postgre-requirements`) + сценарии; обновить `proposal.md` (Modified Capabilities) и `tasks.md` (тесты `varUseAfterCursor` нового поведения).
2. **F2:** сузить паттерн до `m_` или переиспользовать существующий хелпер; зафиксировать отношение к `reStandaloneMacroLine`; добавить negative-тесты `max(`/`min(`/`month(`.
3. **F3:** записать baseline `datatype`/`varUseAfterCursor` для `Cons_OMCV_End_Accrual.sql` (1 finding line 92 / 0) и сделать батч-прогон 3.3 воспроизводимым.
4. **F4/F5:** явные Non-Goals + тесты для макросов без скобок и многострочных макровызовов.
5. **F6/F7:** редакционная чистка и решение по `\s*`.

## Заключение

Change качественный: он действительно устраняет подтверждённый FP `cursorFetchArguments` (N−1 переменных) и симметричные FN `datatype`/`varUseAfterCursor`, а выбранные точки фикса минимальны и локальны. Блокирующих проблем в самом подходе нет. Перед передачей в реализацию нужно закрыть F1 (дельта для третьего затронутого правила) и F2 (паттерн/дублирование), а также привести приёмочные baseline F3 в воспроизводимый вид.

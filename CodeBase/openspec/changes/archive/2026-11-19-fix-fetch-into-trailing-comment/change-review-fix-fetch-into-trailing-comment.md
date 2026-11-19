# Change review: fix-fetch-into-trailing-comment

Дата: 2026-11-18
Объём ревью: proposal.md, design.md, tasks.md, дельты `review/postgre-requirements` и `review/fine-code`, код `internal/review` (parser/rules/tests), эталонные файлы FA.

## Вердикт

Подход верный, корневая причина и точка фикса определены правильно, дельты спецификаций формально корректны (`openspec validate` — OK). Реализовывать **можно после правки F1**: описанный в Decision 2 перенос стрипнутых строк в `statementLines` может породить **новый FP `varUseAfterCursor`** и противоречит и Non-Goal, и сценарию из tasks 2.3. F2 (учёт `inBlock` при проверке границ) и F3 (`fetchIntoStatementLine` на сыром тексте) — существенные, но не блокирующие; F4–F7 — уточнения.

## Проверено фактически

| Проверка | Результат |
|---|---|
| `openspec validate fix-fetch-into-trailing-comment` | `Change ... is valid` |
| Заголовки/полнота MODIFIED-дельт | Названия совпадают с main spec, полный текст воспроизведён, старые сценарии сохранены (postgre — 4+2, fine-code — 8+1) |
| FP-эталон `Cons_BankPartnerInstPrepare.sql`, `--rules cursorFetchArguments` | 1 finding, line 918: «2 переменных, а в DECLARE 3 выражений» (FP подтверждён) |
| `--rules datatype` на эталоне (baseline) | 3 findings: line 208 `@OpenAccBP`, line 273 `@BankAlgMode`, line 522 `@CntFundID` |
| `--rules varUseAfterCursor` на эталоне (baseline) | 0 findings; `@AccountsRequestID` после `__DEALLOCATE_CURSOR__` (948) не используется, новый finding не ожидается |
| Регрессия `InstrumentSO_LoadStates.sql` | 11 findings ровно с разбивкой из tasks 3.3 (4 inSubQuery, 2 useFuncInIndCol, 1 execNotExistsProc, 3 datatype, 1 varUseAfterCursor `@ChangedBrief`) — критерий корректен и воспроизводим |

## Анализ кода

- Пути данных подтверждены: `checkCursorFetchArguments` (review_rules.go:5803) и `checkDatatypeFetchInto` (:112) → `parseFetchIntoStatement(fragment.QueryText)`; `checkVarUseAfterCursor` (:5079) → `parseAllFetchIntoStatements` (review_parser.go:422).
- Диагноз асимметрии верен: `parseCursorDeclarations` стриппит продолжения (review_parser.go:338), FETCH-пути — нет. Анкорный `reParserFetchIntoVar` (review_parser.go:20) отбрасывает CSV-часть с хвостовым `--`.
- Choke point в `parseFetchIntoStatement` — правильное решение: покрывает оба пути без правок правил; `stripLineComments` (review_helpers.go:1106) побайтово работает по ASCII-маркерам, для CP866-контента безопасен (Decision 4 корректен).
- Уточнение по semantics `stripLineComments`: строка `@X --комм.` превращается в `@X ` (префикс сохраняется), в пустую — только чисто-комментарная строка. Поэтому Decision 2 не ломает `LineEnd` на эталонной строке 920 (там реальная переменная + комментарий). Это важно для оценки F1.
- Построчный стриппинг не меняет нумерацию строк (split/join по `\n`), координаты `LineStart`/`LineEnd` остаются в expanded-координатах.

## Findings

### F1 (Major, блокирует) — Decision 2 может создать новый FP `varUseAfterCursor` и противоречит артефактам

Механизм:
1. `parseAllFetchIntoStatements` собирает продолжения до boundary; **чисто-комментарная строка** (например, завершающая `-- @B используется ниже`) не является boundary и попадает в `statementLines`.
2. Сейчас `fetchVariablesLineEnd` (review_parser.go:482) идёт по **сырым** строкам: если в комментарии упомянута переменная из `Variables`, `LineEnd` включает эту строку → skip-span покрывает её → FP нет.
3. После Decision 2 («`statementLines` хранит стрипнутые строки») эта строка станет `""`, `LineEnd` сожмётся до последней строки с переменной, а `checkVarUseAfterCursor` сканирует **сырые** `lines` (review_rules.go:5130–5167) и матчит `@B` в комментарии → **новый finding `varUseAfterCursor`** для FETCH ниже DEALLOCATE.

Это противоречит:
- требованию main spec «Строки-продолжения многострочного `FETCH ... INTO` ... MUST NOT классифицироваться как использование переменных» (openspec/specs/review/postgre-requirements/spec.md:111);
- design.md Non-Goals, строка 27 («наблюдаемое поведение окна не меняется — дельта не требуется»);
- tasks 2.3, который ожидает, что `-- @var`-продолжение «не продлевает skip-спан» (что и даёт FP).

Рекомендация (выбрать и зафиксировать в design):
- **Вариант A (предпочтительно)**: не менять `statementLines` для целей `LineEnd` — оставить сырые строки (переменные извлекаются из стрипнутого текста внутри `parseFetchIntoStatement`, этого достаточно для FP/FN). Skip-span тогда по-прежнему накрывает строки-продолжения, включая комментарные. Дополнительно добавить тест: FETCH ниже DEALLOCATE с завершающей строкой `-- @B` → 0 findings.
- **Вариант B**: оставить сжатие `LineEnd`, но в `checkVarUseAfterCursor` перед матчингом переменных стриппить комментарии в сканируемой строке (`stripLineComments`), т.е. комментарные строки не считаются использованием. Тогда меняется наблюдаемое поведение правила → нужна дельта спецификации `review/postgre-requirements` (требование «Использование переменной после закрытия курсора») и правка Non-Goals.

В текущем виде артефакты внутренне противоречивы: design утверждает инвариантность поведения, tasks 2.3 — его изменение.

### F2 (Medium) — проверка границ игнорирует `inBlock`, поддержка блочных комментариев неполна

Decision 3 / task 1.3: boundary/stop-проверки (`isCursorDeclarationBoundary`, `isFetchStatementStopLine`) выполняются по сырой строке до стриппинга. Для строки внутри многострочного `/* ... */`, начинающейся с `select`/`set`/... , проверка сработает и оборвёт сбор оператора, хотя это комментарий:

```
fetch c into @A, /* start
select ...            -- внутри блока: сбор оборвётся здесь
*/ @B
```

Итог — потеря `@B` и тот же класс FP, который чинится. Обоснование в Decision 3 покрывает только `--`-строки (префикс `--` не матчится), но не внутренность блока.

Рекомендация: в цикле сбора вести `inBlock`; проверять boundary/stop только когда строка не является продолжением блока (проще — сначала стриппить строку с текущим `inBlock`, затем проверять границы по стрипнутому trimmed-тексту: `-- select` станет пустой строкой и границей не будет). Добавить тест с многострочным блок-комментарием внутри FETCH.

### F3 (Medium) — `fetchIntoStatementLine` остаётся на сыром тексте

После choke point `parseFetchIntoStatement` может выбрать **реальный** FETCH, а `fetchIntoStatementLine(fragment.QueryText, ...)` (review_rules.go:119, :5810) по сырому тексту найдёт первым FETCH-подобный текст в комментарии (например, `-- fetch c into @a` выше реального оператора в том же фрагменте). Тогда `fetchLine` разойдётся с разобранным оператором, а `nearestCursorDeclaration` может спарить с чужой жизнью переобъявленного курсора. Design Decision 1 называет phantom-FETCH «строго улучшением», но line-path остаётся сырым.

Рекомендация: вычислять строку по тому же стрипнутому тексту (стриппинг сохраняет `\n`) либо возвращать offset префикса из `parseFetchIntoStatement`. Добавить тест «комментарий с `fetch ... into ...` перед реальным FETCH».

### F4 (Minor, pre-existing) — сдвиг индексов `LineEnd` при пустых строках внутри FETCH

В цикле сбора (review_parser.go:450–459) пустые строки пропускаются (`continue`) и **не** добавляются в `statementLines`, а `fetchVariablesLineEnd` считает `lineStart + k`. При пустой строке внутри многострочного FETCH `LineEnd` смещается. Дефект не вносится change'ем, но change делает спаны семантически важными. Рекомендация: либо добавлять `""` для пустых строк (индекс сохраняется), либо вести физические номера строк; добавить тест.

### F5 (Minor) — неверная ссылка в tasks 2.3

tasks 2.3 ссылается на `review_rules_test.go:3153–3293`, но это таблица datatype-тестов; тесты `varUseAfterCursor` — 3279–3543, спаны FETCH — `TestParseAllFetchIntoStatements_Spans` (1347). Поправить ссылку.

### F6 (Minor) — приёмка 3.2 сформулирована неоднозначно

«`--rules datatype` — без новых находок относительно состояния до изменения» не является проверяемым критерием и может «покраснеть» легитимно (третья переменная теперь проверяется). Зафиксировать baseline: **3 findings (208, 273, 522)** и требование «diff = ожидаемый список; новых пар нет». Также добавить в 3.2 прогон `--rules varUseAfterCursor` на эталоне: baseline **0**, после фикса тоже 0 (проверено: `@AccountsRequestID` после DEALLOCATE 948 не используется).

### F7 (Nit) — «текст без комментариев возвращается без изменений» верно только для LF

Если `stripCommentsFromText` собирает `strings.Split(text, "\n")` и `Join(..., "\n")`, CRLF-текст нормализуется (теряется `\r`). Либо нормализовать явно и так и назвать тест, либо сохранять разделители.

### F8 (Nit) — заявленная «симметрия с `parseCursorDeclarations`» неполна

`parseCursorDeclarations` стриппит только продолжения; `remainder` первой строки декларации (review_parser.go:327–329) добавляется сырым. На текущий change не влияет, но фразу о симметрии в design стоит уточнить (или завести follow-up).

## Что в артефактах сделано хорошо

- Причина (FP + FN) и эталон описаны точно, с координатами и объяснением асимметрии.
- Выбор choke point обоснован и минимизирует правки правил; альтернатива со «ослаблением регэкспа» корректно отклонена.
- Дельты спецификаций полные, названия требований совпадают, старые сценарии не потеряны, новые сценарии behavior-oriented и покрывают и FP, и реальный рассинхрон.
- Критерии регрессии (11 findings на `InstrumentSO_LoadStates.sql`) точные и воспроизводимые; отсутствие FETCH-комментариев в этом файле подтверждено.

## Рекомендуемые правки артефактов перед реализацией

1. design.md: переписать Decision 2 под выбранный вариант F1; синхронизировать Non-Goals (строка 27) и Risks.
2. tasks.md:
   - 1.3 — сформулировать под F1, добавить проверку «FETCH ниже DEALLOCATE + завершающая строка `-- @var` → 0 findings»;
   - добавить задачу на `inBlock` в проверках границ (F2) с тестом;
   - добавить задачу/тест на `fetchIntoStatementLine` (F3);
   - поправить ссылку в 2.3 (F5);
   - в 3.2 зафиксировать baseline datatype (3: 208/273/522) и добавить varUseAfterCursor (0).
3. При выборе варианта B в F1 — добавить дельту к требованию `varUseAfterCursor` в `review/postgre-requirements`.

## Заключение

Change качественный и близок к готовности: он действительно устраняет FP `cursorFetchArguments` и FN `datatype` на эталоне. Единственная принципиальная проблема — F1: текущая формулировка Decision 2 создаёт риск FP `varUseAfterCursor` и расходится с собственными Non-Goals/tasks. После фиксации решения по F1 (и, желательно, F2/F3) change можно передавать в реализацию.

# Код-ревью: fix-fetch-tail-whitelist-union

Дата: 2026-11-19
Область: реализация change по `openspec/changes/fix-fetch-tail-whitelist-union`
(build 1709) — код `internal/review`, тесты, bump сборки.

## Вердикт

**Принято.** Реализация соответствует запланированным изменениям и замечаниям
change-ревью; регрессий не выявлено. Все проверки приёмки (build/tests/validate/
baselines/батч/регресс) воспроизводятся. Замечания носят характер minor
(полнота тестов, читаемость) и не блокируют доработку. Остаточные находки —
pre-existing и не связаны с изменением.

## 1. Что изменено

`git diff --stat` (изменения только по плану):

| Файл | Изменение |
|---|---|
| `CodeBase/cmd/root.go` | buildNumber `1708` → `1709` (`root.go:29`) |
| `CodeBase/internal/review/review_parser.go` | белый список продолжений FETCH; удаление 4 сущностей; union-арность |
| `CodeBase/internal/review/review_rules_test.go` | новые/заменённые тесты (+474) |
| `CodeBase/CodeBase.exe` | пересобран (build 1709) |

Никаких других файлов кода/схемы/CLI не затронуто.

## 2. Верификация (фактические прогоны)

- `go build ./...` — OK; `gofmt -l internal/review/` — пусто; `go vet ./internal/review/` — чисто.
- `go test ./... -count=1` — все пакеты `ok` (включая `internal/review`, 3.7 s).
- `openspec validate fix-fetch-tail-whitelist-union` — `Change ... is valid`.
- `CodeBase.exe --version` — `0.9.5 build 1709`.
- grep удалённых сущностей (`reParserFetchIntoBoundary`,
  `standaloneMacroCallPattern`, `reParserStandaloneMacroCall`,
  `isStandaloneMacroCallLine`) по `*.go` — пусто.

### 2.1 Эталонные FP (задача 4.2)

| Файл | 1708 | 1709 |
|---|---|---|
| `API_LN_AfterRuleA_MassCrtAcc.sql` | 1 (line 118) | **0** |
| `Cons_CalcAnnScheme8.sql` | 4 (111/117/200/206) | **0** |

`datatype`/`varUseAfterCursor` на обоих файлах: новых находок нет (diff old/new
пуст) — для Cons_CalcAnnScheme8 это ожидаемо и согласовано с обновлёнными
артефактами (первая ветка из `@`-литералов, типы не резолвятся).

### 2.2 Батч (задача 4.3)

`review --files fa_fetch_files.txt --rules cursorFetchArguments --json`:
`success=true`, `total=1049`, `count=0`.

### 2.3 Регрессия (задача 4.4)

- `Cons_OMCV_End_Accrual.sql` (`Consumer/SERVER/Accrual`): 0 `cursorFetchArguments`,
  1 `datatype` (line 92), 0 `varUseAfterCursor` — совпадает.
- `Cons_BankPartnerInstPrepare.sql`: среди трёх правил 3 `datatype` (208/273/522),
  0/0 — совпадает.
- `InstrumentSO_LoadStates.sql`: ровно 11 находок с прежней разбивкой
  (4 inSubQuery 214×2/251/263, 3 datatype 350×2/369, 2 useFuncInIndCol 434/451,
  1 execNotExistsProc 591, 1 varUseAfterCursor 652) — совпадает.

### 2.4 Сравнение 1708 ↔ 1709 на реальных наборах

Собран baseline-бинарник из `HEAD` (build 1708) и прогнан параллельно на тех же
файлах/БД.

1. **250 файлов с «пограничными» формами FETCH** (238 файлов с многострочным
   заголовком `fetch`/`into` + 12 файлов с закомментированной переменной внутри
   списка), 3 правила: 4586 находок **идентичны** (включая 2 pre-existing
   `cursorFetchArguments` в `MT101_SequenceB_Copy2SL.sql`). Регресса B1/B2 нет.
2. **97 файлов с UNION в блоке DECLARE CURSOR** (fa-contracts + fa-payments),
   3 правила: `datatype` 310 = 310, `varUseAfterCursor` 953 = 953,
   `cursorFetchArguments` 19 → 4. Удалены 15 FP, все — класс B:
   - `API_LN...` 118 (1), `Cons_CalcAnnScheme8` 111/117/200/206 (4);
   - дополнительно (сверх документированных эталонов, тот же класс):
     `CBAContractService_Verify.sql` 162/241, `ED503_Process_Envelope.sql`
     187/214, `QIW_RegistryDocCreate.sql` 681/695,
     `RAP_RegistryDocumentCreate.sql` 969/983,
     `RKO_GetPrcByPeriod.sql` 543/1152.
   Добавлений нет; `datatype`/`varUseAfterCursor` не изменились.
3. **Курированный набор 21 файл** (эталоны класса A/B, BaseAlg*, регрессионные
   и non-contracts файлы, `PrcGen.sql` с многострочным заголовком): 660 → 655,
   diff — ровно 5 удалений `cursorFetchArguments` (API_LN:118 +
   Cons_CalcAnnScheme8:111/117/200/206). Никаких иных изменений.
4. **Non-contracts файлы с закомментированной переменной** (задача 4.5):
   `MB_InstBaseReuters_Insert_pp.sql` (0), `Card_LoyaltyProgramRep1.sql` (149),
   `Warranty_ChangeAccount.sql` (21), `RKO_DebtTransfer_Calc.sql` (27) —
   1708 и 1709 идентичны по составу находок (список правил
   `cursorFetchArguments,datatype,varUseAfterCursor`).
5. `PrcGen.sql` (многострочный заголовок, 4 вхождения) — 348 = 348.

## 3. Соответствие плану change

- **1.1** `isFetchVariablesContinuationLine` (`review_parser.go:613-627`):
  пустая после trim строка → `true`; иначе `@`/`,` — совпадает с design
  Decision 1. Тесты (`review_rules_test.go:1313`) покрывают `@A`, `@B,`, `, @B`,
  `,@B`, пустую/пробельную, `M_*` (с/без скобок, многострочный), `PROFILE_TIME_*`,
  `max`/`min`, `while`/`select`.
- **1.2** `isFetchStatementStopLine` (`:629-644`) — инверсия продолжения +
  исключение заголовка `into` (`into`, `into <space>`, `into\t`). Это
  stateless-эквивалент `sawInto`, обоснован в design Decision 2; проверен
  Span-тестами и `TestParseAllFetchIntoStatements_MultilineIntoHeaderPreserved`
  (`:1701`). Многострочный заголовок распознаётся (см. 2.4.1/2.4.5).
- **1.3** `findFetchIntoTailBoundary` (`:290-305`) — построчный скан,
  пустые строки прозрачны. Тесты fragment-пути: профайл-хвост, `max(x)`,
  закомментированная переменная (`:1679`), запятая-продолжение, `M_X(@a,` —
  в существующем наборе. B2 закрыт, регрессионный
  `TestParseAllFetchIntoStatements_BlockCommentAndEmptyLines` зелёный.
- **1.4** Удаление 4 сущностей и чистка комментариев: выполнено, потребителей не
  осталось (grep пуст), комментарий у `parseAllFetchIntoStatements` актуализирован
  (`:503`).
- **1.5** Span-тесты и BlockComment-тест зелёные; `TestParseCursorDeclarations_
  MacrosInsideSelect_NotBoundary` не изменён и проходит.
- **2.1** `parseSelectSourceStatement` (`:550-583`): `boundary = min(from, union)`,
  ранний выход заменён на `boundary < 0`. Тест `UnionArityByFirstBranch`
  (`:1446`) покрывает: from в первой ветке (прежнее поведение), from только во
  второй, `union all`, union без from вовсе (арность 2, FromClause с `union`).
- **2.2** Потребители `FromClause` зафиксированы: только `checkDatatypeFetchInto`
  (`parseAliasMap`/`parseFirstFromTable`) и `checkCursorFetchArguments`
  (`SelectExpressions`); `isTableExistsNoiseName` читает только имя курсора.
  Обязательный тест `TestParseFirstFromTable_UnionFromClause` (`:1515`) есть.
- **3.1-3.3** Добавлены тесты правил: профайл-хвост 0/1 (`:4208` и рядом),
  union-арность 0/1 (finding 1 vs 2), `datatype` — профайл-хвост, union-первая
  ветка, закомментированная переменная; `varUseAfterCursor` — профайл-аргумент
  ниже DEALLOCATE (finding) и профайл без переменных окна (0) (`:4417`).
- **4.1-4.6** Проверено фактически (см. раздел 2); чекбоксы проставлены
  обоснованно.

Отклонение от первоначального плана (в пользу реализации): вместо stateful
`sawInto` выбран stateless-префикс `into`, эквивалентный для синтаксиса FETCH;
обосновано в design и подтверждено тестами/эмпирикой. Дополнительный эффект —
10 недокументированных удалений `cursorFetchArguments` в fa-payments (тот же
класс B) — не регресс, спека это допускает.

**Проверка невакуумности нового теста `datatype`-UNION:** тот же сценарий,
прогнанный на коде 1708 во временном worktree, даёт `findings=0` (до фикса
`SelectExpressions[1] = "Amount union select X"` — bare-колонки нет), на 1709 —
1 finding `@Amount`. Тест действительно охраняет изменение.

## 4. Анализ кода

- `findFetchIntoTailBoundary`: скан `strings.Split(value, "\n")`; первая строка
  всегда остаётся списком (same-line tail — Non-Goal), далее первая
  не-продолжение даёт byte-offset начала строки. Пустые/комментарные строки
  (`isFetchVariablesContinuationLine` → true) прозрачны, что сохраняет B2.
  Арифметика offset корректна и для `\r\n` (`\r` входит в `len(line)`, `+1` — на
  `\n`; `TrimSpace` снимает остаток).
- `isFetchStatementStopLine`: guard по пустой строке + `into`-исключение +
  инверсия продолжения. Это устраняет B1: строка `into @A, @B` не считается
  границей; всё, что раньше было границей (`while`/`select`/`M_*`), остаётся ею.
  Для валидного SQL других строк между `fetch c` и `into` нет, поэтому
  stateless-проверка эквивалентна `sawInto` (design Decision 2).
- `parseCursorDeclarations` не менялся; union-граница с `findTopLevelKeywordPosition`
  учитывает скобочную глубину и word-boundary: тест с `union` в подзапросе и
  `tX_union` (`:1495`) проходит. При отсутствии `from` и `union` поведение
  прежнее (`boundary < 0`).
- File-путь `varUseAfterCursor`: `LineEnd` теперь не захватывает
  профайл-хвост, поэтому skip-спан не маскирует реальное использование
  (сценарий 3.3 подтверждён тестом).
- `isCursorDeclarationBoundary` не тронут (Non-Goal), тест-фиксатор
  `MacrosInsideSelect` зелёный.

## 5. Замечания

Блокирующих и существенных дефектов нет.

- **M1 (тест, minor).** Task 3.1 обещал «составной тест с правилом
  `varUseAfterCursor`» на многострочный заголовок `fetch c` / `into @A, @B` ниже
  DEALLOCATE; добавлен только parser-уровневый
  `TestParseAllFetchIntoStatements_MultilineIntoHeaderPreserved` (и Span-тесты).
  Механизм покрыт, но FN-сценарий правила напрямую не зафиксирован; тест был бы
  полезен.
- **M2 (читаемость, minor).** В `findFetchIntoTailBoundary` признак «не первая
  строка» реализован через `offset > 0`; индекс строки читался бы проще.
  Также неявно предполагается, что `variablesPart` не начинается с пустой
  строки — это гарантирует жадный `\s+` в `reParserFetchIntoPrefix`; стоит
  оставить комментарий или тест на `\r\n`-вход.
- **M3 (принятая широта, minor).** Stateless-исключение `into` считает
  не-границей любую строку, начинающуюся с `into` (не только заголовок). В
  валидном T-SQL других операторов, начинающихся с `into`, нет; риск
  теоретический, зафиксирован в design.
- **M4 (pre-existing, informational).** В `parseCursorDeclarations` первая строка
  декларации не стрипится от комментариев (стрипятся только продолжения),
  поэтому `union`/`from` в inline-комментарии на строке
  `DECLARE ... CURSOR FOR ...` теоретически может повлиять на границу. Это
  унаследованное ограничение лексического поиска (design фиксирует только
  строковые литералы/`[]`), изменением не вносится.
- **M5 (тест, negligible).** Из старого набора предиката выпал кейс `month(@d)`;
  `max`/`min` покрывают тот же класс.

## 6. Остаточные (pre-existing) находки, не связанные с изменением

Старые и новые бинарники дают по ним одинаковый результат — это не регресс:

- 4 `cursorFetchArguments` в наборе union-файлов: `API_PaymentOrder_MassInsert.sql`
  (PmOrdCursor, 45 vs 67; FETCH приходит из макроса
  `M_FETCHCUR_PMORDCURSOR_INSERT`) и `CBACalcPercent_FactRest.sql`
  (CalcFactRest_Cur, 7 vs 8 — переменная `@�AccNumber` с не-ASCII символом не
  матчится `reParserFetchIntoVar`, т.е. ограничение набора символов
  идентификатора/кодировки).
- `MT101_SequenceB_Copy2SL.sql` (2 находки 2 vs 68) — вне батча fa-contracts,
  поведение не изменилось.
- `datatype` на union-файлах в реальных данных не изменился (первая ветка —
  `@`-литералы); эффект union-фикса для `datatype` подтверждён синтетическим
  тестом (см. раздел 3), а не реальными файлами — это согласовано с артефактами.

Кандидаты на отдельные доработки: не-ASCII идентификаторы переменных в
`reParserFetchIntoVar`; разбор макро-FETCH вида `M_FETCHCUR_*` (45 vs 67).

## 7. Заключение

Изменение реализовано аккуратно и локально, артефакты change синхронизированы с
кодом (B1/B2/S1-S3 учтены, включая формулировки спек «первая непустая строка»,
многострочный `into`, union без `from` и non-vacuous сценарий `datatype`).
Эмпирика подтверждает: 5 целевых FP устранены, дополнительно сняты 10 FP того же
класса, регрессий на 250 «пограничных» файлах и non-contracts регресс-наборе нет,
`datatype`/`varUseAfterCursor` не претерпели нежелательных изменений. Доработку
можно архивировать; замечания M1-M3 — опциональные улучшения.

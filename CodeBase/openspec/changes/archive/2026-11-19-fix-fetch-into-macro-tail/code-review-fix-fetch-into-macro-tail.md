# Code review: fix-fetch-into-macro-tail

Дата: 2026-11-19
Объём: реализация в `internal/review` (`review_parser.go`, `review_rules_test.go`), актуализированные артефакты change (proposal/design/tasks/дельты), `cmd/root.go`, бинарь `CodeBase.exe` (build 1708).
Источник: `git diff` по `internal/review` и `cmd/root.go`; прогоны `go test`, `gofmt`, `go vet`, CLI `review` на эталонах, регрессионных файлах и батче fa-contracts; `openspec validate`.

## Вердикт

Реализация соответствует design.md и закрывает все заявленные эффекты:

- `cursorFetchArguments` перестаёт терять последнюю переменную перед `M_*`-макровызовом (FP устранён);
- `datatype` спаривает последнюю переменную (FN устранён);
- `varUseAfterCursor` получает полный список переменных и видит аргументы макроса ниже DEALLOCATE (FN устранён, поведение закреплено дельтой требования);
- `isCursorDeclarationBoundary` не изменён — декларации с `M_ISOLAT`/`M_NOLOCK_INDEX`/`M_FORCEORDER` парсятся как раньше.

Все четыре группы приёмки воспроизведены на build 1708, регрессов на подтверждённых baseline нет. Блокирующих замечаний нет; C1–C5 — Minor/Nit (дублирование паттерна, дрейф артефактов, уточнение сценария).

## Изменения в коде

| Файл | Суть |
|---|---|
| `internal/review/review_parser.go` | `reParserFetchIntoBoundary` (:21) получил альтернативу `\n[ \t]*m_[a-z0-9_]+[ \t]*\(`; новый `reParserStandaloneMacroCall` (:35, `(?i)^m_[a-z0-9_]+[ \t]*\(`); предикат `isStandaloneMacroCallLine` (:607); `isFetchStatementStopLine` (:615) возвращает предикат как стоп-строку; `isCursorDeclarationBoundary` не изменён |
| `internal/review/review_rules_test.go` | +231 строка: +2 кейса таблицы `parseFetchIntoStatement` (:1244, :1252), `TestIsStandaloneMacroCallLine` (:1286, 10 кейсов), `TestParseAllFetchIntoStatements_MacroTailStopsCollection` (:1516), `TestParseCursorDeclarations_MacrosInsideSelect_NotBoundary` (:3467), `TestCheckCursorFetchArguments_MacroTailCounted_NoFinding` (:3782), `TestCheckCursorFetchArguments_RealDesyncWithMacroTail_Finding` (:3807), `TestCheckVarUseAfterCursor_MacroArgAfterDeallocate_Finding` (:3838), `TestCheckVarUseAfterCursor_MacroNoWindowVarsAfterDeallocate_NoFinding` (:3867), `TestCheckDatatypeFetchInto_MacroTailLastVarChecked` (:3984) |
| `cmd/root.go` | `buildNumber` 1707 → 1708 |
| `CodeBase.exe` | пересобран, `0.9.5 build 1708` |

Область изменений замкнута на FETCH-пути: `reParserFetchIntoBoundary` → `findFetchIntoTailBoundary` → `parseFetchIntoStatement` (правила `cursorFetchArguments`/`datatype`), `isFetchStatementStopLine` → `parseAllFetchIntoStatements` (`varUseAfterCursor`). Других потребителей у фильтров нет.

## Соответствие plan

| Decision | Реализация | Тест |
|---|---|---|
| 1. Паттерн `(?i)^m_[a-z0-9_]+[ \t]*\(`, обязательный `_`, `[ \t]` вместо `\s` | :21, :35 | :1286 (`max`/`min`/`month` → false, `M_FORCEORDER` → false) |
| 2. Синхронное расширение обоих фильтров, `isCursorDeclarationBoundary` не трогать | :21 + :625 (`return isStandaloneMacroCallLine(trimmed)`); `isCursorDeclarationBoundary` без правок | :3467 (M_ISOLAT/M_NOLOCK_INDEX/M_FORCEORDER внутри SELECT) |
| 3. Спаны сжимаются, поведение `varUseAfterCursor` меняется и отражено дельтой | макро-строка не попадает в `statementLines` → `LineEnd` = последняя строка переменной | :1516 (span `[3,3]`), :3838 (finding на строке макроса), :3867 (без переменных → 0) |
| 4. Отдельный префиксный предикат вместо `reStandaloneMacroLine`, с обоснованием | :22–34 комментарий, :35 | :1286 (многострочный вызов по первой строке → true) |
| 5. Паттерн ограничен FETCH-фильтрами | изменены только два фильтра | :3782/:3807 (fragment-путь), :1516 (file-путь) |

Дельты:
- `review/postgre-requirements` — добавлена MODIFIED-дельта требования «Использование переменной после закрытия курсора» (F1 прошлого ревью закрыт) + 2 сценария; требование `cursorFetchArguments` дополнено макро-хвостом;
- `review/fine-code` — дополнено требование «Потенциальная потеря точности» + сценарий;
- `proposal.md` — Modified Capabilities и Impact синхронизированы; `design.md` — Non-Goals (макросы без скобок, макрос на той же строке), Risks (batch, FP-классы), Decision 4 с обоснованием отдельного предиката.

## Фактическая приёмка

| Проверка | Результат |
|---|---|
| `go test -count=1 ./internal/review/` | `ok` (1.517s, без кэша) |
| `go test ./...` | зелёный (все пакеты) |
| `gofmt -l internal/review/` | пуст |
| `go vet ./internal/review/` | без замечаний |
| `openspec validate fix-fetch-into-macro-tail` | `Change ... is valid` |
| Версия бинаря | `CodeBase 0.9.5 build 1708` |
| `Cons_OMCV_End_Accrual.sql`, `--rules cursorFetchArguments` | **0** (было 1: line 217) |
| Тот же файл, `--rules datatype` | **1** — baseline line 92 `API_Loan_FindListIDForPrd` (varchar→DSOPERDAY), без новых |
| Тот же файл, `--rules varUseAfterCursor` | **0** — baseline |
| `Cons_BankPartnerInstPrepare.sql` (все три правила) | datatype **3** (208 `@OpenAccBP`, 273 `@BankAlgMode`, 522 `@CntFundID`), cursor/varUse **0** — diff = 0 |
| `InstrumentSO_LoadStates.sql`, все правила | **11** с прежней разбивкой: 4 inSubQuery (214/214/251/263), 3 datatype (350×2/369), 2 useFuncInIndCol (434/451), 1 execNotExistsProc (591), 1 varUseAfterCursor (652 `@ChangedBrief`) — diff = 0 |
| Батч `review --files ... --rules cursorFetchArguments --json` | `success=true`, `files_failed=0`, `count=5` (список 1058 ASCII-путей; тот же результат, что в tasks 3.3 для 1049) |
| Остаточные 5 находок | Все — pre-existing FP двух других классов, вне скоупа: `API_LN_AfterRuleA_MassCrtAcc.sql:118` (`PROFILE_TIME_EX(...)` — не-`M_` макрос, теряется `@RelType`), `Cons_CalcAnnScheme8.sql:111/117/200/206` (UNION в DECLARE без FROM в первой ветке → 3 «выражения» вместо 2). Механизмы проверены по исходникам |

## Findings

### C1 (Minor) — заявленное «единое определение паттерна» не выполнено

Design Decision 4: «обе точки (`isFetchStatementStopLine` и boundary-альтернатива) выводятся из одного определения — разъезжаться паттерны не могут». Фактически в коде две независимые регэксп-константы:

- `review_parser.go:21` — альтернатива `\n[ \t]*m_[a-z0-9_]+[ \t]*\(`;
- `review_parser.go:35` — `reParserStandaloneMacroCall = (?i)^m_[a-z0-9_]+[ \t]*\(`.

Фрагмент `m_[a-z0-9_]+[ \t]*\(` продублирован; при будущем изменении паттерна (например, добавлении макросов без скобок или `PROFILE_*`) легко поправить только одну точку, и фильтры разъедутся — ровно то, что Decision 4 обещал исключить.

Рекомендация: вынести фрагмент в константу и собирать оба регэкспа из неё, например:

```go
const standaloneMacroCallPattern = `m_[a-z0-9_]+[ \t]*\(`
reParserStandaloneMacroCall = regexp.MustCompile(`(?i)^` + standaloneMacroCallPattern)
reParserFetchIntoBoundary  = regexp.MustCompile(`(?im)(?:...|\n[ \t]*` + standaloneMacroCallPattern + `)`)
```

Либо поправить формулировку Decision 4 («два паттерна синхронизируются, как и стоп-слова B1»), если осознанно оставляем дублирование.

### C2 (Nit) — Risks в design.md устарел относительно фактического результата

`design.md:44`: «Батч fa-contracts может вскрыть реальные рассинхроны среди 8 находок → … ожидание — 0 (все 8 имеют сигнатуру N−1)». Фактический итог зафиксирован только в `tasks.md:19`: после фикса остаётся 5 находок двух других pre-existing классов. Для целостности артефактов стоит обновить Risks/Notes: «после фикса остаточные 5 — FP классов non-M_ macro и UNION-in-DECLARE, вынесены в кандидаты-последователи».

### C3 (Nit) — сценарий дельты слабее покрывающего теста

`specs/review/postgre-requirements/spec.md:31–35` («Макровызов без переменных окна ниже DEALLOCATE») описывает `DEALLOCATE c` и макровызов, но не упоминает FETCH ниже DEALLOCATE, который и создаёт skip-спан. В таком виде сценарий не проверяет изменённую механику (без FETCH ниже DEALLOCATE находки не было бы и до фикса). Unit-тест `:3867` включает FETCH ниже DEALLOCATE и покрывает именно её.

Рекомендация: добавить в GIVEN «FETCH ниже DEALLOCATE», как в соседнем сценарии и в тесте.

### C4 (Nit) — двойной trim/lower в `isFetchStatementStopLine`

`isFetchStatementStopLine` (:616) делает `strings.ToLower(strings.TrimSpace(line))`, затем `isStandaloneMacroCallLine(trimmed)` (:608) снова вызывает `strings.TrimSpace`. Поведение корректное (case-insensitive задан в регэкспе), но лишняя нормализация. Можно передавать исходную строку или убрать trim из хелпера — на выбор.

### C5 (Nit) — `(` на следующей строке не признаётся границей

Из-за `[ \t]*` (осознанно, design Decision 1) форма

```
M_BUSINESSLOG_PARAM
  (@x, DSINT_KEY)
```

границей не является. Это согласуется с текущими данными FA, но в Non-Goals перечислены только макросы без скобок и макрос на одной строке со списком переменных. Стоит добавить этот случай в Non-Goals (или в комментарий предиката), чтобы поведение было явно зафиксировано.

## Прочее / наблюдения

- `--min-severity` в CLI работает как **верхняя** граница: `--min-severity 1` оставляет только severity 1, а не «1 и выше». Это pre-existing поведение CLI, не связанное с change, но при приёмке нужно использовать default (3); иначе `datatype` (severity 3) и `varUseAfterCursor` (severity 2) пропадут из вывода. В tasks 3.2/3.4 так и сделано.
- Батч-режим требует проиндексированные файлы; список с не-UTF8 путями даёт `success=false` с пустой агрегацией. Это отражено в design Non-Goals и tasks 3.3; воспроизводимая приёмка идёт по чистому ASCII-списку.
- `go test ./...` в основном отдал кэш; критичный пакет дополнительно прогнан с `-count=1` — зелёный.
- Pre-existing классы (non-`M_` макросы, UNION в DECLARE) корректно вынесены в кандидаты-последователи и не маскируются изменением; регресса по ним нет.

## Заключение

Реализация функционально корректна и соответствует design.md: оба FETCH-фильтра синхронно распознают `M_*`-макровызов, `isCursorDeclarationBoundary` не затронут, все три правила получают полный список переменных. Acceptance 3.2–3.4 воспроизведена на build 1708, батч даёт `success=true` и 5 объяснённых pre-existing FP, `go test ./...`, `gofmt`, `go vet` и `openspec validate` чистые. Блокирующих дефектов нет. Перед архивацией рекомендуется закрыть C1 (единая константа паттерна) и, опционально, C2/C3.

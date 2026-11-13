# Change Review: add-review-debug-profile-disabled-rule

- **Дата**: 2026-11-13
- **Ревьюер**: opencode (L3/pro/fast)
- **Вердикт**: ⚠️ **ТРЕБУЕТ ДОРАБОТКИ АРТЕФАКТОВ (F1–F5) ДО РЕАЛИЗАЦИИ.** Структура артефактов корректна (`openspec validate --strict` — valid), список макросов-носителей сверен с `MACROS.H` и точен, замеры proposal по FA в основном воспроизводятся (170 активных define, 149 потенциальных findings). Однако design/tasks не учитывают главное: `checkDebugProfileEnabled` не может использовать `r.fileContent` — пайплайн возвращает **expanded**-контент, из которого все `#define` уже вырезаны. Плюс finding будет возвращён в original-пространстве, а runner централизованно ремапит его как expanded. Без исправления этих двух пунктов правило либо не сработает вообще, либо даст неверные номера строк.

## Что проверялось

1. **Артефакты change**: `proposal.md`, `design.md`, `tasks.md`, дельта `specs/review/deploy-stoppers/spec.md`; `openspec validate add-review-debug-profile-disabled-rule --strict` — valid, `show --json --deltas-only` распознаёт 1 delta (ADDED).
2. **Код review-пайплайна**: `internal/review/{runner,review_parser,review_rules,catalog,types}.go`, `internal/review/review_api_proc_direct_call_test.go`, `internal/review/review_scope_integration_test.go`, `internal/parser/sql/sql_parser.go`, `internal/config/config.go`, `internal/fswalk/fswalk.go`, `README.md`.
3. **Живая кодовая база FA**: `rg` по `#define PROFILE` + replica `sanitizeSQLForMacroScan` на Go (временная программа в `%TEMP%\opencode`, удалена) — проверены множества файлов, первые строки define, include `macros.h`, точки профилирования и порядок директив.

## Подтверждённые положительно

- **Валидность и структура**: обязательные секции proposal/design/tasks на месте, дельта ADDED под `review/deploy-stoppers` валидна, имя capability совпадает с main-спекой.
- **Список носителей точен** (сверено с `FA\fa-commonfiles\CommonFiles\SERVER\Include\MACROS.H`):
  - `PROFILE_BEGIN/TIME/END` и `_EX`, `PROFILE_TIME_VAR1/2/3` — MACROS.H:534-587;
  - `RETURN_PROFILE(_EX)` — MACROS.H:590-599;
  - `DCL_PROC_BODY` — MACROS.H:751-754 (`PROFILE_BEGIN_EX`), `DCL_PROC_END` — 760-770 (`PROFILE_END_EX`);
  - `COMMIT_TRAN` — 1190-1197 (через `RETURN_PROFILE_EX`);
  - `__END_TRAN__` — 1335-1340 (`PROFILE_TIME_VAR1`), `__BEGIN/END_PROCEDURE__` — 1346-1354, `__CHECK_ERROR__` — 1356-1360;
  - `M_TRACE_DCL_PROC_END_START` — 2378-2388.
  - `DCL_PROC_BEGIN` (731-748), `GOEND` (1185-1188), `M_TRACE_DCL_PROC_END_FINISH` (2390-2395) действительно не содержат `PROFILE_*` — исключены корректно.
- **`PROFILE_AVAILABLE` мёртв в скриптах**: MACROS.H:5-15 сначала `#undef PROFILE_AVAILABLE`, затем include `local.h`, затем при отсутствии `PROFILE_AVAILABLE` убивает `PROFILE`; утверждение design подтверждено. В `MACROS.H:532` рубильник закомментирован (`--#define PROFILE`), копии заголовка не попадут в findings.
- **Переиспользование `sanitizeSQLForMacroScan` уместно**: однопроходной лексер `review_rules.go:1593-1649` маскирует `--`/`/* */` и содержимое `'...'` (включая `''`), сохраняет `\n`/`\r` и длину — номера строк считаются корректно. Паттерн `PROFILE\b` отсекает `PROFILE_AVAILABLE`; вхождения в комментариях/литералах не матчатся.
- **Замеры proposal воспроизводятся**:
  - 170 файлов с активным `#define PROFILE` (166 `.sql`/`.h` + 4 `.SQL` в fa-generalledger; после маскирования комментариев — 169, один define только внутри блочного комментария);
  - 149 файлов с define + точкой профилирования по методике B′ (~150 из proposal);
  - 20 файлов с активным define без точек — корректно не флагуются (совпадает);
  - 4 `.h`-файла-положительных: `INS_AllSync_Reload_proc.h` ×3 и `FCT_AllSync_Reload_proc.h` ×1.
- **`.h` проходят пайплайн**: `*.h` входит в `include_patterns` (`internal/config/config.go:399-401`), сопоставление include-паттернов регистронезависимо (`internal/fswalk/fswalk.go:121`), `getIndexedFile` не фильтрует по расширению, `ParseContent` возвращает `nil` error (sql_parser.go:1953). Отдельный фильтр расширений в review нет.
- **Регистрация/транспорт**: новое правило автоматически попадёт в `AllRuleIDs`/`RuleListString`/`DefaultEnabledRules` (`catalog.go:420-464`), CLI/MCP менять не нужно; задача 4.2 (README) присутствует.

## Findings

### F1 [High] — `r.fileContent` возвращает expanded-контент без `#define`; правило в описанном виде не сработает

`RunSQLFileCtx` читает файл, но в `r.exec.content` кладёт **результат `replaceMacros`** (`internal/review/runner.go:132-156`), а `fileContent` возвращает именно `r.exec.content` (`runner.go:362-368`). `replaceMacros` вызывает `stripMacroDeclarations`, который удаляет **все** строки `#define` из `Content` (`internal/review/review_parser.go:601-623, 742-762`). Проверка `#define PROFILE` по такому тексту невозможна: define физически отсутствует.

При этом proposal Impact перечисляет только `types.go`, `catalog.go`, `review_rules.go`, тесты и README — **`runner.go`/`review_parser.go` не указаны**, в design D2/D3 и tasks 2.1 тоже не сказано, откуда брать исходный текст. Реализатор по аналогии с `checkAPIExecOutsideFacade` (`review_rules.go:1651-1680`) возьмёт `r.fileContent` и получит 0 findings на всех сценариях.

Коварство в тестах: helper `newAPIRuleRunner` (`review_api_proc_direct_call_test.go:21-27`) кладёт в `exec.content` **сырой** `content`, а не `macroResult.Content`. Юнит- и интеграционные тесты, построенные по этому шаблону, будут зелёными, хотя production-путь не работает.

**Требуемая правка**:
- добавить в `reviewExecContext` исходный контент (например, `rawContent []byte`) и accessor `rawFileContent` (`runner.go`), либо читать файл в правиле через `encoding.ReadFile` (менее предпочтительно — дублирование I/O);
- явно записать в design D2/D3: «define и точки ищутся в **исходном** тексте (до `replaceMacros`); expanded-контент для этого правила не используется»;
- внести `internal/review/runner.go` (и, при выборе варианта с сохранением define в expanded, `review_parser.go`) в proposal Impact и tasks;
- добавить тест, проходящий через `RunSQLFileCtx` (по образцу `review_memory_unique_integration_test.go:87`), а не только прямой вызов check-функции.

### F2 [High] — Finding вернётся в original-пространстве, а runner ремапит его как expanded → неверная строка

Даже если правило получит исходный текст (F1), номер строки первого `#define` будет **original**. В конце `RunSQLFileCtx` все findings безусловно ремапятся `mapExpandedLineToOriginal(finding.Line, macroResult.SourceMap)` (`runner.go:195-198`). Для строки `#define`, удалённой из expanded-контента, обратного отображения не существует: `SourceMap` содержит только сохранившиеся строки. Пример: `#define PROFILE` в строке 1 → `SourceMap[0] = 2`; finding с `Line=1` превратится в `Line=2` (сдвиг равен числу удалённых `#define`-строк выше). Спека требует «на строке первого активного `#define PROFILE`».

**Требуемая правка** (выбрать и зафиксировать в design D2/D5):
- добавить в `Finding` признак original-пространства (например, `LineInOriginalSpace bool` с `json:"-"`) и в runner пропускать ремап для таких findings; либо
- сохранять для этого правила строку define в SourceMap (изменение `replaceMacros`/`stripMacroDeclarations` — более инвазивно, затрагивает все правила); либо
- вынести define-проверку в отдельный pre-parse шаг `RunSQLFileCtx` до `replaceMacros`, где findings добавляются уже после ремапа.
- Обязательно добавить e2e-тест `RunSQLFileCtx` на файле с несколькими `#define` выше `#define PROFILE` и проверить точную строку finding. Текущие прямые вызовы check-функций (как в `review_scope_integration_test.go:414-448`) этот дефект не ловят.

### F3 [Med] — Не учтён порядок `#define PROFILE` относительно `#include <macros.h>`: 3 ложных срабатывания

Требование считает нарушением любой активный define + точку, но препроцессор раскрывает `PROFILE_*` по состоянию `PROFILE` **на момент обработки `MACROS.H`** (MACROS.H:532-588). Если `#define PROFILE` стоит **после** `#include <macros.h>`, заголовок уже определил `PROFILE_*` как пустые (`#else`-ветка), и поздний define ничего не включает.

Замер: ровно 3 таких файла и все получат ложный deploy-stopper:
- `fa-contracts/Test/Script_Utils/conv-consumer/tag13_PsExtendActions.sql` — `#include <macros.h>` строка 2, активный `#define PROFILE` строка 5, точка строка 105;
- те же файлы в `fa-contracts-11016` и `fa-contracts-11017` (define 5, include 2, точки 97/97).

Файлы тестовые (conv-consumer), но правило позиционируется как zero-FP на безопасных состояниях. Дополнительно не оговорён случай `#define PROFILE` **после** первой точки профилирования (на FA таких файлов 0, но семантика та же: ранние вызовы не инструментируются).

**Требуемая правка**: либо добавить в требование/design условие «первый активный define находится до первого `#include <macros.h>` (или до первой точки)», сценарий и тест; либо явно зафиксировать в Risks, что define-after-include даёт 3 известных FP (список файлов) и они принимаются. Молча оставлять нельзя — это ровно тот класс, который правило обещает не флагует.

### F4 [Med] — Impact/Risks: числа верны, но объяснение «3 мёртвых define» неверно; пропущен класс false-negative через include

Замеры proposal (170/≈150/20) воспроизводятся, но текст Impact содержит фактическую ошибку:

> «3 файла с define без `#include <macros.h>` — мёртвый define, не флагуются».

Это те же 3 `teg13_ConvertPaySchedule.sql`, но они **не мёртвые и будут флаговаться**: файлы делают `#include <dcUtil.h>`, а `dcutil.h` сам содержит `#define PROFILE` (строка 1) + `#include <macros.h>` (строка 2), т.е. профилирование реально включено; точки в файле есть → B′ даёт finding. Настоящие неэффективные define — это 3 файла после include из F3, и они как раз **флагуются ложно**.

Кроме того, design Risk «транзитные точки через include» описывает только половину картины. Обратный случай — **транзитный define через include**: скрипт содержит точки (`DCL_PROC_BODY`, `PROFILE_*`), подключает заголовок с активным `#define PROFILE` (`dcutil.h`) и не имеет собственного define — правило такой define не увидит. На conv-consumer (3 ветки) файловому паттерну «точки + `#include <dcutil.h>` + нет своего define» соответствует до 37 файлов; правда, многие из них подключают `macros.h`/`link_macros.h` раньше `dcutil.h`, из-за чего define в `dcutil.h` уже неэффективен, и точное число активных требует резолва include-цепочек (вне области change). Тем не менее в Risks класс надо назвать явно, а не утверждать, что кластер «прикрыт» хедером: `dcutil.h` сам без точек и не флагуется.

**Требуемая правка**: переписать абзац Impact про «3 мёртвых define» и дополнить Risks двумя классами (define-after-include FP ×3; define-through-include FN — класс `dcutil.h`, до 37 Test-кандидатов, точный размер требует include-резолва); числа внести после задачи 4.3.

### F5 [Med] — Task 2.3 ожидает finding на `dcutil.h`, что противоречит требованию

Task 2.3 предлагает верифицировать `.h` прогоном review на `FA\fa-contracts\...\conv-consumer\dcutil.h` с ожиданием finding. В этом файле есть активный `#define PROFILE`, но **нет ни одной точки профилирования** → по требованию («активный define без точек — finding MUST NOT формироваться») finding быть не должно. Проверено: `rg` по носителям в `dcutil.h` пуст.

Для положительного `.h`-кейса нужно брать `INS_AllSync_Reload_proc.h` (3 копии) или `FCT_AllSync_Reload_proc.h` — там активный define и точки (`__BEGIN/_END_PROCEDURE__`, `PROFILE_TIME_VAR*`). Для отрицательного — как раз `dcutil.h`.

Там же фраза «если текущий review-пайплайн ограничивает вход `.sql`, фильтр расширяется» неактуальна: фильтра расширений в `RunSQLFileCtx` нет, `.h` уже проходит. Вместо этого стоит проверить реальные риски `.h`: `getIndexedFile` (файл должен быть в индексе и иметь `ds_product_id`), CP866-чтение и tolerant-парсинг.

### F6 [Low] — Исключение define-строк для точек неполно

Design D3 исключает только строку `^\s*#\s*define\s+<имя носителя>`. Не исключаются:
- helper-define, в теле которого есть носитель: `#define DBG1(...) PROFILE_TIME_VAR1(...)` (реальные примеры в FA, но среди 170 файлов с активным `PROFILE` таких нет — проверено 0);
- многострочные `#define` с продолжением `\`, где носитель стоит на строке-продолжении (среди 170 — 0).

На текущем FA это не даёт FP, но эвристика хрупкая: гибридный продуктовый хедер с `#define PROFILE` и собственным helper-макросом будет ошибочно флагован из-за самого определения. Рекомендуется исключать весь блок `#define` (первую строку + продолжения) либо явно зафиксировать ограничение и тест.

### F7 [Low] — Унаследованные ограничения `sanitizeSQLForMacroScan`

Лексер не поддерживает вложенные блочные комментарии T-SQL (`/* /* */ ... */`), не маскирует двойные кавычки и bracket-идентификаторы и не знает `#if 0`/`#ifdef`. Для API_EXEC это было приемлемо; для define-детектора вложенный комментарий может дать FP (define внутри внешнего комментария после первого `*/`), а `[a--b]` — сорвать разбор строки. На FA таких кейсов не выявлено, но пункт стоит добавить в Risks (или расширить лексер, если требуется строгая семантика).

### F8 [Low] — Область `.sql`/`.h` в требовании не соответствует фактическому отсутствию фильтра

Требование говорит «Правило применяется к файлам `.sql` и `.h`», но `review` принимает любой индексированный файл; правило будет выполняться и на `.inc`/`.pas`/`.t01`, если его вызвать. Если это ограничение по смыслу, нужен guard в check-функции. При добавлении guard обязательна регистронезависимая проверка расширения: в FA есть 4 боевых файла `fa-generalledger/.../*.SQL` с активным define и точками (`LastDateCalc_New.SQL`, `RulAddrAbbreviation_{Update,Insert,Delete}.SQL`), которые входят в 149 ожидаемых findings. Сравнение `filepath.Ext(path) == ".sql"` их потеряет.

### F9 [Info] — `ruleMeta.Severity` не влияет на вывод

Как и в предыдущем change, поле `Severity` в `ruleCatalog` пайплайном не читается (`buildRuleTasks` берёт только `Build`). Task 1.2 («зарегистрировать severity 1») описывает метаданные; фактическая severity 1 обеспечивается только явным `Severity: SeverityDeployStopper` в `Finding` (task 2.2 это подразумевает — стоит сказать явно).

### F10 [Info] — «Индекс не требуется» вводит в заблуждение

Proposal Impact: «Индекс не требуется (правило textual, per-file)». Само правило не использует таблицы, но `RunSQLFileCtx` требует, чтобы файл был в индексе (`getIndexedFile`), имел `ds_product_id` и была доступна БД (`buildReviewScope`). Корректная формулировка — «новых полей/структур индекса не требуется».

### F11 [Info] — Документация CLI/MCP говорит только про SQL-файл

`codebase review` (`cmd/review.go:48`) и MCP `codebase_review_sql` описаны как «single SQL file», тогда как правило официально распространяется и на `.h`. Функционально `.h` работает, но help/description и release notes стоит синхронизировать.

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| F1 | `r.fileContent` — expanded-контент без `#define`; правило не сработает | High | Raw-content accessor + `runner.go` в Impact/tasks + e2e-тест через `RunSQLFileCtx` |
| F2 | Finding в original-пространстве ремапится как expanded → сдвиг строки | High | Признак original-space / pre-parse шаг / сохранение define в SourceMap + e2e-тест строки |
| F3 | Define после `#include <macros.h>` неактивен → 3 FP (`tag13_PsExtendActions.sql` ×3) | Med | Условие порядка в спеке+design+сценарий/тест либо явный accepted-FP в Risks |
| F4 | «3 мёртвых define» — ошибка; не описан класс FN через include `dcutil.h` | Med | Переписать Impact, дополнить Risks двумя классами, числа после 4.3 |
| F5 | Task 2.3 ждёт finding на `dcutil.h`, где точек нет | Med | Позитив — `*_Reload_proc.h`; негатив — `dcutil.h`; убрать мифический фильтр расширений |
| F6 | Исключение define-строк не покрывает helper/multiline define | Low | Исключать весь блок `#define` или зафиксировать ограничение + тест |
| F7 | Ограничения лексера (nested comments, quotes, `#if`) | Low | Добавить в Risks |
| F8 | `.sql`/`.h` не enforced; guard должен быть case-insensitive (4 `.SQL`) | Low | Уточнить формулировку или добавить guard с `EqualFold` |
| F9 | `ruleMeta.Severity` не влияет на вывод | Info | Явно задавать severity в `Finding` |
| F10 | «Индекс не требуется» неточно | Info | «Новых полей индекса не требуется» |
| F11 | CLI/MCP говорят «SQL file», правило и для `.h` | Info | Синхронизировать help/description/release notes |

Содержательно change полезен и хорошо обоснован: носители выверены по `MACROS.H`, безопасные состояния (комментарий/литерал/`PROFILE_AVAILABLE`/`undef`) продуманы, масштаб первого прогона (170/149/20) подтверждается. Блокируют реализацию только F1–F2 (механика контента и строк), F3–F5 — правки артефактов и фикстур. После их устранения повторное ревью не требуется.

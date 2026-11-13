# Proposal: add-review-debug-profile-disabled-rule

## Why

В Diasoft 5NT существует легаси-механизм отладочного профилирования SQL: макросы `PROFILE_BEGIN/TIME/END(_EX)`, `PROFILE_TIME_VAR1-3` из общего заголовка `MACROS.H` (ветка `#ifdef PROFILE`), разворачивающиеся в вызовы серверных процедур `ProfileBegin/ProfileTime/ProfileEnd/ProfileTimeVar` с таблицами `ProfileLog`/`pProfileVar`. Механизм предназначен для разработки и анализа проблемных ситуаций; в деплой он обязан поставляться выключенным: без макроса `PROFILE` препроцессор разворачивает все `PROFILE_*` в пустую строку. Однако в кодовой базе FA выявлено **170 файлов с активным (незакомментированным) `#define PROFILE`** — забытый отладочный рубильник, попавший в бой, включая API-фасады (`FCD_CRD_AutoNum_GetListNumberInfo.sql`), API-процедуры массовых операций (`API_CCred_MassExecuteAction.sql`) и боевые процедуры (`Cons_MassOverdraft.sql`). На стенде с `local.h`/`PROFILE_AVAILABLE` инструментация активируется: в производственный код попадают `insert` в отладочные таблицы, `print`-вывод и DML в `pProfileVar`. Детектора сегодня нет.

## What Changes

- Новое deploy-stopper правило `debugProfileEnabled` (severity 1): finding формируется, если в проверяемом файле есть **активный** `#define PROFILE` (директива вне комментариев и строковых литералов) **и** присутствует хотя бы одна точка профилирования:
  - прямой вызов `PROFILE_BEGIN(_EX)`, `PROFILE_TIME(_EX)`, `PROFILE_TIME_VAR1/2/3`, `PROFILE_END(_EX)`;
  - или каркасный макрос-носитель, тело которого в `MACROS.H` содержит профилировочные вызовы: `RETURN_PROFILE(_EX)`, `DCL_PROC_BODY`, `DCL_PROC_END`, `COMMIT_TRAN`, `__END_TRAN__`, `__BEGIN_PROCEDURE__`, `__END_PROCEDURE__`, `__CHECK_ERROR__`, `M_TRACE_DCL_PROC_END_START`.
- Безопасные состояния (finding не формируется): define отсутствует; define закомментирован (`-- #define PROFILE`); define активен, но точек профилирования в файле нет — макросам нечего разворачивать, профилирования не будет; первый активный define расположен **после** `#include <macros.h>` — препроцессор обрабатывает заголовок линейно, `PROFILE_*` уже развернуты в пустые строки, поздний define ничего не включает.
- Finding MUST формироваться не более одного раза на файл — на строке первого активного `#define PROFILE` в координатах исходного файла (до подстановки макросов).
- Правило применяется к файлам с расширением `.sql` или `.h` (сравнение без учёта регистра — в fa-generalledger есть боевые `.SQL` с активным define); на прочих расширениях finding не формируется. Реальные `.h`-фигуранты: `INS/FCT_AllSync_Reload_proc.h` (define + точки); `dcutil.h` — носитель define без собственных точек, не флагуется.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: добавлено требование «Включённое отладочное профилирование» (`debugProfileEnabled`).

## Impact

- `internal/review/types.go` — константа `RuleDebugProfileEnabled = "debugProfileEnabled"` и служебное поле `Finding.LineInOriginalSpace` (`json:"-"`): строка finding указана в исходном файле, до подстановки макросов.
- `internal/review/runner.go` — поле `rawContent` в `reviewExecContext` + accessor `rawFileContent()`. Существующий `fileContent` возвращает expanded-контент, из которого `replaceMacros`/`stripMacroDeclarations` уже вырезали все `#define`-строки — правило обязано работать по исходному тексту. Там же: условный пропуск ремапа `mapExpandedLineToOriginal` для findings с `LineInOriginalSpace` — для удалённой define-строки обратного отображения в `SourceMap` нет, безусловный ремап сдвинул бы строку.
- `internal/review/catalog.go` — регистрация правила (severity 1), попадание в `DefaultEnabledRules`.
- `internal/review/review_rules.go` — новая check-функция `checkDebugProfileEnabled` на **исходном** тексте (до `replaceMacros`): санитизация существующим однопроходным лексером `sanitizeSQLForMacroScan` (маскирует комментарии и строковые литералы с сохранением длины и номеров строк), поиск активной директивы, условие порядка относительно `#include <macros.h>`, поиск точек профилирования по фиксированному списку макросов-носителей, guard расширения (`.sql`/`.h`, без учёта регистра).
- `internal/review/review_debug_profile_test.go` — unit-тесты; `internal/review/review_scope_integration_test.go` — интеграционные кейсы, включая e2e через `RunSQLFileCtx` (production-путь `replaceMacros`); `README.md` — каталог правил; help CLI `codebase review` и описание MCP `codebase_review_sql` («single SQL file» → SQL-файл или `.h`-заголовок), release notes.
- CLI `codebase review` и MCP review tool — новый rule ID доступен через каталог, API не меняется. Новых полей/структур индекса не требуется; файл обязан быть проиндексирован и иметь `ds_product_id` (общий контракт `RunSQLFileCtx`).
- **Масштаб первого прогона на FA** (фактический полный свуп build 1694, 2026-11-13; 170 кандидатов rg по всем регистрам расширений): **147 findings severity 1 на 147 файлах, 23 корректных пропуска, 0 ошибок**. Разбивка 147 по продуктам: fa-contracts 44 (15 + branch-копии 11016: 14, 11017: 15), fa-cards 32 (16 + 110109: 16), fa-custody 25, fa-reports 24, fa-generalledger 13, fa-factoring 7, fa-warranty 1, fa-payments 1. Строка finding — первый активный define: 98 файлов стр. 1, 40 — стр. 5, 6 — стр. 2, 3 — стр. 4. Пропуски (23 = 3 подавлены условием порядка + 20 без точек): `tag13_PsExtendActions.sql` ×3 — подавлены условием порядка (define после `#include <macros.h>`); без точек профилирования в самом файле — `tag13_PsExtCreate.sql` ×3 (define предшествует include, но точек нет), `dcutil.h` ×3, `.sql`-обёртки `INS/FCT_AllSync_Reload.sql` ×4, `UCSPCBINLoad` ×2, `GenerateSwiftFromDepoInst_54x`, GL `FillBranch*` ×3 + `CalcRestByCalendar2`, factoring `SDMFactLoad`/`FactoringCommission`/`FactoringActions`. Кластер `teg13_ConvertPaySchedule.sql` (×7, включая `utils/` и branch-копии) и `teg13_FlowConvertPaySchedule.sql` ×3 содержит собственный `#define PROFILE` на стр. 1 — флагуется корректно (не транзитивно). Отклонение от оценки change-review (~146): +1 finding — расхождение методик (реплика лексера ревью vs production-правило), в пределах одного файла. Класс false-negative «define через include» (скрипт с точками и `#include <dcutil.h>`, без собственного define) — осознанное ограничение per-file анализа, см. design Risks.

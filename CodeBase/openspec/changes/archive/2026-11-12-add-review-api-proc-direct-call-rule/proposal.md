# Proposal: add-review-api-proc-direct-call-rule

## Why

API-процедуры (`API_*`) — публичный контракт продукта в архитектуре Diasoft 5NT. Прямой `exec` чужой API-процедуры в обход фасада (`API_EXEC` внутри `FCD_*`/`Facade_*`/`API_*` скрипта) нарушает архитектуру межпродуктового взаимодействия. Сегодня такое нарушение детектируется только generic-правилом `foreignProcedureUsing` (severity 3, FineCode) без семантики «нужен фасад» и не блокирует деплой — хотя следствием нарушения может быть неработающий вызов на продуктиве.

## What Changes

- Новое deploy-stopper правило `apiProcDirectCall` (severity 1): прямой `exec` процедуры с префиксом `API_` (без учёта регистра), владелец которой — другой продукт, формирует blocking finding. Прямой вызов API-процедуры **своего** продукта легален (разрешение на уровне продукта, не каталога). Дедуп: один finding на уникальное имя процедуры (без учёта регистра), на строке первого вхождения.
- Новое deploy-stopper правило `apiExecOutsideFacade` (severity 1): макрос `API_EXEC` допустим только в скриптах, чьё имя (без учёта регистра) начинается с `FCD_`, `Facade_`, `API_` или `_fcd_` — препроцессор Diasoft раскрывает `API_EXEC` только в таких скриптах, в остальных он остаётся нераскрытым и вызов не работает. Вхождения в комментариях и строковых литералах не учитываются.
- Правило `foreignProcedureUsing` сужается: имена с префиксом `API_` (без учёта регистра) исключаются из него (покрываются новым deploy-stopper правилом с более строгой severity) — устраняет дублирование finding'ов на один и тот же вызов.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: добавлены два требования — «Прямой вызов API-процедуры чужого продукта» (`apiProcDirectCall`) и «API_EXEC вне фасадного скрипта» (`apiExecOutsideFacade`).
- `review/fine-code`: сужено требование «Вызов процедур из других продуктов» — `foreignProcedureUsing` MUST NOT формироваться для имён с префиксом `API_`.

## Impact

- `internal/review/types.go` — константы `RuleAPIProcDirectCall`, `RuleAPIExecOutsideFacade`.
- `internal/review/catalog.go` — регистрация обоих правил (severity 1), попадание в `DefaultEnabledRules` автоматически.
- `internal/review/review_rules.go` — новые check-функции (по образцу `checkForeignProcedures`: фильтр `parsed.Calls` по префиксу `API_` + `cachedLookupProcedureScopeInfo` + сравнение с `file.DsProductID`); textual-scan `API_EXEC(` по содержимому файла для level-2; сужение `checkForeignProcedures` (skip `API_`-префикса).
- `internal/review/review_api_proc_direct_call_test.go` — unit-тесты новых правил (фикстуры `reviewExecContext`); `internal/review/review_scope_integration_test.go` — интеграционные кейсы скоупа; `README.md` — каталог правил.
- CLI `codebase review` (`--rules`) и MCP review tool — новые rule IDs доступны автоматически через каталог; API не меняется.
- Владелец API-процедуры берётся из индекса (`BatchLookupProcedureProductIDs`, уже преукешится в `prewarmProcCaches`) — файловых сканов и новых запросов к api_contracts не требуется.
- **Ожидаемый масштаб первого прогона на FA (фактический прогон build 1692, полный свуп)**: 336 severity-1 findings `apiProcDirectCall` на 233 файлах из 2730 файлов-кандидатов с `exec API_*` (18 файлов-кандидатов неиндексированы — Test conv-consumer с нечитаемыми именами, в счёт не входят). Разбивка 336 = 217 findings на 145 файлах основной линии продуктов + 60/40 — branch-копии (fa-contracts-11016/11017, fa-cards-110109 дублируют нарушения основной линии) + 59/48 — Test-файлы (включая Test-каталоги внутри branch-копий; множества branch/Test пересекаются, поэтому сумма по категориям сходится точно: 217+60+59=336). По продуктам основная линия: fa-reports 52, fa-contracts 52, fa-generalledger 40, fa-financialasset 33, fa-custody 25, fa-payments 23, fa-contracts-ext 7 и др. SQL-аппроксимация change-ревью (274/188) отличалась методикой учёта branch/Test-множеств; ранжирование продуктов совпадает. `apiExecOutsideFacade` — 0 findings на текущем индексе. Числа зафиксировать в release notes.
- Документация: каталог правил в `README.md` (~1093-1098) — новые rule IDs и пометка о сужении `foreignProcedureUsing`.

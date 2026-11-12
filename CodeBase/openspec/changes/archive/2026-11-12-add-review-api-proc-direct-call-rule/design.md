# Design: add-review-api-proc-direct-call-rule

## Context

Review-конвейер (`internal/review`) уже предоставляет всю инфраструктуру для правила: `RunSQLFileCtx` парсит файл (после раскрытия `#define`-макросов), батч-преукешит владельцев всех `parsed.Calls` через `prewarmProcCaches` → `cachedLookupProcedureScopeInfo` (`store.ProcedureScopeInfo{ProductID, ExistsInArea}`), и запускает правила каталога (`catalog.go`) параллельно. Существующее правило `checkForeignProcedures` (review_rules.go) — прямой образец: dedupe вызовов → lookup владельца → skip при `ProductID == 0` или `ProductID == file.DsProductID` → иначе finding.

Разведка по индексу FA подтвердила семантику:
- `API_Loan_GetLstTurnDebtByDate` определён в самом `fa-contracts` (подкаталог `API_Credit/Server/Loan/`) → прямой exec из Consumer легален (разрешение product-level).
- `API_FinOper_FindLstByID` — контракт, владелец `fa-administrator`; `fa-contracts` его только потребляет → прямой exec = нарушение.
- `API_EXEC` не матчится exec-regex'ом SQL-парсера (`\bexec`), поэтому фасадные файлы в `parsed.Calls` не шумят; макрос остаётся в раскрытом содержимом файла (определён в `API.h`, а не `#define` файла).

Ограничение: `API_EXEC` в review доступен только textual-scan'ом содержимого — структурированные `APIMacroInvocation` парсятся индексатором (`parser/apimacro`), а не SQL-парсером review.

## Goals / Non-Goals

**Goals**
- Новое deploy-stopper правило `apiProcDirectCall`: exec `API_*` чужого продукта → finding severity 1, дедуп по уникальному имени (без учёта регистра).
- Новое deploy-stopper правило `apiExecOutsideFacade`: `API_EXEC(` в скрипте, имя которого (без учёта регистра) не начинается с `FCD_`/`Facade_`/`API_`/`_fcd_` → finding severity 1.
- Сужение `foreignProcedureUsing`: skip имён с префиксом `API_` без учёта регистра (устранение дубля severity 3 поверх severity 1).

**Non-Goals**
- Сверка с `api_contracts` (DSArchitect) как дискриминатор API-процедуры — принят префикс `API_`.
- Детект фасада по пути `\Facade\` или по наличию `API_EXEC` в файле — критерий только префикс имени.
- Анализ корректности аргументов `API_EXEC` (сигнатура контракта).
- Изменения API CLI/MCP: новые rule IDs появляются в `--rules`/каталоге автоматически.

## Decisions

1. **Дискриминатор — префикс `API_`.** Просто, предсказуемо, не зависит от полноты DSArchitect-данных. Альтернатива (сверка с `api_contracts`) отпала: used_service-декларации потребителей засоряют выборку владельца. Предикат — общий хелпер `isAPIProcedure` на базе `normalizeIdentifier`/lowercase (SQL-идентификаторы регистронезависимы): один и тот же предикат в новом правиле и в сужении `foreignProcedureUsing`, иначе lowercase `api_foo` уйдёт из-под deploy-stopper'а, но останется в severity 3.

2. **Severity 1 (DeployStopper).** Нарушение означает вызов через нераскрытый/чужой контракт — риск на продуктиве. Пользователь подтвердил.

3. **RuleID `apiProcDirectCall`** (и `apiExecOutsideFacade` для level-2). Стиль — camelCase-имена каталога.

4. **Сужение `foreignProcedureUsing` вместо жизни с дублями.** `API_*`-вызовы переквалифицируются из FineCode в DeployStopper; поведение не теряется (только строже). Старое требование fine-code модифицируется в спеке.

5. **Level-2 в scope, критерий — регистронезависимый префикс имени файла.** Пользователь подтвердил: препроцессор раскрывает `API_EXEC` только в скриптах с префиксами `FCD_`/`Facade_`/`API_`. Регистронезависимость — эмпирический факт поставки FA: 1 936 боевых lowercase `fcd_*.sql` фасадов с `API_EXEC` работают (8 154 lowercase-файла на индексе против 9 204 uppercase `FCD_*`). В разрешённые префиксы включён `_fcd_` (seed-шаблон `_fcd_tpl_seed.sql` в `API_Kernel/Server/Facade/` — единственный оставшийся после нормализации регистра файл с `API_EXEC` вне `FCD_`/`Facade_`/`API_`; это шаблон генерации фасада, FP на нём недопустим). Реализация — regex `(?i)API_EXEC\s*\(` по содержимому после `replaceMacros` и `removeComments`; basename из `file.Path` (Build получает `*indexedFile`); finding возвращается в expanded-строках — централизованный ремап выполняет runner. Вне product-lookup: нарушение чисто синтаксическое.

6. **Дедуп на уникальное имя** — rule-local `seen map[string]struct{}` по `normalizeIdentifier(call.Name)`, finding на строку первого вхождения. `dedupeProcedureCalls` дедуплицирует по ключу «имя:строка» и требованию «не более одного finding на имя» НЕ удовлетворяет (замер: 274 findings по имя vs 362 по имя+строка, +32%); он используется только как первичный фильтр (nil/keyword), как у foreign-правил.

7. **Lookup владельца — существующий `cachedLookupProcedureScopeInfo`**, преукешится в `prewarmProcCaches` — новых SQL-запросов и изменения схемы БД не требуется.

## Risks / Trade-offs

- **Ложные срабатывания на не-API `API_*`-процедурах**: если какой-то продукт назвает обычную процедуру с `API_` и она чужая — будет deploy-stopper. Смягчение: по конвенции Diasoft префикс зарезервирован за API-контрактами.
- **Textual-scan `API_EXEC`**: вхождения в комментариях исключаются `removeComments` до скана; вхождения в строковых литералах не учитываются по требованию (эмпирически на FA таких вхождений не найдено).
- **Порядок раскрытия макросов**: `replaceMacros` выполняется до правил; если файловый `#define` подменяет имя API-процедуры, правило видит раскрытое имя — это корректное поведение (деплой-юнит именно раскрытый).
- **Масштаб первого прогона (фактический, build 1692, полный свуп 2730 файлов-кандидатов)**: 336 severity-1 findings на 233 файлах = 217/145 основная линия продуктов + 60/40 branch-копии (fa-contracts-11016/11017, fa-cards-110109) + 59/48 Test-файлы (включая Test-каталоги branch-копий; множества пересекаются, 217+60+59=336); 18 файлов-кандидатов неиндексированы и в счёт не входят. По продуктам основной линии: fa-reports 52, fa-contracts 52, fa-generalledger 40, fa-financialasset 33, fa-custody 25, fa-payments 23, fa-contracts-ext 7. SQL-аппроксимация change-ревью (274/188) отличалась методикой учёта branch/Test-множеств; ранжирование продуктов совпадает. Числа внести в release notes.
- **Пересечение с `execNotExistsProc`**: если чужая `API_*`-процедура существует в индексе, но отсутствует в разрешённой области владельца, на один вызов придут две severity-1 находки (`apiProcDirectCall` + `execNotExistsProc`). Это допустимо: семантики разные (архаитектурный запрет vs отсутствие в области), зафиксировано в требовании спеки.
- **`ruleMeta.Severity` описательное**: пайплайн его не читает, фактическая severity задаётся в `Finding` — check-функции обязаны выставлять `Severity: SeverityDeployStopper` явно.

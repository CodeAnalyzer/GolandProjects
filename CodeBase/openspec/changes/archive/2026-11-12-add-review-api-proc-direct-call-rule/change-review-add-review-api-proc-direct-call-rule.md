# Change Review: add-review-api-proc-direct-call-rule

- **Дата**: 2026-11-12
- **Ревьюер**: opencode (L3/pro/fast)
- **Вердикт**: ⚠️ **ГОТОВ К РЕАЛИЗАЦИИ ПОСЛЕ ИСПРАВЛЕНИЯ F1–F4**. Структура артефактов корректна (`openspec validate --strict` — valid), заявления design о механике review в основном подтверждены кодом. Но два содержательных дефекта дадут массовые ложные deploy-stopper'ы на текущем индексе FA: **регистрозависимая проверка префикса имени файла** (F1) и **шаблон `_fcd_tpl_seed.sql`** (F2). Плюс спецификация требует дедуп «по имени», а запланированный `dedupeProcedureCalls` дедуплицирует «имя+строка» (F3). После правок артефактов — реализация без повторного ревью.

## Что проверялось

1. Структура: `openspec validate add-review-api-proc-direct-call-rule --strict` — valid; 2 дельты (`review/deploy-stoppers` ADDED ×2 требования, `review/fine-code` MODIFIED — имя «Вызов процедур из других продуктов» существует в main-спеке); proposal/design/tasks содержат все обязательные секции.
2. Код: `internal/review/{types,catalog,review_rules,review_helpers,review_lookup,runner,review_parser}.go`, `internal/parser/sql/sql_parser.go`, `internal/parser/apimacro/parser.go`, `internal/parser/pas/pas_parser.go`, `internal/store/db_lookup_sql.go`, `cmd/review.go`, `internal/mcp/registry.go`.
3. Живой индекс (localhost:5435/codebase, SELECT-only; временная Go-программа удалена) + файловая разведка по `D:/GITHUB/GolandProjects/FA`: измерены популяции `EXEC API_*` и `API_EXEC` по продуктам, владельцам и именам файлов.

## Подтверждённые положительно

- **Механика правила apiProcDirectCall воспроизводима по образцу**: `checkForeignProcedures` (review_rules.go:1551) — dedupe → `cachedLookupProcedureScopeInfo` → skip `ProductID == 0`/`== file.DsProductID`. `BatchLookupProcedureProductIDs` (db_lookup_sql.go:533) разрешает владельца «свой продукт раньше чужого», т.е. product-level семантика из требования уже обеспечена существующим lookup'ом. `prewarmProcCaches` (review_lookup.go:393) действительно прогревает все `parsed.Calls`, включая `API_*` — новых запросов не нужно.
- **`API_EXEC` не попадает в `parsed.Calls`**: exec-regex `\bexec(?:ute)?\s+...` (sql_parser.go:43) не матчит `EXEC` внутри `API_EXEC` (перед `E` стоит `_` — word char, границы слова нет). Подтверждено и эмпирически: 18 425 macro-invocation `exec_contract`, при этом `parsed.Calls`-ветка их не видит.
- **Сужение `foreignProcedureUsing` семантически безопасно**: правило и так давало finding только для существующих чужих процедур; `API_*`-имена переквалифицируются в severity 1, поведение не теряется.
- **Дискриминатор-префикс подтверждается данными**: на индексе 3 350 пар (файл, имя) прямых `EXEC API_*`; 3 076 из них — своя API-процедура (легальны, в т.ч. пример design `API_Loan_GetLstTurnDebtByDate` из `BaseAlgAmrtCostSinglePmnt.sql` продукта fa-contracts), 274 — чужие.
- **Impact нового правила измерим**: 274 находки (дедуп по файл+имя) на 188 файлах; по продуктам-потребителям: fa-contracts 107, fa-reports 42, fa-generalledger 42, fa-financialasset 25, fa-custody 21, fa-payments 20, fa-contracts-ext 7 и др.; по владельцам: fa-administrator 127, fa-generalledger 79, fa-contracts 24, fa-warranty 23. Это ожидаемый целевой эффект, но масштаб нужно зафиксировать (F4).
- **Спека fine-code корректна**: MODIFIED воспроизводит полный текст требования + старый сценарий, новый сценарий «не дублируется» согласован с новым правилом.
- **CLI/MCP**: новые rule ID подхватываются каталогом автоматически (`AllRuleIDs`/`RuleListString`/`DefaultEnabledRules`), менять транспорт не нужно.

## Findings

### F1 [High] — Регистрозависимая проверка префикса файла даст ~8 154 ложных severity-1 на текущем FA

Требование/design/tasks формулируют критерий как «имя файла начинается с `FCD_`/`Facade_`/`API_`», а task 3.1 буквально предполагает `strings.HasPrefix` по basename. Это регистрозависимо, тогда как имена файлов в Diasoft-поставке регистронезависимы по факту:

- В индексе **8 149 SQL-файлов с basename `fcd_*`** (lowercase) содержат `API_EXEC` (8 331 invocation), ещё **5 файлов `facade_*`** (11 invocation). Из них 6 213 — тестовые (`/Test/`), но **1 936 — боевые** файлы (`Server/Facade/fcd_Log.sql` — 37 вызовов, десятки `fcd_tpl_seed.sql` и т.п.).
- Для сравнения: uppercase `FCD_*` — 9 204 файла, `Facade_*` — 66 файлов.
- То есть при буквальной реализации правила ~1 936 не-тестовых легальных фасадов получат deploy-stopper «API_EXEC вне фасада» — массовый false positive, блокирующий деплой.

**Требуемая правка**: в требовании и design явно зафиксировать регистронезависимое сравнение, реализацию — по `strings.ToUpper(basename)`/`strings.HasPrefix(strings.ToLower(...))`, добавить сценарий и тест «`fcd_*.sql` (lowercase) → finding не формируется». Заодно проверить/зафиксировать, что сам препроцессор Diasoft раскрывает `API_EXEC` вне зависимости от регистра (иначе критерий неверен в принципе).

### F2 [High] — Шаблон `Facade/_fcd_tpl_seed.sql` — единственный реальный «не-фасадный» файл и он легален

После регистронезависимой нормализации (F1) на всём индексе остаётся ровно **один** файл с `API_EXEC` вне фасадного префикса: `fa-administrator/API_Kernel/Server/Facade/_fcd_tpl_seed.sql` (3 вызова; генерирует `FCD_##CURRENT_MODULEID##_SaveRangeByBrief`). Это seed-шаблон фасада, лежащий в каталоге `Facade`; правило по basename-префиксу даст на нём единственный (но ложный) deploy-stopper. Плюс в продуктах есть `fcd_tpl_seed.sql` (lowercase) — они закроются F1, а версия с ведущим `_` — нет.

**Требуемая правка**: решить судьбу шаблонов в спеке и задаче — либо включить `_fcd_`/`_FCD_` в разрешённые префиксы, либо явно исключить seed-шаблоны (`*_tpl_seed.sql`/файлы с `##CURRENT_MODULEID##`) отдельным сценарием и тестом. Молча оставлять единственный FP в спеке нельзя: он попадёт в первый же прогон на FA.

### F3 [Med] — `dedupeProcedureCalls` не реализует дедуп «по имени», требуемый спекой

Спека (ADDED deploy-stoppers): «Finding MUST формироваться не более одного раза на уникальное имя вызванной процедуры в пределах файла». Design Decision 6 и task 2.1 предписывают переиспользовать `dedupeProcedureCalls`, но он дедуплицирует по ключу `имя:строка` (review_helpers.go:728-749), т.е. три вызова одного имени на разных строках дадут три finding'а. Это прямо противоречит и требованию, и тесту 5.1(e).

Замер на индексе: для чужих `API_*` дедуп по файл+имя даёт **274** находки, по файл+имя+строка — **362** (+88, +32%). То есть дефект не теоретический.

**Требуемая правка**: в task 2.1/design D6 описать rule-local `seen map[string]struct{}` по `normalizeIdentifier(call.Name)` с сохранением строки первого вхождения; `dedupeProcedureCalls` использовать только как первичный фильтр (nil/keyword), но не как источник per-name дедупа.

### F4 [Med] — Масштаб новых severity-1 findings не отражён в Impact/Risks

Proposal Impact и Risks говорят о «всплеске findings» абстрактно. По текущему индексу FA новое правило даст **274 severity-1 findings на 188 файлах** (из них fa-contracts — 107). Сегодня `foreignProcedureUsing` на тех же вызовах даёт 362 (per-line). Это надо внести в proposal Impact/design Risks и в release notes (release-notes уже упомянуты в Risks, но без чисел).

### F5 [Low] — Регистр префикса `API_` в имени процедуры должен проверяться единым предикатом

`parsed.Calls` сохраняет исходный регистр (`CalleeName`), SQL-идентификаторы регистронезависимы. `strings.HasPrefix(call.Name, "API_")` в новой проверке и в сужении `checkForeignProcedures` должен использовать один хелпер на базе `normalizeIdentifier`/`strings.HasPrefix(lower, "api_")`. Иначе `exec api_foo` уйдёт из-под deploy-stopper'а, но останется в `foreignProcedureUsing` (severity 3) — ровно тот дубль/недобор, который change устраняет. На текущем FA всё в uppercase, риск низкий, но предикат дешёво сделать общим и задокументировать.

### F6 [Low] — Внутренние расхождения артефактов по `Facade_`

- design.md строка 18 (Goals): «…имя без префикса `FCD_`/`API_`» — пропущен `Facade_`;
- proposal.md строка 5: «`API_EXEC` внутри `FCD_*`/`API_*` скрипта» — пропущен `Facade_`.

Ниже (proposal What Changes, design D5, дельта deploy-stoppers) `Facade_` учтён. Поправить формулировки, чтобы артефакты не противоречили друг другу и финальному решению владельца.

### F7 [Low] — task 3.1: двойной ремап строк и лишний `getIndexedFile`

- `RunSQLFileCtx` централизованно ремапит `Finding.Line` из expanded-пространства (runner.go:195-198). Правило должно вернуть expanded-строку; вызов `mapExpandedLineToOriginal` внутри правила даст двойное преобразование (ср. спорный `checkDeletePtableMacros`, review_rules.go:1491). Формулировку 3.1 нужно уточнить: «строка из expanded-контента, централизованный ремап выполняет runner».
- design D5 «имя файла из `getIndexedFile`» избыточно: `Build`-замыкание уже получает `file *indexedFile`, basename берётся из `file.Path`.

### F8 [Low] — Textual-scan `API_EXEC`: регистр, комментарии и строки

- Regex в tasks `API_EXEC\s*\(` должен быть `(?i)API_EXEC\s*\(` — как в `parser/apimacro` (`execAPIRe`) и `parser/pas` (`apiExecRe`).
- Сканировать лучше по контенту после `removeComments` (review_lookup.go:786), чтобы вхождения в `--`/`/* */` не давали FP; для строковых литералов — зафиксировать поведение.
- Task 5.2(e) «зафиксировать ожидаемое поведение» для строкового литерала оставляет спеку непроверяемой: нужно либо явное предложение в требовании («вхождения в комментариях и строковых литералах не учитываются»), либо сценарий. Эмпирически на FA таких FP не найдено, но требование должно быть однозначным.

### F9 [Low] — Тесты адресованы не туда; интеграционный сценарий 5.3 неоднозначен

- Task 5.1/5.2 и proposal Impact ссылаются на `internal/review/runner_test.go`, но `runner_test.go` содержит только тесты `runRuleTasks`/`enabledRuleSet`. Unit-тесты правил живут в `review_rules_test.go` (фикстуры через `reviewExecContext{...}`), DB-backed — в `review_scope_integration_test.go` под `//go:build integration` (seed `RV_ForeignUtil`, `scopedRunner`).
- Task 5.3 «файл с чужим `API_EXEC`-инвалидным вызовом» смешивает два разных сценария. Разделить: (а) `exec API_чужой` → `apiProcDirectCall` severity 1; (б) `API_EXEC` в `CON_*`-файле → `apiExecOutsideFacade` severity 1; при возможности добавить кейс «своя/чужая API-процедура» на фикстуре `fa-rv-other`.
- Task 5.4: интеграционные тесты не входят в `go test ./internal/review/...` без `-tags integration` — стоит указать отдельно.

### F10 [Info] — `ruleMeta.Severity` не влияет на вывод; severity задаётся в check-функции

Поле `Severity` в `ruleCatalog` нигде не читается пайплайном (`buildRuleTasks` берёт только `Build`). Task 1.2 («зарегистрировать с severity `SeverityDeployStopper`») полезен для описания, но фактическая severity 1 обеспечивается только явным `Severity: SeverityDeployStopper` в `Finding` внутри check-функций (task 2.1/3.1 это подразумевают — стоит сказать явно).

### F11 [Info] — README-каталог правил не входит в Impact

В `README.md` (~строки 1093-1098) перечислены все rule ID с описаниями; новые `apiProcDirectCall`/`apiExecOutsideFacade` и сужение `foreignProcedureUsing` (исключение `API_`) должны попасть в этот список. В proposal Impact документация не упомянута.

### F12 [Info] — Пересечение с `execNotExistsProc`

Если чужая `API_*`-процедура существует в индексе, но отсутствует в разрешённой области владельца, на один вызов придут две severity-1 находки: `apiProcDirectCall` и `execNotExistsProc` (та же коллизия уже зафиксирована ревью `fix-review-branch-scoping` для `foreignProcedureUsing`). Это допустимо, но стоит одной фразой в Notes спеки, чтобы поведение не выглядело дефектом.

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| F1 | Регистрозависимый префикс файла → ~1 936 боевых FP (всего 8 154 lowercase `fcd_`/`facade_` файла) | High | Регистронезависимое сравнение в спеке/design/task + сценарий и тест lowercase |
| F2 | `_fcd_tpl_seed.sql` — единственный оставшийся не-фасадный файл и он легален | High | Решить судьбу seed-шаблонов (`_fcd_`/`*_tpl_seed.sql`) сценарием и тестом |
| F3 | `dedupeProcedureCalls` дедуплицирует имя+строка, спека требует имя (274 vs 362) | Med | Rule-local seen по имени, первое вхождение |
| F4 | 274 severity-1 на 188 файлах не отражены в Impact/Risks | Med | Внести цифры в proposal/design + release notes |
| F5 | Регистр префикса `API_` в имени процедуры | Low | Общий хелпер-предикат для обоих правил |
| F6 | design Goal и proposal Why потеряли `Facade_` | Low | Синхронизировать формулировки |
| F7 | Двойной ремап строк; лишний `getIndexedFile` | Low | Уточнить task 3.1 / design D5 |
| F8 | Regex без `(?i)`, не оговорены комментарии/строки | Low | `(?i)`, `removeComments`, сценарий/фраза в требовании |
| F9 | Тесты адресованы в runner_test.go; 5.3 неоднозначна | Low | review_rules_test.go + integration, разделить кейсы |
| F10 | `ruleMeta.Severity` не влияет на вывод | Info | Явно задавать severity в Finding |
| F11 | README-каталог правил | Info | Добавить в Impact/задачу |
| F12 | Коллизия с `execNotExistsProc` | Info | Фраза в Notes спеки |

Содержательно change верный: семантика product-level, переиспользование lookup'а и прогрева подтверждены кодом, измерения proposal (274/188, распределение по продуктам) воспроизведены на живом индексе. Блокируют реализацию только F1–F4; остальное — точность формулировок и тестов.

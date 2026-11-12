# Tasks: add-review-api-proc-direct-call-rule

## 1. Каркас правил

- [x] 1.1 `internal/review/types.go`: добавить константы `RuleAPIProcDirectCall` = `"apiProcDirectCall"` и `RuleAPIExecOutsideFacade` = `"apiExecOutsideFacade"`.
- [x] 1.2 `internal/review/catalog.go`: зарегистрировать оба правила с severity `SeverityDeployStopper` и описаниями (поле описательное — фактическая severity задаётся в `Finding` внутри check-функции); убедиться, что `DefaultEnabledRules`, `AllRuleIDs`, `RuleListString` подхватывают автоматически.
- [x] 1.3 `README.md` (каталог правил ~1093-1098): добавить `apiProcDirectCall`, `apiExecOutsideFacade` и пометить сужение `foreignProcedureUsing` (исключение `API_*`).

## 2. apiProcDirectCall

- [x] 2.1 `internal/review/review_rules.go`: реализовать `checkAPIProcDirectCall`: фильтр вызовов общим предикатом `isAPIProcedure(call.Name)` (без учёта регистра, F5) → `cachedLookupProcedureScopeInfo` → skip при `info.ProductID == 0` или `info.ProductID == file.DsProductID` → иначе finding `apiProcDirectCall` severity `SeverityDeployStopper` (Message с именем процедуры, владельцем и продуктом файла; заполнить `CurrentProductID`/`TargetProductID`). Дедуп — rule-local `seen map[string]struct{}` по `normalizeIdentifier(call.Name)`, finding на **строку первого вхождения** имени; `dedupeProcedureCalls` использовать только как первичный фильтр (nil/keyword), НЕ как источник per-name дедупа (F3).
- [x] 2.2 Убедиться, что имена из `parsed.Calls` уже нормализованы (схема/`[]` срезаны парсером) — при необходимости добавить нормализацию только внутри нового правила.

## 3. apiExecOutsideFacade

- [x] 3.1 `internal/review/review_rules.go`: реализовать `checkAPIExecOutsideFacade`: regex `(?i)API_EXEC\s*\(` по содержимому файла после `removeComments` (вхождения в комментариях исключены; строковые литералы не учитываются согласно требованию); критерий «не фасад» — basename из `file.Path` (Build уже получает `*indexedFile`, `getIndexedFile` не нужен) с **регистронезависимым** префиксом `FCD_`/`Facade_`/`API_`/`_fcd_` (`strings.HasPrefix(strings.ToLower(basename), ...)`); если найдено и не фасад → finding `apiExecOutsideFacade` severity `SeverityDeployStopper` на строку первого вхождения **в expanded-пространстве** — централизованный ремап выполняет runner (вызов `mapExpandedLineToOriginal` внутри правила НЕ делать, F7).
- [x] 3.2 Тест-кейс на вхождение `API_EXEC` в строковом литерале обычного скрипта — зафиксировать поведение, согласованное со спекой (не учитывается).

## 4. Сужение foreignProcedureUsing

- [x] 4.1 `internal/review/review_rules.go` `checkForeignProcedures`: добавить skip через тот же предикат `isAPIProcedure` (без учёта регистра, до lookup'а), с комментарием-ссылкой на `apiProcDirectCall`.

## 5. Тесты

- [x] 5.1 Unit-тесты в `internal/review/review_api_proc_direct_call_test.go` (фикстуры `reviewExecContext{...}`; по проектной практике правила живут в per-rule файлах, как `table_exists_metadata_test.go`/`empty_string_date_test.go`): (a) exec чужой `API_*` → finding severity 1; (b) exec своей `API_*` → нет finding; (c) `FCD_*`-вызов → нет finding; (d) `API_*` без владельца в индексе → нет finding; (e) три вызова одного чужого имени на разных строках → ровно один finding на строке первого вхождения; (f) `foreignProcedureUsing` не формируется для `API_*` (в т.ч. lowercase `api_*`); плюс R6-кейсы: `ProductID == 0`, `dbo.`-квалификация и `[bracketed]`-имя.
- [x] 5.2 Тесты `apiExecOutsideFacade`: (a) `API_EXEC` в `CON_STP_*.sql` → finding severity 1; (b) в `FCD_*.sql` → нет; (c) в **lowercase `fcd_*.sql`** → нет (F1); (d) в `Facade_*.sql` → нет; (e) в `API_*.sql` → нет; (f) в `_fcd_tpl_seed.sql` → нет (F2); (g) `API_EXEC` в комментарии → нет; (h) вхождение в строковом литерале обычного скрипта — поведение по спеке.
- [x] 5.3 Интеграционные тесты (`review_scope_integration_test.go`, `//go:build integration`, seed `RV_ForeignUtil`/`scopedRunner`): (a) `exec` чужой `API_*`-процедуры → `apiProcDirectCall` severity 1; (b) `API_EXEC` в `CON_*`-файле → `apiExecOutsideFacade` severity 1; (c) по возможности кейс «своя/чужая API-процедура» на фикстуре `fa-rv-other`.
- [x] 5.4 Прогон `go build ./... && go test ./internal/review/...` + отдельно `go test -tags integration ./internal/review/...`; регресс: существующие тесты review зелёные.
- [x] 5.5 Бамп `buildNumber = "1689"` → `"1692"` в `cmd/root.go` и пересборка `go build -o CodeBase.exe`.

## 6. Верификация на FA

- [x] 6.1 Прогон `codebase review` на реальных файлах: `BaseAlgAmrtCostSinglePmnt.sql` (свой API → чисто), `CON_STP_SaveObject.sql` (фасадный вызов → чисто), `Server/Facade/fcd_Log.sql` (lowercase фасад → чисто, F1), негативный тест с временным файлом `exec API_FinOper_FindLstByID` в продукте fa-contracts → severity 1.
- [x] 6.2 Проверить `--rules` листинг и работу фильтра `--rules apiProcDirectCall,apiExecOutsideFacade`.
- [x] 6.3 Контроль масштаба: полный прогон review по FA — фактически 336 findings `apiProcDirectCall` на 233 файлах из 2730 кандидатов (217/145 основная линия + 60/40 branch-копии + 59/48 Test; 18 неиндексированных файлов вне счёта). Оценка change-ревью 274/188 расходилась методикой учёта branch/Test — отклонение разобрано, числа внесены в proposal/design.

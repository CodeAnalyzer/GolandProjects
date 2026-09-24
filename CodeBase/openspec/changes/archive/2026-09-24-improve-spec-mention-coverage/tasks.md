# Tasks — Improve Spec Mention Coverage

## 1. Парсер: новые kind'ы и режимы (mentions.go)

- [x] 1.1 Добавить `rePathReport = \b[A-Za-z0-9_/\\]+\.(?:tpr|rpt)\b` и `reEventName = \bOn(?:After|Before)[A-Z][A-Za-z0-9_]*`; включить оба в сканирование обоих режимов (Related code и inline) рядом с `rePathSQL`. Верификация: `go build ./...`
- [x] 1.2 В `classifyMention`: суффиксы `.tpr`/`.rpt` → kind `report` (имя — последний сегмент без расширения); для путей с сегментом `Event` и суффиксом `.xml` → kind `event` (имя — последний сегмент без расширения). Верификация: юнит-тест на `ExtractMentionsFromRelatedCode`/`ExtractMentionsInline` с кейсами `ReservePortfolio/Reporting/rbr.tpr`, `form651.tpr`, `.../Event/OnAfterPrtf_UpdateNorm.xml`
- [x] 1.3 В `classifyMention` перед табличной проверкой: префикс `pAPI_` (case-insensitive) → kind `api_table`. Верификация: юнит-тест `pAPI_Accrual_ObjDate` → api_table, `tRPPortfolio` → table (регрессия)
- [x] 1.4 Обновить `openspecmd_test.go`: кейсы event-идентификатора в THEN (`OnAfterPrtf_UpdateNorm` → event), report в THEN вне бэктиков, регресс-кейсы существующих kind'ов. Верификация: `go test ./internal/parser/openspecmd/`

## 2. Индексатор: сканирование GIVEN (indexer_spec.go)

- [x] 2.1 В `insertSpecMentions` заменить `s.When + "\n" + s.Then` на `s.Given + "\n" + s.When + "\n" + s.Then`. Верификация: юнит-тест на mention из GIVEN-строки с корректным `LineNumber` (от `s.LineStart`)

## 3. Store: batch-lookup report_forms (db_lookup_spec_mentions.go)

- [x] 3.1 Добавить `FindReportFormIDsByNames(ctx, names) (map[string]int64, error)` по аналогии с `FindDFMFormIDsByNames` (LOWER(report_name), MAX(id)). Верификация: `go build ./...` + интеграционный тест lookup по имени `form651`
- [x] 3.2 Добавить `FindAPIContractIDsByTableNames(ctx, tableNames) (map[string][]int64, error)` — lookup `api_contract_tables` по LOWER(table_name) с DISTINCT contract_id (мультикарта: таблица → все контракты-владельцы). Верификация: интеграционный тест `pAPI_Accrual_ObjDate` → ≥2 контракта

## 4. Постпроцессор: резолв новых kind'ов и фолбэки (indexer_postprocess_spec.go)

- [x] 4.1 Добавить в `specMentionLookup` поля `Reports`/существующий `APIs`; собрать имена kind `report` через `collectMentionNamesByKind`, выполнить `FindReportFormIDsByNames`; в `buildSpecMentionRelations` — ветка `case "report"` → `report_form`. Верификация: юнит-тест `buildSpecMentionRelations` с report-mention
- [x] 4.2 Добавить ветку `case "event"` → `api_contract` (lookup `FindAPIContractIDsByNames`, общий с kind api — объединить сборщики имён). Верификация: юнит-тест event-mention → relation к api_contract
- [x] 4.3 Фолбэк `.pas`: в `buildSpecMentionRelations` для kind `method` без хита в `pas_methods` — lookup `Forms` по тому же имени, при хите ребро к `dfm_form`. Верификация: юнит-тест метод-без-хита/форма-с-хитом
- [x] 4.4 Second-chance unknown: собрать имена kind `unknown` → `FindLatestSQLProcedureIDsByNames` → при хите ребро к `sql_procedure`. Верификация: юнит-тест unknown `r8938_prc` → relation; unknown `f123_proc` без цели → без relation
- [x] 4.5 Контрактовый резолв `api_table`: собрать имена kind `api_table` → `FindAPIContractIDsByTableNames` → ребро к каждому DISTINCT контракту-владельцу (дедуп source|target в `buildSpecMentionRelations`). Верификация: юнит-тест `pAPI_Accrual_ObjDate` с 2 владельцами → 2 рёбра к api_contract
- [x] 4.6 Интеграционный тест постпроцессора: спека с report/event/pas-fallback/unknown/api_table упоминаниями → рёбра `references_code` всех пяти видов; повторный запуск идемпотентен. Верификация: `go test ./internal/indexer/ -run TestSpecMention`

## 5. Верификация на реальных данных

- [x] 5.1 Прогнать полный re-index на FA-дереве, сравнить метрики: рост `spec_code_mentions` по kind (report/event/api_table), рост `references_code` от spec-сущностей, среднее число контрактов на `api_table`-упоминание (порог пересмотра — >10), отсутствие новых ошибок постпроцессинга. Верификация: `codebase stats` до/после, `codebase query spec by-code --name form651` → resolved, `--name OnAfterPerson_Update` → resolved, `--name pAPI_Accrual_ObjDate` → resolved
- [x] 5.2 Выборочная сверка: `spec_by_code r8938_prc` → ребро к процедуре; `spec_by_code f123_proc` → остаётся в staging без ребра (задокументированный пробел). Верификация: вывод MCP-инструмента `spec_by_code`
- [x] 5.3 `openspec validate --specs` и `openspec validate improve-spec-mention-coverage` проходят без ошибок

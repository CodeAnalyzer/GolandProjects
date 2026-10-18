# Tasks: symbol-report-form-name-fallback

## 1. Fallback в SearchSymbol

- [x] 1.1 Добавить helper `needsReportFormsLookup(primaryType)` в `internal/query/query.go` и ветку в `buildSymbolLookupCondition`: для `report_form` — составное условие exact `LOWER(s.symbol_name) = LOWER($n) OR LOWER(rf.form_name) = LOWER($n) OR LOWER(rf.form_class) = LOWER($n)`, в like-режиме ILIKE по трём полям; ранний exact-return (query.go:263) перестаёт поглощать `report_form`. Проверка: юнит-тест 3.1 зелёный
- [x] 1.2 В `SearchSymbol` добавить `LEFT JOIN report_forms rf ON s.symbol_type = 'report_form' AND s.entity_id = rf.id`, только когда `needsReportFormsLookup(primaryType)` истинен; гейт и условие используют один helper (design D2). Проверка: юнит/SQL-тест 3.2 — JOIN присутствует при type=report_form, отсутствует без type

## 2. form_class в SearchReportForm

- [x] 2.1 В `SearchReportForm` (query.go:432) расширить список полей до `rf.report_name, rf.form_name, rf.form_class`. Проверка: поиск `TFormRepBCHPeriod` инструментом `report_form` возвращает формы (верификация 4.2)

## 3. Тесты

- [x] 3.1 Юнит-тесты `query_test.go` по образцу `TestBuildSymbolLookupCondition_FormTypeSearchesNameAndSignature`: exact по трём полям для type=report_form; like-ILIKE; без типа — условие не ссылается на `rf.*`; другие типы — прежний exact-return. Проверка: `go test ./internal/query/ -run TestBuildSymbolLookupCondition`
- [x] 3.2 Интеграционный тест в `query_symbol_types_integration_test.go`: сидинг report_form с form_name/form_class; поиск symbol по `report_name`, `form_name`, `form_class` при type=report_form; без типа — 0; SQL содержит/не содержит JOIN по фильтру. Проверка: `go test ./internal/query/ -run Integration -tags integration` (или штатный раннер файла)

## 4. Сборка, верификация, артефакты

- [x] 4.1 `go build ./... && go vet ./... && go test ./...` — все зелёные. Проверка: вывод команд без ошибок
- [x] 4.2 Верификация на живой БД: MCP `codebase_query_symbol {"name":"FormRepBCHPeriod","type":"report_form"}` → 6; регрессии `{"name":"BCH_UCH_Period","type":"report_form"}` → 1, `{"name":"FormRepBCHPeriod"}` без type → 0; MCP `codebase_query_report_form {"name":"TFormRepBCHPeriod"}` → найдены. Проверка: фактические count совпадают
- [x] 4.3 Поднять `buildNumber` в `cmd/root.go` до 1609, пересобрать `CodeBase.exe` в корне репо. Проверка: `CodeBase.exe stats` показывает build 1609
- [x] 4.4 Обновить `Modifications/symbol-report-form-name-fallback-c9a2f1.md` (статус «предложение» → «реализовано», ссылка на change) и пометить TC-142 как PASS в актуальном прогоне. Проверка: файлы содержат ссылку на `openspec/changes/symbol-report-form-name-fallback`

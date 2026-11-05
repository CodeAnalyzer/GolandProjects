# Tasks: Материализация owner_name в таблице symbols

## 1. Схема и миграция

- [x] 1.1 Добавить `owner_name TEXT NOT NULL DEFAULT ''` в `CREATE TABLE symbols` и оператором `ALTER TABLE symbols ADD COLUMN IF NOT EXISTS owner_name TEXT NOT NULL DEFAULT ''` в DDL-список `InitSchemaCtx` (db_schema.go); bump `CurrentSchemaVersion` → `codebase_schema_v5`. Верификация: integration-тест схемы видит колонку на свежей БД.
- [x] 1.2 Внести именованную миграцию `symbols_owner_name_backfill_v1` через `applyMigrationTx` (после DDL-списка, до маркера версии): два type-scoped set-based UPDATE из design D2 (method: `pas_methods` LEFT JOIN `pas_classes`; component: `dfm_components` JOIN `dfm_forms`), без self-join `symbols` и CASE. Верификация: миграция применяет backfill на БД с легаси-данными, маркер записан.
- [x] 1.3 Integration-тест схемы (по образцу `TestFilesIsGenerated_BackfillIdempotent`, db_lookup_spec_mentions_integration_test.go, :232): (а) колонка существует на свежей БД; (б) backfill применён и идемпотентен — вставить легаси-строки `method`/`component` с `owner_name=''`, удалить маркеры текущей версии и `symbols_owner_name_backfill_v1`, вызвать `InitSchema` → значения заполнены; повторно вручную изменить `owner_name` строки и убедиться, что повторный прогон его не вернул (маркер есть → UPDATE не выполнился); (в) строки прочих типов не переписываются (DEFAULT сохранён); (г) легаси-метод с нерезолвнутым классом получает `''`. Верификация: тест зелёный.

## 2. Индексатор

- [x] 2.1 `model.Symbol`: добавить поле `OwnerName` (model.go); `BatchInsertSymbols`/`insertSymbolsBatch`: включить `owner_name` в `pq.CopyIn` и Exec с санитизацией `sanitizeUTF8String(symbol.OwnerName)` по существующему паттерну остальных текстовых полей (db_insert_j.go:147-152; `sanitizeUTF8String` не экспортирована из store — санитизация в индексаторе не компилируется, design D3). Верификация: build + unit-тесты пакета store.
- [x] 2.2 PAS-ветка: в цикле создания method-символов (indexer_sql_pas.go, после `FindPASMethodIDsByUnit`) заполнять `OwnerName = method.ClassName` — сырое значение, без TRIM и без санитизации в индексаторе (санитизация — в store, задача 2.1; design D3). Верификация: integration-тест индексатора — после индексации PAS-файла `owner_name` заполнен только у method-символов, значением имени класса как записано.
- [x] 2.3 DFM-ветка: в `parseDFMFile` построить карту `formClassByID` (formID → `form.FormClass`) параллельно `formIDByName` (indexer.go:777-786, без дополнительного lookup — design D3); в цикле создания component-символов заполнять `OwnerName = formClassByID[component.FormID]` — сырое значение (санитизация в store, задача 2.1). Верификация: integration-тест — после индексации DFM-файла `owner_name` заполнен только у component-символов, классом родительской формы.
- [x] 2.4 Integration-тест индексатора: (а) регистр ссылки — класс объявлен `TFormA`, реализация пишет `tforma.CaseRun`: метод резолвится в класс, `owner_name = 'tforma'` (написание из кода метода, D4.2); (б) пин пре-существующего поведения: метод с классом, отсутствующим в юните, символа не создаёт. Верификация: тест зелёный.

## 3. Запросы

- [x] 3.1 `SearchSymbol`/`symbolBaseBranch` и `SearchInContent` (query.go): заменить `` ` + symbolOwnerKeyExpr + ` AS owner_key `` на `COALESCE(s.owner_name, '') AS owner_key`; удалить константу `symbolOwnerKeyExpr` и её комментарий. Верификация: build, существующие integration-тесты query зелёные.
- [x] 3.2 Тест-эталон `legacyReportFormLookup` (query_symbol_types_integration_test.go:328): заменить ссылку на константу инлайн-копией прежнего SQL-выражения с комментарием-якорем «эталон до материализации owner_key» (design D5). Верификация: тест компилируется и остаётся зелёным (роль эталона B3 сохранена).
- [x] 3.3 Regression-тест дедупа (расширить query_branch_dedup_integration_test.go; учёт двухфазного дедупа — одно-веточные копии выживают при любом owner_key, наблюдаемо только кросс-веточное схлопывание): (а) копия метода одного класса (одинаковое написание) в develop и sp → одна строка develop; (б) одноимённые методы нерезолвнутых классов: `TFormA.Execute` только в develop, `TFormB.Execute` только в sp → две строки; (в) контр-кейс чувствительности: те же строки с `owner_name=''` у обеих → одна строка develop. Верификация: тест зелёный, включая контр-кейс.

## 4. Верификация на FA

- [x] 4.1 На копии боевой БД FA (методика: попарное сравнение old/new-выражений на одних данных — литеральная копия БД заблокирована живыми MCP-подключениями): (а) пин backfill: 930 893+35 828=966 721 method — расхождение 0, легаси-нерезолвнутые (35 828, все — standalone-функции без класса, dangling=0) → `''`; (б) 16 кейсов (точные/like/контент × method/component/form/без типа) — 0 расхождений; (в) переиндексация пересоздаёт владельцев — верифицировано Update-фазой integration-теста (FA touch+update заблокирован пре-существующим pre-filter, вне scope, зафиксировано в bug-report). Результат: Bug-reports/BUG-symbols-owner-name-materialization-verification-20261105.md.
- [x] 4.2 `EXPLAIN (ANALYZE, BUFFERS)`: SearchSymbol и SearchInContent — SubPlan-ов владельца нет (46.7 мс / 10 709 buffer hits, было 28 625); warm SQL best-of-5 40 мс (базлайн B4 259–408 мс), CLI warm 281–438 мс. Зафиксировано в Bug-reports/BUG-symbols-owner-name-materialization-verification-20261105.md.
- [x] 4.3 Прогнать CLI-регресс Tests/run-test-cases (базлайн последнего прогона): расхождений, кроме артефактов улучшения, нет. Верификация: summary.ndjson diff чист. (Результат: 226/226 совпали с базлайном 2026-11-04.)

## 5. Финализация

- [x] 5.1 gofmt по изменённым файлам, `go build ./...`, `go vet ./...`, `go test ./...` и integration-набор (store/indexer/query) зелёные; `openspec validate symbols-owner-key-materialization --strict` — valid; бамп `buildNumber` `"1669"` → `"1672"` (cmd/root.go:29); пересборка `go build -o CodeBase.exe` с проверкой `CodeBase.exe --version` → build 1672.

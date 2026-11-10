## 1. Store lookup

- [x] 1.1 Добавить `BatchLookupTableCreators(ctx, names []string) (map[string]bool, error)` в `internal/store/db_lookup_sql.go`: `UNION ALL` из четырёх ветвей с предикатом `LOWER(table_name) = ANY($1)` **в каждой ветви** (`sql_tables.context IN ('create','select_into')`, `sql_column_definitions.definition_kind='select_into'`, `api_contract_tables`, `api_business_object_tables`), без `DISTINCT` (дедуп в Go-мапе); убедиться, что `go build ./...` проходит.
- [x] 1.2 Добавить unit/integration-тест на lookup: созданная через `create table` таблица → `true`, объявленная в `api_contract_tables`/`api_business_object_tables` → `true`, созданная через `select_into` → `true`, отсутствующая → `false`; запустить `go test ./internal/store -run TestBatchLookupTableCreators` и integration-прогон (см. 4.2).

## 2. Review rule

- [x] 2.1 Добавить `RuleTableExistsInMetadata RuleID = "tableExistsInMetadata"` в `internal/review/types.go` и зарегистрировать правило в `internal/review/catalog.go` с `SeverityDeployStopper`.
- [x] 2.2 Реализовать `checkTableExistsInMetadata` в `internal/review/review_rules.go`: обход `parsed.Tables` с `context IN ('select','insert','update','delete','log')`; фильтры D6 — `#`/`##`, `sqlKeywordsMap`, `M_*`, курсоры файла (`parseCursorDeclarations`), rule-local стоп-набор (`inserted`/`deleted`/`master`/`information_schema`/`sys*`/`open`/`fetch`/`goto`), placeholder-токены (regexp); same-file создатели — `context='create'` и `parsed.ColumnDefinitions` с `DefinitionKind='select_into'`; регистронезависимая проверка через кэш; убедиться, что `go build ./...` проходит.
- [x] 2.3 Добавить табличные тесты правила в `internal/review/review_rules_test.go` на сценарии дельты (t-патч, p-TmpTbl, API-таблица, SELECT INTO, отсутствующая таблица, `#`-таблица, курсор declare/open, `FETCH NEXT FROM`/`WHERE CURRENT OF` курсора, `inserted`/`deleted`/`sys*`, placeholder-токены) и запустить `go test ./internal/review -run TestCheckTableExistsInMetadata`.

## 3. Runner cache and prewarm

- [x] 3.1 Добавить `tableCreatorCache map[string]bool` и `prewarmTableCreators(ctx, parsed)` в `internal/review/runner.go`/`review_lookup.go`: предзагрузка до старта параллельных правил, **с негативными записями** (`false` для запрошенных и не найденных), сброс кэша в `RunSQLFileCtx`, вызов рядом с `prewarmTableProductIDs`; ленивый fallback при промахе — без записи в общий map (паттерн `cachedLookupTableProductIDs`); проверить `go vet ./internal/review`.
- [x] 3.2 Добавить тест: prewarm заполняет кэш для всех таблиц файла одним batch-запросом, включая негативные записи для отсутствующих; ленивый fallback не пишет в общий кэш; запустить `go test ./internal/review -run TestPrewarmTableCreators`.

## 4. Verification

- [x] 4.1 Прогнать `gofmt -l internal/review internal/store` (пустой вывод) и `go build ./...`; забампить `buildNumber` в `cmd/root.go` c `"1681"` до `"1684"` и пересобрать бинарник `go build -o CodeBase.exe`.
- [x] 4.2 Прогнать `go test ./internal/review ./internal/store ./cmd` и integration-тесты лукапа за build-тегом: `go test -tags integration ./internal/store -run TestBatchLookupTableCreators`; убедиться, что все тесты проходят.
- [x] 4.3 Проверить правило вручную на реальном файле: `codebase review` для `ContractMassLoad.sql` — ожидаемые findings `tableExistsInMetadata`: `pAPI_Account_MassClose` (подтверждённый дефект), кандидаты `pAPI_Acc_MassCloseNTF`, `pAPI_Acc_MassInsertNTF`, `pAPI_Acc_MassInsProtocol` (возможно реальные необъявленные таблицы — разобрать вручную); алиас `contract` finding давать НЕ должен (урезается фильтрами D6). Контрольный файл без findings собрать из таблиц с подтверждённым create-рядом в индексе (например `pCons_ObjectID`), не из `tConsRestructingDetail` (create-ряд в доступных индексах не подтверждён).
- [x] 4.4 Выполнить `openspec validate add-table-exists-in-metadata --strict` и `openspec validate --all --strict` — без ошибок.
- [x] 4.5 До включения правила по умолчанию перепрогнать пилотный замер шума с полным набором фильтров (D1+D6) на локальном индексе fa-contracts: DML-имена минус creators-UNION минус фильтры (протокол — design.md, ## Measurements); целевой ориентир — affected files сходятся к файлам с реальными дефектами; результат зафиксировать в design.md. Если шум остаётся систематическим — эскалация на пересмотр severity или default-off.

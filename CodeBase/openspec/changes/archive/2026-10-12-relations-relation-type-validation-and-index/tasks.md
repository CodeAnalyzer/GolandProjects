## 1. Канонический словарь и ошибка

- [x] 1.1 Добавить в `internal/model` (рядом с `symbol_types.go`) словари `RelationTypes` (21 значение) и `RelationEntityTypes` (17 значений) с функциями проверки/списка; верифицировать юнит-тестом, что оба набора содержат `depends_on_capability`, `references_code`, `change_modifies`, `usecase_involves` и `dfm_form`, `spec_*`
- [x] 1.2 Добавить `errs.ErrUnknownRelationType` в `internal/errs/errs.go`; верифицировать сборкой пакета
- [x] 1.3 Валидировать `relationType`/`sourceType`/`targetType` в `SearchRelations` (`internal/query/query_relations.go`) до построения SQL; `SearchRelationsByEntity` не валидировать (механика inspect, принимает типы вне словаря); верифицировать юнит-тестом, что неизвестное значение возвращает обёртку `errs.ErrUnknownRelationType` со списком валидных значений

## 2. Тесты валидации

- [x] 2.1 Юнит-тест словаря типов графа (`internal/model`): полный набор, отсутствие дублей, поведение проверки пустого/неизвестного значения; верифицировать `go test ./internal/model/...`
- [x] 2.2 Юнит-тест `SearchRelations`: неизвестный `relation_type`/`source_type`/`target_type` → ошибка, пустое значение → нет ошибки; тест `SearchRelationsByEntity`: неизвестный entity-тип (напр. `pas_class`) НЕ даёт ошибку валидации; верифицировать `go test ./internal/query/...`
- [x] 2.3 Верифицировать маппинг ошибки в `invalid_arguments` в `classifyQueryError` (`cmd/query_execution.go`); верифицировать `go test ./cmd/...`

## 3. Индексы схемы

- [x] 3.1 Добавить в `internal/store/db_schema.go` (блок индексов `relations`, строки 944-947) `CREATE INDEX IF NOT EXISTS idx_relations_relation_type_id ON relations(relation_type, id DESC)` и `CREATE INDEX IF NOT EXISTS idx_relations_source_type_relation_id ON relations(source_type, relation_type, id DESC)`; легаси-DROP (`:1028`) оставить без изменений; верифицировать сборкой
- [x] 3.2 Дополнить `internal/store/schema_integration_test.go` проверкой существования обоих новых индексов (старый список `dropped` не трогать); верифицировать `go test -tags integration ./internal/store/...`

## 4. Документация и тест-кейсы

- [x] 4.1 Обновить описание `codebase_query_relations` в `internal/mcp/registry.go:542` (relation-типы до 21, entity-типы до 17); верифицировать чтением и `go build ./...`
- [x] 4.2 Переформулировать TC-075 в `Tests/TEST_CASES.md` на ожидание ошибки валидации со списком валидных типов (не таймаут pq); верифицировать чтением раздела `relations`

## 5. Итоговая проверка

- [x] 5.1 Прогнать `go build ./... && go test ./... && go vet ./internal/query/... ./internal/store/...` — всё зелёное
- [x] 5.2 Проверить план запроса на живом индексе FA: `EXPLAIN (ANALYZE, BUFFERS)` — type-only использует `Index Only Scan using idx_relations_relation_type_id` (0.226 мс), 3-предикатный — `Index Scan using idx_relations_source_type_relation_id` (0.182 мс), `Rows Removed by Filter = 0`
- [x] 5.3 Проверить end-to-end: `query relations --relation-type __no_such_relation__` / `--source-type __no_such_type__` → мгновенная ошибка `invalid_arguments`; `--relation-type depends_on_capability` → результат; TC-062–072/074/175–177 — PASS. TC-073 ВНЕ объёма (открытый `BUG-relations-kind-type-name-filter-20261011.md`, отдельный change — согласовано)
- [x] 5.4 Верифицировать `openspec validate relations-relation-type-validation-and-index --strict`

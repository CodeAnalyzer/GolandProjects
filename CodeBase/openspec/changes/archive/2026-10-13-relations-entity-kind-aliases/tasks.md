## 1. Центральная нормализация в internal/model

- [x] 1.1 Добавить `NormalizeRelationEntityType(raw string) (string, bool)` в `internal/model` (рядом с `RelationEntityTypes`): trim+lower, канонический тип — как есть, `service`/`event`/`callback_event`/`used_service` → `api_contract`, иначе `false`. Проверка: `go build ./internal/model/...` компилируется.
- [x] 1.2 Покрыть `NormalizeRelationEntityType` юнит-тестами (`internal/model/relation_types_test.go`): kind→api_contract для всех 4 kind-типов, канонический passthrough (`sql_procedure`, `dfm_form`, `spec_capability`), регистр/пробелы, неизвестное → `false`. Проверка: `go test ./internal/model/... -run NormalizeRelationEntity`.

## 2. Применение в фильтре relations

- [x] 2.1 В `internal/query/query_relations.go` нормализовать `sourceType`/`targetType` в начале `SearchRelations` и использовать нормализованные значения в `validateRelationFilters`, `findRelationEntityMatches`, `buildRelationIDsQuery`. Проверка: юнит-тест валидации — `service` проходит, `__no_such_type__` даёт ошибку со списком (как в существующем `query_relations_validation_test.go`).
- [x] 2.2 В `relationEntityMatchQueryParts` добавить 6 источников (`dfm_form`→`dfm_forms.form_name`, `spec_capability`→`spec_capabilities.capability_name`, `spec_change`→`spec_changes.change_name`, `spec_requirement`→`spec_requirements.requirement_name`, `spec_scenario`→`spec_scenarios.scenario_name`, `spec_usecase`→`spec_usecases.usecase_name`) и идемпотентную нормализацию типа. Проверка: юнит-тест `relationEntityMatchQueryParts` возвращает непустые `parts` для `service` (как api_contract), `dfm_form`, `spec_capability` и `false` для неизвестного.
- [x] 2.3 Расширить `relationSearchBaseQuery` LEFT JOIN-блоками имени/файла/строки для 6 новых типов (source и target) по образцу существующих 11. Проверка: `go test ./internal/query/...` — существующие тесты построения запроса проходят; интеграционный запрос relations по `dfm_form` возвращает связи с непустым `source_name`/`target_name`.

## 3. Документация

- [x] 3.1 Обновить описание `codebase_query_relations` в `internal/mcp/registry.go` (:542): kind-типы `service`/`event`/`callback_event`/`used_service` принимаются как алиасы `api_contract`; список типов сущностей — полный (17). Проверка: визуальная сверка описания с `model.RelationEntityTypes`.
- [x] 3.2 Обновить справку флагов `--source-type`/`--target-type` в `cmd/query.go`. Проверка: `codebase query relations --help` содержит упоминание kind-алиасов.

## 4. Тесты и верификация

- [x] 4.1 Добавить интеграционный тест (`-tags integration`): `SearchRelations(sourceType="service", sourceName=...)` эквивалентен `api_contract`; `sourceType="dfm_form"`/`"spec_capability"` с именем не даёт ошибку и резолвит имя. Проверка: `go test -tags integration ./internal/query/...`.
- [x] 4.2 Прогнать регресс кейсов relations из `Tests/TEST_CASES.md` (TC-062–077, TC-172–177), включая TC-073 (kind-тип). Проверка: все кейсы без изменений, TC-073 — PASS.
- [x] 4.3 Полная проверка: `go build ./...`, `go vet ./...`, `go test ./...`, `go test -tags integration ./internal/query/... ./internal/store/...`. Проверка: команды завершаются успешно.
- [x] 4.4 Синхронизировать спеки и заархивировать change после реализации: `openspec validate --strict`, обновление `Tests/CODEBASE_QUERY_TEST_RESULTS_*`, `openspec archive relations-entity-kind-aliases`. Проверка: `openspec validate --specs` без ошибок, change в `changes/archive/`.

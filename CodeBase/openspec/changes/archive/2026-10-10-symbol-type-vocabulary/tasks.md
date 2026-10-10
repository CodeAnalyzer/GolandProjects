# Tasks: symbol-type-vocabulary

## 1. Центральная таблица словаря (internal/model)

- [x] 1.1 Создать `internal/model/symbol_types.go`: набор канонических типов (29), `ResolveSymbolTypeAlias` (алиасы + нормализация регистра/пробелов, `api_contract` → 4 kind-типа), `RelationTypeForSymbol` (полный мост symbols→relations). Проверить юнит-тестами: каждый алиас, каждый канонический тип, неизвестное значение, регистр/пробелы, kind→api_contract
- [x] 1.2 Сверить канонический набор с фактическими данными: `SELECT DISTINCT symbol_type FROM symbols` на БД с индексом FA — расхождений с набором нет. Сверка: 29 значений в БД = 29 в наборе; 29-й (`xml`) — fallback-kind `classifyPath` (dsxml/parser.go:451) для DSArchitect XML вне kind-каталогов, 167 символов

## 2. Фильтр type в SearchSymbol

- [x] 2.1 В `internal/query/query.go` `SearchSymbol`: нормализация входного `symbolType` через `ResolveSymbolTypeAlias`; фильтр `symbol_type = ANY($2)` для набора значений; пустое значение — без фильтра (как сейчас). Юнит-тест: алиас транслируется, канонический тип проходит
- [x] 2.2 Strict-валидация: неизвестное значение → ошибка `unknown symbol type '<x>', valid values: ...` через `errs`. Юнит-тест: ошибка содержит список валидных значений
- [x] 2.3 Интеграционный тест (БД): `--type sql_procedure` находит процедуру (эквивалент `--type procedure`); `--type api_contract` находит контракты всех kind-типов; `--type js_function` находит JS-функцию (`entity_type = js`)

## 3. Мост inspect

- [x] 3.1 Переписать `InspectRelationType` (`internal/querysvc/inspect.go`) на `RelationTypeForSymbol`; в `RunInspectQuery` нормализовать входной `symbolType` перед `PrioritizeExactSymbolMatches`. Юнит-тест: `method→pas_method`, `function→js_function`, kind-типы→`api_contract`, существующие 4 перевода не сломаны
- [x] 3.2 Интеграционный тест (БД): `query inspect --name <PAS-метод с builds_query>` возвращает ненулевые outgoing; `query inspect --name <контракт с implements_contract>` возвращает ненулевые incoming (живые примеры из БД: `DeleteLinkToPolicy`, `API_OCvr_MassInsertAssessment`)

## 4. Документация

- [x] 4.1 `internal/mcp/registry.go`: описание `codebase_query_symbol` (:393) — канонический словарь + алиасы приняты; описание `type` в `codebase_query_inspect` (:556) — те же правила. Проверить: текст содержит все 29 типов или корректную сводку + алиасы
- [x] 4.2 `cmd/query.go:14`: справка флага `--type` — канонические значения кратко. Проверить `codebase query symbol --help` (обновлены также `query inspect --type` и описания параметров в registry)

## 5. Приёмка

- [x] 5.1 Прогнать сценарии дельт на живой БД: все сценарии `query/symbol-search` (алиасы, api_contract, ошибка, kind-тип) и `query/api-relations-queries` (inspect метода и контракта) воспроизводятся. Результаты: alias sql_procedure ≡ procedure (count=1), api_contract/service → контракт service/xml, js_function ≡ function (AcceptClaim, entity=js), unknown (sql/js/pas) → invalid_arguments со списком, пустой тип — без фильтра, component (3), smf_instrument (1), inspect DeleteLinkToPolicy → outgoing builds_query=1 (было 0), inspect API_OCvr_MassInsertAssessment → incoming implements_contract (было 0)
- [x] 5.2 `openspec validate symbol-type-vocabulary` без ошибок; `go test ./...` зелёный (31 пакет ok, `go vet ./...` чисто)
- [x] 5.3 Обновить статус баг-репорта `Bug-reports/BUG-symbol-search-type-naming-drift-20261009.md` (исправлено, ссылка на change)

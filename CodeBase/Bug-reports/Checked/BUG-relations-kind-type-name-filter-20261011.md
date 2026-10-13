# Bug Report: relations name-filter не принимает kind-типы API-контрактов — центральная карта словаря не применена

**Дата:** 2026-10-11
**Файл:** `internal/query/query_relations.go` (`relationEntityMatchQueryParts`, строки 304–340; ошибка — строка 282); MCP `codebase_query_relations`; CLI `query relations`
**Версия CodeBase:** 0.9.3
**Статус:** исправлено (2026-10-13). Исправлено в change [`relations-entity-kind-aliases`](../openspec/changes/archive/2026-10-13-relations-entity-kind-aliases/) (заархивирован 2026-10-13); проверка — `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-13.md` (TC-073 PASS). Обнаружено при прогоне тест-кейсов `Tests/TEST_CASES.md` (TC-073), зафиксировано в `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` (разбор FAIL, п. 5).
**Родственный (закрытый):** `BUG-symbol-search-type-naming-drift-20261009.md` — тот же класс проблемы для `codebase_query_symbol` / `inspect`, закрыт change [`2026-10-10-symbol-type-vocabulary`](../openspec/changes/archive/2026-10-10-symbol-type-vocabulary/).

## Summary

Инструмент `codebase_query_relations` отклоняет канонические kind-типы API-контрактов (`service`, `event`, `callback_event`, `used_service`) в `source_type`/`target_type` с ошибкой `unsupported relation entity type for name filter: service`, хотя:

1. kind-типы — **легитимный словарь `symbols.symbol_type`**, введённый/закреплённый архивированным change `symbol-type-vocabulary` (центральная таблица словаря в `internal/model`, карты алиасов relations↔symbols, развёртывание `api_contract` → 4 kind-типа);
2. для `symbol`-поиска и `inspect` та же нормализация **уже реализована** (change закрыт 2026-10-10), а третий потребитель — name-filter `relations` — остался на локальном hard-coded списке из 11 relation-стильных типов.

Критичность: **MEDIUM** (нарушен принцип «единый словарь — все точки входа», принятый в `symbol-type-vocabulary`; LLM-агент, естественно использующий канонический тип `service` в `relations`, получает ошибку вместо результата).

---

## Environment

- CodeBase 0.9.3, MCP-инструмент `codebase_query_relations`, CLI `codebase query relations`
- БД с проиндексированным проектом FA (прогон 2026-10-11)

---

## Reproduction Steps

1. MCP: `codebase_query_relations` с `{"source_name":"API_ObjectEvent_MassCreate","source_type":"service"}`.
2. CLI: `codebase query relations --source-name API_ObjectEvent_MassCreate --source-type service`.
3. Для сравнения (работает): `{"source_name":"API_ObjectEvent_MassCreate","source_type":"api_contract","relation_type":"implements_contract"}` → `count=0` (без ошибки; вопрос направления связей — отдельная тема TC-066–068, не этого бага).
4. Для сравнения (работает, та же нормализация в соседнем инструменте): `codebase_query_symbol` с `{"name":"API_ObjectEvent_MassCreate","type":"service"}` → count=1.

## Expected Result

`source_type`/`target_type` = `service` (а также `event`, `callback_event`, `used_service`) нормализуется в `api_contract` через центральную карту алиасов `internal/model` — та же семантика, что задокументирована и реализована для `codebase_query_symbol` (proposal `symbol-type-vocabulary`, пункт «Нормализация алиасов фильтра»).

## Actual Result

Ошибка: `query failed: unsupported relation entity type for name filter: service`.

---

## Root Cause Analysis

`relationEntityMatchQueryParts` (`query_relations.go:304-340`) содержит **локальный hard-coded список** из 11 entity-источников:

```go
sources := []entitySource{
    {"sql_procedure", ...}, {"sql_table", ...}, {"pas_method", ...},
    {"js_function", ...}, {"api_contract", ...}, {"report_form", ...},
    {"report_field", ...}, {"report_param", ...}, {"vb_function", ...},
    {"query_fragment", ...}, {"smf_instrument", ...},
}
```

Перед сравнением выполняется только `strings.ToLower/TrimSpace` (`:324`) — никакой нормализации алиасов. Kind-типы в списке отсутствуют → `parts` пуст → `false` → ошибка на `:282`.

Change `2026-10-10-symbol-type-vocabulary` ввёл центральную таблицу словаря в `internal/model` (карта алиасов relations→symbols, карта symbols→relations, развёртывание `api_contract` → 4 kind-типа) и применил её в `SearchSymbol` (`internal/query/query.go`) и `InspectRelationType` (`internal/querysvc/inspect.go`). **Name-filter `relations` (`findRelationEntityMatchesWithCondition`) остался вне зоны изменения** — неполная экстраполяция центрального словаря на третий потребитель.

Документация инструмента (`codebase_query_relations` в `registry.go`) перечисляет допустимые entity-типы в relations-стиле (`api_contract`, без kind-типов) — формально агент «предупреждён», но словарь kind-типов легитимен в соседних инструментах того же профиля (`symbol`, `inspect`), и единообразие входных словарей — принятый принцип.

---

## Impact

- **MEDIUM:** ошибка вместо результата при использовании канонических kind-типов; нарушение консистентности словарей между `symbol`/`inspect` (нормализуют) и `relations` (отклоняет).
- Затронуты CLI (`--source-type`/`--target-type`) и MCP (`codebase_query_relations`).
- На данные индекса не влияет; реверс направления контрактных связей (TC-066–068) — **не** предмет этого бага.

---

## Suggested Fix

В `findRelationEntityMatchesWithCondition` (или в `relationEntityMatchQueryParts`) перед сопоставлением со списком источников нормализовать входной тип через центральную карту из `internal/model`:

```go
// pseudo: kind-типы → api_contract (переиспользовать карту из symbol-type-vocabulary)
normalized := model.NormalizeRelationEntityType(entityType)
// service|event|callback_event|used_service → api_contract
```

- Карта уже существует (изменение `symbol-type-vocabulary`); нужен только вызов в `relations`-фильтре.
- Опционально: strict-валидация неизвестного типа со списком валидных значений (по аналогии с `SearchSymbol`) вместо молчаливого `false`.
- Спека `query/api-relations-queries`: дополнить требование нормализации entity-типов (дельта через OpenSpec change).

---

## Verification

1. `codebase_query_relations` `{"source_name":"API_ObjectEvent_MassCreate","source_type":"service"}` → без ошибки, поведение эквивалентно `source_type=api_contract` (count=0 при `implements_contract` — направление связей не меняется).
2. `{"target_name":"OnAfterCCes_MassCreate","target_type":"event","relation_type":"publishes_event"}` → count=1 (publisher `FCD_CON_AfterCCes_MassCreate`).
3. `go build ./... && go test ./internal/query/...` (+ юнит-тест нормализации: `service`→`api_contract` и т.д.).
4. Регресс: все 20+4 кейса `relations` из `Tests/TEST_CASES.md` (TC-062–077, TC-172–177) — без изменений.

---

## Resolution (2026-10-13)

Исправлено в change **`relations-entity-kind-aliases`** (заархивирован как [`2026-10-13-relations-entity-kind-aliases`](../openspec/changes/archive/2026-10-13-relations-entity-kind-aliases/)); дельта применена к `openspec/specs/query/api-relations-queries/spec.md`.

Что сделано (вариант A — узкая нормализация kind→`api_contract`):

- `internal/model/relation_types.go`: `NormalizeRelationEntityType(raw) (string, bool)` — trim+lower, канонический тип как есть, kind-типы (`service`, `event`, `callback_event`, `used_service`) → `api_contract`.
- `internal/query/query_relations.go`: нормализация `source_type`/`target_type` на входе `SearchRelations` (до `validateRelationFilters`) и применение нормализованных значений в name-резолве и type-only SQL. Побочно закрыт **разрыв словаря 11→17**: `relationEntityMatchQueryParts` и display-запрос `relationSearchBaseQuery` дополнены 6 типами (`dfm_form`, `spec_capability`, `spec_change`, `spec_requirement`, `spec_scenario`, `spec_usecase`), которые ранее проходили валидацию, но падали в name-фильтре с той же ошибкой `unsupported relation entity type`.
- Описание MCP-инструмента (`internal/mcp/registry.go`) и справка флагов CLI (`cmd/query.go`) дополнены упоминанием kind-алиасов.
- `SearchRelationsByEntity` (inspect-путь) намеренно не валидируется/не нормализуется — не затронут.

Проверка: `go build/vet/test ./...`, `go test -tags integration ./internal/query/... ./internal/store/...` — ok; TC-073 PASS (E2E на живом FA), TC-062–075/172–177 — все PASS; отчёт `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-13.md`.

Вне объёма (остаётся открытым): реверс направления контрактных связей (контракт как source не находится) — TC-066/067/068.

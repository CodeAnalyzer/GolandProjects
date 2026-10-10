## Why

В системе два легитимных словаря имён типов — `symbols.symbol_type` (короткий: `procedure`, `method`, `function`, …, плюс kind-типы API-контрактов `service`/`event`/`callback_event`/`used_service`) и `relations.*_type` (префиксный: `sql_procedure`, `pas_method`, `js_function`, `api_contract`, …) — и нет центрального моста между ними. Следствия (подтверждены на живой БД с индексом FA):

1. **Фильтр `type` в symbol search**: 7 из 10 значений, предлагаемых описанием MCP-инструмента `codebase_query_symbol` (`registry.go:393`), мертвы — `sql_procedure`, `sql_table`, `pas_method`, `js_function`, `dfm_form`, `dfm_component`, `api_contract` молча возвращают `[]`; спека `query/symbol-search:43` фиксирует несуществующий `js_function`.
2. **Inspect теряет связи**: `InspectRelationType` (`querysvc/inspect.go:67`) маппит только 4 случая (sql, dfm), поэтому `query inspect` / `codebase_query_inspect` для PAS-методов, JS-функций и API-контрактов возвращает **пустые incoming/outgoing**, хотя связи в графе существуют (метод `DeleteLinkToPolicy` — 0/0 при существующих `builds_query`; контракт `API_OCvr_MassInsertAssessment` — 0/0 при существующем `implements_contract`). Это нарушение действующего требования спеки `query/api-relations-queries` («Inspect … всех входящих и исходящих связей»).

LLM-агенты, следующие описанию инструмента, систематически получают false-negative; inspect — главная точка входа для исследования графа — неработоспособен для основных не-SQL типов.

## What Changes

- **Центральная таблица словаря** в `internal/model`: канонический список `symbol_type` (29 значений, включая fallback-тип `xml` для DSArchitect XML вне kind-каталогов), карта алиасов relations→symbols, карта symbols→relations. Единый источник истины для нормализации, валидации и моста inspect.
- **Нормализация алиасов фильтра**: `type` в `SearchSymbol` принимает relations-стиль (`sql_procedure`→`procedure`, …); `api_contract` разворачивается во все 4 kind-типа (фильтр по множеству).
- **Strict-валидация**: неизвестное значение `type` (не алиас и не канонический тип) → ошибка со списком валидных значений вместо молчаливого `[]`.
- **Достроенный мост inspect**: `InspectRelationType` пополняется переводами `method→pas_method`, `function→js_function`, kind-типы→`api_contract` — inspect возвращает связи для методов, JS-функций и контрактов.
- **Документация**: описание `codebase_query_symbol` (`registry.go:393`) переписывается на канонический словарь (алиасы остаются поддерживаемыми); справка CLI `--type` уточняется.
- **Спеки**: `query/symbol-search` — честный список типов + новое требование нормализации/валидации; `query/api-relations-queries` — сценарии inspect для метода и контракта.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/symbol-search`: требование «Поддерживаемые типы символов» переписывается на фактический словарь (29 типов, включая kind-типы и `api_table`/`api_param`/`api_table_index`/`spec_*`); добавляется требование «Нормализация алиасов и валидация типа» (алиасы relations-стиля, `api_contract` → 4 kind-типа, ошибка на неизвестное значение).
- `query/api-relations-queries`: требование «Inspect сущности с graph context» дополняется сценариями inspect PAS-метода и API-контракта (сейчас единственный сценарий — процедура, единственная работающая ветка).

## Impact

- **Код**:
  - `internal/model/` — новый файл с таблицей словаря (типы, алиасы, мост в relations);
  - `internal/query/query.go` — `SearchSymbol`: нормализация, strict-валидация, фильтр по множеству типов;
  - `internal/querysvc/inspect.go` — `InspectRelationType`: полная карта переводов; нормализация входного `symbolType` в `RunInspectQuery` (скоринг `PrioritizeExactSymbolMatches`);
  - `internal/mcp/registry.go` — описания `codebase_query_symbol` (:393) и `codebase_query_inspect` (:556); перекрёстная ссылка `codebase_query_procedure` (:449) после ввода алиасов становится корректной;
  - `cmd/query.go` — справка флага `--type`.
- **Контракты**: поведение фильтра `type` при неизвестном значении меняется с «молчаливый пустой результат» на ошибку со списком валидных значений — **изменение контракта** (улучшение диагностики; ни один документированный сценарий не ломается, алиасы делают существующие упоминания `sql_procedure` валидными, включая сценарий `mcp-transport-tools:259`).
- **Тесты**: модульные на таблицу словаря и нормализацию; интеграционные на фильтр по алиасу и inspect метода/контракта.
- **Связанные баг-репорты**: закрывает `BUG-symbol-search-type-naming-drift-20261009.md` (вариант A) и расширяет его — inspect-симптом в репорте не был описан.

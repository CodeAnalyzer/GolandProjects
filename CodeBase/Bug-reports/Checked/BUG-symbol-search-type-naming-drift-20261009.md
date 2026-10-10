# Bug Report: naming-дрейф типов в symbol-search — фильтры из спеки и MCP-описания возвращают пустой результат

**Дата:** 2026-10-09
**Файл:** `internal/mcp/registry.go` (`codebase_query_symbol`, строки 393–403); `internal/query/query.go` (`SearchSymbol`, строка 276); спека `openspec/specs/query/symbol-search/spec.md:43`
**Версия CodeBase:** 0.9.2
**Статус:** Исправлено 2026-10-10 в change [`symbol-type-vocabulary`](../openspec/changes/symbol-type-vocabulary/) (центральная таблица словаря `internal/model/symbol_types.go`: алиасы `sql_procedure`/`sql_table`/`pas_method`/`js_function`/`dfm_form`/`dfm_component`/`api_contract` транслируются в канонические типы, `api_contract` разворачивается в 4 kind-типа; неизвестное значение — ошибка со списком валидных; заодно починен мост inspect для PAS-методов/JS-функций/API-контрактов). Спеки обновлены дельтами в change; применяются при архивировании.

## Summary

Фильтр по типу символа (`--type` в CLI, `type` в MCP `codebase_query_symbol`) документирован в одном словаре, а в `symbols.type` хранятся значения другого словаря. Половина значений, предложенных спекой и описанием MCP-инструмента, **не существует** в данных: фильтр по ним молча возвращает `[]`.

- Спека `query/symbol-search` (строка 43) перечисляет тип `js_function` — его в `symbols` нет (JS-функции пишутся как `type = "function"`, `entity_type = "js"`: `internal/indexer/indexer.go:883,1337`).
- Описание MCP-инструмента `codebase_query_symbol` (`registry.go:393`) предлагает агенту: `sql_procedure`, `sql_table`, `pas_method`, `js_function`, `vb_function`, `dfm_form`, `dfm_component`, `smf_instrument`, `api_contract`, `report_form` — из них реально работают только **3 из 10** (`smf_instrument`, `vb_function`, `report_form`).

Критичность: **MEDIUM** (нарушен контракт «задокументированное значение фильтра → результат»; LLM-агенты, следующие описанию инструмента, систематически получают пустые ответы).

---

## Environment

- CodeBase 0.9.2, CLI `codebase query symbol` и MCP-инструмент `codebase_query_symbol`
- БД с проиндексированным проектом FA

---

## Reproduction Steps

1. Через CLI: `codebase query symbol --name <любая известная процедура> --type js_function` → `items: []`.
2. Через MCP: `codebase_query_symbol` с `{"name": "Ins_Check_ExistsLinkObject", "type": "sql_procedure"}` → пустой результат, хотя без `type` символ находится.
3. Проверка содержимого: `SELECT DISTINCT type FROM symbols ORDER BY 1`.

## Expected Result

Фильтр по каждому значению типа, перечисленному в спеке (строка 43) и в описании MCP-инструмента, возвращает символы этого типа.

## Actual Result

Значения из документа не совпадают со значениями в данных. Реальный словарь `symbols.type` (по факту записи из индексатора):

```
api_business_object, api_param, api_table, api_table_index, class,
column_definition, component, constant, define, function, index,
method, procedure, report_form, report_param, smf_instrument,
spec_capability, spec_change, spec_requirement, spec_usecase,
table, unit, vb_function
```

Соответствие «документ → данные»: `sql_procedure`→`procedure`, `sql_table`→`table`, `pas_method`→`method`, `js_function`→`function`, `dfm_form`→`form`, `dfm_component`→`component`, `api_contract`→`api_business_object`. Строка `type` передаётся в `SearchSymbol` как есть (`registry.go:403`), трансляции нет — SQL-условие `LOWER(type) = LOWER($2)` не находит ничего.

---

## Root Cause Analysis

### 1. Два независимых словаря имён

Слой хранения (`symbols.type`, заполняется индексатором) использует краткие имена: `procedure`, `function`, `method`, `form`, `component`, `table`. Слой документации и MCP-описаний — доменные префиксные имена (`sql_procedure`, `js_function`, `dfm_form`), совпадающие со стилем `entity_type`/relation-типов (`sql_procedure` и т.п. используются в `relations` и в ответах `codebase_query_relations`). Словари пересекаются частично, никто не отвечает за трансляцию.

### 2. Описание инструмента — единственная «документация» для агентов

LLM-агент выбирает значения `type` из описания `codebase_query_symbol` (`registry.go:393`). 7 из 10 предложенных значений дают пустой ответ без ошибки и подсказки: агент делает ложный вывод «символа нет в индексе».

### 3. Спека воспроизводит дрейф

`query/symbol-search:43` фиксирует `js_function` в списке типов `symbols` — то есть спека сама описывает несуществующее значение (код пишет `function` + `entity_type = "js"`). Инвариант «спека — источник истины» здесь нарушен в обратную сторону: спека не соответствует коду.

### 4. Прецедент трансляции уже есть

`querysvc/inspect.go` (`InspectRelationType`) уже маппит symbol-type → relation-type (`procedure`→`sql_procedure`, `form`→`dfm_form`, ...) — то же место, куда логично добавить обратную нормализацию алиасов для фильтра.

---

## Impact

- **MEDIUM:** молчаливый пустой результат вместо ошибки/подсказки; для MCP-агентов — систематический false-negative при следовании документации инструмента.
- Затронуты CLI (`--type`), MCP (`codebase_query_symbol`, косвенно `codebase_query_inspect` с параметром `type`) и спека `query/symbol-search`.

---

## Suggested Fix

### Вариант A (рекомендуется): нормализация алиасов на границе + честный список

1. `internal/mcp/registry.go` — перед `SearchSymbol` прогонять `type` через карту алиасов:
   `sql_procedure→procedure, sql_table→table, pas_method→method, js_function→function, dfm_form→form, dfm_component→component, api_contract→api_business_object`; канонические значения проходят без изменений. Обратная совместимость полная.
2. То же для CLI-флага `--type` (`cmd/query_commands.go`, точка входа `SearchSymbol`).
3. Пустой результат при заданном `type` дополнять подсказкой со списком валидных значений (или валидировать `type` до запроса с ошибкой «unknown type, valid values: ...»).
4. Описание `codebase_query_symbol` в `registry.go:393` переписать на реальный словарь (или оставить алиасы, но после нормализации).
5. Спеку `query/symbol-search:43` поправить: `js_function` → `function` (с пометкой `entity_type = "js"`), зафиксировать правило нормализации алиасов как requirement.

### Вариант B (минимальный): привести документацию к реальности

1. `registry.go:393` — описание со значениями `procedure, table, method, function, form, component, api_business_object, ...`.
2. `query/symbol-search:43` — убрать `js_function`, добавить фактический список (включая `spec_*`, `api_param`, `api_table`, `api_table_index`).
3. Минус: существующие клиенты/промпты, уже использующие `sql_procedure` и т.п., не чинятся; дрейф может повториться при развитии словаря.

Вариант A предпочтителен: устраняет класс ошибки (два словаря), а не конкретный срез; `InspectRelationType` показывает, что проект уже практикует трансляцию на границе сервисного слоя.

### Тесты

- `internal/mcp`: `TestQuerySymbolTypeAliases` — для каждого алиаса (`sql_procedure`, `sql_table`, `pas_method`, `js_function`, `dfm_form`, `dfm_component`, `api_contract`) хендлер передаёт в `SearchSymbol` каноническое значение.
- `internal/query`: интеграционный тест — символ с `type = function, entity_type = js` находится по `--type js_function` (после нормализации) и по `--type function`.

### Файлы для изменения (вариант A)

1. **`internal/mcp/registry.go`** — хендлер `codebase_query_symbol` (строки ~393–407): нормализация `type`, правка описания.
2. **`cmd/query_commands.go`** — флаг `--type`: та же нормализация.
3. **`internal/query/query.go`** — опционально: валидация `type` с сообщением о валидных значениях.
4. **`openspec/specs/query/symbol-search/spec.md`** — строка 43 и описание нормализации алиасов.

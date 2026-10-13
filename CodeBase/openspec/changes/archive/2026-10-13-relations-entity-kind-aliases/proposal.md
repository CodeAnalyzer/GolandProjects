## Why

Инструмент `codebase_query_relations` (и CLI `query relations`) отклоняет канонические
kind-типы API-контрактов (`service`, `event`, `callback_event`, `used_service`) в
`source_type`/`target_type`, хотя тот же словарь принимается соседними инструментами
`codebase_query_symbol` и `codebase_query_inspect`. LLM-агент, естественно использующий
`source_type=service`, получает ошибку вместо результата — принцип «единый словарь — все
точки входа», введённый change `symbol-type-vocabulary`, нарушен.

Дополнительно вскрыто рассогласование словарей типов сущностей графа: словарь строгой
валидации (`relations-relation-type-validation-and-index`) содержит 17 типов, а name-резолв
`query relations` умеет разрешать имя только для 11 из них. Шесть легитимных типов
(`dfm_form`, `spec_capability`, `spec_change`, `spec_requirement`, `spec_scenario`,
`spec_usecase`) проходят валидацию, но при указании имени падают с `unsupported relation
entity type for name filter`. Источник проблемы — `BUG-relations-kind-type-name-filter-20261011.md`.

## What Changes

- **Центральный алиас в `internal/model`**: новая функция нормализации типа сущности графа
  связей, отображающая kind-типы API-контрактов (`service`, `event`, `callback_event`,
  `used_service`) в канонический `api_contract`; канонические типы проходят без изменений.
- **Применение алиаса во всех точках `relations`**:
  - строгая валидация (`validateRelationFilters`) принимает kind-типы как алиасы `api_contract`;
  - name-резолв (`relationEntityMatchQueryParts`) разрешает имя контракта для kind-алиаса;
  - type-only SQL-ветка (`buildRelationIDsQuery`) фильтрует по нормализованному `api_contract`.
- **Закрытие разрыва 11→17**: name-резолв и display-обогащение результата покрывают все 17
  типов словаря сущностей графа — добавляются источники `dfm_form` и пять `spec_*`.
- **Документация**: описание MCP-инструмента `codebase_query_relations` и справка CLI
  дополняются упоминанием kind-алиасов и полного списка разрешаемых типов.
- **Спецификация**: дельта к требованию «Строгая валидация фильтров графа связей» и
  дополнение требования «Запрос к графу связей» (нормализация алиасов, полное покрытие
  имён по словарю).

Поведенческая совместимость: ранее валидные значения и поведение не меняются; изменение
чисто расширяющее (kind-тип раньше давал ошибку, теперь эквивалентен `api_contract`).
Фильтрация по `contract_kind` (различение service/event) в объём не входит — kind-алиас
намеренно эквивалентен `api_contract`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/api-relations-queries`: требование «Строгая валидация фильтров графа связей»
  расширяется приёмом kind-алиасов и требованием полного name-резолва по словарю;
  требование «Запрос к графу связей» дополняется нормализацией типа и покрытием имён
  для всех типов сущностей графа.

## Impact

- **Код**:
  - `internal/model/relation_types.go` (или `symbol_types.go`) — функция нормализации типа
    сущности графа (kind → `api_contract`);
  - `internal/query/query_relations.go` — `validateRelationFilters`, `SearchRelations`,
    `relationEntityMatchQueryParts`, `buildRelationIDsQuery`, `relationSearchBaseQuery`
    (display-обогащение для новых типов);
  - `internal/mcp/registry.go` — описание `codebase_query_relations` (`:542`);
  - `cmd/query.go` — справка флагов `--source-type`/`--target-type`.
- **Контракты**: расширение принимаемых значений `source_type`/`target_type` в
  `query relations`; строгая валидация сохраняется для прочих неизвестных значений.
- **Тесты**: модульные на нормализацию в `model` (kind → `api_contract`, регистр/пробелы),
  unit на `relationEntityMatchQueryParts` для kind-алиаса и новых 6 типов, интеграционные
  на name-фильтр `source_type=service` и `source_type=dfm_form`/`spec_capability`.
- **Связанные баг-репорты**: закрывает `BUG-relations-kind-type-name-filter-20261011.md`
  (TC-073 из `Tests/TEST_CASES.md`).

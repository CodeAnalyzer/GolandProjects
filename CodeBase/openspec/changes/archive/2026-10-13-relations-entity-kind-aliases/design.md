## Context

См. proposal.md — Why. Текущее устройство фильтрации `query relations`:

```
SearchRelations(sourceType, sourceName, targetType, targetName, relationType)
   │
   ├─ validateRelationFilters()            гейт 1: словарь RelationEntityTypes (17)
   │     service → ErrUnknownRelationType
   │
   ├─ findRelationEntityMatches(name, type)
   │     └─ relationEntityMatchQueryParts(type)   гейт 2: локальный список (11)
   │           dfm_form/spec_* → "unsupported relation entity type for name filter"
   │
   └─ buildRelationIDsQuery(...)           type-only ветка: r.source_type = $N
```

Центральные словари уже есть: `model.RelationEntityTypes` (17), `model.RelationTypes` (21).
Мосты symbol↔relations — `model.ResolveSymbolTypeAlias` (relations→symbols, разворот
`api_contract` в 4 kind-типа) и `model.RelationTypeForSymbol` (symbols→relations, свёртка
kind-типов в `api_contract`). Направление, нужное `relations`, — свёртка kind→`api_contract`;
`RelationTypeForSymbol` даёт его, но заодно переводит `procedure→sql_procedure` и т.п.,
что шире требуемого.

`SearchRelationsByEntity` (используется `inspect`) намеренно не валидируется и принимает
типы вне словаря графа (`pas_unit`, `pas_class`, `dfm_component`, `api_business_object`,
`xml`, `sql`) через `InspectRelationType` — это ограничение менять нельзя.

## Goals / Non-Goals

**Goals:**

- Единый источник нормализации «тип сущности графа» в `internal/model`.
- kind-типы API-контрактов принимаются всеми точками фильтра `relations` и эквивалентны
  `api_contract`.
- Name-резолв покрывает ровно словарь `RelationEntityTypes` (17) — гейты 1 и 2 согласованы.

**Non-Goals:**

- Различение kind при фильтрации (вариант B: `api_contract WHERE contract_kind='service'`) —
  отложено; kind-алиас намеренно эквивалентен `api_contract`.
- Приём всего symbol-словаря (29) в `relations` (вариант C) — пересмотр решения
  `relations-relation-type-validation-and-index`; вне объёма.
- Валидация/нормализация `SearchRelationsByEntity` (путь inspect) — не трогается.
- Смена словаря хранения (`relations.source_type`) — данные не меняются.

## Decisions

### 1. Нормализация типа сущности — новая функция в `internal/model`

Добавить в `internal/model` (рядом с `RelationEntityTypes`) функцию вида
`NormalizeRelationEntityType(raw string) (string, bool)`:

- trim + lower;
- key ∈ `relationEntityTypeSet` → вернуть key как есть, `true`;
- key ∈ {service, event, callback_event, used_service} → вернуть `api_contract`, `true`;
- иначе → `"", false`.

Альтернативы: переиспользовать `RelationTypeForSymbol("service","")` — отвергнуто, тянет
полный symbol→relations мост и смазывает семантику «нормализация типа сущности графа»;
разместить алиас в `query` — отвергнуто, тогда валидатор и name-резолв смотрят в разные
таблицы, дрейф повторится.

### 2. Применение — в точке входа `SearchRelations`

Нормализация выполняется один раз в начале `SearchRelations` (до `validateRelationFilters`),
нормализованные `sourceType`/`targetType` используются и валидатором, и name-резолвом, и
`buildRelationIDsQuery`. `validateRelationFilters` проверяет уже нормализованное значение
(при `false` — прежняя ошибка со списком валидных значений). `relationEntityMatchQueryParts`
дополнительно вызывает нормализацию идемпотентно (защита от прямых вызовов в тестах).

Альтернатива: нормализовать только в `relationEntityMatchQueryParts` — отвергнуто, тогда
гейт валидации продолжит отклонять `service` до резолва.

### 3. Закрытие разрыва 11→17 в name-резолве и display

В `relationEntityMatchQueryParts` добавить 6 источников (таблица → колонка имени):

| Тип | Таблица | Колонка |
|---|---|---|
| `dfm_form` | `dfm_forms` | `form_name` |
| `spec_capability` | `spec_capabilities` | `capability_name` |
| `spec_change` | `spec_changes` | `change_name` |
| `spec_requirement` | `spec_requirements` | `requirement_name` |
| `spec_scenario` | `spec_scenarios` | `scenario_name` |
| `spec_usecase` | `spec_usecases` | `usecase_name` |

Чтобы результат не терял читаемость, `relationSearchBaseQuery` (display-обогащение)
дополняется LEFT JOIN-блоками этих таблиц (source и target) по тому же образцу, что и
существующие 11 типов; COALESCE-цепочки имени/файла/строки расширяются.

Альтернатива: резолвить имена всех типов через единую таблицу `symbols`
(`symbol_name, symbol_type, entity_type, entity_id` + `RelationTypeForSymbol`) — отвергнуто:
это смена модели резолва и профиля производительности существующих 11 типов, более крупная
правка с риском регресса exact-first/ILIKE поведения.

### 4. Документация

Описание `codebase_query_relations` (`internal/mcp/registry.go:542`) и справка CLI
(`cmd/query.go`) дополняются: kind-типы `service`/`event`/`callback_event`/`used_service`
принимаются как алиасы `api_contract`; список типов сущностей — полный (17).

## Risks / Trade-offs

- [kind-алиас теряет различие service/event] → намеренно (вариант A), зафиксировано в спеке:
  `service` ≡ `api_contract`; при потребности в различении — отдельный change (вариант B).
- [Добавление 6 LEFT JOIN-блоков в display-запрос утяжеляет `loadRelationDetails`] →
  JOIN'ы типизированы условием `r.source_type = '<type>'` (как существующие) и не меняют
  план для остальных типов; проверка EXPLAIN/времени на живой FA.
- [Нормализация `source_type=service` возвращает связи всех kind-контрактов] → ожидаемо и
  задокументировано; не регресс, ранее было ошибкой.
- [Расхождение словаря `RelationEntityTypes` с реально записываемыми типами (spec_*)] →
  источники добавлены симметрично словарю; пустой результат по ненаполненному типу безвреден.

## Migration Plan

Изменение кода без миграции данных. Деплой — пересборка бинаря; БД и индекс не трогаются
(схема не меняется, индексы relations не добавляются). Откат — предыдущий бинарь.

## Open Questions

(нет — развилка A/B/C решена в explore-сессии в пользу A)

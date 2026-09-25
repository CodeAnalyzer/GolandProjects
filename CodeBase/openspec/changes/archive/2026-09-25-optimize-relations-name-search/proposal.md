## Why

Фильтрация графа связей по имени (`query relations --source-name/--target-name`) в общем случае строит коррелированный `OR` из 11 `EXISTS` с `ILIKE '%…%'` (`buildRelationAnyNameExistsCondition`, `internal/query/query_relations.go:185`), который не использует ни один индекс по `relations` и выполняется секундами. При этом для узкого частного случая (одно имя, другая сторона без типа) уже существует корректный двухпроходный алгоритм на индексированных lookup'ах. Две ветки кода дают ещё и разную семантику сопоставления имени (exact-first против чистого `ILIKE`), из-за чего поведение команды зависит от комбинации аргументов.

## What Changes

- Унифицировать поиск relations по имени: имя всегда резолвится в пары `(entity_type, entity_id)` через типизированные индексированные lookup'ы, после чего `relations` фильтруются по составному ключу (`source_type/source_id`, `target_type/target_id`).
- Распространить семантику exact-first (точное совпадение, затем подстрочный фолбэк) на все комбинации фильтров: одно имя, оба имени, смешанные типизированные/нетипизированные.
- Сохранить пересечение при двух именах: результат содержит relations, где source совпал с первым именем AND target — со вторым.
- Убрать `buildRelationAnyNameExistsCondition` и `buildRelationNameExistsCondition` из горячего пути поиска (коррелированные `EXISTS` больше не используются).
- **BREAKING** (для фильтров по имени): при нечётком (подстрочном) совпадении результаты теперь возвращаются только при отсутствии точных совпадений — как уже сделано в быстром пути; раньше общий путь всегда возвращал подстроку.

## Capabilities

### New Capabilities

- (нет)

### Modified Capabilities

- `query/api-relations-queries`: уточняется требование «Запрос к графу связей» — единая семантика сопоставления имён (exact-first с подстрочным фолбэком) и резолв имён через типизированные индексированные lookup'ы для всех комбинаций `source_name`/`target_name`/типов.

## Impact

- `internal/query/query_relations.go` — `SearchRelations`, `searchRelationsByNameMatches`, `findRelationEntityMatches`, `relationEntityMatchQueryParts`, `selectRelationIDsByEntityMatches`; удаляются `buildRelationAnyNameExistsCondition`, `buildRelationNameExistsCondition`.
- `internal/query/query_test.go` — покрытие семантики фильтров по имени.
- Схема БД не меняется: используются уже существующие индексы `idx_*_*_name_lower`, `idx_relations_source_type_id`, `idx_relations_target_type_id`.
- Поведение CLI `query relations` и MCP `codebase_query_relations`: внешний контракт фильтров сохраняется, меняется латентность и (для нечётких имён) правило exact-first.
- Графовые запросы верхнего уровня (`inspect`) не затрагиваются: они используют `SearchRelationsByEntity` по ID.

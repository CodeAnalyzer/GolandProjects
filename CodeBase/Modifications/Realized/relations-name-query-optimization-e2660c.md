# План оптимизации query relations по source-name/target-name

План минимально безопасно ускоряет `query relations --source-name/--target-name` за счет предварительного поиска entity ID по имени и последующего поиска relations по `(type, id)`, сохраняя текущую `EXISTS`-логику как fallback.

## Цель

Снизить время проблемных запросов из отчета `query-performance-verification-report.md`:

- `query relations --source-name Cons_Aim_Update`: сейчас около `1959ms`
- `query relations --target-name API_CCred_FindListIDByParam`: сейчас около `1903ms`
- `query relations --target-name pConsDocAmount`: сейчас около `1706ms`
- `query relations --target-name tAim`: сейчас около `952ms`

Целевой ориентир: приблизить name-only сценарии к диапазону `<500-700ms`, не ухудшая типизированный сценарий `query relations --source-type ... --target-type ...`.

## Проблема

Текущая реализация `SearchRelations` при отсутствии `sourceType` или `targetType` строит большой `OR` из `EXISTS` по разным entity-таблицам:

- `sql_procedures`
- `sql_tables`
- `pas_methods`
- `js_functions`
- `api_contracts`
- `report_forms`
- `report_fields`
- `report_params`
- `vb_functions`
- `query_fragments`
- `smf_instruments`

Даже при наличии `trgm` и `LOWER(...)` индексов PostgreSQL вынужден планировать фильтрацию от `relations` к множеству entity-таблиц, что плохо работает для name-only поиска.

## Подход

Развернуть направление поиска только для проблемных сценариев:

1. Сначала найти подходящие entity по имени в специализированных таблицах через индексы.
2. Затем найти relations по уже известным `(entity_type, entity_id)` через индексы `relations(source_type, source_id)` или `relations(target_type, target_id)`.
3. Детали relations догружать существующим `buildRelationDetailsQueryByIDs`.
4. Если новый быстрый путь не применим или вернул ошибку, оставить возможность fallback на текущую `EXISTS`-логику.

## Этап 1. Добавить внутреннюю модель matched entity

Файл: `internal/query/query_relations.go`

Добавить небольшую внутреннюю структуру:

```go
type relationEntityMatch struct {
    Type string
    ID   int64
}
```

Назначение: передавать найденные entity из предварительного поиска в выборку relations.

## Этап 2. Добавить exact-first поиск entity по имени

Файл: `internal/query/query_relations.go`

Добавить функцию уровня `Query`, например:

```go
func (q *Query) findRelationEntityMatches(name string, entityType string, limit int) ([]relationEntityMatch, error)
```

Поведение:

- Если `entityType` задан, искать только в соответствующей таблице.
- Если `entityType` пустой, выполнять поиск по поддерживаемым relation entity-таблицам через `UNION ALL`.
- Сначала использовать точное case-insensitive условие:
  - `LOWER(name_column) = LOWER($1)`
- Если точных совпадений нет, fallback на partial:
  - `name_column ILIKE $1`, где аргумент `"%" + name + "%"`
- Ограничить число найденных entity разумным лимитом, например `max(limit*4, 50)`, чтобы не раздувать следующий запрос.

Почему exact-first важно:

- Проверочные запросы выглядят как точные имена.
- Уже добавлены `LOWER(...)` индексы.
- Это должно убрать дорогое `%...%` там, где оно не нужно.

## Этап 3. Добавить выбор relations по matched entity

Файл: `internal/query/query_relations.go`

Добавить функцию, например:

```go
func (q *Query) selectRelationIDsByEntityMatches(side string, matches []relationEntityMatch, extraConditions []string, extraArgs []interface{}, argPos int, limit int) ([]int64, error)
```

Логика:

- Для `side == "source"` искать по:
  - `r.source_type = match.Type`
  - `r.source_id = match.ID`
- Для `side == "target"` искать по:
  - `r.target_type = match.Type`
  - `r.target_id = match.ID`
- Использовать `JOIN` к `VALUES` или CTE `matched_entities(entity_type, entity_id)`.
- Сохранять дополнительные фильтры, если они заданы:
  - `relationType`
  - противоположный тип, если передан
- Сортировка и лимит остаются прежними:
  - `ORDER BY r.id DESC LIMIT $N`

Ожидаемое использование существующих индексов:

- `idx_relations_source_type_id`
- `idx_relations_target_type_id`
- `idx_relations_relation_type`

## Этап 4. Подключить быстрый путь в SearchRelations

Файл: `internal/query/query_relations.go`

В `SearchRelations` добавить раннюю ветку только для минимально безопасных случаев:

- `sourceName != "" && sourceType == "" && targetName == ""`
- `targetName != "" && targetType == "" && sourceName == ""`

Для этих случаев:

1. Найти matches через `findRelationEntityMatches`.
2. Если matches пустой, вернуть пустой результат.
3. Выбрать relation IDs через `selectRelationIDsByEntityMatches`.
4. Догрузить детали через существующий `buildRelationDetailsQueryByIDs`.

Текущая `EXISTS`-логика остается без изменений для:

- типизированных запросов;
- запросов с одновременно `sourceName` и `targetName`;
- нестандартных комбинаций фильтров.

## Этап 5. Сохранить совместимость поведения

Проверить, что сохраняются текущие свойства:

- `limit` ограничивает итоговое количество relations.
- Сортировка остается `ORDER BY r.id DESC`.
- Формат результата не меняется.
- `query relations (типизированный)` не регрессирует.
- Ошибка `at least one relation filter must be provided` остается для пустых фильтров.

## Этап 6. Минимальные тесты

Добавить unit-тесты на SQL-builder/helper функции, если они будут выделены:

- exact-first entity lookup выбирает `LOWER(...) = LOWER($1)`.
- partial fallback использует `ILIKE`.
- `source`-side строит join по `source_type/source_id`.
- `target`-side строит join по `target_type/target_id`.

Если SQL останется внутри методов без чистых builder-функций, ограничиться ручной проверкой сборки и runtime-замерами.

## Риски

- Слишком много matched entity при коротком имени вроде `Aim` может увеличить размер `VALUES` и нагрузку на relations.
- `UNION ALL` по всем entity-таблицам нужно ограничивать, иначе partial search может вернуть слишком много ID.
- Exact-first может изменить порядок fallback-поведения только в лучшую сторону, но нужно убедиться, что partial поиск остается при отсутствии exact совпадений.

## Контрольные проверки после реализации

Рекомендуемые команды:

```powershell
.\CodeBase.exe query relations --source-name Cons_Aim_Update --json
.\CodeBase.exe query relations --target-name API_CCred_FindListIDByParam --json
.\CodeBase.exe query relations --target-name pConsDocAmount --json
.\CodeBase.exe query relations --target-name tAim --json
.\CodeBase.exe query relations --source-type sql_procedure --source-name AIC_AccrualFindTemplate --target-type sql_table --json
```

Ожидаемые критерии:

- name-only запросы должны заметно ускориться относительно `952-1959ms`.
- типизированный запрос должен остаться примерно в прежнем диапазоне `200-500ms`.
- количество и структура результатов должны соответствовать текущему поведению.

# План оптимизации query-команд CodeBase

План описывает конкретные изменения в коде и схеме БД для устранения узких мест из `Logs/query-performance-report.md`.

## Цели и критерии готовности

- **`query table --name --like`**: снизить время с 3.5-4.6s до целевого диапазона <500ms.
- **`query table --name`**: снизить типичные 1.5-3.0s до <500ms для точного поиска.
- **`query sql-fragment --text`**: снизить 1.5-2.5s до <500ms.
- **`query inspect`**: снизить повторяющиеся 1.0-1.6s за счет ограничения fan-out и повторного использования уже найденных данных.
- **Не регрессировать исправленный `query relations`**: сохранить диапазон 200-500ms для типизированных relations-запросов.

## Приоритет 1. Ускорить `query table`

### 1.1 Добавить недостающие индексы в `internal/store/db.go`

В `DB.InitSchema()` после существующих индексов для `sql_tables`/`sql_columns` добавить:

- `idx_sql_tables_table_name_lower` на `LOWER(table_name)` для точного case-insensitive поиска.
- `idx_sql_tables_table_name_trgm` на `table_name gin_trgm_ops` для `ILIKE '%...%'`.
- `idx_sql_columns_table_name` на `sql_columns(table_name)` для догрузки колонок.
- `idx_sql_tables_table_name_file_id` на `(table_name, file_id)` для списка файлов по таблице.
- При необходимости после EXPLAIN — индекс на `relations(target_type, target_id, relation_type, source_type, source_id)` для блока процедур в `SearchTable`.

### 1.2 Убрать N+1 в `internal/query/query_sql.go:SearchTable`

Текущая реализация делает для каждого найденного результата отдельные запросы к `files`, `sql_columns`, `relations/sql_procedures/sql_tables`.

Заменить на batch-загрузку:

- Сначала получить список таблиц `[]TableResult` основным запросом с `LIMIT`.
- Собрать `tableNames`.
- Одним запросом получить до 20 файлов на каждую таблицу через `ROW_NUMBER() OVER (PARTITION BY st.table_name ORDER BY f.rel_path)`.
- Одним запросом получить до 50 колонок на каждую таблицу через `ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY column_name)`.
- Одним запросом получить до 20 процедур на каждую таблицу через relations join и `ROW_NUMBER() OVER (PARTITION BY st.table_name ORDER BY sp.proc_name)`.
- Разложить результаты в map `table_name -> *TableResult`.

### 1.3 Проверить `LIMIT`

Убедиться, что `LIMIT $2` применяется до догрузки связанных данных, а batch-запросы используют только найденные `tableNames`, а не весь набор таблиц.

## Приоритет 2. Ускорить `query symbol`

### 2.1 Добавить индексы для точного и LIKE-поиска

В `internal/store/db.go` добавить:

- `idx_symbols_symbol_name_lower` на `LOWER(symbol_name)`.
- `idx_symbols_symbol_name_type_lower` на `(LOWER(symbol_name), symbol_type)`.
- `idx_symbols_symbol_name_trgm` на `symbol_name gin_trgm_ops`.
- `idx_symbols_signature_trgm` на `signature gin_trgm_ops`, потому что `buildSymbolLookupCondition()` ищет также по `signature` для form.

### 2.2 Разделить точный и LIKE SQL в `internal/query/query.go:SearchSymbol`

- Для `like=false` использовать `LOWER(s.symbol_name) = LOWER($1)` вместо общего условия с `OR` по `signature`, если `symbolType != "form"`.
- Для `symbolType == "form"` оставить поиск по `signature`, но проверить план запроса после добавления trgm-индекса.
- Оставить `ORDER BY s.symbol_name LIMIT n`, но проверить, не мешает ли сортировка индексу на больших выборках.

## Приоритет 3. Ускорить `query sql-fragment --text`

### 3.1 Проверить текущую реализацию поиска фрагментов

Найти функцию, обслуживающую `query sql-fragment --text`, вероятнее всего в `internal/query/query_sql.go` или соседних файлах.

### 3.2 Использовать существующий trgm-индекс

В `DB.InitSchema()` уже есть `idx_query_fragments_query_text_trgm ON query_fragments USING GIN (query_text gin_trgm_ops)`.

Проверить, что поиск `--text` реально идет по `query_fragments.query_text ILIKE $1`, а не по другому полю или через `LOWER(query_text) LIKE LOWER($1)`, который может не использовать этот индекс.

### 3.3 Если поиск идет по другому полю

Добавить соответствующий индекс:

- Для `context`: `idx_query_fragments_context_trgm ON query_fragments USING GIN (context gin_trgm_ops)`.
- Для `component_name`: точный/LIKE индекс в зависимости от SQL.

## Приоритет 4. Стабилизировать `query relations --source-name/--target-name`

### 4.1 Не трогать уже исправленный двухэтапный поиск

Сохранить текущую архитектуру `selectRelationIDs()` + `buildRelationDetailsQueryByIDs()` в `internal/query/query_relations.go`.

### 4.2 Добавить индексы для name-фильтров по справочным таблицам

Так как `buildRelationAnyNameExistsCondition()` делает EXISTS по разным таблицам, добавить недостающие индексы на name-поля, которые участвуют в `ILIKE`:

- `sql_procedures.proc_name` trgm/LOWER.
- `sql_tables.table_name` уже покрывается Приоритетом 1.
- `pas_methods.method_name` trgm/LOWER.
- `js_functions.function_name` trgm/LOWER.
- `api_contracts.contract_name` trgm/LOWER.
- `report_forms.report_name`, `report_fields.field_name`, `report_params.param_name` trgm/LOWER.
- `vb_functions.function_name`, `query_fragments.component_name`, `smf_instruments.instrument_name` trgm/LOWER.

## Приоритет 5. Оптимизировать `query inspect`

### 5.1 Ограничить fan-out в `cmd/query_execution.go:runInspectQuery`

Текущий алгоритм:

- `SearchSymbol(name, type, true, limit)`.
- Для каждого символа выполняет 2 relation-запроса: outgoing и incoming.

Изменения:

- После `prioritizeExactSymbolMatches()` ограничить число inspect-сущностей отдельным максимумом, например `min(limit, 5)` или новым внутренним лимитом.
- Для точного `--name` сначала пробовать `SearchSymbol(name, type, false, limit)`, и только если результатов нет — fallback на `like=true`.
- Не запрашивать relations для низкоприоритетных fuzzy-совпадений, если уже найдены точные совпадения.

### 5.2 Возможная batch-оптимизация relations

Если после предыдущего шага `inspect` остается >1s:

- Добавить метод `SearchRelationsForEntities()` в `internal/query/query_relations.go`.
- Загружать incoming/outgoing для группы `(type, entity_id)` одним-двумя запросами вместо 2*N запросов.


# Индексы для InternalPTables из XML

Добавить парсинг секции `<Indexses>` для не-API p-таблиц (InternalPTables) из XML-файлов DSArchitectData, чтобы правило `indexExistsInDB` могло находить их индексы.

## Контекст

InternalPTables (p-таблицы не из API-модулей, например `pAccrualParams.xml`) парсятся в `internal/parser/dsxml/parser.go` (ветка `else`, строки 273-299). Сейчас парсятся только поля (columns), но **индексы игнорируются**. Индексы API-таблиц сохраняются в `api_business_object_table_indexes`, индексы обычных SQL-таблиц — в `sql_index_definitions`. Правило `indexExistsInDB` проверяет обе таблицы, но индексы InternalPTables не попадают ни в одну.

## План

### Шаг 1: Добавить поля в ParseResult и модель

**Файл:** `internal/parser/dsxml/parser.go`

- Добавить в `ParseResult` новые поля:
  - `InternalPTableIndexes []*model.SQLIndexDefinition`
  - `InternalPTableIndexFields []*model.SQLIndexDefinitionField`
- В `ParseContent` инициализировать их в `res`.

### Шаг 2: Парсить индексы в ветке InternalPTables

**Файл:** `internal/parser/dsxml/parser.go`, ветка `else` (строки 273-299)

- После цикла по `table.Fields` добавить цикл по `table.Indexes`:
  - Создать `model.SQLIndexDefinition` с полями: `TableName`, `IndexName`, `IndexFields`, `IndexType` (преобразовать int→string: 0=nonclustered, 1=clustered, 2=unique nonclustered), `IsUnique` (IndexType==2), `DefinitionKind="create"`, `LineNumber=1`.
  - Добавить в `res.InternalPTableIndexes`.
  - Для каждого `index.Fields` создать `model.SQLIndexDefinitionField` с `ParentIndexName`, `ParentTableName`, `FieldName`, `FieldOrder`, `LineNumber=1`.
  - Добавить в `res.InternalPTableIndexFields`.

### Шаг 3: Сохранять индексы в indexer

**Файл:** `internal/indexer/indexer.go`, функция `parseXMLFile` (после строки 1157)

- Проставить `FileID` для `result.InternalPTableIndexes`.
- Вызвать `idx.db.BatchInsertSQLIndexDefinitions(result.InternalPTableIndexes, ...)`.
- Проставить `FileID` для `result.InternalPTableIndexFields`.
- Вызвать `idx.db.BatchInsertSQLIndexDefinitionFields(result.InternalPTableIndexFields, ...)`.
- Обновить `stats` (добавить к существующему счётчику `SQLTableIndexes` или `APITableIndexes`).

### Шаг 4: Тесты парсера

**Файл:** `internal/parser/dsxml/parser_test.go`

- Расширить `TestParseContent_InternalTableGoesToInternalPTables`: добавить `<Indexses>` с индексом в тестовые данные, проверить `InternalPTableIndexes` (1 индекс) и `InternalPTableIndexFields` (2 поля).
- Новый тест: InternalPTable без `<Indexses>` — `InternalPTableIndexes` пуст.

### Шаг 5: Проверка

- `go test ./internal/parser/dsxml/... ./internal/encoding/... -count=1`
- `go build ./...`
- `codebase init` (переиндексация) → `codebase review API_CON_BeforeRuleD_MassCreate.sql` — finding `indexExistsInDB` для `XPKpAccrualParams` должен исчезнуть.

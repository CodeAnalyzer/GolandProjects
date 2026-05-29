# План расширения `query symbol` для приоритетных сущностей

План поэтапно добавляет в unified index `symbols` приоритетные сущности, причём каждая сущность дорабатывается отдельной фазой, завершается сборкой продукта и останавливается до явного акцепта пользователя.

## Цель

Сделать `codebase query symbol` полезным единым входом для поиска ключевых сущностей, которые уже извлекаются парсерами и сохраняются в специализированные таблицы, но сейчас не представлены в `symbols`.

## Общие правила реализации

- Каждая сущность реализуется отдельной фазой.
- После каждой фазы выполняется только обязательная проверка сборкой: `go build ./...`.
- После успешной сборки работа останавливается до явного акцепта пользователя.
- Следующая фаза начинается только после акцепта предыдущей.
- Для каждой сущности нужно:
  - найти место, где сущность уже сохраняется в БД;
  - дождаться/получить persisted `entity_id`;
  - создать `model.Symbol` с корректными `FileID`, `SymbolName`, `SymbolType`, `EntityType`, `EntityID`, `LineNumber`, `Signature`;
  - сохранить symbols через `BatchInsertSymbols`;
  - не менять parser logic без необходимости.

## Предлагаемые `symbol_type` / `entity_type`

| Сущность | `symbol_type` | `entity_type` |
|---|---|---|
| PAS unit | `unit` | `pas` |
| PAS class | `class` | `pas` |
| PAS method | `method` | `pas` |
| DFM component | `component` | `dfm` |
| SQL index definition | `index` | `sql` |
| SQL column definition | `column_definition` | `sql` |
| API business object | `api_business_object` | `api` |
| JS constant | `constant` | `js` |

## Фаза 1: PAS classes

### Что сделать

- В `parsePASFile` после `BatchInsertPASClasses` и разрешения `classIDByLookup` сформировать symbols для `PASClass`.
- Использовать:
  - `SymbolName`: `ClassName`
  - `SymbolType`: `class`
  - `EntityType`: `pas`
  - `EntityID`: persisted `pas_classes.id`
  - `LineNumber`: `LineStart`
  - `Signature`: `ParentClass`

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <ClassName> --type class`

## Фаза 2: PAS methods

### Что сделать

- В `parsePASFile` после `BatchInsertPASMethods` и разрешения `methodIDByLookup` сформировать symbols для `PASMethod`.
- Использовать:
  - `SymbolName`: `MethodName`
  - `SymbolType`: `method`
  - `EntityType`: `pas`
  - `EntityID`: persisted `pas_methods.id`
  - `LineNumber`: `LineNumber`
  - `Signature`: предпочтительно `Signature`, fallback `ClassName.MethodName`

### Особое внимание

- Методы могут иметь одинаковое имя в разных классах, это нормально: `query symbol` вернёт несколько строк.
- В `Signature` желательно сохранить class-qualified контекст, чтобы результат был различим.

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <MethodName> --type method`
- `codebase query symbol --name <PartOfMethod> --type method --like`

## Фаза 3: PAS units

### Что сделать

- В `parsePASFile` после `BatchInsertPASUnits` и разрешения `unitID` сформировать symbol для `PASUnit`.
- Использовать:
  - `SymbolName`: `UnitName`
  - `SymbolType`: `unit`
  - `EntityType`: `pas`
  - `EntityID`: persisted `pas_units.id`
  - `LineNumber`: `LineStart`
  - `Signature`: можно оставить пустым или сохранить краткий список `uses`, если это не усложнит изменение.

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <UnitName> --type unit`

## Фаза 4: DFM components

### Что сделать

- В `parseDFMFile` после `BatchInsertDFMComponents` и разрешения component IDs сформировать symbols для `DFMComponent`.
- Если уже есть helper `FindDFMComponentIDsByForm`, использовать его по каждой форме/`FormID`.
- Использовать:
  - `SymbolName`: `ComponentName`
  - `SymbolType`: `component`
  - `EntityType`: `dfm`
  - `EntityID`: persisted `dfm_components.id`
  - `LineNumber`: `LineStart`
  - `Signature`: `ComponentType`, при желании с `Caption`

### Особое внимание

- Компоненты привязаны к форме; нужно не потерять `FormID`.
- Не дублировать уже существующий специализированный `query form-component`, а только добавить unified symbol-доступ.

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <ComponentName> --type component`

## Фаза 5: SQL index definitions

### Что сделать

- В `parseSQLLikeFile` после `BatchInsertSQLIndexDefinitions` и разрешения persisted IDs сформировать symbols для `SQLIndexDefinition`.
- Уже есть lookup key `BuildSQLIndexDefinitionLookupKey`; при необходимости использовать существующий `FindSQLIndexDefinitionIDsByFile`.
- Использовать:
  - `SymbolName`: `IndexName`
  - `SymbolType`: `index`
  - `EntityType`: `sql`
  - `EntityID`: persisted `sql_index_definitions.id`
  - `LineNumber`: `LineNumber`
  - `Signature`: например `TableName(IndexFields)` или `IndexType`

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <IndexName> --type index`

## Фаза 6: SQL column definitions

### Что сделать

- В `parseSQLLikeFile` после `BatchInsertSQLColumnDefinitions` и разрешения persisted IDs сформировать symbols для `SQLColumnDefinition`.
- Если helper поиска IDs отсутствует, добавить минимальный store-helper по file/table/column/line/order или использовать устойчивый существующий ключ, если он уже есть.
- Использовать:
  - `SymbolName`: `ColumnName`
  - `SymbolType`: `column_definition`
  - `EntityType`: `sql`
  - `EntityID`: persisted `sql_column_definitions.id`
  - `LineNumber`: `LineNumber`
  - `Signature`: например `TableName.ColumnName DataType` или `DataType`

### Особое внимание

- Не добавлять все `SQLColumn` usages на этом этапе, только definitions, чтобы не раздуть `symbols`.

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <ColumnName> --type column_definition`
- `codebase query symbol --name <PartOfColumn> --type column_definition --like`

## Фаза 7: API business objects

### Что сделать

- В XML indexing flow после `BatchInsertAPIBusinessObjects` и `FindAPIBusinessObjectIDsByFile` сформировать symbols для `APIBusinessObject`.
- Использовать:
  - `SymbolName`: `BusinessObject`
  - `SymbolType`: `api_business_object`
  - `EntityType`: `api`
  - `EntityID`: persisted `api_business_objects.id`
  - `LineNumber`: line start / line number из модели, если есть
  - `Signature`: `ModuleName` или другой owner/module контекст из модели

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <BusinessObject> --type api_business_object`

## Фаза 8: JS constants

### Что сделать

- Проверить, сохраняются ли `JSConstant` сейчас в БД. Если не сохраняются, минимально добавить persistence только настолько, насколько нужно для корректного `entity_id`, либо согласовать вариант `entity_id = 0` недопустим из-за схемы `symbols.entity_id NOT NULL`.
- После сохранения/разрешения ID сформировать symbols для `JSConstant`.
- Использовать:
  - `SymbolName`: `Name`
  - `SymbolType`: `constant`
  - `EntityType`: `js`
  - `EntityID`: persisted ID новой/существующей сущности
  - `LineNumber`: если модель не содержит line number, потребуется аккуратно расширить parser/model или отложить до отдельного согласования
  - `Signature`: `Value`

### Особое внимание

- Это самая рискованная из приоритетных фаз, потому что `JSConstant` в модели сейчас содержит только `Name` и `Value`; нужно проверить наличие таблицы/сохранения/line number.
- Если persisted storage для JS constants отсутствует, фазу следует оформить как отдельное небольшое изменение схемы/модели после дополнительного подтверждения.

### Проверка

- Выполнить `go build ./...`.
- Остановиться и запросить акцепт.

### Ожидаемый CLI после переиндексации

- `codebase query symbol --name <ConstantName> --type constant`

## Итоговые замечания

- Изменения начнут проявляться в `query symbol` только после переиндексации соответствующих файлов.
- Для уже проиндексированной БД понадобится `codebase update --modified=false` или полный `codebase init`, если нужно гарантированно заполнить `symbols` по всем старым файлам.
- План намеренно не включает новые специализированные команды `query pas-class`, `query column`, `query include`: цель этапа — расширить именно unified `query symbol`.

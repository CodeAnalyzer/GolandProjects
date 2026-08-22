# DSArchitect XML Parsing

## Purpose

Парсинг DSArchitect XML-файлов: извлечение API-контрактов (service, event, callback_event, used_service), API-таблиц (api_table), индексов standalone API-таблиц (api_table_index), параметров BObject (api_param) и бизнес-объектов.

## Requirements

### Requirement: Извлечение API-контрактов

Система SHALL извлекать из DSArchitect XML-файлов контракты типов `service`, `event`, `callback_event`, `used_service` с их именем, kind, параметрами (scalar и tabular), return values и contexts.

#### Scenario: Service-контракт

- **GIVEN** XML-файл с service-контрактом `API_CCred_BindClassifier`, содержащим scalar-параметры и return values
- **WHEN** выполняется индексация файла
- **THEN** контракт `API_CCred_BindClassifier` сохранён в `api_contracts` с kind `service`
- **AND** параметры сохранены в `api_contract_params` и `api_contract_tables`
- **AND** return values сохранены в `api_contract_return_values`

#### Scenario: Event-контракт

- **GIVEN** XML-файл с event-контрактом `OnAfterLoan_MassInsert`
- **WHEN** выполняется индексация файла
- **THEN** контракт сохранён в `api_contracts` с kind `event`
- **AND** доступен через `query api-contract --name OnAfterLoan_MassInsert`

### Requirement: Извлечение API-таблиц и индексов

Система SHALL извлекать из standalone XML-таблиц (`<Table>`) поля таблицы и секцию `Indexses` с индексами и составом полей индекса.

#### Scenario: Standalone таблица с индексом

- **GIVEN** XML-файл с standalone таблицей `pAPI_CCred_Agreement`, содержащей поля и индекс `XIE0pAPI_CCred_Agreement`
- **WHEN** выполняется индексация файла
- **THEN** таблица сохранена в `api_business_object_tables`
- **AND** поля сохранены в `api_business_object_table_fields`
- **AND** индекс сохранён в `api_business_object_table_indexes` с полями в `api_business_object_table_index_fields`

### Requirement: Извлечение BObject-параметров

Система SHALL извлекать standalone параметры BObject (`<Param>`) с их именем, типом данных и направлением.

#### Scenario: Standalone параметр

- **GIVEN** XML-файл с standalone параметром `BranchID` типа `int`
- **WHEN** выполняется индексация файла
- **THEN** параметр `BranchID` сохранён в `api_business_object_params`
- **AND** доступен через `query api-param --name BranchID`

### Requirement: Извлечение бизнес-объектов

Система SHALL извлекать из DSArchitect XML-файлов бизнес-объекты (`<Object>`) с их именем и module_name (извлечённым из path — имени каталога перед `DSArchitectData`).

#### Scenario: Бизнес-объект

- **GIVEN** XML-файл в каталоге `Credit/DSArchitectData/` с объектом `CreditBObject`
- **WHEN** выполняется индексация файла
- **THEN** бизнес-объект `CreditBObject` сохранён в `api_business_objects` с `module_name = Credit`
- **AND** доступен через `query symbol --name CreditBObject --type api_business_object`

### Requirement: Пропуск неподдерживаемых форматов

Система SHALL пропускать XML-файлы с root element `<message>` или `<fasdocument>` без ошибки, так как они не являются DSArchitect-схемами. Парсер возвращает пустой результат.

#### Scenario: XML с root element message

- **GIVEN** XML-файл с root element `<message>...</message>`
- **WHEN** выполняется индексация файла
- **THEN** файл пропущен без ошибки
- **AND** возвращён пустой результат (без сущностей)

#### Scenario: XML с root element fasdocument

- **GIVEN** XML-файл с root element `<fasdocument>...</fasdocument>`
- **WHEN** выполняется индексация файла
- **THEN** файл пропущен без ошибки
- **AND** возвращён пустой результат (без сущностей)

### Requirement: Поддержка кодировки windows-1251

Система SHALL корректно декодировать XML-файлы с declared encoding `windows-1251` через `CharsetReader`.

#### Scenario: XML в кодировке windows-1251

- **GIVEN** XML-файл с `<?xml version="1.0" encoding="windows-1251"?>`
- **WHEN** выполняется индексация файла
- **THEN** файл корректно декодирован и проиндексирован

### Requirement: Внутренние p-таблицы из DSArchitect XML

Система SHALL для standalone XML-таблиц (`<Table>`) в путях, не являющихся API module path, создавать внутренние p-таблицы: временную SQL-таблицу (`IsTemporary = true`), определения колонок (`sql_column_definitions` с `definition_kind = create`), определения индексов (`sql_index_definitions`) и symbol типа `table`. Для путей, являющихся API module path, таблицы сохраняются как API tables (`api_business_object_tables`).

#### Scenario: Internal p-table из не-API пути

- **GIVEN** XML-файл standalone таблицы `pTempTable` в пути `SomeModule/BObject/Table/` (не API module path)
- **WHEN** выполняется индексация файла
- **THEN** таблица `pTempTable` сохранена в `sql_tables` с `IsTemporary = true`
- **AND** колонки сохранены в `sql_column_definitions` с `definition_kind = create`
- **AND** индексы сохранены в `sql_index_definitions` с полями в `sql_index_definition_fields`
- **AND** symbol типа `table` добавлен в `symbols`

#### Scenario: API table из API пути

- **GIVEN** XML-файл standalone таблицы `pAPI_CredS_InsertCCred` в пути API module path
- **WHEN** выполняется индексация файла
- **THEN** таблица сохранена в `api_business_object_tables` (не в `sql_tables`)
- **AND** поля сохранены в `api_business_object_table_fields`
- **AND** индексы сохранены в `api_business_object_table_indexes`

## Related code

- `internal/parser/dsxml/parser.go` — `Parse`, `classifyPath`, `isAPIModulePath`, извлечение контрактов, таблиц, параметров, BObjects, internal p-tables
- `internal/store/api_store.go` — persistence для API/DSArchitect сущностей
- `internal/indexer/indexer.go` — `parseXMLFile`, сохранение internal p-tables

## Notes

- XML-файлы читаются с авто-детекцией кодировки (UTF-8/UTF-16/windows-1251)
- `module_name` для BObjects и `owner_module` для контрактов извлекаются только из path
- `*.xml` может потребовать явного добавления в `include_patterns`

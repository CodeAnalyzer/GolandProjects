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

### Requirement: Извлечение контрактов InfoPath event-формата

Система SHALL извлекать API-контракты из DSArchitect XML-событий в InfoPath-формате: корневой элемент `<event>` (перед ним допустимы processing instructions `<?mso-infoPathSolution …?>` и `<?mso-application …?>`), теги в kebab-нотации. Маппинг на модель контракта: `name` → имя контракта; `descr-short` → короткое описание; `description` → полное описание; `implemented` (`true`/`false`) → булев признак реализованности; `architect-approval` со значением `OK` → признак одобрения архитектора; `input-tables`/`output-tables` → таблицы соответствующего направления, каждая `table` — имя, описание и поля `field` (`name`, `name-rus` → русское имя, `type`, `required`, `description`); `retvals`, `contexts`, `return-resultset`, `output-params`, `module-depend` — секции без сущностей (в `api_contracts` нет соответствующих колонок, значения не сохраняются). Признак `required` SHALL распознаваться в обеих встречающихся записях — строковой (`true`/`false`, у `table`) и числовой (`1`/`0`, у `field`). kind контракта определяется по пути файла существующей классификацией (`/Event/` → `event`). Элементы с whitespace-контентом (например, `<output-params>` из пробелов и переводов строк) SHALL трактоваться как пустые.

#### Scenario: Событие OnAfterScr_MassDel из фикстуры

- **GIVEN** XML-файл `API_RTG/DSArchitectData/BObject/Rating/Event/OnAfterScr_MassDel.xml` с корнем `<event>`, PI-инструкциями InfoPath и русской таблицей `pAPI_Scr_AfterDelScr` (фикстура `OnAfterScr_MassDel.xml`)
- **WHEN** выполняется индексация файла
- **THEN** контракт `OnAfterScr_MassDel` сохранён в `api_contracts` с kind `event` и русскими описаниями
- **AND** таблица `pAPI_Scr_AfterDelScr` сохранена в `api_contract_tables` с направлением input и описанием
- **AND** поля `ScoreID`, `Date` сохранены в `api_contract_table_fields` с типами и русскими именами из `name-rus`
- **AND** русские описания доступны в desc-search

#### Scenario: Смешанная запись required

- **GIVEN** `<event>`-файл, где у `table` признак `required` записан как `true`, а у вложенных `field` — как `1`
- **WHEN** выполняется индексация
- **THEN** оба признака распознаны: таблица обязательна, поля с `1` — обязательны, с `0`/`false` — нет

#### Scenario: Булевы флаги контракта

- **GIVEN** `<event>`-файл с `<implemented>true</implemented>` и `<module-depend>false</module-depend>`
- **WHEN** выполняется индексация
- **THEN** контракт сохранён с `implemented = true`
- **AND** значение `module-depend` не сохраняется (колонки в `api_contracts` нет, на контракт не влияет)

### Requirement: Recovery незакрытых элементов XML

Система SHALL при синтаксической ошибке вложенности вида «element closed by» (закрывающий тег встречен, когда поверх соответствующего ему открытого элемента в стеке остались незакрытые) восстанавливать разметку: перед закрывающим тегом автоматически вставляются закрывающие теги всех элементов, лежащих поверх совпадающего, остальной контент не меняется, и файл разбирается по исправленному контенту. Восстановленный файл SHALL индексироваться без ошибки (`errors_count` не растёт), а индексатор SHALL фиксировать предупреждение о восстановлении в логе. Recovery SHALL ограничиваться числом автозакрытий на файл: при превышении лимита, а также при ошибках другого рода (обрезанный файл, закрывающий тег без пары) автозакрытие не выполняется, исходная ошибка остаётся ошибкой файла.

#### Scenario: Дублированный Indexses при незакрытом Index

- **GIVEN** XML-файл standalone-таблицы `Table/pAPI_DFA_BfrMassDelPmnt.xml`, где первый блок `<Indexses><Index>` не закрыт, вместо закрытия вставлен дубликат блока `<Indexses>` с тем же индексом (фикстура `Bug-reports/pAPI_DFA_BfrMassDelPmnt.xml`)
- **WHEN** выполняется индексация файла
- **THEN** перед закрывающим `</Table>` автоматически вставлены `</Index></Indexses>`, файл разобран без ошибки
- **AND** таблица сохранена в `api_business_object_tables` с русским именем, поля и индекс `XPKpAPI_DFA_BfrMassDelPmnt` — в дочерних таблицах
- **AND** предупреждение о восстановлении записано в лог, `errors_count` не возрос

#### Scenario: Превышение лимита автозакрытий

- **GIVEN** XML-файл со стеком незакрытых элементов глубже лимита автозакрытий до ближайшего совпадающего закрывающего тега
- **WHEN** выполняется индексация файла
- **THEN** автозакрытие не выполняется, контент не модифицируется
- **AND** исходная синтаксическая ошибка остаётся ошибкой файла

#### Scenario: Закрывающий тег без пары

- **GIVEN** XML-файл с закрывающим тегом, для которого нет открытого элемента (orphan close)
- **WHEN** выполняется индексация файла
- **THEN** автозакрытие не помогает (пары для тега нет), ошибка парсинга остаётся ошибкой файла

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

Система SHALL пропускать XML-файлы с root element `<message>` или `<fasdocument>` без ошибки, так как они не являются DSArchitect-схемами. Парсер возвращает пустой результат. Кроме того, XML-файл, корневой элемент которого не соответствует ожиданию выбранной по пути ветки разбора — default-ветка принимает корни `Object`, `event`, `message`, `fasdocument`; ветка `/BObject/.../Table/` — корень `Table`; ветка `/BObject/.../Param/` — корень `Param`; любой прочий корень (в том числе `<Table>` вне `/Table/`-пути) — SHALL пропускаться без ошибки (graceful-skip): файл индексируется, но без DSArchitect-сущностей; результат парсера несёт признак skip, по которому индексер ведёт отдельный счётчик skip-файлов в сводке прогона (не в `errors_count`). Синтаксически битый XML остаётся ошибкой файла, за исключением ошибки вложенности «element closed by», к которой применяется recovery незакрытых элементов (отдельное требование).

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

#### Scenario: Неизвестный корень — graceful-skip

- **GIVEN** синтаксически валидный XML-файл с корневым элементом `<somethingNew>`, появившийся в дереве при дрейфе формата генератора
- **WHEN** выполняется индексация файла
- **THEN** файл не попадает в `errors_count` и не роняет прогон
- **AND** сущности из файла не извлекаются, счётчик skip-файлов XML увеличен

#### Scenario: Известный корень вне своей path-ветки — graceful-skip

- **GIVEN** валидный XML-файл с корнем `<Table>`, лежащий вне пути `/BObject/.../Table/` (попадает в default-ветку)
- **WHEN** выполняется индексация файла
- **THEN** файл пропущен без ошибки (несоответствие корня ожиданию ветки), счётчик skip-файлов XML увеличен

#### Scenario: Битый XML остаётся ошибкой

- **GIVEN** XML-файл с синтаксической ошибкой, не поддающейся автозакрытию (обрезанный файл с незакрытыми элементами до конца файла, например `API_GSOD_MassCreate.xml` в fa-stockmarket)
- **WHEN** выполняется индексация файла
- **THEN** ошибка парсинга зафиксирована в `errors_count` и логе ошибок индексатора

### Requirement: Поддержка кодировки windows-1251

Система SHALL корректно декодировать XML-файлы с declared encoding `windows-1251` через `CharsetReader`. Charset-маппинг декодера XML SHALL покрывать все кодировки, которые возвращает детекция XML-кодировки: `windows-1251` (`cp1251`/`windows1251`) — через CharsetReader (существующее поведение); `cp866` (`ibm866`), `utf-16`/`utf-16le`/`utf-16be`/`ucs-2` — pass-through (контент уже предекодирован детекцией в UTF-8, но декларация в тексте файла остаётся прежней, и декодер обращается к CharsetReader); пустая кодировка и `utf-8` — pass-through. Неподдерживаемая кодировка остаётся ошибкой файла.

#### Scenario: XML в кодировке windows-1251

- **GIVEN** XML-файл с `<?xml version="1.0" encoding="windows-1251"?>`
- **WHEN** выполняется индексация файла
- **THEN** файл корректно декодирован и проиндексирован

#### Scenario: XML предекодирован из UTF-16, декларация осталась UTF-16

- **GIVEN** XML-файл в UTF-16LE с декларацией `encoding="UTF-16"`, контент уже предекодирован детекцией в UTF-8
- **WHEN** xml-декодер встречает декларацию и обращается к CharsetReader
- **THEN** `utf-16` обслужен pass-through (повторное транскодирование не выполняется), файл разобран без ошибки `unsupported xml charset`

#### Scenario: XML с декларацией cp866

- **GIVEN** XML-файл в CP866 с декларацией `encoding="cp866"`, контент уже предекодирован в UTF-8
- **WHEN** xml-декодер обращается к CharsetReader
- **THEN** `cp866` обслужен pass-through (как и остальные предекодированные случаи), файл разобран без ошибки

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
- `internal/parser/dsxml/recover.go` — `recoverUnclosedElements`: автозакрытие незакрытых элементов при ошибке вложенности «element closed by»
- `internal/store/api_store.go` — persistence для API/DSArchitect сущностей
- `internal/indexer/indexer.go` — `parseXMLFile`, сохранение internal p-tables

## Notes

- XML-файлы читаются с авто-детекцией кодировки (UTF-8/UTF-16/windows-1251)
- `module_name` для BObjects и `owner_module` для контрактов извлекаются только из path
- `*.xml` может потребовать явного добавления в `include_patterns`

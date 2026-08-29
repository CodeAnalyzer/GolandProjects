# SQL Parsing

## Purpose

Парсинг SQL-файлов (.sql) и SQL-like файлов (.t01): извлечение процедур, таблиц, колонок, индексов, schema patches, query fragments и SQL statements с сохранением в БД.

## Requirements

### Requirement: Извлечение SQL-процедур

Система SHALL извлекать из SQL-файлов определения хранимых процедур (`CREATE PROCEDURE`, `CREATE PROC`) с их именем, телом, номерами строк и параметрами.

#### Scenario: Простая процедура

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc AS BEGIN ... END`
- **WHEN** выполняется индексация файла
- **THEN** процедура `MyProc` сохранена в `sql_procedures` с указанием файла и строк
- **AND** тело процедуры сохранено целиком

#### Scenario: Процедура с параметрами

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc @Param1 int, @Param2 varchar(50) AS ...`
- **WHEN** выполняется индексация файла
- **THEN** процедура сохранена с извлечёнными параметрами и их типами

### Requirement: Извлечение SQL-таблиц и колонок

Система SHALL извлекать из SQL-файлов определения таблиц (`CREATE TABLE`) с колонками, их типами и nullability. Парсер SHALL корректно обрабатывать блочные комментарии `/* ... */`: строка, открывающая блочный комментарий (содержащая `/*` без `*/`), SHALL полностью пропуститься — она не должна сопоставляться с `createTableRe`, `selectIntoRe` или любым другим паттерном. Флаг `inCreateTableDefinition` SHALL НЕ устанавливаться для таблиц, упоминаемых внутри блочных комментариев.

Парсер SHALL распознавать `ELSE` как SQL-ключевое слово в `isKeyword`, чтобы строки `else` внутри CASE-выражений не обрабатывались как определения колонок.

#### Scenario: Создание таблицы

- **GIVEN** SQL-файл с `CREATE TABLE tContract (ID int NOT NULL, Name varchar(100) NULL)`
- **WHEN** выполняется индексация файла
- **THEN** таблица `tContract` сохранена в `sql_tables`
- **AND** колонки `ID` (int, NOT NULL) и `Name` (varchar(100), NULL) сохранены в `sql_columns`

#### Scenario: CREATE TABLE внутри блочного комментария

- **GIVEN** SQL-файл со строкой `/* create table tConsRuleAccSync` (открытие блочного комментария), за которым следует закрывающий `*/`, а затем `SELECT ... INTO tConsRuleAccSync`
- **WHEN** выполняется индексация файла
- **THEN** таблица `tConsRuleAccSync` НЕ добавляется как `create_table` из закомментированного блока
- **AND** флаг `inCreateTableDefinition` НЕ остаётся активным после закомментированного блока
- **AND** колонки SELECT-проекции парсятся с `definition_kind = select_into`, а не `create_table`

#### Scenario: ELSE внутри CASE-выражения

- **GIVEN** SQL-файл с CASE-выражением, содержащим строку `else ""` как ветку CASE
- **WHEN** выполняется индексация файла
- **THEN** `else` НЕ извлекается как columnName (определение колонки)
- **AND** `isKeyword("else")` возвращает `true`

### Requirement: Schema patches

Система SHALL индексировать schema patches для обычных SQL-таблиц из Consumer patch-файлов: `ALTER TABLE ... ADD`, `M_ADD_FIELD`, `CREATE INDEX`, `M_CRT_INDEX`, с сохранением в `sql_column_definitions` и `sql_index_definitions`.

#### Scenario: ALTER TABLE ADD

- **GIVEN** SQL-файл с `ALTER TABLE tContract ADD NewColumn numeric(15,2) NULL`
- **WHEN** выполняется индексация файла
- **THEN** колонка `NewColumn` сохранена в `sql_column_definitions` с `definition_kind = alter_add`

#### Scenario: M_ADD_FIELD макрос

- **GIVEN** SQL-файл с `M_ADD_FIELD('tContract', 'Status int NULL')`
- **WHEN** выполняется индексация файла
- **THEN** колонка `Status` сохранена в `sql_column_definitions` с `definition_kind = macro_add_field`

#### Scenario: CREATE INDEX

- **GIVEN** SQL-файл с `CREATE INDEX IX_Contract ON tContract(Name)`
- **WHEN** выполняется индексация файла
- **THEN** индекс `IX_Contract` сохранён в `sql_index_definitions` с полями в `sql_index_definition_fields`

### Requirement: Query fragments

Система SHALL извлекать SQL-фрагменты (query fragments) из тел процедур и скриптов, включая отдельные SQL statements, и сохранять их в `query_fragments` с полнотекстовым поиском через GIN-индекс `pg_trgm`.

#### Scenario: Поиск фрагмента по тексту

- **GIVEN** проиндексированная процедура, содержащая `SELECT * FROM tContract WHERE ID = @ID`
- **WHEN** выполняется `query sql-fragment --text "from tContract"`
- **THEN** фрагмент найден по триграммному поиску
- **AND** результат содержит имя процедуры, файл и строку

### Requirement: Извлечение вызовов процедур

Система SHALL извлекать вызовы хранимых процедур (`EXEC`, `EXECUTE`) из тел процедур и query fragments для построения графа вызовов.

#### Scenario: Вызов процедуры

- **GIVEN** процедура `ProcA`, содержащая `EXEC ProcB @Param = 1`
- **WHEN** выполняется индексация
- **THEN** в графе связей создана relation `calls_procedure` от `ProcA` к `ProcB`

### Requirement: SELECT INTO — вывод типов колонок

Система SHALL для определений колонок с `definition_kind = select_into` разрешать тип данных через lookup из БД: сопоставлять проекцию SELECT с выходными колонками, разрешать алиасы таблиц, запрашивать тип исходной колонки через `FindLatestSQLColumnDefinitionType` и заменять `DSUNKNOWN` найденным типом.

#### Scenario: SELECT INTO с DSUNKNOWN

- **GIVEN** SQL-файл с `SELECT Col1, Col2 INTO #Temp FROM tContract` и определениями колонок `#Temp.Col1` с типом `DSUNKNOWN`
- **WHEN** выполняется индексация и `enrichSelectIntoDataTypes`
- **THEN** тип `DSUNKNOWN` заменён на фактический тип колонки `Col1` из `tContract` (например `int`)

#### Scenario: SELECT INTO с алиасом таблицы

- **GIVEN** SQL-файл с `SELECT t.Col1 INTO #Temp FROM tContract t` и определением колонки с `DSUNKNOWN`
- **WHEN** выполняется `enrichSelectIntoDataTypes`
- **THEN** алиас `t` разрешён к `tContract` и тип колонки `Col1` получен из `tContract`

#### Scenario: SELECT INTO с явным типом — без замены

- **GIVEN** SQL-файл с `SELECT CAST(Col1 AS int) INTO #Temp FROM tContract` и определением колонки с типом `int`
- **WHEN** выполняется `enrichSelectIntoDataTypes`
- **THEN** тип не заменяется (только `DSUNKNOWN` подлежит разрешению)

### Requirement: SELECT INTO — извлечение имён колонок

Система SHALL извлекать имена колонок из проекции `SELECT ... INTO` путём разделения проекции по запятым верхнего уровня. Функция `splitSQLByTopLevelComma` SHALL удалять `--` комментарии из текста перед разделением по запятым, чтобы запятые внутри комментариев не создавали лишние сегменты.

#### Scenario: SELECT INTO с запятой в -- комментарии

- **GIVEN** SQL-файл с `SELECT tal.NodeID, --- Идентификатор Узла или Идентификатор счёта, общего для ФО` (запятая внутри `--` комментария)
- **WHEN** выполняется `splitSQLByTopLevelComma` для проекции
- **THEN** `--` комментарий удалён перед разделением
- **AND** запятая внутри комментария НЕ создаёт лишний сегмент
- **AND** `tal.NodeID` извлечён как одно имя колонки

#### Scenario: SELECT INTO с многострочным CASE и алиасами

- **GIVEN** SQL-файл с `SELECT ... CASE rn.ResourceType WHEN 0 THEN ... WHEN 1 THEN ... ELSE "" END as NodeNumber, tal.FundID as CurrencyID ... INTO tConsRuleAccSync`
- **WHEN** выполняется индексация файла
- **THEN** `NodeNumber` извлечён как имя колонки (из алиаса `as NodeNumber`)
- **AND** `CurrencyID` извлечён как имя колонки (из алиаса `as CurrencyID`)
- **AND** фрагменты CASE-выражения (`trim(rn.Brief)),`, `else`, `replicate(...)`) НЕ извлекаются как имена колонок

### Requirement: Извлечение return-кодов из SQL

Система SHALL извлекать из SQL-файлов return-коды через `internal/parser/retcode`: вызовы `ReturnCode_Insert`, макросы `_ADD_RETCODE_*` и `__Notification_Save`, и сохранять их в `ds_return_codes` для последующего разрешения `LOC_RETCODE_*` констант в постобработке (см. `relations-postprocessing`).

#### Scenario: ReturnCode_Insert

- **GIVEN** SQL-файл с `ReturnCode_Insert(1001, 'MyProc', 'Not found', 5)`
- **WHEN** выполняется индексация файла
- **THEN** запись сохранена в `ds_return_codes` с `ret_code = 1001`, `proc_name = 'MyProc'`, сообщением и `module_id`

#### Scenario: Макрос _ADD_RETCODE_*

- **GIVEN** SQL-файл с `_ADD_RETCODE_ERROR('MyProc', 'Bad input')`
- **WHEN** выполняется индексация
- **THEN** return-code извлечён через макрос и сохранён в `ds_return_codes`

### Requirement: SQL `#define` и include-директивы внутри SQL

Система SHALL извлекать SQL `#define`-макросы из SQL-файлов и сохранять их в `h_files_defines` через `BatchInsertHDefines` (переиспользование той же таблицы, что и для H-файлов). Include-директивы внутри SQL-файлов (`#include "..."` или аналогичные) обрабатываются и сохраняются через `saveIncludeDirective` в `include_directives` транзакционно вместе с другими сущностями файла (доработка «fix-include-directive-tx»).

#### Scenario: SQL #define

- **GIVEN** SQL-файл с `#define MAX_ROWS 1000`
- **WHEN** выполняется индексация
- **THEN** define `MAX_ROWS` сохранён в `h_files_defines` со значением `1000`

#### Scenario: Include-директива в SQL — транзакционное сохранение

- **GIVEN** SQL-файл с include-директивой
- **WHEN** индексация файла завершается (или откатывается из-за ошибки парсинга)
- **THEN** include-директива сохраняется в `include_directives` только если файл сохранён целиком (атомарность с остальными сущностями файла)

## Related code

- `internal/parser/sql/sql_parser.go` — `Parse`, извлечение процедур, таблиц, колонок, query fragments
- `internal/parser/retcode/retcode.go` — `ReturnCode_Insert`, `_ADD_RETCODE_*`, `__Notification_Save`, `HasReturnCodes`, `Parse` (retcode-прескрининг)
- `internal/indexer/indexer_sql_pas.go` — `parseSQLFile`, `parseSQLLikeFile`, `enrichSelectIntoDataTypes`, batch insert, `BatchInsertHDefines`, `saveIncludeDirective`, retcode extraction
- `internal/store/db_insert_sql.go` — `BatchInsertSQLProcedures`, `BatchInsertSQLTables`, `BatchInsertSQLColumns`
- `internal/store/db_lookup_sql.go` — lookup для SQL entities, `FindLatestSQLColumnDefinitionType`
- `internal/indexer/indexer_postprocess_fragments.go` — извлечение query fragments

## Notes

- SQL-файлы читаются в кодировке CP866
- Schema patches индексируются отдельно от CREATE TABLE — в `sql_column_definitions` с полем `definition_kind`
- Query fragments используются для полнотекстового поиска и построения графа связей
- SQL `#define` сохраняются в `h_files_defines` (общая таблица с H-файлами) — это позволяет использовать `query symbol --type define` единообразно
- Include-директивы внутри SQL сохраняются транзакционно с другими сущностями файла (атомарность через `WithBatchTxCtx`, доработка «fix-include-directive-tx»)

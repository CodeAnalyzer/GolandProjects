# SQL Parsing

## Purpose

Парсинг SQL-файлов (.sql) и SQL-like файлов (.t01): извлечение процедур, таблиц, колонок, индексов, schema patches, query fragments и SQL statements с сохранением в БД.

## Requirements

### Requirement: Извлечение SQL-процедур

Система SHALL извлекать из SQL-файлов определения хранимых процедур (`CREATE PROCEDURE`, `CREATE PROC`) с их именем, телом, номерами строк и параметрами. Имя процедуры MAY быть квалифицировано schema-префиксом (`create procedure dbo.MyProc`): именем процедуры SHALL считаться последний сегмент dotted-имени, сам префикс SHALL NOT попадать в индекс как имя процедуры.

#### Scenario: Простая процедура

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc AS BEGIN ... END`
- **WHEN** выполняется индексация файла
- **THEN** процедура `MyProc` сохранена в `sql_procedures` с указанием файла и строк
- **AND** тело процедуры сохранено целиком

#### Scenario: Процедура с параметрами

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc @Param1 int, @Param2 varchar(50) AS ...`
- **WHEN** выполняется индексация файла
- **THEN** процедура сохранена с извлечёнными параметрами и их типами

#### Scenario: Процедура с schema-префиксом

- **GIVEN** SQL-файл с `CREATE PROCEDURE dbo.MyProc @Param1 int AS ...`
- **WHEN** выполняется индексация файла
- **THEN** процедура `MyProc` сохранена в `sql_procedures`
- **AND** процедуры с именем `dbo` нет

### Requirement: Извлечение SQL-таблиц и колонок

Система SHALL извлекать из SQL-файлов определения таблиц (`CREATE TABLE`) с колонками, их типами и nullability. Имя таблицы в `CREATE TABLE` MAY быть квалифицировано schema/db-префиксом (`dbo.tReplTables`, `MIDDLE..dcSubcontoConnect`) и модификатором создания (`UNLOGGED`, `TEMP`, `TEMPORARY`, `GLOBAL TEMPORARY`). Парсер SHALL распознавать такие формы: именем таблицы SHALL считаться последний непустой сегмент dotted-имени (без учета регистра), сам префикс SHALL NOT попадать в индекс как имя таблицы; модификаторы SHALL не мешать распознаванию. То же правило последнего сегмента SHALL применяться при извлечении ссылок на таблицы после DML-ключевых слов (`FROM`, `JOIN`, `INTO`, `UPDATE`, `INSERT INTO`, `DELETE`, `SELECT … INTO`): `left join dbo.tConfigParam` даёт ссылку на `tConfigParam`; а также к таблице UPDATE-SET-аккумулятора: `update dbo.tX set a = 1` привязывает SET-колонки к `tX`. Ссылки, квалифицированные системной схемой (`sys.*`, `information_schema.*` — каталоги SQL Server/PostgreSQL, не таблицы продукта), SHALL NOT извлекаться как ссылки на таблицы. Строка-продолжение списка FROM, начинающаяся с пары `x.y`, где `x` — зарегистрированный алиас таблицы, SHALL NOT давать ссылку на таблицу (продолжение условия ON/WHERE); если `x` не алиас, пара трактуется как `schema.table` и даёт ссылку на `y`.

Парсер SHALL корректно обрабатывать блочные комментарии `/* ... */`: строка, открывающая блочный комментарий (содержащая `/*` без `*/`), SHALL полностью пропуститься — она не должна сопоставляться с `createTableRe`, `selectIntoRe` или любым другим паттерном. Флаг `inCreateTableDefinition` SHALL НЕ устанавливаться для таблиц, упоминаемых внутри блочных комментариев.

Парсер SHALL распознавать `ELSE` как SQL-ключевое слово в `isKeyword`, чтобы строки `else` внутри CASE-выражений не обрабатывались как определения колонок.

При разборе `UPDATE`-статементов парсер SHALL НЕ включать режим продолжения списка таблиц после строки, начинающейся с `UPDATE`, и SHALL завершать этот режим на строках с префиксами `set ` и `values `. Идентификаторы левых частей `SET`-присваиваний SHALL НЕ попадать в `sql_tables` как имена таблиц.

Парсер SHALL игнорировать как кандидатов в имена таблиц идентификаторы хинт-макросов Diasoft с префиксом `M_` и `#M_` (например `M_FORCEORDER`, `M_KEEPPLAN`, `M_ISOLAT`, `M_NOLOCK_INDEX`, `M_UPDLOCK_INDEX`) — во всех контекстах извлечения таблиц (select, insert, update, delete).

Парсер SHALL НЕ сохранять в `sql_tables` идентификаторы макро-плейсхолдеров подстановки имени таблицы в шаблонах DDL — имена, начинающиеся с `##` (например `##M_TABNAME##`, `##_TABLENAME_##`, `##OUT_COMMIS_TABLE`).

Парсер SHALL извлекать колонки из `SET`-присваиваний `UPDATE`-статемента — в том числе когда `UPDATE`, `SET` и присваивания расположены на разных строках — и сохранять их в `sql_columns` с привязкой к таблице `UPDATE`-статемента.

#### Scenario: Создание таблицы

- **GIVEN** SQL-файл с `CREATE TABLE tContract (ID int NOT NULL, Name varchar(100) NULL)`
- **WHEN** выполняется индексация файла
- **THEN** таблица `tContract` сохранена в `sql_tables`
- **AND** колонки `ID` (int, NOT NULL) и `Name` (varchar(100), NULL) сохранены в `sql_columns`

#### Scenario: CREATE TABLE с schema-префиксом

- **GIVEN** SQL-файл со строкой `create table dbo.tReplTables (Tbl varchar(30) NOT NULL)`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена таблица `tReplTables` с context=`create`
- **AND** имя `dbo` НЕ сохранено как таблица

#### Scenario: CREATE TABLE с формой db..table

- **GIVEN** SQL-файл со строкой `create table MIDDLE..dcSubcontoConnect (ID DSIDENTIFIER)`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена таблица `dcSubcontoConnect` с context=`create`
- **AND** имя `MIDDLE` НЕ сохранено как таблица

#### Scenario: CREATE UNLOGGED TABLE

- **GIVEN** SQL-файл со строкой `CREATE UNLOGGED TABLE pAPI_ExpProperty_Type (spid DSSPID)`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена таблица `pAPI_ExpProperty_Type` с context=`create`

#### Scenario: Колонки таблицы с префиксом привязаны к реальному имени

- **GIVEN** SQL-файл с `create table dbo.pSeed (SPID DSIDENTIFIER, Type char(25))` и продолжением списка колонок на следующих строках
- **WHEN** выполняется индексация файла
- **THEN** определения колонок сохранены в `sql_column_definitions` с `table_name = pSeed`
- **AND** записей с `table_name = dbo` нет

#### Scenario: Ссылка FROM/JOIN с schema-префиксом

- **GIVEN** SQL-файл со стейтментом `left join dbo.tConfigParam c on ...`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена ссылка на `tConfigParam` (context select)
- **AND** имя `dbo` НЕ сохранено как таблица

#### Scenario: Ссылка на системный каталог sys.* не извлекается

- **GIVEN** SQL-файл со стейтментом `from sys.indexes ind inner join sys.tables t ...`
- **WHEN** выполняется индексация файла
- **THEN** ссылок на `indexes` и `tables` в `sql_tables` нет (системные каталоги не индексируются)
- **AND** ссылка на таблицу продукта с префиксом `dbo.` из того же стейтмента извлекается

#### Scenario: Продолжение условия alias.column не является таблицей

- **GIVEN** SQL-файл с многострочным UPDATE-стейтментом `... inner join pContractCredit c ...` и строкой-продолжением условия `c.InterfaceObjectID not in (…)`
- **WHEN** выполняется индексация файла
- **THEN** `InterfaceObjectID` НЕ сохранена в `sql_tables` (квалификатор `c` — зарегистрированный алиас)
- **AND** строка-продолжение `dbo.tConfigParam c,` списка FROM даёт ссылку на `tConfigParam`

#### Scenario: UPDATE с schema-префиксом — SET-колонки на реальном имени

- **GIVEN** SQL-файл со стейтментом `update dbo.tX set a = 1, b = 2`
- **WHEN** выполняется индексация файла
- **THEN** SET-колонки `a` и `b` сохранены в `sql_columns` с привязкой к таблице `tX`
- **AND** записей с `table_name = dbo` нет

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

#### Scenario: Многострочный UPDATE SET — колонки не становятся таблицами

- **GIVEN** SQL-файл с многострочным стейтментом: строка `update pCreditDocument`, далее строки `set OperationType = f.Condition,` и `TemplateSysName = f.UserTag`, далее `from pCreditDocument inner join pAPI_FO_Template ...` и `where ...`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранены `pCreditDocument` (context=update) и `pAPI_FO_Template` (из `JOIN`)
- **AND** `TemplateSysName` и `OperationType` НЕ сохранены в `sql_tables`

#### Scenario: UPDATE и SET в одной строке

- **GIVEN** SQL-файл со строкой `update t set a = 1,` и следующей строкой `b = 2`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена только таблица `t`
- **AND** идентификаторы `a` и `b` НЕ сохранены в `sql_tables`

#### Scenario: Хинт-макрос после списка JOIN

- **GIVEN** SQL-файл со стейтментом `UPDATE t ... INNER JOIN x ...`, где между последним `JOIN` и концом стейтмента нет `WHERE`, за которым следует отдельная строка `M_FORCEORDER` (или `#M_FORCEORDER`)
- **WHEN** выполняется индексация файла
- **THEN** идентификатор хинт-макроса НЕ сохранён в `sql_tables`

#### Scenario: Хинт-макрос в SELECT

- **GIVEN** SQL-файл со стейтментом `SELECT ... FROM t ...`, за которым следует отдельная строка `M_ISOLAT(2)` (или `#M_ISOLAT(...)`)
- **WHEN** выполняется индексация файла
- **THEN** идентификатор `M_ISOLAT` НЕ сохранён в `sql_tables`
- **AND** таблицы из `FROM`/`JOIN` сохранены как прежде

#### Scenario: Макро-плейсхолдер в шаблоне DDL

- **GIVEN** SQL-файл со строкой `create table ##M_TABNAME## (ID int NULL)` (имя таблицы — плейсхолдер макроподстановки)
- **WHEN** выполняется индексация файла
- **THEN** `##M_TABNAME##` НЕ сохранена в `sql_tables`
- **AND** колонки такого шаблона (`ID`) не привязаны к фиктивной таблице

#### Scenario: Локальная временная таблица сохраняется

- **GIVEN** SQL-файл с `SELECT Col1, Col2 INTO #Temp FROM tContract`
- **WHEN** выполняется индексация файла
- **THEN** таблица `#Temp` сохранена в `sql_tables` с `is_temporary = true` (одно `#` — временная таблица, фильтр `##`-плейсхолдеров её не задевает)

#### Scenario: Колонки многострочного SET сохраняются в sql_columns

- **GIVEN** SQL-файл с многострочным стейтментом `UPDATE t SET a = 1,` и `b = 2 FROM ...` (присваивания на разных строках)
- **WHEN** выполняется индексация файла
- **THEN** колонки `a` и `b` сохранены в `sql_columns` с привязкой к таблице `t`

#### Scenario: Регресс — список FROM через запятую

- **GIVEN** SQL-файл со строкой `FROM t1,` и продолжением `t2,` / `t3` на следующих строках
- **WHEN** выполняется индексация файла
- **THEN** все три таблицы `t1`, `t2`, `t3` извлечены в `sql_tables`

#### Scenario: Регресс — INSERT INTO с VALUES на следующей строке

- **GIVEN** SQL-файл со строкой `INSERT INTO t (a, b)` и следующей строкой `VALUES (@a, @b)`
- **WHEN** выполняется индексация файла
- **THEN** в `sql_tables` сохранена только таблица `t`
- **AND** `VALUES` и элементы списка значений НЕ сохранены как таблицы

### Requirement: Schema patches

Система SHALL индексировать schema patches для обычных SQL-таблиц из Consumer patch-файлов: `ALTER TABLE ... ADD`, `M_ADD_FIELD`, `CREATE INDEX`, `M_CRT_INDEX`, с сохранением в `sql_column_definitions` и `sql_index_definitions`. Имя таблицы в `ALTER TABLE <таблица>` и `CREATE INDEX ... ON <таблица>` MAY быть квалифицировано schema/db-префиксом (`alter table dbo.tContract`, `create index ... on MIDDLE..dcSubcontoConnect`): именем SHALL считаться последний сегмент dotted-имени. Определение в `ALTER TABLE ... ADD` SHALL записываться как колонка только если начинается с имени колонки; constraint-определения — определение, начинающееся с ключевого слова `CONSTRAINT`, `PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE`, `CHECK` или `DEFAULT` (без учёта регистра) — SHALL NOT записываться в `sql_column_definitions` (это не колонка). Ведущее слово `COLUMN` (PostgreSQL-форма `ADD COLUMN x int`) SHALL срезаться перед разбором: определение трактуется как колонка.

#### Scenario: ALTER TABLE ADD

- **GIVEN** SQL-файл с `ALTER TABLE tContract ADD NewColumn numeric(15,2) NULL`
- **WHEN** выполняется индексация файла
- **THEN** колонка `NewColumn` сохранена в `sql_column_definitions` с `definition_kind = alter_add`

#### Scenario: ALTER TABLE с schema-префиксом

- **GIVEN** SQL-файл с `ALTER TABLE dbo.tContract ADD NewColumn numeric(15,2) NULL`
- **WHEN** выполняется индексация файла
- **THEN** колонка `NewColumn` сохранена в `sql_column_definitions` с `table_name = tContract` и `definition_kind = alter_add`

#### Scenario: ALTER TABLE ADD CONSTRAINT не записывается как колонка

- **GIVEN** SQL-файл с `ALTER TABLE pAPI_ExpProperty_Type ADD CONSTRAINT XPK UNIQUE (SPID, ExpPropertyType) DEFERRABLE`
- **WHEN** выполняется индексация файла
- **THEN** запись с `column_name = CONSTRAINT` НЕ сохранена в `sql_column_definitions`

#### Scenario: ALTER TABLE ADD PRIMARY KEY без слова CONSTRAINT не записывается

- **GIVEN** SQL-файл с `ALTER TABLE tContract ADD PRIMARY KEY (ContractID)`
- **WHEN** выполняется индексация файла
- **THEN** запись с `column_name = PRIMARY` НЕ сохранена в `sql_column_definitions`

#### Scenario: ALTER TABLE ADD COLUMN — колонка сохраняется

- **GIVEN** SQL-файл с `ALTER TABLE tContract ADD COLUMN NewCol int NULL`
- **WHEN** выполняется индексация файла
- **THEN** колонка `NewCol` сохранена в `sql_column_definitions` с `definition_kind = alter_add`
- **AND** записи с `column_name = COLUMN` нет

#### Scenario: CREATE INDEX на таблицу с префиксом

- **GIVEN** SQL-файл с `CREATE INDEX IX_Conn ON MIDDLE..dcSubcontoConnect(F1)`
- **WHEN** выполняется индексация файла
- **THEN** индекс `IX_Conn` сохранён в `sql_index_definitions` с `table_name = dcSubcontoConnect`

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

Система SHALL извлекать SQL `#define`-макросы из SQL-файлов и сохранять их в `h_files_defines` через `BatchInsertHDefines` (переиспользование той же таблицы, что и для H-файлов). Include-директивы внутри SQL-файлов (`#include "..."` или аналогичные) обрабатываются и сохраняются через `saveIncludeDirective` в `include_directives` транзакционно вместе с другими сущностями файла (доработка «fix-include-directive-tx»). Define-ветки парсера SHALL потреблять continuation-строки многострочных макросов целиком: строки, следующие за `#define` и завершающиеся `\`, не попадают в основной цикл парсинга и не порождают процедур, таблиц, фрагментов и других сущностей.

#### Scenario: SQL #define

- **GIVEN** SQL-файл с `#define MAX_ROWS 1000`
- **WHEN** выполняется индексация
- **THEN** define `MAX_ROWS` сохранён в `h_files_defines` со значением `1000`

#### Scenario: Include-директива в SQL — транзакционное сохранение

- **GIVEN** SQL-файл с include-директивой
- **WHEN** индексация файла завершается (или откатывается из-за ошибки парсинга)
- **THEN** include-директива сохраняется в `include_directives` только если файл сохранён целиком (атомарность с остальными сущностями файла)

#### Scenario: Многострочный #define потребляется целиком

- **GIVEN** SQL-файл с многострочным макросом `#define DIAG_PROCNAME \`, за которым следуют continuation-строки, оканчивающиеся на `\`, и тело содержит маркер `DCL_PROC_BEGIN(`
- **WHEN** файл парсится SQL-парсером
- **THEN** continuation-строки не попадают в основной цикл парсинга
- **AND** из тела макроса не извлекаются процедуры и query-фрагменты

#### Scenario: Однострочный #define не затрагивается

- **GIVEN** SQL-файл с однострочными `#define` (без `\` в конце)
- **WHEN** файл парсится SQL-парсером
- **THEN** поведение не меняется: define извлекается, последующие строки парсятся как обычно

### Requirement: Извлечение header-описаний SQL-процедур

SQL-парсер SHALL извлекать описание процедуры: первый блок-комментарий
`/* ... */`, встреченный в окне между строкой декларации процедуры и первой
кодовой строкой тела. Окно открывается на декларации любого из трёх стилей —
`DCL_PROC_BEGIN(Name)`, `create proc|procedure Name`, `API_CREATE_PROC(Name)` —
и не закрывается параметрами сигнатуры и ключевым словом `as`. Извлечённый
текст SHALL очищаться: удаляется первая строка, если она является именем файла
(`Name.sql`/`Name.h`), удаляются декоративные линии из символов `-` и `=`;
текст обрезается до 8 КБ с конца. Если блок-комментарий в окне отсутствует,
описание SHALL быть пустой строкой. Комментарии, начинающиеся с `--`, описанием
не считаются. Окно принадлежит только своей процедуре: комментарий-разделитель
между процедурами и хвост незарегистрированной декларации в чужое описание
не утекают.

#### Scenario: Типовой header после as

- **GIVEN** SQL-файл с процедурой `DCL_PROC_BEGIN(P)`, параметрами, `as`
  и блок-комментарием с описанием до тела
- **WHEN** файл парсится
- **THEN** процедура `P` имеет непустое описание с текстом комментария

#### Scenario: API-стиль без as

- **GIVEN** SQL-файл с `API_CREATE_PROC(API_X)`, за которым следует
  блок-комментарий, затем `__BEGIN_PROCEDURE__(API_X)`
- **WHEN** файл парсится
- **THEN** процедура `API_X` имеет описание из блок-комментария

#### Scenario: Очистка имени файла и декоративных линий

- **GIVEN** блок-комментарий, первая строка которого `OldSource.sql`,
  ограниченный линиями из символов `-`
- **WHEN** описание извлекается
- **THEN** строка `OldSource.sql` и декоративные линии в описании отсутствуют

#### Scenario: Описание отсутствует

- **GIVEN** SQL-файл с процедурой без блок-комментария между декларацией и телом
- **WHEN** файл парсится
- **THEN** описание процедуры равно пустой строке

#### Scenario: Обрезка по лимиту

- **GIVEN** блок-комментарий длиннее 8 КБ
- **WHEN** описание извлекается
- **THEN** длина описания не превышает 8 КБ

#### Scenario: Межпроцедурная утечка

- **GIVEN** скрипт с двумя процедурами, между которыми комментарий-разделитель
- **WHEN** файл парсится
- **THEN** разделитель не становится описанием второй процедуры

### Requirement: Извлечение реестра таблиц продукта

Система SHALL извлекать из SQL-файлов регистрации таблиц продукта — вызовы макроса `__REG_TABLE_NAME__(<имя таблицы>)` (имя MAY быть заключено в квадратные скобки: `__REG_TABLE_NAME__([tPoolAccountLimitTable])`; реестр `DB/ProductTable.sql`: перечень таблиц, принадлежащих продукту, под маркером `__MASS_DELETE_TABLE_NAME__`) — и сохранять их в таблицу индекса `reg_table_name` с привязкой к файлу и строке вызова. Маркер `__MASS_DELETE_TABLE_NAME__` сам по себе SHALL NOT порождать записей. При переиндексации файла записи реестра этого файла SHALL обновляться атомарно: после индексации в `reg_table_name` присутствуют в точности регистрации текущего содержимого файла, регистрации удалённых строк исчезают.

#### Scenario: Реестр ProductTable.sql

- **GIVEN** SQL-файл `ProductTable.sql` с блоком `__MASS_DELETE_TABLE_NAME__` и строками `__REG_TABLE_NAME__(hDocument)` и `__REG_TABLE_NAME__(tKey)`
- **WHEN** выполняется индексация файла
- **THEN** в `reg_table_name` сохранены имена `hDocument` и `tKey` с file_id и line_number соответствующих строк
- **AND** запись для `__MASS_DELETE_TABLE_NAME__` отсутствует

#### Scenario: Скобочное имя регистрации

- **GIVEN** SQL-файл `ProductTable.sql` со строкой `__REG_TABLE_NAME__([tPoolAccountLimitTable])`
- **WHEN** выполняется индексация файла
- **THEN** в `reg_table_name` сохранено имя `tPoolAccountLimitTable` (без квадратных скобок)

#### Scenario: Переиндексация изменённого реестра

- **GIVEN** проиндексированный `ProductTable.sql` с регистрациями `hDocument` и `tKey`, файл изменён: `tKey` удалён, добавлен `tNovosibNumbers`
- **WHEN** файл переиндексируется
- **THEN** в `reg_table_name` для этого файла присутствуют `hDocument` и `tNovosibNumbers`
- **AND** регистрация `tKey` для этого файла отсутствует (дублей от старого содержимого нет)

## Related code

- `internal/parser/sql/sql_parser.go` — `Parse`, извлечение процедур, таблиц, колонок, query fragments
- `internal/parser/retcode/retcode.go` — `ReturnCode_Insert`, `_ADD_RETCODE_*`, `__Notification_Save`, `HasReturnCodes`, `Parse` (retcode-прескрининг)
- `internal/indexer/indexer_sql_pas.go` — `parseSQLFile`, `parseSQLLikeFile`, `enrichSelectIntoDataTypes`, batch insert, `BatchInsertHDefines`, `saveIncludeDirective`, retcode extraction
- `internal/store/db_insert_sql.go` — `BatchInsertSQLProcedures`, `BatchInsertSQLTables`, `BatchInsertSQLColumns`, `BatchInsertRegTables`
- `internal/store/db_lookup_sql.go` — lookup для SQL entities, `FindLatestSQLColumnDefinitionType`
- `internal/indexer/indexer_postprocess_fragments.go` — извлечение query fragments

## Notes

- SQL-файлы читаются в кодировке CP866
- Schema patches индексируются отдельно от CREATE TABLE — в `sql_column_definitions` с полем `definition_kind`
- Query fragments используются для полнотекстового поиска и построения графа связей
- SQL `#define` сохраняются в `h_files_defines` (общая таблица с H-файлами) — это позволяет использовать `query symbol --type define` единообразно
- Include-директивы внутри SQL сохраняются транзакционно с другими сущностями файла (атомарность через `WithBatchTxCtx`, доработка «fix-include-directive-tx»)
- Реестр таблиц продукта (`__REG_TABLE_NAME__` из `DB/ProductTable.sql`) хранится в `reg_table_name` и служит свидетельством существования таблицы в review (см. `review/deploy-stoppers`, «Существование таблицы в метаданных»); регистрации не попадают в контексты DML и `tables_referenced` фрагментов

## MODIFIED Requirements

### Requirement: Извлечение SQL-таблиц и колонок

Система SHALL извлекать из SQL-файлов определения таблиц (`CREATE TABLE`) с колонками, их типами и nullability. Парсер SHALL корректно обрабатывать блочные комментарии `/* ... */`: строка, открывающая блочный комментарий (содержащая `/*` без `*/`), SHALL полностью пропуститься — она не должна сопоставляться с `createTableRe`, `selectIntoRe` или любым другим паттерном. Флаг `inCreateTableDefinition` SHALL НЕ устанавливаться для таблиц, упоминаемых внутри блочных комментариев.

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

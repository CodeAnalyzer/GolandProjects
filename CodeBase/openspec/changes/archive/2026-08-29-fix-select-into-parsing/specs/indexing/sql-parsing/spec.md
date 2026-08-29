## ADDED Requirements

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

## MODIFIED Requirements

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

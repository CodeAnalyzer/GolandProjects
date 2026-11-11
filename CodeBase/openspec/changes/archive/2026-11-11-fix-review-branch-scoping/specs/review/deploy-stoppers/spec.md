## MODIFIED Requirements

### Requirement: Вызов несуществующих процедур

Система SHALL обнаруживать `EXEC`-вызовы процедур, отсутствующих в индексе
`sql_procedures` в разрешённой области видимости соответствующего продукта, и
формировать finding `execNotExistsProc`. Область поиска MUST определяться по
`review/review-scope`: для продукта проверяемого файла — область файла (ветка
файла; для EXT-продуктов — объединение `develop ∪ prj`), для чужого продукта —
область, разрешённая по профилю dsinfo или default-ветке. Процедура,
существующая только в ветке того же продукта вне разрешённой области, MUST NOT
считаться существующей.

#### Scenario: Вызов несуществующей процедуры

- **GIVEN** SQL-файл с `EXEC NonExistentProc @Param = 1` и проиндексированный проект, где `NonExistentProc` не существует
- **WHEN** выполняется `codebase review file.sql`
- **THEN** сформирован finding с rule `execNotExistsProc`, severity 1

#### Scenario: Процедура есть только в другой ветке своего продукта

- **GIVEN** процедура `Cons_Calc` создана только в ветке `develop` продукта `fa-contracts`, а проверяется файл ветки `fa-contracts-11-01-7` того же продукта
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `execNotExistsProc`, severity 1

#### Scenario: Процедура есть в целевой ветке чужого продукта

- **GIVEN** процедура `Util_DateFmt` создана в разрешённой ветке продукта `fa-payments`, а проверяется файл продукта `fa-contracts`
- **WHEN** выполняется review
- **THEN** finding `execNotExistsProc` не формируется

#### Scenario: Процедура ext-develop в prj-файле

- **GIVEN** файл ветки `prj/novikom-persform` продукта `fa-contracts-ext` вызывает процедуру, созданную в ветке `develop` этого продукта
- **WHEN** выполняется review
- **THEN** finding `execNotExistsProc` не формируется: поставка банка включает develop

### Requirement: Дублирование процедур

Система SHALL обнаруживать дублирование имени процедуры в пределах
разрешённой области своего продукта (по `review/review-scope`; для файлов вне
EXT-веток это продукт и ветка проверяемого файла) и формировать finding
`procDuplicate` severity 1. Дублированием MUST считаться несколько
`CREATE PROCEDURE` с одним именем в одном файле и одинаковое имя в разных
файлах одной области. Копии того же имени вне разрешённой области — в других
ветках того же продукта, не входящих в область, и в других продуктах — MUST
NOT считаться дублированием. Источники `upload` и `.t01` MUST исключаться.

#### Scenario: Дублирование процедуры

- **GIVEN** SQL-файл с двумя определениями `CREATE PROCEDURE MyProc`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `procDuplicate`, severity 1

#### Scenario: Дублирование в разных файлах одной ветки

- **GIVEN** процедура `MyProc` создана в двух разных файлах одной ветки одного продукта
- **WHEN** проверяется файл этой ветки
- **THEN** сформирован finding с rule `procDuplicate`, severity 1

#### Scenario: Одна процедура в нескольких ветках

- **GIVEN** процедура `Cons_BlockLimitAuto_Check` создана в develop, `fa-contracts-11-01-6` и `fa-contracts-11-01-7` продукта `fa-contracts`
- **WHEN** проверяется файл ветки `fa-contracts-11-01-7`
- **THEN** finding `procDuplicate` не формируется

### Requirement: Использование индексов, отсутствующих в БД

Система SHALL обнаруживать использование индексов, отсутствующих в БД в
разрешённой области продукта таблицы, и формировать finding `indexExistsInDB`.
Область поиска MUST определяться по `review/review-scope`: индекс должен
существовать в области, разрешённой для продукта таблицы (ветка; для
EXT-продуктов — объединение `develop ∪ prj`). Индекс, существующий только в
ветке того же продукта вне разрешённой области, MUST NOT считаться
существующим.

#### Scenario: Несуществующий индекс

- **GIVEN** SQL-файл с `WITH (INDEX(NonExistentIndex))` и БД, где индекс не существует
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

#### Scenario: Индекс есть только в другой ветке своего продукта

- **GIVEN** индекс `IX_Contract_Name` объявлен только в ветке `develop` продукта `fa-contracts`, а проверяется файл ветки `fa-contracts-11-01-7`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

### Requirement: Существование таблицы в метаданных

Система SHALL проверять, что для каждой таблицы, к которой SQL-скрипт
обращается в DML-контексте, в индексе существует источник, создающий эту
таблицу в разрешённой области продукта таблицы, и формировать finding
`tableExistsInMetadata` (severity 1), если такого источника нет. Область
поиска MUST определяться по `review/review-scope`: для продукта проверяемого
файла — область файла (ветка файла; для EXT-продуктов — объединение
`develop ∪ prj`), для чужого продукта — разрешённая область профиля или
default-ветки. Источник, существующий только в ветке того же продукта вне
разрешённой области, MUST NOT подтверждать существование таблицы.

Источником создания таблицы считается любое из:
- DDL-скрипт с `CREATE TABLE` (патч постоянной t-таблицы или TmpTbl-скрипт p-таблицы);
- таблица, объявленная в метаданных DSArchitect как таблица API-контракта или таблица бизнес-объекта;
- создание через `SELECT ... INTO`;
- `CREATE TABLE` в самом анализируемом файле.

Проверка MUST рассматривать только реальные обращения к таблицам (SELECT/INSERT/UPDATE/DELETE/LOG) и MUST игнорировать:
- временные таблицы с префиксом `#` (включая `##`);
- имена, извлечённые в неопределённом контексте (например, аргументы `DROP_IF`);
- курсоры, объявленные в самом анализируемом файле, включая их использование как источника чтения (`FETCH ... FROM`) и цели позиционного обновления (`WHERE CURRENT OF`);
- триггерные pseudo-таблицы `inserted`/`deleted` и системные каталоги (`sys*`, `master`, `information_schema`);
- placeholder-токены генераторов (имена из подчёркиваний, содержащие `##` или начинающиеся с `__`);
- имена, встречающиеся на строке обращения только внутри строковых литералов (например, профилировочные сообщения вида `PROFILE_TIME_EX('update contract')`).

Сравнение имён таблиц MUST быть регистронезависимым. Finding MUST указывать имя таблицы, файл и строку обращения.

#### Scenario: t-таблица создана патчем

- **GIVEN** SQL-скрипт использует `tConsRestructingDetail` в `SELECT`
- **AND** в индексе есть патч с `create table tConsRestructingDetail`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` не формируется

#### Scenario: p-таблица создана TmpTbl-скриптом

- **GIVEN** SQL-скрипт использует `pCons_ObjectID` в `INSERT`
- **AND** в индексе есть скрипт `pCons_ObjectID_TmpTbl.sql` с `create table pCons_ObjectID`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` не формируется

#### Scenario: p-таблица объявлена в DSArchitect

- **GIVEN** SQL-скрипт использует `pAPI_Acc_CreateAccount` в `SELECT`
- **AND** таблица `pAPI_Acc_CreateAccount` объявлена в метаданных API-таблиц
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` не формируется

#### Scenario: Таблица создана через SELECT INTO

- **GIVEN** SQL-скрипт создаёт `tReportData` через `SELECT ... INTO` и затем читает её
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` не формируется

#### Scenario: Использование несуществующей таблицы

- **GIVEN** SQL-скрипт использует `pAPI_Account_MassClose` в `INSERT`
- **AND** ни один DDL-скрипт, API-контракт или бизнес-объект в разрешённой области не создаёт и не объявляет `pAPI_Account_MassClose`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata`, severity 1
- **AND** в finding указаны имя `pAPI_Account_MassClose`, файл и строка `INSERT`

#### Scenario: Создатель таблицы есть только в другой ветке своего продукта

- **GIVEN** таблица `tContract` создана в продукте `fa-contracts` только в ветке `develop`, а проверяется файл ветки `fa-contracts-11-01-7`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata`, severity 1

#### Scenario: Таблица ext-develop в prj-файле

- **GIVEN** файл ветки `prj/novikom-persform` продукта `fa-contracts-ext` использует в DML таблицу, созданную только в ветке `develop` этого продукта
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` не формируется: поставка банка включает develop

#### Scenario: Временная #-таблица не проверяется

- **GIVEN** SQL-скрипт использует `#Rest` без создания в индексе
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `#Rest` не формируется

#### Scenario: Курсор не считается таблицей

- **GIVEN** SQL-скрипт объявляет и открывает курсор `pAPI_ContractKind`, но не использует его как таблицу в DML
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `pAPI_ContractKind` не формируется

#### Scenario: Чтение и позиционное обновление курсора не дают finding

- **GIVEN** SQL-скрипт объявляет курсор `dateselectcursor`, читает его через `FETCH NEXT FROM dateselectcursor` и обновляет через `UPDATE ... WHERE CURRENT OF dateselectcursor`
- **AND** ни один источник индекса не создаёт таблицу с таким именем
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `dateselectcursor` не формируется

#### Scenario: Pseudo-таблицы триггеров и системные каталоги не проверяются

- **GIVEN** SQL-скрипт (триггер) читает `inserted` и `deleted`, а также системный каталог `sysobjects`
- **WHEN** выполняется review
- **THEN** findings `tableExistsInMetadata` для `inserted`, `deleted` и `sysobjects` не формируются

#### Scenario: Placeholder-токен генератора не проверяется

- **GIVEN** SQL-шаблон генератора содержит обращение к таблице `__table__`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `__table__` не формируется

#### Scenario: Имя из строкового литерала не проверяется

- **GIVEN** SQL-скрипт содержит профилировочный вызов `PROFILE_TIME_EX('update contract')` и не обращается к таблице `contract` вне строковых литералов
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `contract` не формируется

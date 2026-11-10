## ADDED Requirements

### Requirement: Существование таблицы в метаданных

Система SHALL проверять, что для каждой таблицы, к которой SQL-скрипт обращается в DML-контексте, в индексе существует источник, создающий эту таблицу, и формировать finding `tableExistsInMetadata` (severity 1), если такого источника нет.

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
- **AND** ни один DDL-скрипт, API-контракт или бизнес-объект в индексе не создаёт и не объявляет `pAPI_Account_MassClose`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata`, severity 1
- **AND** в finding указаны имя `pAPI_Account_MassClose`, файл и строка `INSERT`

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

## MODIFIED Requirements

### Requirement: Существование таблицы в метаданных

Система SHALL проверять, что для каждой таблицы, к которой SQL-скрипт
обращается в DML-контексте, в индексе существует источник, подтверждающий
существование этой таблицы в разрешённой области продукта таблицы, и
формировать finding `tableExistsInMetadata` (severity 1), если такого источника
нет. Область поиска MUST определяться по `review/review-scope`: для продукта
проверяемого файла — область файла (ветка файла; для EXT-продуктов —
объединение `develop ∪ prj`), для чужого продукта — разрешённая область
профиля или default-ветки. Источник, существующий только в ветке того же
продукта вне разрешённой области, MUST NOT подтверждать существование таблицы.

Создающим источником таблицы считается любое из:
- DDL-скрипт с `CREATE TABLE` (патч постоянной t-таблицы или TmpTbl-скрипт p-таблицы);
- таблица, объявленная в метаданных DSArchitect как таблица API-контракта или таблица бизнес-объекта;
- создание через `SELECT ... INTO`;
- `CREATE TABLE` в самом анализируемом файле.

Если создающего источника нет, существование таблицы подтверждает свидетельство
изменения её структуры скриптом индекса (базовые таблицы, DDL которых
поставляется вне исходников): изменение существующей таблицы не выполняется на
несуществующей. Свидетельством считается любое из:
- добавление колонки — `ALTER TABLE … ADD` или `M_ADD_FIELD` с именем таблицы;
- создание индекса — `CREATE INDEX … ON <таблица>` или `M_CRT_INDEX` с именем таблицы.

Свидетельства берутся из индекса, а не из текста анализируемого файла:
свидетельство, появившееся в файле после индексации, учитывается только после
обновления индекса; свидетельство, записанное в индексе для самого
анализируемого файла, существование таблицы подтверждает. Свидетельства
подчиняются той же областной привязке, что и создающие источники. Таблица с
найденным свидетельством finding `tableExistsInMetadata` не даёт.

Обращение к имени, распознанному как представление (view) в индексе (фрагмент
`create [or replace] view <имя>`), MUST NOT формировать finding
`tableExistsInMetadata`: такое обращение атрибутируется правилу
`viewUsageInServerCode`.

Проверка MUST рассматривать только реальные обращения к таблицам (SELECT/INSERT/UPDATE/DELETE/LOG) и MUST игнорировать:
- временные таблицы с префиксом `#` (включая `##`);
- имена, извлечённые в неопределённом контексте (например, аргументы `DROP_IF`);
- курсоры, объявленные в самом анализируемом файле, включая их использование как источника чтения (`FETCH ... FROM`) и цели позиционного обновления (`WHERE CURRENT OF`);
- триггерные pseudo-таблицы `inserted`/`deleted` и системные каталоги (`sys*`, `master`, `information_schema`);
- placeholder-токены генераторов (имена из подчёркиваний, содержащие `##` или начинающиеся с `__`);
- имена, встречающиеся на строке обращения только внутри строковых литералов (например, профилировочные сообщения вида `PROFILE_TIME_EX('update contract')`).

Сравнение имён таблиц MUST быть регистронезависимым. Finding MUST указывать имя таблицы, файл и строку обращения; сообщение finding MUST перечислять проверенные виды источников (создающий DDL, изменение структуры патчем, SELECT INTO, объявление DSArchitect).

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

#### Scenario: Базовая таблица без DDL с патчем добавления поля

- **GIVEN** SQL-скрипт `ParticipantSB_Delete.sql` читает `tRequisitParticipantSB` в `SELECT`
- **AND** в индексе нет `create table tRequisitParticipantSB`, но есть запись добавления колонки `M_ADD_FIELD('tRequisitParticipantSB', 'InstitutionID …')` из патча продукта
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tRequisitParticipantSB` не формируется

#### Scenario: Базовая таблица без DDL с ALTER TABLE ADD

- **GIVEN** SQL-скрипт использует `tLegacyRef` в `UPDATE`
- **AND** в индексе нет `create table tLegacyRef`, но есть `alter table tLegacyRef add …`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tLegacyRef` не формируется

#### Scenario: Базовая таблица без DDL с индексным патчем

- **GIVEN** SQL-скрипт использует `tWebUser` в `DELETE`
- **AND** в индексе нет `create table tWebUser` и записей добавления колонок, но есть `M_CRT_INDEX(…, 'XIE1tWebUser', 'tWebUser', …)` или `create index … on tWebUser`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tWebUser` не формируется

#### Scenario: Свидетельство патча есть только в другой ветке своего продукта

- **GIVEN** таблица `tLegacyRef` не имеет создающего источника, а `M_ADD_FIELD('tLegacyRef', …)` встречается только в ветке `develop` продукта `fa-contracts`; проверяется файл ветки `fa-contracts-11-01-7`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata`, severity 1

#### Scenario: Файл сам добавляет колонку используемой таблице

- **GIVEN** SQL-файл содержит `M_ADD_FIELD('tLegacyRef', 'NewCol …')` и `UPDATE tLegacyRef`
- **AND** файл проиндексирован, создающего источника `tLegacyRef` в индексе нет
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tLegacyRef` не формируется: свидетельство есть в индексе

#### Scenario: Устаревший индекс не видит свидетельство из файла

- **GIVEN** SQL-файл содержит `M_ADD_FIELD('tLegacyRef', 'NewCol …')` и `UPDATE tLegacyRef`
- **AND** индекс не обновлялся после добавления `M_ADD_FIELD`, записей `tLegacyRef` в индексе нет
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata` для `tLegacyRef`, severity 1

#### Scenario: Использование несуществующей таблицы

- **GIVEN** SQL-скрипт использует `pAPI_Account_MassClose` в `INSERT`
- **AND** ни один DDL-скрипт, патч изменения структуры, API-контракт или бизнес-объект в разрешённой области не создаёт, не изменяет и не объявляет `pAPI_Account_MassClose`
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

#### Scenario: Обращение к view не даёт tableExistsInMetadata

- **GIVEN** SQL-файл использует `batch` в `SELECT`, и в индексе `batch` определена фрагментом `create view Batch` (создателя-таблицы нет)
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `batch` не формируется
- **AND** сформирован finding `viewUsageInServerCode` для `batch`

#### Scenario: Ошибка БД не отключает проверку существования

- **GIVEN** запрос множества имён представлений к БД завершается ошибкой
- **AND** SQL-файл использует view-имя без создателя-таблицы (например, `batch` в `SELECT`)
- **WHEN** выполняется review
- **THEN** имя не распознано как представление
- **AND** finding `tableExistsInMetadata` для `batch` формируется, как до изменения

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

### Requirement: Использование индексов, отсутствующих в БД

Система SHALL обнаруживать использование индексов, отсутствующих в БД в
разрешённой области продукта таблицы, и формировать finding `indexExistsInDB`.
Область поиска MUST определяться по `review/review-scope`: индекс должен
существовать в области, разрешённой для продукта таблицы (ветка; для
EXT-продуктов — объединение `develop ∪ prj`). Индекс, существующий только в
ветке того же продукта вне разрешённой области, MUST NOT считаться
существующим.

Если для таблицы в разрешённой области нет создающего источника (нет
`CREATE TABLE`, `SELECT ... INTO` или объявления DSArchitect), набор её
индексов в индексе неполон и проверка неверифицируема: система MUST NOT
формировать finding `indexExistsInDB` для индексов такой таблицы — ни по
index-хинтам, ни по индексам макросов `M_DELETE_PTABLE*` — независимо от
наличия свидетельств изменения её структуры. Отсутствие такой таблицы
атрибутируется правилу `tableExistsInMetadata`, а не `indexExistsInDB`.

#### Scenario: Несуществующий индекс

- **GIVEN** SQL-файл с `WITH (INDEX(NonExistentIndex))` и БД, где индекс не существует
- **AND** таблица под хинтом имеет в индексе создающий DDL
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

#### Scenario: Индекс есть только в другой ветке своего продукта

- **GIVEN** таблица `tContract` создана (`create table`) в ветках `develop` и `fa-contracts-11-01-7` продукта `fa-contracts`
- **AND** индекс `IX_Contract_Name` объявлен только в ветке `develop`, а проверяется файл ветки `fa-contracts-11-01-7`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

#### Scenario: PK-индекс базовой таблицы без DDL

- **GIVEN** SQL-файл `ParticipantSB_Delete.sql` содержит `from tRequisitParticipantSB rsb M_NOLOCK_INDEX(XPKtRequisitParticipantSB)`
- **AND** в индексе нет `create table tRequisitParticipantSB` и индекса `XPKtRequisitParticipantSB`; единственный источник таблицы — патч добавления поля
- **WHEN** выполняется review
- **THEN** finding `indexExistsInDB` для `XPKtRequisitParticipantSB` не формируется

#### Scenario: Индекс таблицы без источников не дублирует finding

- **GIVEN** SQL-файл содержит `M_NOLOCK_INDEX(XPKtTypo)` на таблице `tTypo`, для которой в индексе нет ни создающего источника, ни свидетельств
- **WHEN** выполняется review
- **THEN** finding `indexExistsInDB` для `XPKtTypo` не формируется
- **AND** сформирован finding `tableExistsInMetadata` для `tTypo`

#### Scenario: Индекс таблицы, объявленной в DSArchitect, проверяется

- **GIVEN** таблица `pAPI_Acc_CreateAccount` объявлена в метаданных API-таблиц, а SQL-файл содержит хинт с индексом, отсутствующим в её индексах
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

#### Scenario: Индекс макроса M_DELETE_PTABLE_INDEX на базовой таблице без DDL

- **GIVEN** SQL-файл содержит `M_DELETE_PTABLE_INDEX(tRequisitParticipantSB, XPKtRequisitParticipantSB)`, а создающего источника таблицы в индексе нет
- **WHEN** выполняется review
- **THEN** finding `indexExistsInDB` для `XPKtRequisitParticipantSB` не формируется

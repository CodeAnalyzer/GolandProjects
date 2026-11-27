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
- DDL-скрипт с `CREATE TABLE` (патч постоянной t-таблицы или TmpTbl-скрипт p-таблицы), включая форму с schema/db-префиксом имени (`dbo.tSeed`, `MIDDLE..dcSubcontoConnect`) и модификаторы (`UNLOGGED`, `TEMP`, `TEMPORARY`, `GLOBAL TEMPORARY`);
- таблица, объявленная в метаданных DSArchitect как таблица API-контракта или таблица бизнес-объекта;
- создание через `SELECT ... INTO`;
- `CREATE TABLE` в самом анализируемом файле.

Если создающего источника нет, существование таблицы подтверждает свидетельство
изменения её структуры скриптом индекса (базовые таблицы, DDL которых
поставляется вне исходников): изменение существующей таблицы не выполняется на
несуществующей. Свидетельством считается любое из:
- добавление колонки — `ALTER TABLE … ADD` или `M_ADD_FIELD` с именем таблицы;
- создание индекса — `CREATE INDEX … ON <таблица>` или `M_CRT_INDEX` с именем таблицы;
- регистрация таблицы в реестре продукта — `__REG_TABLE_NAME__` в `DB/ProductTable.sql` (таблицы, принадлежащие продукту, чей DDL поставляется вне исходников).

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

Сравнение имён таблиц MUST быть регистронезависимым. Finding MUST указывать имя таблицы, файл и строку обращения; сообщение finding MUST перечислять проверенные виды источников (создающий DDL, изменение структуры патчем, реестр таблиц продукта, SELECT INTO, объявление DSArchitect).

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

#### Scenario: Таблица создана с schema-префиксом

- **GIVEN** SQL-скрипт использует `tReplTables` в DML
- **AND** в индексе есть скрипт `DB/eis_5nt.sql` со строкой `create table dbo.tReplTables (...)`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tReplTables` не формируется

#### Scenario: Таблица создана CREATE UNLOGGED TABLE

- **GIVEN** SQL-скрипт использует `pAPI_ExpProperty_Type` в DML
- **AND** в индексе есть скрипт с `CREATE UNLOGGED TABLE pAPI_ExpProperty_Type (...)`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `pAPI_ExpProperty_Type` не формируется

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

#### Scenario: Системная таблица зарегистрирована в реестре продукта

- **GIVEN** SQL-скрипт продукта `fa-payments` использует `tKey` в `UPDATE`
- **AND** в индексе нет `create table tKey` и патчей изменения структуры, но в `reg_table_name` есть регистрация `tKey` из `Payments/SERVER/DB/ProductTable.sql` продукта `fa-payments`
- **WHEN** выполняется review
- **THEN** finding `tableExistsInMetadata` для `tKey` не формируется

#### Scenario: Реестр чужого продукта вне разрешённой области

- **GIVEN** SQL-скрипт продукта `fa-contracts` использует `tKey` в DML
- **AND** регистрация `tKey` в `reg_table_name` есть только в продукте `fa-payments` вне разрешённой области для проверяемого файла
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableExistsInMetadata`, severity 1

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
- **AND** ни один DDL-скрипт, патч изменения структуры, реестр продукта, API-контракт или бизнес-объект в разрешённой области не создаёт, не изменяет, не регистрирует и не объявляет `pAPI_Account_MassClose`
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

### Requirement: Запрет представлений в серверном коде

Система SHALL формировать finding `viewUsageInServerCode` (severity 1) для
каждого обращения SQL-файла к представлению (VIEW) в DML-контексте
(SELECT/INSERT/UPDATE/DELETE/LOG). Finding MUST указывать имя представления,
файл и строку обращения. Правило применяется ко всем SQL-файлам, принимаемым
review (`.sql`, `.h`).

Представлением считается имя, для которого в индексе существует фрагмент
`create [or replace] view <имя>` (SQL-скрипт любого продукта или ветки:
представления живут в общем namespace БД, сопоставление по имени без
product-scope). Схемный префикс в определении (`public.`) MUST
отбрасываться. В обращении SQL-парсер извлекает последний сегмент
dotted-имени (`from dbo.X` даёт ссылку на `X`), поэтому квалифицированные
обращения к view распознаются и дают finding (прежде — известный FN
срезания до первого токена). Если парсер сохранил префикс в извлечённом
имени, префикс (`dbo.`) MUST отбрасываться при сопоставлении. Сравнение
имён MUST быть регистронезависимым. Findings дедуплицируются по имени и
строке обращения (как у `tableExistsInMetadata`).

Проверка MUST применять те же исключения ложных ссылок, что и
`tableExistsInMetadata`: временные таблицы `#`/`##`, курсоры файла,
триггерные pseudo-таблицы, системные каталоги, placeholder-токены, имена
внутри строковых литералов.

Ошибка получения множества имён представлений из БД MUST приводить к
пропуску проверки без падения review (fail-open: бан-правило при сбое не
формирует ложных findings).

#### Scenario: SELECT из представления в серверной процедуре

- **GIVEN** SQL-файл содержит `select ... from tfund`, и в индексе есть фрагмент `create view tfund`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `viewUsageInServerCode`, severity 1
- **AND** в finding указаны имя `tfund`, файл и строка обращения

#### Scenario: Квалифицированное обращение к view

- **GIVEN** SQL-файл содержит `select ... from dbo.tfund`, и в индексе есть фрагмент `create view tfund`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `viewUsageInServerCode` для `tfund`
- **AND** префикс `dbo` не попадает в findings

#### Scenario: UPDATE представления

- **GIVEN** SQL-файл содержит `update Batch set ...`, и в индексе есть фрагмент `create view Batch`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `viewUsageInServerCode`, severity 1

#### Scenario: Определение представления не является нарушением

- **GIVEN** SQL-файл содержит `create view Batch as select ... from tPropertyUsr, tUserResSel`
- **AND** тела view обращаются к обычным таблицам (не view)
- **WHEN** выполняется review
- **THEN** finding `viewUsageInServerCode` по факту определения view и по обращениям к обычным таблицам не формируется

#### Scenario: View поверх view

- **GIVEN** SQL-файл содержит `create view vA as select ... from vB`, и `vB` определена как view в индексе
- **WHEN** выполняется review
- **THEN** сформирован finding `viewUsageInServerCode` для обращения к `vB`

#### Scenario: Схемный префикс отбрасывается

- **GIVEN** в индексе view определена фрагментом `create or replace view public.tinstracclink`
- **AND** SQL-файл обращается к `tinstracclink` в `SELECT`
- **WHEN** выполняется review
- **THEN** сформирован finding `viewUsageInServerCode` для `tinstracclink`

#### Scenario: Обычная таблица не даёт finding

- **GIVEN** SQL-файл обращается к `tContract` в `SELECT`, и `tContract` — таблица (не view)
- **WHEN** выполняется review
- **THEN** finding `viewUsageInServerCode` не формируется

#### Scenario: Ошибка БД не валит review

- **GIVEN** запрос множества имён представлений к БД завершается ошибкой
- **WHEN** выполняется review
- **THEN** review завершается успешно
- **AND** findings `viewUsageInServerCode` не формируются

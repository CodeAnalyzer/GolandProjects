# Deploy Stoppers

## Purpose

Статический анализ SQL-файлов для выявления критических проблем (severity=1), блокирующих деплой: правила каталога — несуществующие и дублированные процедуры, небезопасные конструкции, отсутствие required hints, неправильное использование индексов, запрещённые синтаксические паттерны.

## Requirements

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

### Requirement: Параметр без значения по умолчанию

Система SHALL обнаруживать параметры процедуры без значения по умолчанию, которые требуют явного присвоения в начале процедуры, и формировать finding `procParamDefValue`.

#### Scenario: Параметр без default

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc @Param int` без значения по умолчанию и без присвоения `@Param` в начале процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `procParamDefValue`, severity 1

### Requirement: Использование SELECT *

Система SHALL обнаруживать использование `SELECT *` в SQL-процедурах и формировать finding `useSelectAll`.

#### Scenario: SELECT * в процедуре

- **GIVEN** SQL-файл с `SELECT * FROM tContract` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useSelectAll`, severity 1

### Requirement: Использование TRUNCATE TABLE

Система SHALL обнаруживать использование `TRUNCATE TABLE` и формировать finding `truncTbl`.

#### Scenario: TRUNCATE TABLE

- **GIVEN** SQL-файл с `TRUNCATE TABLE tTemp`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `truncTbl`, severity 1

### Requirement: ANSI-89 join syntax

Система SHALL обнаруживать использование comma joins (ANSI-89 join syntax) и формировать finding `ansiInJoin`.

#### Scenario: Comma join

- **GIVEN** SQL-файл с `FROM tA, tB WHERE tA.id = tB.id`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `ansiInJoin`, severity 1

### Requirement: INSERT без ROWLOCK

Система SHALL обнаруживать `INSERT` без указания `ROWLOCK` и формировать finding `insertRowLock`.

#### Scenario: INSERT без ROWLOCK

- **GIVEN** SQL-файл с `INSERT INTO tContract (ID) VALUES (1)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `insertRowLock`, severity 1

### Requirement: Отсутствие required table hints

Система SHALL обнаруживать отсутствие required table hints (M_UPDLOCK_INDEX, M_FORCEORDER, и др.) и формировать finding `tableHintExists`.

#### Scenario: Отсутствие hint

- **GIVEN** SQL-файл с `UPDATE tContract SET Name = 'test'` без required hint
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableHintExists`, severity 1

### Requirement: Некорректные table hints

Система SHALL обнаруживать некорректное использование table hints (неправильный индекс, неправильный SPID для p-таблиц) и формировать finding `tableHintIsRight`.

#### Scenario: Неправильный hint

- **GIVEN** SQL-файл с `UPDATE tContract SET Name = 'test' FROM tContract WITH (M_UPDLOCK_INDEX(WrongIndex))`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableHintIsRight`, severity 1

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

### Requirement: Full scan условия

Система SHALL обнаруживать условия в WHERE/JOIN, приводящие к full scan (отсутствие SARGable предикатов), и формировать finding `tableFullScan`. При анализе FROM-части система MUST корректно обрабатывать derived tables (подзапросы в скобках с алиасом): таблицы внутри подзапроса анализируются по WHERE/ON этого подзапроса, а finding MUST ссылаться на реальную таблицу — объект finding'а не может быть SQL-ключевым словом. Для INSERT…SELECT finding MUST быть привязан к строке начала оператора (`insert`), анализу подлежит только SELECT-часть.

#### Scenario: Non-SARGable условие

- **GIVEN** SQL-файл с `WHERE SUBSTRING(Name, 1, 3) = 'abc'`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableFullScan`, severity 1

#### Scenario: Derived table с фильтром внутри — нет finding

- **GIVEN** SQL-файл с `select ... from (select ... from pSrc where k.SPID = @@spid group by k.K) t left join (select ... from pLnk where l.SPID = @@spid) g on ...`
- **WHEN** выполняется review
- **THEN** finding `tableFullScan` не формируется
- **AND** объект любого finding'а не равен `select`

#### Scenario: Derived table без фильтра внутри — finding по внутренней таблице

- **GIVEN** SQL-файл с `select ... from (select ... from pSrc) t` без условий фильтрации ни снаружи, ни внутри подзапроса
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableFullScan`, severity 1
- **AND** объект finding'а — имя таблицы внутри подзапроса (`psrc`)

#### Scenario: INSERT…SELECT привязан к началу оператора

- **GIVEN** SQL-файл с `insert into pTarget (SPID, K)` на строке N и `select ...` на строке N+1, SELECT-часть без FROM-таблиц
- **WHEN** выполняется review
- **THEN** finding `tableFullScan` не формируется (цель INSERT не подлежит требованию фильтрации)
- **AND** при наличии нарушения в SELECT-части line finding'а указывает на строку начала оператора INSERT

### Requirement: Отсутствие SPID для p-таблиц

Система SHALL обнаруживать отсутствие условия по `SPID` для p-таблиц и формировать finding `pTableSpid`.

#### Scenario: p-таблица без SPID

- **GIVEN** SQL-файл с `SELECT * FROM pAPI_Accrual` без условия по `SPID`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `pTableSpid`, severity 1

### Requirement: Запрос с 2+ таблицами без M_FORCEORDER

Система SHALL обнаруживать запросы с 2 и более таблицами без `M_FORCEORDER` и формировать finding `forceOrder2Tbl`.

#### Scenario: JOIN без M_FORCEORDER

- **GIVEN** SQL-файл с `SELECT * FROM tA JOIN tB ON tA.id = tB.id` без `M_FORCEORDER`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `forceOrder2Tbl`, severity 1

### Requirement: Использование SAVE TRAN

Система SHALL обнаруживать использование `SAVE TRAN`/`SAVE TRANSACTION` и формировать finding `saveTran`.

#### Scenario: SAVE TRAN

- **GIVEN** SQL-файл с `SAVE TRAN MySavePoint`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `saveTran`, severity 1

### Requirement: Использование DROP в теле процедуры

Система SHALL обнаруживать использование `DROP`/`DROP_CREATE` в теле процедуры и формировать finding `useDrop`.

#### Scenario: DROP в процедуре

- **GIVEN** SQL-файл с `DROP TABLE tTemp` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useDrop`, severity 1

### Requirement: Математические операции в BETWEEN и сравнениях

Система SHALL обнаруживать математические операции в `BETWEEN` и сравнениях (риск overflow) и формировать finding `mathOperations`.

#### Scenario: Математика в BETWEEN

- **GIVEN** SQL-файл с `WHERE Amount BETWEEN @Start + 1 AND @End - 1`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `mathOperations`, severity 1

### Requirement: IF с запросом к таблицам и AND

Система SHALL обнаруживать `IF` с запросом к таблицам и `AND` (нет гарантии short-circuit) и формировать finding `existsWithAndInIf`.

#### Scenario: IF EXISTS с AND

- **GIVEN** SQL-файл с `IF EXISTS (SELECT 1 FROM tA) AND EXISTS (SELECT 1 FROM tB) BEGIN ... END`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `existsWithAndInIf`, severity 1

### Requirement: CASE без ветки ELSE

Система SHALL обнаруживать оператор `CASE` без ветки `ELSE` и формировать finding `procElseCase`.

#### Scenario: CASE без ELSE

- **GIVEN** SQL-файл с `CASE WHEN @Status = 1 THEN 1 WHEN @Status = 2 THEN 2 END` без ветки `ELSE`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `procElseCase`, severity 1

### Requirement: Сравнение столбца с самим собой

Система SHALL обнаруживать сравнение столбца с самим собой в условиях (`WHERE`/`ON`/`HAVING`) и формировать finding `useEqColumn`. Сравнения с переменными (`@var`), числовыми литералами и битовыми операциями (`&`, `|`, `^`) finding не формируют.

#### Scenario: Столбец равен себе

- **GIVEN** SQL-файл с `WHERE tA.ID = tA.ID`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useEqColumn`, severity 1

#### Scenario: Битовая маска не считается сравнением с собой

- **GIVEN** SQL-файл с `WHERE Flags & Mask = Mask`
- **WHEN** выполняется review
- **THEN** finding `useEqColumn` не сформирован

### Requirement: UPDATE только переменных

Система SHALL обнаруживать `UPDATE`, изменяющий только переменные без обновления полей таблицы, и формировать finding `updateOnlyVar`.

#### Scenario: UPDATE переменных

- **GIVEN** SQL-файл с `UPDATE @TableVar SET Col = 1` без обновления реальных таблиц
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `updateOnlyVar`, severity 1

### Requirement: Неправильное использование индекса для p-таблиц

Система SHALL обнаруживать неправильное использование индекса (проверка SPID для p-таблиц) и формировать finding `indexWrong`.

#### Scenario: Неправильный индекс для p-таблицы

- **GIVEN** SQL-файл с использованием индекса без учёта SPID для p-таблицы
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexWrong`, severity 1

### Requirement: Отложенное обновление индексированной колонки

Система SHALL обнаруживать `UPDATE`, который изменяет колонку, входящую в индекс из table hint того же оператора (эффект deferred update), и формировать finding `deferredUpdate`.

#### Scenario: UPDATE колонки из хинтового индекса

- **GIVEN** SQL-файл с `UPDATE tContract SET Name = @Name FROM tContract t WITH (M_UPDLOCK_INDEX(IX_Contract_Name))` где `Name` входит в индекс `IX_Contract_Name`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `deferredUpdate`, severity 1

### Requirement: IN с подзапросом

Система SHALL обнаруживать использование `IN`/`NOT IN` с подзапросом (рекомендуется `EXISTS`) и формировать finding `inSubQuery`.

#### Scenario: IN с подзапросом

- **GIVEN** SQL-файл с `WHERE ID IN (SELECT ID FROM tOther)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `inSubQuery`, severity 1

### Requirement: VARCHAR без размера

Система SHALL обнаруживать `VARCHAR`/`NVARCHAR` без указания размера в параметрах процедур и в `DECLARE` и формировать finding `varcharSize`.

#### Scenario: VARCHAR без размера

- **GIVEN** SQL-файл с `DECLARE @Var VARCHAR`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varcharSize`, severity 1

### Requirement: INSERT без явного указания колонок

Система SHALL обнаруживать `INSERT` без явного перечисления столбцов и формировать finding `columnInsert`.

#### Scenario: INSERT без колонок

- **GIVEN** SQL-файл с `INSERT INTO tContract VALUES (1, 'test')`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `columnInsert`, severity 1

### Requirement: Изменение полей уникального индекса таблицы в памяти

Система SHALL обнаруживать `UPDATE`, целевая таблица которого объявлена «таблицей в памяти» (есть запись в признаке in-memory по имени таблицы, без привязки к продукту), и в секции `SET` которого изменяется колонка, входящая в состав уникального индекса этой таблицы, и формировать finding `memoryUniqueIndexUpdate` severity 1. Уникальными считаются только индексы, отмеченные признаком уникальности в индексе: уникальный индекс обычной p-таблицы либо уникальный индекс API-таблицы `pAPI_*` (признак уникален для обоих физических видов: 2 и 3). Индекс без признака уникальности — в том числе первичный ключ — уникальным не считается: имя индекса конвенцией уникальности не является. Сопоставление целевой таблицы с признаком in-memory выполняется по имени таблицы.

#### Scenario: UPDATE колонки уникального индекса in-memory p-таблицы

- **GIVEN** SQL-файл с `UPDATE pRuleDoc_QtyStack SET Brief = @Brief WHERE ...`, таблица `pRuleDoc_QtyStack` объявлена в памяти, колонка `Brief` входит в её уникальный индекс `XPKpRuleDoc_QtyStack` (объявлен `M_CRT_INDEX('UNIQUE', ...)`)
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: UPDATE колонки уникального индекса pAPI-таблицы

- **GIVEN** SQL-файл с `UPDATE pAPI_LstTpl SET TemplateID = @TemplateID ...`, таблица `pAPI_LstTpl` объявлена в памяти, колонка `TemplateID` входит в её индекс `XPKpAPI_LstTpl` с признаком уникальности вида 2 или 3
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: UPDATE неуникальной колонки in-memory таблицы

- **GIVEN** SQL-файл с `UPDATE pRuleDoc_QtyStack SET Qty = @Qty ...`, колонка `Qty` не входит ни в один уникальный индекс таблицы
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: UPDATE уникальной колонки обычной таблицы

- **GIVEN** SQL-файл с `UPDATE tContract SET ContractID = @ID ...`, колонка `ContractID` входит в уникальный индекс `XPKtContract`, но таблица `tContract` НЕ объявлена в памяти
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: Индекс без признака уникальности

- **GIVEN** SQL-файл с `UPDATE pObjectForRight SET ObjectID = @ID ...`, таблица `pObjectForRight` объявлена в памяти, колонка `ObjectID` входит только в индекс `XPKpObjectForRight`, объявленный без признака уникальности (`M_CRT_INDEX('', ...)`)
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: UPDATE через алиас целевой таблицы

- **GIVEN** SQL-файл с `UPDATE x SET Brief = @Brief FROM pRuleDoc_QtyStack x WHERE ...`, таблица `pRuleDoc_QtyStack` объявлена в памяти, колонка `Brief` входит в её уникальный индекс
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: Декларация in-memory в другом продукте

- **GIVEN** SQL-файл любого продукта с `UPDATE pAPI_LstTpl SET TemplateID = @TemplateID ...`, признак in-memory для `pAPI_LstTpl` объявлен декларацией extension-репозитория установки (файл `ModuleTableInMemory.lst` вне продуктов кодовой базы)
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1 (сопоставление по имени таблицы, без product-scope)

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

## Related code

- `internal/review/types.go` — типы `Finding`, `RuleID`, `Severity`, константы правил
- `internal/review/catalog.go` — каталог правил (ruleCatalog), единый источник rule metadata
- `internal/review/review_rules.go` — реализация проверок (deploy stoppers)
- `internal/review/runner.go` — review runner и execution pipeline
- `internal/review/review_parser.go` — SQL-парсер для review (split statements, CASE tracking)
- `internal/review/review_helpers.go` — вспомогательные функции
- `internal/review/review_lookup.go` — lookup-функции для типов данных и макросов
- `cmd/review.go` — CLI command `review`

## Notes

- Deploy stoppers имеют severity=1 — критические ошибки, блокирующие деплой
- По умолчанию `codebase review` проверяет все deploy stoppers
- Флаг `--rules` позволяет указать конкретные правила для проверки
- Флаг `--min-severity` позволяет понизить порог до severity 2 или 3
- Правило `execNotExistsProc` требует подключения к БД для проверки существования процедур
- Execution-слой `internal/reviewsvc/runtime.go` — общая точка входа для CLI (`cmd/review.go`) и MCP-инструмента `codebase_review_sql`; устраняет дублирование оркестрации. Поведение review-команд специфицировано здесь на уровне правил; транспорт MCP — в `mcp-server/mcp-transport-tools`.

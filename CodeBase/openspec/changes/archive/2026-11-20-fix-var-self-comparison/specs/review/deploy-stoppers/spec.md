## MODIFIED Requirements

### Requirement: Сравнение столбца с самим собой

Система SHALL обнаруживать в условиях (`WHERE`/`ON`/`HAVING`) заведомо истинные сравнения: колонку с самой собой и переменную с самой собой (`@X = @X`, регистронезависимо). Такие сравнения формируют finding `useEqColumn` severity 1. Для самосравнения переменной сообщение finding SHALL иметь вид `Заведомо истинное сравнение: @X = @X`, а поле `Object` SHALL содержать имя переменной с `@` в нижнем регистре (например, `@bankproductid`); для самосравнения колонки сохраняется сообщение `Нельзя сравнивать столбец с самим собой`.

Система SHALL NOT формировать finding для сравнений разных переменных (`@A = @B`), сравнений переменной с колонкой или литералом (`@var = column`, `@var = 1`), числовых литералов (`1 = 1`), битовых операций (`&`, `|`, `^`), а также для одинаковых имён в аргументах `EXEC`, в списках `SET`/`SELECT`-алиасах и в закомментированном коде.

#### Scenario: Столбец равен себе

- **GIVEN** SQL-файл с `WHERE tA.ID = tA.ID`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useEqColumn`, severity 1

#### Scenario: Переменная равна самой себе в WHERE

- **GIVEN** SQL-файл с `delete pBPConsCoolingOff ... where SPID = @@spid and @BankProductID = @BankProductID`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useEqColumn`, severity 1
- **AND** сообщение finding содержит `Заведомо истинное сравнение: @BankProductID = @BankProductID`
- **AND** поле `Object` finding равно `@bankproductid`

#### Scenario: Разные переменные не считаются самосравнением

- **GIVEN** SQL-файл с `WHERE @LastPrcnt = @Prcnt and @LastParentID = @ParentID`
- **WHEN** выполняется review
- **THEN** finding `useEqColumn` не сформирован

#### Scenario: Named-args EXEC не считаются самосравнением

- **GIVEN** SQL-файл с `exec BPConsCoolingOff_Transfer @BankProductID = @BankProductID, @Mode = 1`
- **WHEN** выполняется review
- **THEN** finding `useEqColumn` не сформирован

#### Scenario: Битовая маска не считается сравнением с собой

- **GIVEN** SQL-файл с `WHERE Flags & Mask = Mask`
- **WHEN** выполняется review
- **THEN** finding `useEqColumn` не сформирован

### Requirement: Неправильное использование индекса для p-таблиц

Система SHALL обнаруживать неправильное использование индекса (проверка SPID для p-таблиц) и формировать finding `indexWrong`. При оценке пригодности индекса система SHALL учитывать только реальные ссылки на колонки в условиях `WHERE`/`ON`; идентификаторы внутри переменных `@name` и системных переменных `@@name` колонками не считаются.

#### Scenario: Неправильный индекс для p-таблицы

- **GIVEN** SQL-файл с использованием индекса без учёта SPID для p-таблицы
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexWrong`, severity 1

#### Scenario: Переменная в условии не подменяет колонку

- **GIVEN** SQL-файл с `delete pBPConsCoolingOff from pBPConsCoolingOff M_ROWLOCK_INDEX(XPKpBPConsCoolingOff) where SPID = @@spid and @BankProductID = @BankProductID`
- **AND** у таблицы есть индексы `XPKpBPConsCoolingOff (SPID, AutoID)` и `XIE1pBPConsCoolingOff (SPID, BankProductID, AmountMin)`
- **WHEN** выполняется review
- **THEN** finding `indexWrong` не сформирован
- **AND** `@BankProductID` не учитывается как предикат по колонке `BankProductID`

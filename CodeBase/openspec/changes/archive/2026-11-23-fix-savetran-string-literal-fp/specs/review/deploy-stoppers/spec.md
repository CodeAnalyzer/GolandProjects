## MODIFIED Requirements

### Requirement: Использование SAVE TRAN

Система SHALL обнаруживать использование `SAVE TRAN`/`SAVE TRANSACTION` в коде вне строковых литералов и формировать finding `saveTran`. Вхождения `SAVE TRAN`/`SAVE TRANSACTION` внутри строкового литерала в одинарных кавычках (в том числе в литералах-аргументах макросов, например `PROFILE_TIME_EX('...')`) MUST игнорироваться; удвоенный апостроф `''` внутри литерала не закрывает его.

#### Scenario: SAVE TRAN

- **GIVEN** SQL-файл с `SAVE TRAN MySavePoint`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `saveTran`, severity 1

#### Scenario: SAVE TRAN в строковом литерале

- **GIVEN** SQL-файл со строкой `PROFILE_TIME_EX('BEGIN tran and SAVE tran trnCardAccRelation_MassLoad...')` без настоящего оператора `SAVE TRAN`
- **WHEN** выполняется review
- **THEN** finding с rule `saveTran` не сформирован

#### Scenario: Экранированный апостроф в литерале и настоящий SAVE TRAN

- **GIVEN** SQL-файл со строкой `select @s = 'save tran ''quoted'' text'` и отдельной строкой `SAVE TRAN MySavePoint`
- **WHEN** выполняется review
- **THEN** сформирован ровно один finding с rule `saveTran`, severity 1
- **AND** finding указывает на строку с настоящим `SAVE TRAN MySavePoint`

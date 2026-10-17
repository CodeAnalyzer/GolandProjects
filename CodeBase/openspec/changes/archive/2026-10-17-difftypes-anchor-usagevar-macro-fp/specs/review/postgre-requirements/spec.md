## MODIFIED Requirements

### Requirement: Сравнение выражений разных типов

Система SHALL обнаруживать сравнение выражений разных типов в `WHERE`/`ON`/`IF`/`CASE WHEN` и формировать finding `diffTypesComparison`. Якорь finding'а (line) MUST указывать на строку фактического сравнения внутри оператора, а не на первую строку query-фрагмента; строки standalone-макросов (`M_DELETE_PTABLE(...)`, `M_LOG_TABLE(...)` и т.п.), склеенные парсером с оператором, не смещают якорь.

#### Scenario: Сравнение datetime и date

- **GIVEN** SQL-файл с `WHERE tA.DSDATETIME = tB.DSOPERDAY`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `diffTypesComparison`, severity 2
- **AND** line finding'а указывает на строку сравнения в WHERE

#### Scenario: Якорь при макросе, склеенном с INSERT…SELECT

- **GIVEN** SQL-файл, где строке с `M_DELETE_PTABLE(pX)` непосредственно следует `INSERT INTO pX ... SELECT ... INNER JOIN ... ON a.Col1 = b.Col2` с несовпадающими типами колонок
- **AND** макрос и INSERT…SELECT попадают в один query-фрагмент со стартовой строкой макроса
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `diffTypesComparison`, severity 2
- **AND** line finding'а указывает на строку сравнения `a.Col1 = b.Col2` в ON, а не на строку макроса

### Requirement: Использование переменной в том же SELECT

Система SHALL обнаруживать чтение переменной в том же `SELECT`, где она присваивается, и формировать finding `usageVarInSameSelect`. Ссылками на переменную НЕ считаются: вхождения переменной в standalone-вызовы макросов (`M_BUSINESSLOG_PARAM(@Var, ...)`, склеенные с SELECT) и вхождения внутри системных переменных `@@` (`@@rowcount`, `@@error`).

#### Scenario: Чтение @Var в том же SELECT

- **GIVEN** SQL-файл с `SELECT @A = Col, @B = @A FROM tX`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `usageVarInSameSelect`, severity 2

#### Scenario: SELECT присваиваний со склеенными макросами

- **GIVEN** SQL-файл с `SELECT @RowCount = @@rowcount, @RetVal = @@error`, за которым без пустой строки следуют `M_BUSINESSLOG_PARAM(@RowCount, DSINT_KEY, INT)` и `M_BUSINESSLOG_PARAM(@RetVal, DSINT_KEY, INT)`
- **WHEN** выполняется review
- **THEN** finding с rule `usageVarInSameSelect` не формируется

#### Scenario: Системная переменная @@rowcount не считается чтением @rowcount

- **GIVEN** SQL-файл с `SELECT @RetVal = @@error, @RowCount = @@rowcount`
- **WHEN** выполняется review
- **THEN** finding с rule `usageVarInSameSelect` не формируется

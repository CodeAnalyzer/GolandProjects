## MODIFIED Requirements

### Requirement: Сравнение с NULL через =/<>

Система SHALL обнаруживать сравнение с `NULL` через `=`/`<>` вместо `IS NULL`/`IS NOT NULL` и формировать finding `nullComparison`. Строка объявления параметра или переменной со значением по умолчанию `= null` (в том числе с указанием направления выходного параметра ключевым словом `output` или его синонимом `out`, в любом регистре, с необязательной хвостовой запятой и необязательной размерностью типа в скобках) SHALL классифицироваться как объявление значения по умолчанию, а не сравнение, и finding не формирует.

#### Scenario: Сравнение с NULL через =

- **GIVEN** SQL-файл с `WHERE Col = NULL`
- **WHEN** выполняется review с `--min-severity 2`
- **THEN** сформирован finding с rule `nullComparison`, severity 2

#### Scenario: Объявление OUT-параметра с default null

- **GIVEN** SQL-файл с объявлением выходного параметра процедуры в короткой форме `@CheckClient DSINT_KEY = null out`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление OUT-параметра с default null и хвостовой запятой

- **GIVEN** SQL-файл со строкой объявления `@Message DSCOMMENT = null out,` внутри списка параметров процедуры
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление OUT-параметра с default null в верхнем регистре

- **GIVEN** SQL-файл со строкой объявления `@ProtocolID DSIDENTIFIER = NULL OUT,`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление параметра с default null в длинной форме output

- **GIVEN** SQL-файл со строкой объявления `@AmountDelinq DSMONEY = null output,`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

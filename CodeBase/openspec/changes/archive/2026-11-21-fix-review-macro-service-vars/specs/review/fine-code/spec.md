## ADDED Requirements

### Requirement: Встроенные типы сервисных переменных нераскрытых макросов

При разрешении типов переменных для правил статического анализа система SHALL распознавать в анализируемом контенте нераскрытые макросы Diasoft, декларирующие сервисные переменные, и назначать этим переменным встроенные типы:

- нераскрытый `__BEGIN_PROCEDURE__(...)` → `@RetVal int`, `@trancount int`;
- нераскрытый `DCL_PROC_BODY(...)` (PG-стиль) → `@tran_count int`, `@err_code int`, `@msg varchar(255)`.

Встроенные типы MUST применяться только к переменным, тип которых не определён более сильными источниками: параметрами сигнатуры процедуры и явными `declare`-блоками анализируемого файла. Макрос MUST считаться нераскрытым, если его вызов присутствует в контенте текстуально (после подстановки макросов, определённых в самом файле).

#### Scenario: Потеря точности в @RetVal из нераскрытого __BEGIN_PROCEDURE__

- **GIVEN** SQL-файл с сигнатурой `DCL_PROC_BEGIN(MyProc) @Param DSIDENTIFIER as`, нераскрытым `__BEGIN_PROCEDURE__(MyProc)` и оператором `select @RetVal = max(NTFID) from pTable where SPID = @@spid`, где колонка `pTable.NTFID` имеет тип `DSIDENTIFIER`, а `declare @RetVal` в файле отсутствует
- **WHEN** выполняется review
- **THEN** формируется finding с rule `datatype`, severity 3, с сообщением о потере точности `DSIDENTIFIER -> int` для `@RetVal`

#### Scenario: Явная конвертация подавляет finding для @RetVal

- **GIVEN** SQL-файл с нераскрытым `__BEGIN_PROCEDURE__(MyProc)` и оператором `select @RetVal = convert(int, NTFID) from pTable`, где `pTable.NTFID` имеет тип `DSIDENTIFIER`
- **WHEN** выполняется review
- **THEN** finding с rule `datatype` по этому оператору не формируется

#### Scenario: Явный declare в файле переопределяет встроенный тип

- **GIVEN** SQL-файл с нераскрытым `DCL_PROC_BODY(MyProc)` и явным `declare @msg varchar(1000)` в теле процедуры
- **WHEN** выполняется review
- **THEN** тип `@msg` разрешается из declare-блока файла (`varchar(1000)`), а не из встроенного набора сервисных переменных

#### Scenario: Сервисные переменные PG-стиля получают встроенные типы

- **GIVEN** SQL-файл с нераскрытым `DCL_PROC_BODY(MyProc)` и оператором `select @err_code = max(ErrID) from pTable`, где `pTable.ErrID` имеет тип `DSBIGINT`, а `declare @err_code` в файле отсутствует
- **WHEN** выполняется review
- **THEN** формируется finding с rule `datatype`, severity 3, по паре `DSBIGINT -> int`

#### Scenario: Без макросов встроенные типы не назначаются

- **GIVEN** SQL-файл без `__BEGIN_PROCEDURE__` и без `DCL_PROC_BODY` (например, самодостаточный patch-скрипт), использующий `@RetVal` без декларации
- **WHEN** выполняется review
- **THEN** встроенные типы сервисных переменных не применяются: поведение не меняется по сравнению с прежним (тип `@RetVal` неизвестен, finding по нему не формируется)

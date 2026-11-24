## MODIFIED Requirements

### Requirement: JOIN без алиасов таблиц

Система SHALL в запросах с JOIN (две и более таблицы в `FROM`, включая соединение через запятую) обнаруживать неквалифицированные ссылки на столбцы в списке выборки, `WHERE`, `ON`-условиях и правой части `UPDATE ... SET` и формировать finding `statementsWithJoinsRequireAliases` (severity 2). Ссылка считается квалифицированной, если она записана как `table.column` или `alias.column`, в том числе с пробелами вокруг точки (`alias. column`). Определение колоночного алиаса — явного (`expr AS alias`), неявного (`expr alias` без `AS`) и assignment-формы T-SQL (`alias = expr`) — MUST NOT трактоваться как ссылка на столбец, при этом в assignment-форме анализируется правая часть. `ORDER BY` MUST NOT анализироваться (может ссылаться на выходные алиасы `SELECT`). Строки препроцессора (`#ifdef`/`#ifndef`/`#else`/`#elif`/`#endif`/`#define`/`#undef`/`#include`) с их аргументами и аргументы-имена p-таблиц в макросах `M_DELETE_PTABLE[_INDEX/_SPID_INDEX](...)` MUST NOT считаться ссылками на столбцы. Наличие или отсутствие алиасов у самих таблиц правилом отдельно не проверяется; заголовок требования исторический.

#### Scenario: JOIN без алиаса

- **GIVEN** SQL-файл с `SELECT ID FROM tA JOIN tB ON tA.ID = tB.ID`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `statementsWithJoinsRequireAliases`, severity 2

#### Scenario: Квалифицированные ссылки при JOIN не дают finding

- **GIVEN** SQL-файл с `SELECT tA.ID FROM tA JOIN tB ON tA.ID = tB.ID`
- **WHEN** выполняется review
- **THEN** finding с rule `statementsWithJoinsRequireAliases` не формируется

#### Scenario: Неквалифицированный столбец в UPDATE SET

- **GIVEN** SQL-файл с `UPDATE tA SET name = b FROM tA JOIN tB ON tA.id = tB.id`
- **WHEN** выполняется review
- **THEN** сформирован finding по объекту `b`

#### Scenario: Неявный алиас столбца не считается ссылкой

- **GIVEN** SQL-файл с `SELECT u.Brief meUser FROM hDealTransact dt, pAPI_User_FindLst u WHERE dt.UserID = u.UserID`
- **WHEN** выполняется review
- **THEN** finding по объекту `meUser` не формируется

#### Scenario: Неявный алиас литерала или функции не считается ссылкой

- **GIVEN** SQL-файл с `SELECT 1 flag, isnull(t.b, 0) total FROM tA t JOIN tB x ON t.id = x.id`
- **WHEN** выполняется review
- **THEN** findings по объектам `flag` и `total` не формируются

#### Scenario: Операнд выражения не срезается как алиас

- **GIVEN** SQL-файл с `SELECT a + b FROM tA JOIN tB ON tA.id = tB.id`
- **AND** `a` и `b` неквалифицированы
- **WHEN** выполняется review
- **THEN** сформированы findings по объектам `a` и `b`

#### Scenario: Модификатор TOP не приводит к срезанию колонки

- **GIVEN** SQL-файл с `SELECT TOP 10 col FROM tA JOIN tB ON tA.id = tB.id`
- **WHEN** выполняется review
- **THEN** сформирован finding по объекту `col`

#### Scenario: Модификатор DISTINCT не мешает обнаружить неквалифицированную колонку

- **GIVEN** SQL-файл с `SELECT DISTINCT col FROM tA JOIN tB ON tA.id = tB.id`
- **WHEN** выполняется review
- **THEN** сформирован finding по объекту `col`

#### Scenario: DISTINCT с неявным алиасом не даёт finding

- **GIVEN** SQL-файл с `SELECT DISTINCT tA.a myAlias FROM tA JOIN tB ON tA.id = tB.id`
- **WHEN** выполняется review
- **THEN** finding по объекту `myAlias` не формируется

#### Scenario: Assignment-style алиас не считается ссылкой

- **GIVEN** SQL-файл с `SELECT Alias = b FROM tA dt JOIN tB x ON dt.id = x.id`
- **WHEN** выполняется review
- **THEN** сформирован finding по объекту `b`
- **AND** finding по объекту `Alias` не формируется

#### Scenario: Ссылка с пробелом вокруг точки считается квалифицированной

- **GIVEN** SQL-файл с `SELECT i. Name FROM tA i JOIN tB x ON i.id = x.id`
- **WHEN** выполняется review
- **THEN** finding по объекту `Name` не формируется

#### Scenario: Препроцессорные директивы не считаются ссылками

- **GIVEN** SQL-файл с `CASE WHEN a THEN` / `#ifdef PG_CONVERT b` / `#else c` / `#endif` / `END` в списке выборки запроса с JOIN
- **WHEN** выполняется review
- **THEN** findings сформированы по объектам `a`, `b`, `c`
- **AND** findings по `ifdef`, `PG_CONVERT`, `endif` не формируются

#### Scenario: Аргумент макроса управления p-таблицей не считается ссылкой

- **GIVEN** SQL-файл с `... WHERE pp.SPID = @X M_DELETE_PTABLE(pAPI_Translit_List_In)`
- **WHEN** выполняется review
- **THEN** finding по объекту `pAPI_Translit_List_In` не формируется

#### Scenario: ORDER BY по выходному алиасу не даёт finding

- **GIVEN** SQL-файл с `SELECT dt.Date, count(*) Cnt FROM tA dt JOIN tB x ON dt.id = x.id WHERE dt.SPID = @X GROUP BY dt.Date ORDER BY Cnt, dt.Date`
- **WHEN** выполняется review
- **THEN** finding по объекту `Cnt` не формируется

### Requirement: Использование переменной в том же SELECT

Система SHALL обнаруживать чтение переменной в том же `SELECT`, где она присваивается, и формировать finding `usageVarInSameSelect`. Ссылками на переменную НЕ считаются: вхождения переменной в standalone-вызовы макросов (`M_BUSINESSLOG_PARAM(@Var, ...)`, склеенные с SELECT); вхождения внутри системных переменных `@@` (`@@rowcount`, `@@error`); упоминания `@var` внутри комментариев (`-- ...` до конца строки и `/* ... */`), в том числе в конце выражения assignment'а.

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

#### Scenario: @Var внутри комментария не считается чтением

- **GIVEN** SQL-файл с `SELECT @A = 2, @B = 3 --@A`
- **WHEN** выполняется review
- **THEN** finding с rule `usageVarInSameSelect` не формируется

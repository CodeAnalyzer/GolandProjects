# PostgreSQL Requirements

## Purpose

Статический анализ SQL-файлов для выявления нарушений требований PostgreSQL (severity=2): 24 правила — некорректные сравнения с NULL, кодировки, лимиты JOIN/параметров, управление транзакциями, типизация, курсоры, UNION/ISNULL/алиасы.

## Requirements

### Requirement: Сравнение с NULL через =/<>

Система SHALL обнаруживать сравнение с `NULL` через `=`/`<>` вместо `IS NULL`/`IS NOT NULL` и формировать finding `nullComparison`.

#### Scenario: Сравнение с NULL через =

- **GIVEN** SQL-файл с `WHERE Col = NULL`
- **WHEN** выполняется review с `--min-severity 2`
- **THEN** сформирован finding с rule `nullComparison`, severity 2

### Requirement: Строковые литералы не в CP866

Система SHALL обнаруживать строковые литералы, не соответствующие кодировке CP866, и формировать finding `shouldBeCP866`.

#### Scenario: Литерал не в CP866

- **GIVEN** SQL-файл со строковым литералом, содержащим символы, не входящие в CP866
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `shouldBeCP866`, severity 2

### Requirement: Превышение лимита JOIN

Система SHALL обнаруживать запросы с более чем 12 JOIN и формировать finding `tooManyJoins`.

#### Scenario: Более 12 JOIN

- **GIVEN** SQL-файл с запросом, содержащим 13 JOIN
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tooManyJoins`, severity 2

### Requirement: Превышение лимита параметров процедуры

Система SHALL обнаруживать процедуры с более чем 90 параметрами и формировать finding `maxProcParam`.

#### Scenario: 91 параметр

- **GIVEN** SQL-файл с процедурой, имеющей 91 параметр
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `maxProcParam`, severity 2

### Requirement: Модификация выходных параметров

Система SHALL обнаруживать модификацию выходных параметров процедуры в теле процедуры и формировать finding `modifyOutProc`.

#### Scenario: Модификация OUTPUT параметра

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc @OutParam int OUTPUT AS SET @OutParam = 5`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `modifyOutProc`, severity 2

### Requirement: Пустой RETURN без значения

Система SHALL обнаруживать `RETURN` без значения и формировать finding `emptyReturn`.

#### Scenario: RETURN без значения

- **GIVEN** SQL-файл с `RETURN` без значения в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `emptyReturn`, severity 2

### Requirement: Прямое управление транзакциями

Система SHALL обнаруживать прямое управление транзакциями (`BEGIN TRAN`/`COMMIT`/`ROLLBACK`) и формировать finding `rawTransactionControl`.

#### Scenario: BEGIN TRAN в процедуре

- **GIVEN** SQL-файл с `BEGIN TRAN` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `rawTransactionControl`, severity 2

### Requirement: Использование GOTO и меток

Система SHALL обнаруживать использование `GOTO` и меток и формировать finding `postgreLabelGotoLevel`.

#### Scenario: GOTO в процедуре

- **GIVEN** SQL-файл с `GOTO MyLabel` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

### Requirement: Приведение даты к строке

Система SHALL обнаруживать приведение даты к строке и формировать finding `dateIntoString`.

#### Scenario: CONVERT varchar from datetime

- **GIVEN** SQL-файл с `CONVERT(varchar, @Date)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `dateIntoString`, severity 2

### Requirement: Сравнение даты с пустой строкой

Система SHALL обнаруживать сравнение даты с пустой строкой и формировать finding `emptyStringDate`.

#### Scenario: Сравнение с пустой строкой

- **GIVEN** SQL-файл с `WHERE @Date = ''`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `emptyStringDate`, severity 2

### Requirement: Использование переменной после закрытия курсора

Система SHALL обнаруживать использование переменной после закрытия курсора и формировать finding `varUseAfterCursor`.

#### Scenario: Переменная после CLOSE CURSOR

- **GIVEN** SQL-файл с `CLOSE MyCursor; SET @Var = @FetchVar` где `@FetchVar` была получена из курсора
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2

### Requirement: Использование курсоров без объявления

Система SHALL обнаруживать использование курсоров (OPEN, FETCH, CLOSE, DEALLOCATE) без предварительного объявления (DECLARE CURSOR) и формировать finding `useOnlyDeclaredCursors`.

#### Scenario: OPEN без DECLARE

- **GIVEN** SQL-файл с `OPEN MyCursor` без `DECLARE MyCursor CURSOR FOR`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useOnlyDeclaredCursors`, severity 2

### Requirement: Сравнение выражений разных типов

Система SHALL обнаруживать сравнение выражений разных типов в `WHERE`/`ON`/`IF`/`CASE WHEN` и формировать finding `diffTypesComparison`.

#### Scenario: Сравнение datetime и date

- **GIVEN** SQL-файл с `WHERE tA.DSDATETIME = tB.DSOPERDAY`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `diffTypesComparison`, severity 2

### Requirement: Приведение FLOAT к строке

Система SHALL обнаруживать приведение `FLOAT`/`DSFLOAT` к строке через `CONVERT`/`CAST` и формировать finding `floatToStringConvert`.

#### Scenario: CONVERT varchar from FLOAT

- **GIVEN** SQL-файл с `CONVERT(varchar, @FloatVal)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `floatToStringConvert`, severity 2

### Requirement: SELECT после SET ROWCOUNT без ORDER BY

Система SHALL обнаруживать `SELECT` в переменные и `INSERT...SELECT` после `SET ROWCOUNT N` без `ORDER BY` и формировать finding `selectAfterSetRowcount`.

#### Scenario: SET ROWCOUNT без ORDER BY

- **GIVEN** SQL-файл с `SET ROWCOUNT 10; SELECT @ID = ID FROM tContract`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `selectAfterSetRowcount`, severity 2

### Requirement: Избыточные параметры в вызове процедуры

Система SHALL обнаруживать передачу лишних или дублированных параметров в `EXEC` относительно сигнатуры процедуры из индекса и формировать finding `excessProcParams`.

#### Scenario: Лишний именованный параметр

- **GIVEN** проиндексированная процедура `MyProc` с параметрами `@A`, `@B` и SQL-файл с `EXEC MyProc @A = 1, @B = 2, @C = 3`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `excessProcParams`, severity 2

### Requirement: Дублирование OUTPUT-переменной

Система SHALL обнаруживать повторное использование одной и той же переменной для нескольких OUTPUT-параметров в одном `EXEC` и формировать finding `duplicateOutputVariable`.

#### Scenario: Одна переменная на два OUTPUT

- **GIVEN** SQL-файл с `EXEC MyProc @Out1 = @X OUTPUT, @Out2 = @X OUTPUT`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `duplicateOutputVariable`, severity 2

### Requirement: Несовпадение аргументов FETCH с DECLARE CURSOR

Система SHALL обнаруживать `FETCH ... INTO`, где число переменных не совпадает с числом выражений в `DECLARE CURSOR`, и формировать finding `cursorFetchArguments`.

#### Scenario: FETCH с другим числом переменных

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и `FETCH NEXT FROM c INTO @A`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `cursorFetchArguments`, severity 2

### Requirement: Использование переменной в том же SELECT

Система SHALL обнаруживать чтение переменной в том же `SELECT`, где она присваивается, и формировать finding `usageVarInSameSelect`.

#### Scenario: Чтение @Var в том же SELECT

- **GIVEN** SQL-файл с `SELECT @A = Col, @B = @A FROM tX`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `usageVarInSameSelect`, severity 2

### Requirement: Присвоение переменной в UPDATE SET

Система SHALL обнаруживать присвоение переменной (`@var = ...`) в списке `UPDATE ... SET` и формировать finding `varAssignInUpdate`.

#### Scenario: SET @Var в UPDATE

- **GIVEN** SQL-файл с `UPDATE tX SET @Var = Col, Name = @Name`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varAssignInUpdate`, severity 2

### Requirement: JOIN без алиасов таблиц

Система SHALL обнаруживать операторы с JOIN, где таблицы не имеют алиасов, и формировать finding `statementsWithJoinsRequireAliases`.

#### Scenario: JOIN без алиаса

- **GIVEN** SQL-файл с `SELECT tA.ID FROM tA JOIN tB ON tA.ID = tB.ID` без алиасов таблиц
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `statementsWithJoinsRequireAliases`, severity 2

### Requirement: Функция в индексированном столбце

Система SHALL обнаруживать применение функции к индексированному столбцу в условии фильтрации и формировать finding `useFuncInIndCol`.

#### Scenario: Функция над индексной колонкой

- **GIVEN** SQL-файл с `WHERE CONVERT(varchar, tX.IndexedCol) = @Val` где `IndexedCol` входит в индекс таблицы
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useFuncInIndCol`, severity 2

### Requirement: ISNULL с разными типами

Система SHALL обнаруживать вызов `ISNULL(arg1, arg2)`, где типы аргументов не эквивалентны, и формировать finding `isNullSameTypes`. Литерал во втором аргументе finding не формирует.

#### Scenario: ISNULL разных типов

- **GIVEN** SQL-файл с `ISNULL(@DateTimeVar, @DateVar)` где типы `DSDATETIME` и `DSOPERDAY`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `isNullSameTypes`, severity 2

### Requirement: Алиасы столбцов при UNION и ORDER BY

Система SHALL обнаруживать `UNION` с `ORDER BY` по имени, которое не совпадает с алиасом/именем колонки первого `SELECT`, и формировать finding `aliasWhenUsingUnion`.

#### Scenario: ORDER BY по имени из второго SELECT

- **GIVEN** SQL-файл с `SELECT A AS Name FROM t1 UNION SELECT B FROM t2 ORDER BY B`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `aliasWhenUsingUnion`, severity 2

## Related code

- `internal/review/review_rules.go` — реализация проверок (PostgreSQL requirements)
- `internal/review/types.go` — константы правил severity 2
- `internal/review/catalog.go` — каталог правил
- `internal/review/review_helpers.go` — вспомогательные функции (resolveArgType, и др.)
- `internal/review/review_lookup.go` — lookup-функции для типов данных

## Notes

- PostgreSQL requirements имеют severity=2 — не блокируют деплой, но требуют исправления
- Правило `useOnlyDeclaredCursors` поддерживает макросы `__DECLARE_CURSOR__` и `__DEALLOCATE_CURSOR__`
- Правило `diffTypesComparison` определяет потерю точности (datetime → date) как finding
- Правило `shouldBeCP866` проверяет строковые литералы на соответствие кодировке CP866
- Правила `deferredUpdate`, `inSubQuery`, `varcharSize`, `columnInsert` относятся к deploy-stoppers (severity 1), не к PostgreSQL requirements
- Execution-слой `internal/reviewsvc/runtime.go` — общая точка входа для CLI (`cmd/review.go`) и MCP-инструмента `codebase_review_sql`; устраняет дублирование оркестрации. Поведение review-команд специфицировано здесь на уровне правил; транспорт MCP — в `mcp-server/mcp-transport-tools`.

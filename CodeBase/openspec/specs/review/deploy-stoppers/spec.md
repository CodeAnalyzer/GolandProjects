# Deploy Stoppers

## Purpose

Статический анализ SQL-файлов для выявления критических проблем (severity=1), блокирующих деплой: 25 правил — несуществующие и дублированные процедуры, небезопасные конструкции, отсутствие required hints, неправильное использование индексов, запрещённые синтаксические паттерны.

## Requirements

### Requirement: Вызов несуществующих процедур

Система SHALL обнаруживать `EXEC`-вызовы процедур, отсутствующих в индексе `sql_procedures`, и формировать finding `execNotExistsProc`.

#### Scenario: Вызов несуществующей процедуры

- **GIVEN** SQL-файл с `EXEC NonExistentProc @Param = 1` и проиндексированный проект, где `NonExistentProc` не существует
- **WHEN** выполняется `codebase review file.sql`
- **THEN** сформирован finding с rule `execNotExistsProc`, severity 1

### Requirement: Дублирование процедур

Система SHALL обнаруживать дублирование имени процедуры в одном файле и формировать finding `procDuplicate`.

#### Scenario: Дублирование процедуры

- **GIVEN** SQL-файл с двумя определениями `CREATE PROCEDURE MyProc`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `procDuplicate`, severity 1

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

Система SHALL обнаруживать использование индексов, отсутствующих в БД, и формировать finding `indexExistsInDB`.

#### Scenario: Несуществующий индекс

- **GIVEN** SQL-файл с `WITH (INDEX(NonExistentIndex))` и БД, где индекс не существует
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `indexExistsInDB`, severity 1

### Requirement: Full scan условия

Система SHALL обнаруживать условия в WHERE/JOIN, приводящие к full scan (отсутствие SARGable предикатов), и формировать finding `tableFullScan`.

#### Scenario: Non-SARGable условие

- **GIVEN** SQL-файл с `WHERE SUBSTRING(Name, 1, 3) = 'abc'`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableFullScan`, severity 1

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

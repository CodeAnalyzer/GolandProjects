# Fine Code

## Purpose

Статический анализ SQL-файлов для выявления рекомендаций по качеству кода (severity=3): 4 правила — использование таблиц/p-таблиц/процедур из других продуктов и потенциальная потеря точности при assignment/conversion.

## Requirements

### Requirement: Использование таблиц из других продуктов

Система SHALL обнаруживать использование таблиц из других продуктов Diasoft и формировать finding `foreignTablesUsing`.

#### Scenario: Таблица из другого продукта

- **GIVEN** SQL-файл с `SELECT * FROM OtherProduct.tTable` где `OtherProduct` отличается от текущего продукта
- **WHEN** выполняется review с `--min-severity 3`
- **THEN** сформирован finding с rule `foreignTablesUsing`, severity 3, с указанием имени таблицы

### Requirement: Использование p-таблиц из других продуктов

Система SHALL обнаруживать использование p-таблиц (препроцессированных таблиц) из других продуктов и формировать finding `foreignPTablesUsing`.

#### Scenario: p-таблица из другого продукта

- **GIVEN** SQL-файл с `SELECT * FROM pOtherProduct_tTable` где префикс указывает на другой продукт
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `foreignPTablesUsing`, severity 3

### Requirement: Вызов процедур из других продуктов

Система SHALL обнаруживать вызов процедур из других продуктов Diasoft и формировать finding `foreignProcedureUsing`.

#### Scenario: Вызов внешней процедуры

- **GIVEN** SQL-файл с `EXEC OtherProduct_ProcName @Param = 1` где `OtherProduct_ProcName` принадлежит другому продукту
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `foreignProcedureUsing`, severity 3

### Requirement: Потенциальная потеря точности

Система SHALL обнаруживать потенциальную потерю точности при assignment (SELECT @var =, INSERT...SELECT) и conversion (convert/cast) между типами данных, и формировать finding `datatype`.

#### Scenario: Сужение datetime к date

- **GIVEN** SQL-файл с `SELECT @DateVar = CONVERT(DSOPERDAY, @DateTimeVar)` где `@DateVar` имеет тип `DSOPERDAY`, а `@DateTimeVar` — `DSDATETIME`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3, с сообщением о потере точности

#### Scenario: INSERT SELECT с потерей точности

- **GIVEN** SQL-файл с `INSERT INTO tTarget (DateCol) SELECT DateTimeCol FROM tSource` где `DateCol` имеет тип `DSOPERDAY`, а `DateTimeCol` — `DSDATETIME`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

#### Scenario: FETCH INTO с потерей точности

- **GIVEN** SQL-файл с `FETCH NEXT FROM MyCursor INTO @DateVar` где тип курсора шире типа `@DateVar`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

## Related code

- `internal/review/review_rules.go` — реализация проверок (fine code rules)
- `internal/review/types.go` — константы правил severity 3
- `internal/review/catalog.go` — каталог правил
- `internal/review/review_helpers.go` — `collectVariableTypes`, `enrichVariableTypesFromAPI`, `hasExplicitConversion`
- `internal/review/review_lookup.go` — `FindLatestSQLColumnDefinitionType`, `lookupProcedureParams`

## Notes

- Fine code rules имеют severity=3 — рекомендации, не блокирующие деплой
- Правило `datatype` поддерживает анализ `INSERT...SELECT`, `FETCH INTO`, `EXEC @param =` и `SELECT @var =`
- Правило `datatype` учитывает параметры API_CREATE_PROC через `enrichVariableTypesFromAPI`
- Явное преобразование (convert/cast к целевому типу) подавляет finding, если нет потери точности
- Продукты Diasoft определяются через каталог продуктов в БД (`db_products.go`)
- Execution-слой `internal/reviewsvc/runtime.go` — общая точка входа для CLI (`cmd/review.go`) и MCP-инструмента `codebase_review_sql`; устраняет дублирование оркестрации. Поведение review-команд специфицировано здесь на уровне правил; транспорт MCP — в `mcp-server/mcp-transport-tools`.

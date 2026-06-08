# Bug Report: `review` не выявляет потерю точности при `SELECT @var = column` (Numeric/Decimal)

**Дата:** 2026-06-08  
**Статус:** Open  
**Приоритет:** Medium-High  
**Компонент:** SQL Review (`datatype` rule)

## Описание проблемы

Команда `codebase review` не обнаруживает потенциальную потерю точности при присваивании значения колонки в переменную через конструкцию:

```sql
select @OpenAccBP = t.ID
  from tConsConfigParamSync t M_NOLOCK_INDEX(XPKtConsConfigParamSync)
 where t.SysName = 'CONSUMER_OPENACC_BANKPARTNER'
```

Для кейса выше корпоративный инспектор фиксирует замечание:

`Numeric (15, 0) -> Numeric (10, 0)`

Но `CodeBase review` возвращает только одно замечание по другому правилу (`procParamDefValue`).

## Файл воспроизведения

`C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_BankPartner_Update.sql`

## Шаги воспроизведения

```powershell
PS C:\NT\FA#\7.2GIT> codebase review C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_BankPartner_Update.sql
```

Фактический результат:
- Findings: 1
- `procParamDefValue` (line=12, object=isLongProcess)

Ожидаемый результат:
- Дополнительно finding по `datatype` (или отдельному правилу) для присваивания `@OpenAccBP = t.ID` с сообщением о потере точности `Numeric(15,0) -> Numeric(10,0)`.

## Технический анализ (корневая причина)

### 1) `datatype` проверяет только два шаблона операторов

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go`

- `checkDatatype` вызывает только:
  - `checkDatatypeInsertSelect` (примерно `:2049`)
  - `checkDatatypeUpdateSet` (примерно `:1986`)

Сценарий `SELECT @var = ... FROM ...` не анализируется.

### 2) Отсутствует парсинг `SELECT`-присваиваний в review parser

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_parser.go`

Есть разбор только:
- `parseUpdateSetStatement`
- `parseInsertSelectStatement`

Разбора конструкции `select @x = expr from ...` нет.

### 3) Логика потери точности покрывает только datetime/date

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go`

Функция `isPotentialPrecisionLoss` (примерно `:2177`) сравнивает только ранги даты/времени (`datetimePrecisionRank`) и возвращает `false` для numeric/decimal кейсов.

## Вывод

Замечание не появляется из-за комбинации двух ограничений:
1. нет проверки `SELECT @var = ...`;
2. нет numeric/decimal логики потери точности в `isPotentialPrecisionLoss`.

## Предлагаемое исправление

1. Добавить анализ `SELECT`-присваиваний, например новый чекер `checkDatatypeSelectAssign`:
   - извлекать пары `@variable <- expression` из `SELECT ... FROM ...`;
   - определять тип источника через `resolveExpressionTypes`;
   - определять тип приемника (переменной) из `DECLARE` и параметров процедуры.

2. Расширить `isPotentialPrecisionLoss`:
   - добавить разбор `numeric(p,s)` / `decimal(p,s)`;
   - считать потерю точности, если у target меньше `p` или `s` (с учетом бизнес-правил проекта).

3. Добавить unit-tests:
   - позитивный кейс: `numeric(15,0) -> numeric(10,0)` через `select @var = t.col from ...`;
   - негативный кейс: безопасное присваивание без потери точности;
   - кейс с явным `convert(...)` (должен suppress-ить finding, если conversion корректный).

## Влияние

- Потеря parity с корпоративным инспектором по важному классу ошибок типизации.
- Риск пропуска дефектов, связанных с усечением данных при присваивании в переменные.

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_parser.go`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_lookup.go`
- `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\Cons_BankPartner_Update.sql`

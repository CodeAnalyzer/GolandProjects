# Ложное срабатывание правила nullComparison на OUTPUT-параметрах с default = null

**Дата:** 2026-06-21  
**Файл:** C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Accrual\Cons_CalcAllPayments.sql  
**Правило:** nullComparison  
**Версия CodeBase:** 0.7.6 build 1017  
**Статус:** Не исправлено

## Описание

Правило `nullComparison` выдаёт ложные срабатывания на строки объявления OUTPUT-параметров хранимой процедуры, имеющих значение по умолчанию `= null output`. Правило интерпретирует `= null` как сравнение с NULL через оператор, хотя на самом деле это декларация параметра со значением по умолчанию.

## Найденные ложные срабатывания

### Строки 29–40 (объявление параметров процедуры Cons_CalcAllPayments)

```sql
DCL_PROC_BEGIN(Cons_CalcAllPayments)
                 @ContractID        DSIDENTIFIER,
                 @Date              DSOPERDAY,
                 @AmountDelinq      DSMONEY   = null output,
                 @AmountPrc         DSMONEY   = null output,
                 @AmountMain        DSMONEY   = null output,
                 @AmountWtPrc       DSMONEY   = null output,
                 @AmountTotal       DSMONEY   = null output,
                 @AmountCommis      DSMONEY   = null output,
                 @AmountPenaltyDebt DSMONEY   = null output,
                 @AmountPenaltyPrc  DSMONEY   = null output,
                 @Course            DSFLOAT   = null,
                 @AmountOverDebt    DSMONEY   = null output,
                 @AmountOverPrc     DSMONEY   = null output,
                 @OperAttr          DSINT_KEY = null
as
```

### Ложные срабатывания (10 finding'ов)

```
- [2] nullComparison line=29 object=@AmountDelinq      DSMONEY   = null output,
  Сравнение с NULL через оператор недопустимо, используйте IS NULL или IS NOT NULL
- [2] nullComparison line=30 object=@AmountPrc         DSMONEY   = null output,
  Сравнение с NULL через оператор недопустимо, используйте IS NULL или IS NOT NULL
- [2] nullComparison line=31 object=@AmountMain        DSMONEY   = null output,
  ... (и так для всех строк с `= null output,`)
```

### Корректные строки (НЕ вызывают срабатываний)

Строки 37 и 40 с `= null` без `output` правильно пропускаются:
```sql
                 @Course            DSFLOAT   = null,
                 @OperAttr          DSINT_KEY = null
```

## Почему это ложное срабатывание

1. **Это декларация параметра, а не сравнение:** Конструкция `@AmountDelinq DSMONEY = null output,` объявляет OUTPUT-параметр со значением по умолчанию `null`. Это стандартный синтаксис T-SQL для объявления параметров хранимой процедуры.
2. **Ключевое слово `output`:** Наличие ключевого слова `output` после `null` однозначно указывает на декларацию параметра, а не на операцию сравнения.
3. **Срабатывает только для OUTPUT-параметров:** Параметры с `= null` без `output` (строки 37, 40) правильно пропускаются существующей регуляркой `nullParamDefaultRe`.

## Анализ причины

### Регулярка `nullParamDefaultRe` (types.go, строка 136)

```go
var nullParamDefaultRe = regexp.MustCompile(`(?i)@\w+\s+\w+\s*=\s*null\s*,?\s*$`)
```

Регулярка ожидает, что после `null` идёт опциональный пробел, опциональная запятая и конец строки (`\s*,?\s*$`). Однако в строках с OUTPUT-параметрами между `null` и запятой находится ключевое слово `output`:

```
@AmountDelinq      DSMONEY   = null output,
                                    ^^^^^^
                                    это слово не учтено в регулярке
```

Регулярка не матчит строку, и она не пропускается на этапе фильтрации (строка 3160 в `review_rules.go`).

### Регулярка `nullComparisonBinaryRe` (types.go, строка 132)

```go
var nullComparisonBinaryRe = regexp.MustCompile(`(?i)(?:^|[^a-zA-Z_])((?:=|<>|!=|<=|>=|<|>)\s*null\b|\bnull\s*(?:=|<>|!=|<=|>=|<|>))`)
```

Поскольку строка не была пропущена `nullParamDefaultRe`, она попадает в проверку `nullComparisonBinaryRe`, которая находит `= null` и сообщает о сравнении с NULL.

### Логика проверки (review_rules.go, строки 3159–3176)

```go
// Пропускаем объявления параметров/переменных с дефолтом = null: @Name DSTYPE = null
if nullParamDefaultRe.MatchString(stripped) {
    continue
}

// Проверяем бинарные операторы: = NULL, <> NULL, < NULL и т.д.
if nullComparisonBinaryRe.MatchString(stripped) {
    findings = append(findings, Finding{
        Rule:             RuleNullComparison,
        Severity:         SeverityPostgreReq,
        Message:          "Сравнение с NULL через оператор недопустимо, используйте IS NULL или IS NOT NULL",
        ...
    })
    continue
}
```

## Предлагаемое исправление

### Вариант 1: Обновить регулярку `nullParamDefaultRe` (минимальное изменение)

**Файл:** `internal/review/types.go`, строка 136

**Было:**
```go
var nullParamDefaultRe = regexp.MustCompile(`(?i)@\w+\s+\w+\s*=\s*null\s*,?\s*$`)
```

**Стало:**
```go
var nullParamDefaultRe = regexp.MustCompile(`(?i)@\w+\s+\w+\s*=\s*null\s*(?:output\s*)?,?\s*$`)
```

Добавлена опциональная группа `(?:output\s*)?` между `null` и опциональной запятой, что позволяет регулярке матчить строки вида `@Name DSTYPE = null output,` и `@Name DSTYPE = null output`.

### Тест-кейсы для добавления

В `internal/review/review_rules_test.go`, функция `TestCheckNullComparison_NoFalsePositive`, добавить:

```go
{name: "proc param default null output", content: "@AmountDelinq      DSMONEY   = null output,"},
{name: "proc param default null output no comma", content: "@AmountPrc         DSMONEY   = null output"},
{name: "proc param default null output indented", content: "                 @AmountMain        DSMONEY   = null output,"},
```

## Файлы для изменения

1. **`internal/review/types.go`** — строка 136: обновить регулярку `nullParamDefaultRe`
2. **`internal/review/review_rules_test.go`** — функция `TestCheckNullComparison_NoFalsePositive`: добавить тест-кейсы для OUTPUT-параметров

## Ожидаемый результат

После исправления:
- На файле `Cons_CalcAllPayments.sql` правило `nullComparison` больше не срабатывает на строках 29–36, 38–39 (OUTPUT-параметры с `= null output`)
- Строки 37, 40 (параметры с `= null` без `output`) продолжают корректно пропускаться
- Все существующие unit-тесты проходят
- Новые тест-кейсы подтверждают исправление

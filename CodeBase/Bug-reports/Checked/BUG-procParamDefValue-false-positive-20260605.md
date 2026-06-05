# Ложное срабатывание правила procParamDefValue

**Дата:** 2026-06-05  
**Файл:** D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\ContractOver\ConsChngOMCVTermUndo.sql  
**Правило:** procParamDefValue  
**Версия CodeBase:** 0.7.4 build 844  
**Статус:** Исправлено (build 849)

## Описание

Правило `procParamDefValue` выдавало ложное срабатывание на параметр с `default=null`, даже если в теле процедуры есть явная инициализация этого параметра с помощью `select @param = isnull(@param, ...)`.

## Найденное ложное срабатывание

### Строка 4 (объявление параметра)
```sql
@ParentProtocolID DSIDENTIFIER = null
```

### Строка 25 (инициализация в теле процедуры)
```sql
select @ParentProtocolID = isnull(@ParentProtocolID, 0)
```

Полный контекст процедуры:
```sql
DCL_PROC_BEGIN(ConsChngOMCVTermUndo)
              @ContractID       DSIDENTIFIER,
              @InstrumentID     DSIDENTIFIER,
              @ParentProtocolID DSIDENTIFIER = null
as
  __BEGIN_PROCEDURE__(ConsChngOMCVTermUndo)
  M_BUSINESSLOG_BEGIN
  M_BUSINESSLOG_BLOCK_BEGIN('ConsChngOMCVTermUndo')

  declare @DealProtocolID DSIDENTIFIER

  select @ParentProtocolID = isnull(@ParentProtocolID, 0)
  ...
```

## Почему это ложное срабатывание

1. **Есть инициализация:** Параметр `@ParentProtocolID` инициализируется в начале процедуры с помощью `select @ParentProtocolID = isnull(@ParentProtocolID, 0)`
2. **Корректный паттерн:** Использование `isnull(@param, default_value)` является стандартным паттерном для обработки nullable параметров
3. **Правило не анализировало тело:** Функция `hasDefaultAssignmentInBody` в `internal/review/review_rules.go` всегда возвращала `false` с TODO-комментарием

## Анализ причины

В файле `internal/review/review_rules.go` (строки 1627-1631) была заглушка:

```go
func (r *Runner) hasDefaultAssignmentInBody(_ *model.SQLProcedure, _ string) bool {
	// Для Варианта 1 простой реализации - всегда возвращаем false
	// TODO: в будущем реализовать анализ тела процедуры (потребуются proc и paramName)
	return false
}
```

Правило проверяло только наличие `default=null` у параметра, но НЕ анализировало тело процедуры на наличие инициализации.

## Исправление

Реализован анализ тела процедуры в функции `hasDefaultAssignmentInBody`:

1. **Извлечение тела процедуры:** Добавлена функция `extractProcedureBody` для извлечения тела по границам строк `LineStart`/`LineEnd`
2. **Удаление комментариев:** Используется существующая функция `removeBlockComments` для удаления блок-комментариев `/* ... */`
3. **Пропуск заголовка:** Анализ начинается после ключевого слова `as` (заголовок процедуры с параметрами игнорируется)
4. **Поиск присваиваний:** Ищутся паттерны `select @param =` и `set @param =`
5. **Поиск использования:** Ищется первое использование параметра (кроме присваивания)
6. **Проверка порядка:** Правило срабатывает только если присваивание происходит ДО первого использования параметра

## Изменения в коде

**Файл:** `internal/review/review_rules.go`

- Изменена сигнатура `hasDefaultAssignmentInBody(procBody string, paramName string)` - теперь принимает тело процедуры как строку
- Добавлена функция `extractProcedureBody(lines []string, lineStart, lineEnd int)` для извлечения тела процедуры
- В `checkProcParamDefValue` добавлено чтение файла и извлечение тела для каждой процедуры
- Реализована логика анализа: поиск присваиваний и первого использования параметра

**Файл:** `internal/review/runner_test.go`

- Добавлены тесты `TestHasDefaultAssignmentInBody` с 8 кейсами:
  - Присваивание до использования (select isnull)
  - Присваивание до использования (set)
  - Присваивание после использования (должно сработать правило)
  - Без присваивания (должно сработать правило)
  - Присваивание без использования (OK)
  - Имя параметра без @
  - Присваивание в комментарии (должно сработать правило)
  - Реальное тело процедуры из ConsChngOMCVTermUndo
- Добавлены тесты `TestExtractProcedureBody` с 6 кейсами для проверки извлечения тела

## Результат

После исправления:
- На файле `ConsChngOMCVTermUndo.sql` правило больше не срабатывает: `Findings: 0`
- Все unit-тесты проходят
- Сборка успешна

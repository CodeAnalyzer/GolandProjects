# Bug: `datatype` — отсутствие проверки потери точности при `FETCH cursor INTO @var`

**Правило:** `datatype`  
**Severity:** 1 (стоппер деплоя)  
**Статус:** Closed  
**Дата обнаружения:** 2026-06-10  
**Файл:** `fa-contracts/Insurance/SERVER/Insurance/Insp_ActionPeriod_Copy.sql`

## Описание проблемы

Команда `codebase review` не обнаруживает потенциальную потерю точности при присваивании значений из курсора в переменные через конструкцию:

```sql
fetch ActionPeriod_cur into @ActionPeriodID,
                            @ActionID        ,
                            @SchemePeriodID  ,
                            @Priority        ,
                            @UpdateFlag      ,
                            @InsertFlag      ,
                            @DeleteFlag
```

Для этого кода корпоративный инспектор фиксирует замечания:

- `Переменная: @UpdateFlag - Происходит присваивание в меньший по размерности тип данных 'Numeric (10, 0)->Numeric (3, 0)'`
- `Переменная: @InsertFlag - Происходит присваивание в меньший по размерности тип данных 'Numeric (10, 0)->Numeric (3, 0)'`
- `Переменная: @DeleteFlag - Происходит присваивание в меньший по размерности тип данных 'Numeric (10, 0)->Numeric (3, 0)'`

Но `CodeBase review` не возвращает никаких замечаний по `datatype` для этих переменных.

## Типы данных в конфликте

| Переменная | Тип переменной | Поле в таблице `pIns_ActionPeriod` | Тип поля | Проблема |
|------------|----------------|-----------------------------------|----------|----------|
| `@UpdateFlag` | `DSTINYINT` (Numeric 3,0) | `UpdateFlag` | `DSINT_KEY` (Numeric 10,0) | Numeric(10,0) → Numeric(3,0) |
| `@InsertFlag` | `DSTINYINT` (Numeric 3,0) | `InsertFlag` | `DSINT_KEY` (Numeric 10,0) | Numeric(10,0) → Numeric(3,0) |
| `@DeleteFlag` | `DSTINYINT` (Numeric 3,0) | `DeleteFlag` | `DSINT_KEY` (Numeric 10,0) | Numeric(10,0) → Numeric(3,0) |

## Шаги воспроизведения

```powershell
PS C:\NT\FA#\7.2GIT> codebase review C:\NT\FA#\7.2GIT\fa-contracts\Insurance\SERVER\Insurance\Insp_ActionPeriod_Copy.sql
```

**Фактический результат:**
- Findings: 2
- `procParamDefValue` (line=11, object=Date)
- `ansiInJoin` (line=86, object=tInsurancePolicy)

**Ожидаемый результат:**
- Дополнительно findings по `datatype` для присваиваний `@UpdateFlag`, `@InsertFlag`, `@DeleteFlag` с сообщением о потере точности `Numeric(10,0) -> Numeric(3,0)`.

## Технический анализ (корневая причина)

### 1) `datatype` проверяет только три сценария

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go`

Функция `checkDatatype` (строка 1990) вызывает только:
- `checkDatatypeInsertSelect` — проверка `INSERT...SELECT`
- `checkDatatypeUpdateSet` — проверка `UPDATE...SET`
- `checkDatatypeSelectAssign` — проверка `SELECT @var = column FROM...`

Сценарий `FETCH cursor INTO @var1, @var2, ...` **не анализируется вообще**.

### 2) Отсутствует парсинг `FETCH INTO` в review parser

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_parser.go`

Есть разбор для:
- `parseUpdateSetStatement` — `UPDATE...SET`
- `parseInsertSelectStatement` — `INSERT...SELECT`
- `parseSelectAssignStatement` — `SELECT @var = ... FROM...`

**Разбора конструкции `FETCH cursor INTO @var, ...` нет.**

### 3) FETCH упоминается только как ключевое слово начала оператора

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_parser.go:352`

FETCH есть в списке ключевых слов для `isNewSQLStatement`, но только для разделения операторов:

```go
keywords := []string{"if", "exec", "select", "update", "delete", ..., "fetch", "close", "open", ...}
```

Для анализа типов `FETCH INTO` не используется.

### 4) Для проверки FETCH INTO требуется разрешение типов курсора

Особенность `FETCH INTO` — источником типов является не таблица напрямую, а **курсор**, объявленный ранее:

```sql
declare ActionPeriod_cur cursor local for
    select ActionPeriodID, ActionID, SchemePeriodID, ...
      from pIns_ActionPeriod ...
```

Для проверки типов нужно:
1. Найти объявление курсора (`DECLARE cursor CURSOR FOR ...`)
2. Определить типы колонок SELECT-запроса курсора
3. Сопоставить с типами переменных в `FETCH INTO`

## Ожидаемое поведение

Правило `datatype` должно:
- Анализировать конструкции `FETCH cursor INTO @var1, @var2, ...`
- Разрешать типы колонок через объявление курсора (`DECLARE CURSOR FOR ...`)
- Проверять совместимость типов источника (курсор) и приёмника (переменные)
- Выявлять потерю точности (например, `Numeric(10,0) -> Numeric(3,0)`)

## Предложение по исправлению

### Вариант 1: Полная поддержка FETCH INTO (рекомендуется)

1. **Добавить парсер `FETCH INTO`** в `review_parser.go`:
   ```go
   func parseFetchIntoStatement(queryText string) (fetchIntoStatement, bool)
   ```
   Структура:
   ```go
   type fetchIntoStatement struct {
       CursorName   string
       Variables    []string
   }
   ```

2. **Добавить разрешение типов курсора** в `review_lookup.go`:
   ```go
   func resolveCursorColumnTypes(cursorName string, parsed *sqlparser.ParseResult) []string
   ```
   - Искать `DECLARE cursorName CURSOR FOR ...`
   - Извлекать типы колонок из SELECT-запроса курсора

3. **Добавить чекер `checkDatatypeFetchInto`** в `review_rules.go`:
   - Вызывать из `checkDatatype`
   - Для каждой переменной в `FETCH INTO` определять тип источника (колонка курсора) и приёмника (DECLARE переменной)
   - Использовать `isPotentialPrecisionLoss` для проверки

4. **Добавить unit-tests**:
   - Позитивный кейс: `FETCH INTO` с потерей точности
   - Негативный кейс: `FETCH INTO` без потери точности
   - Кейс с несколькими колонками в курсоре

### Вариант 2: Упрощённая поддержка (покрывает 80% случаев)

Если разрешение типов курсора сложно реализовать:

1. Искать ближайший `DECLARE cursor CURSOR FOR select ... FROM table`
2. Извлекать типы колонок напрямую из таблицы (если курсор — простой `SELECT * FROM table` или `SELECT col1, col2 FROM table`)
3. Для сложных курсоров (с JOIN, подзапросами) — пропускать проверку или использовать эвристики

### Связанные правила, которые также не покрывают FETCH

- `useSelectAll` — проверка `SELECT *` в курсорах
- `tableHintExists` — проверка хинтов таблиц в курсорах
- `forceOrder2Tbl` — проверка `M_FORCEORDER` в курсорах

## Влияние

- **Потеря parity** с корпоративным инспектором по важному классу ошибок типизации.
- **Риск пропуска дефектов**, связанных с усечением данных при FETCH INTO, особенно для флагов и идентификаторов.
- **Неполное покрытие** `datatype` правила — разработчики могут полагаться на CodeBase, но пропускать критические ошибки.

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_parser.go`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_lookup.go`
- `C:\NT\FA#\7.2GIT\fa-contracts\Insurance\SERVER\Insurance\Insp_ActionPeriod_Copy.sql`
- `C:\NT\FA#\7.2GIT\fa-contracts\Insurance\SERVER\Insurance\pIns_ActionPeriod_TmpTbl.sql` — определение структуры таблицы

## Примеры других процедур с потенциальной проблемой

Для поиска аналогичных случаев можно использовать:

```powershell
# Поиск FETCH INTO с DSTINYINT переменными
grep -r "fetch.*into" --include="*.sql" | grep -i "tinyint"

# Поиск DECLARE CURSOR с последующим FETCH
grep -r -A 10 "declare.*cursor" --include="*.sql" | grep -B 5 -A 5 "fetch.*into"
```

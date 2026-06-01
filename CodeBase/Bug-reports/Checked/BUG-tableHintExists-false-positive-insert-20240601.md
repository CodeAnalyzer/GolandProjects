# Bug Report: Ложное срабатывание tableHintExists на INSERT-операциях

**Дата:** 2024-06-01
**Статус:** Open
**Приоритет:** Medium
**Компонент:** SQL Review - правило tableHintExists

## Описание проблемы

Правило `tableHintExists` ложно срабатывает на INSERT-операциях, требуя хинт индекса для таблиц. Однако для INSERT операций хинт индекса не требуется, так как нет выборки данных — данные просто вставляются в таблицу.

## Файл с ошибками

**Файл:** `C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\CCred_FixPenaltyAmount.sql`

## Ложные срабатывания (2 штуки)

| Строка | Правило | Таблица | Сообщение |
|--------|---------|---------|-----------|
| 317 | `tableHintExists` | `pconserrmass` | Таблица pconserrmass не имеет допустимого хинта индекса |
| 332 | `tableHintExists` | `pconserrmass` | Таблица pconserrmass не имеет допустимого хинта индекса |

## Проблемный код

Строка 317:
```sql
insert pConsErrMass M_P_WITH_ROWLOCK (SPID, ContractID, NTFID, NTFMessage, NTFMessageTmp)
select @@spid, @ObjectID, @RetVal, @Message, @MessageTmp
```

Строка 332:
```sql
insert pConsErrMass M_P_WITH_ROWLOCK (SPID, ContractID, NTFID, NTFMessage, NTFMessageTmp)
select @@spid, @ObjectID, @RetVal, @Message, @MessageTmp
```

## Причина ошибки

**Файл:** `Source/internal/review/review_rules.go`

Правило `tableHintExists` проверяет наличие хинта индекса для всех таблиц, не различая тип операции:

```go
// Псевдокод логики tableHintExists
for _, table := range parsed.Tables {
    if !hasIndexHint(table) {
        findings = append(findings, Finding{...})
    }
}
```

**Проблема:** Для INSERT операций индекс не используется для выборки данных. Хинт индекса имеет смысл только для:
- SELECT — выборка данных
- UPDATE — поиск строк для обновления
- DELETE — поиск строк для удаления

## Почему для INSERT не нужен хинт индекса

| Операция | Использование индекса | Нужен хинт |
|----------|----------------------|------------|
| SELECT | Да — для поиска данных | ✅ Да |
| UPDATE | Да — для поиска строк | ✅ Да |
| DELETE | Да — для поиска строк | ✅ Да |
| INSERT | Нет — просто добавление | ❌ Нет |

Для INSERT SQL Server/Sybase добавляет запись в таблицу и обновляет все индексы автоматически. Хинт конкретного индекса не имеет смысла.

## Рекомендации по исправлению

### Вариант 1: Исключить INSERT из проверки (рекомендуется)

Добавить проверку типа операции в `checkTableHintExists`:

```go
func (r *Runner) checkTableHintExists(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
    findings := make([]Finding, 0)

    for _, table := range parsed.Tables {
        // Пропускаем INSERT операции
        if table.Operation == "INSERT" || table.Operation == "INSERT_SELECT" {
            continue
        }

        if !hasIndexHint(table) {
            findings = append(findings, Finding{...})
        }
    }

    return findings, nil
}
```

### Вариант 2: Проверять контекст строки

Анализировать строку на наличие ключевого слова INSERT перед таблицей:

```go
func isInInsertContext(lines []string, tableLine int) bool {
    for i := tableLine - 1; i >= 0 && i >= tableLine - 5; i-- {
        line := strings.ToLower(lines[i])
        if strings.Contains(line, "insert") && !strings.Contains(line, "select") {
            return true
        }
        if strings.Contains(line, "select") {
            return false
        }
    }
    return false
}

// В checkTableHintExists:
if isInInsertContext(lines, table.Line) {
    continue
}
```

### Вариант 3: Проверять только SELECT, UPDATE, DELETE

Инвертировать логику — явно указывать операции, где нужен хинт:

```go
var operationsRequiringIndexHint = map[string]bool{
    "SELECT":       true,
    "UPDATE":       true,
    "DELETE":       true,
    "MERGE":        true,  // если поддерживается
    // INSERT не включён
}

func requiresIndexHint(operation string) bool {
    return operationsRequiringIndexHint[strings.ToUpper(operation)]
}
```

## Примеры валидного INSERT без хинта индекса

```sql
-- Все эти INSERT корректны без хинта индекса:
insert into tTable (col1, col2) values (1, 2)

insert tTable M_P_WITH_ROWLOCK (SPID, ID)  -- rowlock есть, но индекс не нужен
select @@spid, @ID

insert into tTable select * from sTable
```

## Особый случай: INSERT ... SELECT

Конструкция `INSERT ... SELECT FROM` содержит **разные типы таблиц**:

| Таблица | Роль | Нужен хинт индекса | Пример |
|---------|------|-------------------|--------|
| **Целевая** | INSERT INTO | ❌ Нет | `insert tTarget ...` |
| **Источники** | FROM, JOIN | ✅ Да | `from tSource s M_NOLOCK_INDEX(...)` |

### Пример разбора

```sql
insert tTarget M_P_WITH_ROWLOCK (col1, col2)  -- целевая таблица
select s.col1, j.col2
  from tSource s M_NOLOCK_INDEX(XPKtSource)      -- источник 1, хинт нужен
  join tJoin   j M_NOLOCK_INDEX(XPKtJoin)        -- источник 2, хинт нужен
 where s.Status = 1
```

**Ожидаемое поведение правила:**
- `tTarget` — **пропускаем**, это целевая таблица INSERT
- `tSource` — **проверяем**, хинт `XPKtSource` указан ✅
- `tJoin` — **проверяем**, хинт `XPKtJoin` указан ✅

### Важное уточнение к исправлению

Исправление должно различать **контекст** таблицы:

```go
func (r *Runner) checkTableHintExists(parsed *sqlparser.ParseResult, file *indexedFile) ([]Finding, error) {
    findings := make([]Finding, 0)

    for _, table := range parsed.Tables {
        // Пропускаем только целевые таблицы INSERT
        // Таблицы в FROM/JOIN должны проверяться!
        if table.Context == "INSERT_TARGET" {
            continue
        }

        if !hasIndexHint(table) {
            findings = append(findings, Finding{...})
        }
    }

    return findings, nil
}
```

**Ошибочное поведение (если не различать контекст):**
```sql
insert tTarget (col1, col2)           -- требует хинт? ❌ НЕТ (ложное)
select s.col1
  from tSource s                     -- требует хинт? ✅ ДА (валидно)
```

## Заключение

Требование хинта индекса для INSERT операций является технически некорректным. INSERT не использует индексы для поиска данных, поэтому хинт не имеет смысла. Это приводит к массовым ложным срабатываниям на валидном коде.

**Рекомендуемый приоритет:** Medium
**Сложность исправления:** Low
**Затронутое правило:** `tableHintExists`
**Частота:** Часто — INSERT используется повсеместно

**Примечание:** Исправление варианта 1 требует доступа к типу операции в `sqlparser.ParseResult`. Если это недоступно, вариант 2 (проверка контекста строки) является надёжным fallback.

# `rti_details`: неверный `elapsed_ms`, отсутствие `params` и `checkpoints` при загрузке из БД

**Дата:** 2026-07-10
**Файлы:** `internal/rti/parser.go`, `internal/rti/store.go`, `internal/mcp/registry.go`
**Версия CodeBase:** 0.8.3 build 1164
**Статус:** Не исправлено

## Описание

Команда `codebase_rti_details` при вызове с `session_id` возвращает:
1. **Неверный `elapsed_ms`** для всех вызовов — значения меньше реальных (баг парсера, проявляется и при `file_path`).
2. **Отсутствуют `params`** — параметры вызовов не загружаются из БД (сохраняются, но не читаются).
3. **Отсутствуют `checkpoints`** — чекпоинты не загружаются из БД (сохраняются, но не читаются).

### Затронутые поля

| Поле | `session_id` | `file_path` | Ожидаемое значение |
|------|-------------|-------------|-------------------|
| `elapsed_ms` | **неверно** (баг) | **неверно** (баг) | значение из RTI-лога |
| `params` | **отсутствует** (баг) | корректно | список параметров вызова |
| `checkpoints` | **отсутствует** (баг) | корректно | список чекпоинтов |

## Воспроизведение

### Тестовый лог

```
C:\NT\FA#\7.2GIT\logs\qtskt-742802\KDOTS11NEW_diasoft_KDO1MS68new_test_SRV_25.rti
Session ID: 18
Процедура: FCD_CNE_Deposit_FindListByOwner (4 вызова)
```

### Шаг 1: Вызвать `codebase_rti_details` через MCP

```
rti_details(session_id=18, procedure="FCD_CNE_Deposit_FindListByOwner")
```

### Шаг 2: Сравнить с прямым grep по RTI-логу

```powershell
grep "FCD_CNE_Deposit_FindListByOwner" log.rti
```

### Результат сравнения

| Вызов | `enter_line` | `exit_line` | `elapsed_ms` (MCP) | `elapsed_ms` (лог) | Разница |
|-------|-------------|-------------|---------------------|---------------------|---------|
| 1 | 13279 ✅ | 13569 ✅ | **233** ❌ | **267** | -34 |
| 2 | 13581 ✅ | 14721 ✅ | **716** ❌ | **733** | -17 |
| 3 | 14733 ✅ | 15023 ✅ | **156** ❌ | **170** | -14 |
| 4 | 15035 ✅ | 16735 ✅ | **860** ❌ | **876** | -16 |

`enter_line`, `exit_line`, `nest_level`, `module_id`, `module_name`, `ret_val` — **корректны**.
`elapsed_ms` — **неверно для всех 4 вызовов**.

`params` — **отсутствуют** в MCP ответе, хотя в логе есть:
```
@OwnerID                       : DSIDENTIFIER                   = 95022795
@OwnerBrief                    : DSUSERNAME                     = ...
```

`checkpoints` — **отсутствуют** в MCP ответе, хотя в логе есть:
```
ICT_GetMessageForCancel_Begin_1
ICT_GetMessageForCancel_Begin_3
```

`blog_tables` — **корректны** (4 таблицы с правильными колонками и данными).
`enrichment` — **корректен** (`source_file`, `line_start`, `line_end`, `params` процедуры).

---

## Анализ причины

### Баг 1: `elapsed_ms` назначается parent-вызову вместо завершённого

**Файл:** `internal/rti/parser.go`, строки 338–346

```go
// Elapsed
if m := reElapsed.FindStringSubmatch(line); m != nil {
    ms, _ := strconv.Atoi(m[1])
    stack := stacks[currentSPID]
    if len(stack) > 0 {
        stack[len(stack)-1].call.ElapsedMs = ms   // ← БАГ: назначает top-of-stack (parent)
    } else if lastExited != nil {
        lastExited.ElapsedMs = ms                  // ← работает только когда стек пуст
    }
    continue
}
```

**Логика ошибки:**

Структура RTI-лога для каждого вызова:
```
Enter ProcName ...        ← вызов помещается в стек
Elapsed, ms: 0            ← начальный elapsed (до Exit)
... параметры, тело ...
Exit ProcName ...         ← вызов снимается со стека, lastExited = этот вызов
Elapsed, ms: 267          ← реальный elapsed — должен назначаться lastExited
Return 0                  ← return value
```

После `Exit` (строка 222) вызов **уже снят со стека**: `stacks[currentSPID] = stack[:len(stack)-1]`. Если в стеке остался parent (что бывает почти всегда для вложенных вызовов), то `len(stack) > 0` = true, и `Elapsed, ms: 267` назначается **parent-вызову**, а не завершённому.

`lastExited` используется только если стек **полностью пуст** (root-вызов).

**Почему значения меньше реальных:**

`Elapsed, ms: 0` после `Enter` (начальный elapsed = 0) назначается корректно завершённому вызову через `lastExited` (когда стек пуст после root Exit) или через top-of-stack. Затем `Elapsed, ms: 267` после `Exit` назначается parent. В итоге завершённый вызов получает `elapsed_ms` от **последнего потомка** или от начального `Elapsed, ms: 0`, а parent получает реальное значение завершённого потомка.

Для вызовов без потомков (leaf): `Elapsed, ms: 0` после Enter назначается на себя (top of stack), затем `Elapsed, ms: N` после Exit назначается на parent (или lastExited если стек пуст). Leaf-вызов получает `elapsed_ms = 0`.

Для вызовов с потомками: вызов получает `elapsed_ms` последнего потомка, а не свой собственный.

Тот же баг влияет на **все** MCP-команды, использующие `elapsed_ms`: `rti_summary` (`top_slow`), `rti_slow`, `rti_tree`, `rti_errors`, `rti_timeline`.

### Баг 2: `params` не загружаются из БД

**Файл:** `internal/rti/store.go`, функция `LoadCalls`, строки 642–715

`LoadCalls` загружает:
- `rti_calls` — основные поля вызовов (строки 644–648)
- `rti_blog_blocks` — через `loadAllBLogBlocks` (строка 705)
- `rti_blog_tables` — через `loadAllBLogTables` (строка 710)

**Не загружается:**
- `rti_params` — параметры сохраняются через `insertRTIParams` (строки 36–38, 321–354), но функция `loadAllParams` **не существует** и не вызывается.

В результате при загрузке через `session_id` поле `Params` у всех вызовов пустое, и `json:"params,omitempty"` скрывает его в ответе.

### Баг 3: `checkpoints` не загружаются из БД

**Файл:** `internal/rti/store.go`, функция `LoadCalls`, строки 642–715

`checkpoints` сохраняются через `insertRTICheckpoints` (строки 41–43, 357–405), но функция `loadAllCheckpoints` **не существует** и не вызывается в `LoadCalls`.

В результате при загрузке через `session_id` поле `Checkpoints` у всех вызовов пустое, и `json:"checkpoints,omitempty"` скрывает его в ответе.

---

## Влияние

### `elapsed_ms` (Баг 1 — критический)

- **Все MCP-команды** (`rti_details`, `rti_slow`, `rti_summary.top_slow`, `rti_tree`, `rti_errors`, `rti_timeline`) возвращают неверные значения `elapsed_ms`.
- `rti_slow` может **не включать** медленные вызовы в результат или включать быстрые — фильтрация по порогу работает на неверных данных.
- `rti_summary.top_slow` содержит **неверный топ** — самые медленные вызовы могут оказаться в конце списка или отсутствовать.
- `rti_tree` показывает неверное время выполнения узлов.
- Анализ производительности RTI-сессий **невозможен** — все временные метки искажены.

### `params` (Баг 2 — высокий)

- `rti_details` при `session_id` не показывает параметры вызовов — невозможно анализировать входные данные процедур.
- `rti_tree` и `rti_errors` также не показывают параметры.
- Обогащение `enrichment.params` работает (загружается из CodeBase), но фактические значения параметров из лога — отсутствуют.

### `checkpoints` (Баг 3 — средний)

- `rti_details` при `session_id` не показывает чекпоинты — невозможно отслеживать этапы выполнения процедур.
- `rti_blog` читает чекпоинты отдельно через `rti_blog_blocks` / `rti_blog_tables`, но `rti_details` не использует `rti_blog` — он использует `LoadCalls` напрямую.

---

## Предлагаемое исправление

### Исправление Бага 1: `elapsed_ms`

**Файл:** `internal/rti/parser.go`, строки 338–346

Заменить логику: `Elapsed, ms:` после `Exit` должен назначаться `lastExited`, а не top-of-stack.

```go
// Elapsed
if m := reElapsed.FindStringSubmatch(line); m != nil {
    ms, _ := strconv.Atoi(m[1])
    stack := stacks[currentSPID]
    if len(stack) > 0 && lastExited == nil {
        // Elapsed после Enter (до Exit) — назначается текущему вызову на стеке
        stack[len(stack)-1].call.ElapsedMs = ms
    } else if lastExited != nil {
        // Elapsed после Exit — назначается завершённому вызову
        lastExited.ElapsedMs = ms
        lastExited = nil  // сброс, чтобы следующий Elapsed не перезаписал
    }
    continue
}
```

**Альтернативное решение:** Использовать `lastExited` всегда, когда он не nil, и только потом проверять стек:

```go
if lastExited != nil {
    lastExited.ElapsedMs = ms
    lastExited = nil
} else if len(stack) > 0 {
    stack[len(stack)-1].call.ElapsedMs = ms
}
```

Аналогичный баг существует для `Return` (строки 349–363) — `Return` после `Exit` также назначается top-of-stack вместо `lastExited`. Но `RetVal` обрабатывается раньше через `RetVal = 0#context` (строки 282–334), поэтому `Return` в большинстве случаев дублирует уже установленное значение.

### Исправление Бага 2: `params` не загружаются

**Файл:** `internal/rti/store.go`, функция `LoadCalls`, после строки 712

Добавить загрузку параметров:

```go
// Load params for all calls in one query
if err := loadAllParams(db, sessionID, calls); err != nil {
    return nil, fmt.Errorf("failed to load params: %w", err)
}
```

И реализовать функцию `loadAllParams`:

```go
func loadAllParams(db *store.DB, sessionID int64, calls []*RTICall) error {
    rows, err := db.Query(
        `SELECT call_id, name, type, value
         FROM rti_params
         WHERE call_id IN (SELECT id FROM rti_calls WHERE session_id = $1)
         ORDER BY id`,
        sessionID,
    )
    if err != nil {
        return err
    }
    defer rows.Close()

    callMap := make(map[int64]*RTICall)
    for _, c := range calls {
        callMap[c.ID] = c
    }

    for rows.Next() {
        var callID int64
        var p RTIParam
        if err := rows.Scan(&callID, &p.Name, &p.Type, &p.Value); err != nil {
            return err
        }
        if c, ok := callMap[callID]; ok {
            c.Params = append(c.Params, p)
        }
    }
    return rows.Err()
}
```

### Исправление Бага 3: `checkpoints` не загружаются

**Файл:** `internal/rti/store.go`, функция `LoadCalls`, после строки 712

Добавить загрузку чекпоинтов:

```go
// Load checkpoints for all calls in one query
if err := loadAllCheckpoints(db, sessionID, calls); err != nil {
    return nil, fmt.Errorf("failed to load checkpoints: %w", err)
}
```

И реализовать функцию `loadAllCheckpoints`:

```go
func loadAllCheckpoints(db *store.DB, sessionID int64, calls []*RTICall) error {
    rows, err := db.Query(
        `SELECT call_id, label, timestamp, elapsed_ms, line_no
         FROM rti_checkpoints
         WHERE call_id IN (SELECT id FROM rti_calls WHERE session_id = $1)
         ORDER BY id`,
        sessionID,
    )
    if err != nil {
        return err
    }
    defer rows.Close()

    callMap := make(map[int64]*RTICall)
    for _, c := range calls {
        callMap[c.ID] = c
    }

    for rows.Next() {
        var callID int64
        var cp RTICheckpoint
        var ts sql.NullTime
        if err := rows.Scan(&callID, &cp.Label, &ts, &cp.ElapsedMs, &cp.LineNo); err != nil {
            return err
        }
        if ts.Valid {
            cp.Timestamp = ts.Time
        }
        if c, ok := callMap[callID]; ok {
            c.Checkpoints = append(c.Checkpoints, cp)
        }
    }
    return rows.Err()
}
```

---

## Тесты

### Тест Бага 1: `TestParser_ElapsedMs_AssignedToLastExited`

```
Входной RTI-лог:
  Enter Parent @@NestLevel = 1
  Elapsed, ms: 0
  Enter Child @@NestLevel = 2
  Elapsed, ms: 0
  Exit Child
  Elapsed, ms: 50
  Return 0
  Exit Parent
  Elapsed, ms: 100
  Return 0

Ожидаемый результат:
  Parent.ElapsedMs = 100
  Child.ElapsedMs = 50

Фактический результат (до исправления):
  Parent.ElapsedMs = 50  (от последнего потомка)
  Child.ElapsedMs = 0    (от начального Elapsed после Enter)
```

### Тест Бага 2: `TestLoadCalls_LoadsParams`

```
1. Спарсить RTI-лог с параметрами через ParseFile
2. Сохранить в БД через SaveSession
3. Загрузить через LoadCalls
4. Проверить: calls[0].Params содержит параметры из лога

Фактический результат (до исправления): calls[0].Params = nil
```

### Тест Бага 3: `TestLoadCalls_LoadsCheckpoints`

```
1. Спарсить RTI-лог с чекпоинтами через ParseFile
2. Сохранить в БД через SaveSession
3. Загрузить через LoadCalls
4. Проверить: calls[0].Checkpoints содержит чекпоинты из лога

Фактический результат (до исправления): calls[0].Checkpoints = nil
```

---

## Файлы для изменения

1. **`internal/rti/parser.go`**, строки 338–346: исправить логику назначения `ElapsedMs` — приоритет `lastExited` над top-of-stack. Аналогично проверить строки 349–363 (обработка `Return`).
2. **`internal/rti/store.go`**, функция `LoadCalls` (строки 642–715): добавить вызовы `loadAllParams` и `loadAllCheckpoints` после `loadAllBLogTables`.
3. **`internal/rti/store.go`**: добавить функции `loadAllParams` и `loadAllCheckpoints`.

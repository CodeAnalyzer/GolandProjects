# Оптимизация RTI-анализа для очень больших файлов

План оптимизации производительности RTI-инструментов (MCP + CLI) при работе с файлами размером 5+ ГБ (1.45M+ вызовов): переход от загрузки всех данных в память к server-side SQL-запросам с фильтрацией и лимитами.

---

## Фаза 1: Фикс O(n²) parent-child linking в LoadCalls (критично)

**Проблема:** `LoadCalls` в `internal/rti/store.go:690-700` использует двойной цикл по всем calls для построения Children — O(n²). Для 1.45M вызовов это миллиарды сравнений.

**Изменения:**

- `internal/rti/store.go` — функция `LoadCalls`: заменить двойной цикл на map-based lookup
  ```go
  callMap := make(map[int64]*RTICall, len(calls))
  for _, c := range calls { callMap[c.ID] = c }
  for _, c := range calls {
      if c.ParentID != nil {
          if p, ok := callMap[*c.ParentID]; ok {
              p.Children = append(p.Children, c.ID)
          }
      }
  }
  ```

- Добавить unit-тест `TestLoadCallsParentChildMap` (mock БД или тест на малом наборе)

**Проверка:** `go test ./internal/rti/... -count=1`

---

## Фаза 2: Server-side SQL-запросы для rti_summary (критично)

**Проблема:** `loadRTIFromArgs` грузит все 1.45M calls + params + checkpoints + blog_blocks + blog_tables в память, чтобы посчитать summary. Summary можно вычислить одним SQL-запросом.

**Изменения:**

- `internal/rti/store.go` — новая функция `LoadSummary(db, sessionID) (*RTISummary, error)`:
  ```sql
  -- агрегаты из rti_calls
  SELECT count(*),
         count(*) FILTER (WHERE ret_val IS NOT NULL AND ret_val != 0),
         max(elapsed_ms), max(nest_level),
         count(*) FILTER (WHERE elapsed_ms >= 100)
  FROM rti_calls WHERE session_id = $1;

  -- top 10 slow (без params/checkpoints/blog)
  SELECT id, procedure, enter_line, exit_line, enter_time, exit_time,
         elapsed_ms, nest_level, module_id, module_name, tran_count,
         begin_cnt, ret_val, ret_val_context, parent_id, spid
  FROM rti_calls WHERE session_id = $1 ORDER BY elapsed_ms DESC LIMIT 10;
  ```
  + клиентские агрегаты из `rti_client_events` (count, errors, slow_sql)

- `internal/rti/store.go` — новая функция `LoadTopSlowClientSQL(db, sessionID, n)` — top-N клиентских SQL-блоков

- `internal/mcp/registry.go` — `codebase_rti_summary` handler: при `session_id > 0` использовать `LoadSummary` вместо `loadRTIFromArgs`

- `cmd/rti.go` — `runRTISummary`: при `--session > 0` использовать `LoadSummary` вместо `loadRTICalls`

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 3: Server-side SQL для rti_slow с limit (критично)

**Проблема:** `rti_slow` грузит все 1.45M calls, фильтрует в Go, возвращает 160K записей. Нужно: SQL с WHERE + ORDER BY + LIMIT.

**Изменения:**

- `internal/rti/store.go` — новая функция `LoadSlowCalls(db, sessionID, thresholdMs, limit int) ([]*RTICall, error)`:
  ```sql
  SELECT ... FROM rti_calls
  WHERE session_id = $1 AND elapsed_ms >= $2
  ORDER BY elapsed_ms DESC LIMIT $3;
  ```
  Не загружает params/checkpoints/blog — только базовые поля вызова.

- `internal/rti/store.go` — новая функция `LoadSlowClientSQL(db, sessionID, thresholdSec float64, limit int) ([]*RTIClientEvent, error)`:
  ```sql
  SELECT ... FROM rti_client_events
  WHERE session_id = $1 AND kind = 'sql_block'
  ORDER BY elapsed_ms DESC LIMIT $2;
  ```
  (клиентские SQL-блоки с duration >= threshold)

- `internal/mcp/registry.go` — `codebase_rti_slow` handler:
  - Добавить параметр `limit` (intProp, default 100, max 1000) в InputSchema
  - При `session_id > 0` использовать `LoadSlowCalls` + `LoadSlowClientSQL` вместо `loadRTIFromArgs`
  - Params/checkpoints грузить только для возвращённого top-N через `LoadParamsForCalls(db, callIDs)` и `LoadCheckpointsForCalls(db, callIDs)`

- `cmd/rti.go` — `runRTISlow`: при `--session > 0` использовать `LoadSlowCalls` + `LoadSlowClientSQL`
  - Добавить флаг `--limit` (default 100)

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 4: Server-side SQL для rti_errors с limit (высокий приоритет)

**Проблема:** `rti_errors` грузит все 1.45M calls, фильтрует по ret_val != 0 в Go. 194K ошибок возвращается полностью.

**Изменения:**

- `internal/rti/store.go` — новая функция `LoadErrorCalls(db, sessionID, limit int) ([]*RTICall, error)`:
  ```sql
  SELECT ... FROM rti_calls
  WHERE session_id = $1 AND ret_val IS NOT NULL AND ret_val != 0
  ORDER BY id LIMIT $2;
  ```

- `internal/rti/store.go` — новая функция `LoadClientErrors(db, sessionID, limit int) ([]*RTIClientEvent, error)`:
  ```sql
  SELECT ... FROM rti_client_events
  WHERE session_id = $1 AND kind = 'error' AND payload->>'error_text' != ''
  ORDER BY id LIMIT $2;
  ```

- `internal/mcp/registry.go` — `codebase_rti_errors` handler:
  - Добавить параметр `limit` (intProp, default 100, max 1000)
  - При `session_id > 0` использовать `LoadErrorCalls` + `LoadClientErrors`

- `cmd/rti.go` — `runRTIErrors`: при `--session > 0` использовать `LoadErrorCalls` + `LoadClientErrors`
  - Добавить флаг `--limit` (default 100)

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 5: Server-side SQL для rti_details и rti_blog (высокий приоритет)

**Проблема:** `rti_details` и `rti_blog` грузят все 1.45M calls, потом фильтруют по имени процедуры в Go. Нужно: SQL с WHERE procedure = $name.

**Изменения:**

- `internal/rti/store.go` — новая функция `LoadCallsByProcedure(db, sessionID, procName string, limit int) ([]*RTICall, error)`:
  ```sql
  SELECT ... FROM rti_calls
  WHERE session_id = $1 AND procedure = $2
  ORDER BY id LIMIT $3;
  ```
  + загрузка params/checkpoints/blog только для найденных вызовов.

- `internal/mcp/registry.go` — `codebase_rti_details` и `codebase_rti_blog` handlers:
  - При `session_id > 0` использовать `LoadCallsByProcedure` вместо `loadRTIFromArgs`
  - Добавить параметр `limit` (default 100, max 1000) к `rti_details`

- `cmd/rti.go` — `runRTIDetails` и `runRTIBlog`: при `--session > 0` использовать `LoadCallsByProcedure`
  - Добавить флаг `--limit` (default 100) к details

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 6: Server-side SQL для rti_tree (средний приоритет)

**Проблема:** `rti_tree` грузит все calls для построения дерева. Для больших файлов дерево может быть огромным.

**Изменения:**

- `internal/rti/store.go` — новая функция `LoadCallsForTree(db, sessionID, rootProcedure string, maxDepth int) ([]*RTICall, error)`:
  - Если `rootProcedure` задан: загрузить корневой вызов + всех потомков через recursive CTE:
    ```sql
    WITH RECURSIVE call_tree AS (
      SELECT * FROM rti_calls WHERE session_id = $1 AND procedure = $2
      UNION ALL
      SELECT c.* FROM rti_calls c JOIN call_tree t ON c.parent_id = t.id
      WHERE c.session_id = $1
    )
    SELECT * FROM call_tree LIMIT 5000;
    ```
  - Если `rootProcedure` пустой: найти NestLevel=1 вызов с max descendants (через SQL), затем recursive CTE от него
  - LIMIT 5000 для защиты от гигантских деревьев

- `internal/mcp/registry.go` — `codebase_rti_tree` handler: при `session_id > 0` использовать `LoadCallsForTree`

- `cmd/rti.go` — `runRTITree`: при `--session > 0` использовать `LoadCallsForTree`

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 7: Server-side SQL для rti_timeline и rti_client_tree (средний приоритет)

**Проблема:** Оба инструмента грузят все calls + все client_events в память.

**Изменения:**

- `internal/rti/store.go` — новые функции:
  - `LoadCallsTimeRange(db, sessionID, timeFrom, timeTo *time.Time, procedure string, limit int)` — серверные вызовы с фильтром по времени/процедуре
  - `LoadClientEventsFiltered(db, sessionID, filter TimelineFilter, limit int)` — клиентские события с фильтром

- `internal/mcp/registry.go` — `codebase_rti_timeline` и `codebase_rti_client_tree` handlers:
  - Добавить параметр `limit` (default 100, max 1000)
  - При `session_id > 0` использовать server-side SQL с фильтрами

- `cmd/rti.go` — `runRTITimeline` и `runRTIClientTree`: аналогично

**Проверка:** `go test ./internal/rti/... ./internal/mcp/... -count=1` + `go build ./...`

---

## Фаза 8: Оптимизация insertRTICalls — batch UPDATE parent_id (средний приоритет)

**Проблема:** После COPY IN вставки 1.45M строк, `insertRTICalls` делает 1.45M индивидуальных `UPDATE rti_calls SET parent_id = $1 WHERE id = $2` для remap parent_id. Это десятки минут.

**Изменения:**

- `internal/rti/store.go` — `insertRTICalls`:
  - Вместо 1.45M UPDATE — создать temp table `rti_id_map(original_id, new_id)`, заполнить через COPY IN
  - Затем один batch UPDATE:
    ```sql
    UPDATE rti_calls c SET parent_id = m.new_id
    FROM rti_id_map m
    WHERE c.parent_id = m.original_id AND c.session_id = $1;
    ```
  - Или: использовать `RETURNING id` при COPY IN (но pq.CopyIn не поддерживает RETURNING) — альтернатива через `INSERT ... SELECT ... RETURNING` batch

**Проверка:** `go test ./internal/rti/... -count=1` + `go build ./...`

---

## Фаза 9: Опциональные lazy-load функции для params/checkpoints/blog (низкий приоритет)

**Проблема:** `LoadCalls` грузит params/checkpoints/blog для всех вызовов. Для rti_slow/rti_errors нужны только базовые поля, а params только для top-N.

**Изменения:**

- `internal/rti/store.go` — новые функции:
  - `LoadParamsForCalls(db, callIDs []int64) (map[int64][]RTIParam, error)` — params для конкретных call IDs
  - `LoadCheckpointsForCalls(db, callIDs []int64) (map[int64][]RTICheckpoint, error)`
  - `LoadBLogBlocksForCalls(db, sessionID, callIDs []int64) (map[int64][]RTIBLogBlock, error)`
  - `LoadBLogTablesForCalls(db, sessionID, callIDs []int64) (map[int64][]RTIBLogTable, error)`

- `LoadCalls` — добавить параметр `withDetails bool` (или `LoadCallsLite` — без params/checkpoints/blog)

**Проверка:** `go test ./internal/rti/... -count=1` + `go build ./...`

---

## Фаза 10: Добавить DB-индексы для оптимизации (низкий приоритет)

**Изменения:**

- `internal/store/db_schema.go` — добавить индексы:
  ```sql
  CREATE INDEX IF NOT EXISTS idx_rti_calls_session_elapsed ON rti_calls(session_id, elapsed_ms DESC);
  CREATE INDEX IF NOT EXISTS idx_rti_calls_session_retval ON rti_calls(session_id, ret_val) WHERE ret_val IS NOT NULL AND ret_val != 0;
  CREATE INDEX IF NOT EXISTS idx_rti_calls_session_procedure ON rti_calls(session_id, procedure);
  CREATE INDEX IF NOT EXISTS idx_rti_calls_session_parent ON rti_calls(session_id, parent_id);
  CREATE INDEX IF NOT EXISTS idx_rti_client_events_session_kind ON rti_client_events(session_id, kind);
  ```

**Проверка:** `go build ./...` + запуск `codebase init`/`codebase health` для проверки схемы

---

## Сводка по файлам

| Файль | Фазы | Тип изменений |
|-------|-------|---------------|
| `internal/rti/store.go` | 1-9 | Новые SQL-функции, фикс O(n²), batch UPDATE |
| `internal/mcp/registry.go` | 2-7 | Handlers используют server-side SQL, добавлен limit |
| `cmd/rti.go` | 2-7 | CLI использует server-side SQL, добавлен --limit |
| `internal/store/db_schema.go` | 10 | Новые индексы |
| `internal/rti/store_test.go` | 1-9 | Unit-тесты для новых функций |
| `internal/mcp/server_test.go` | 2-7 | Тесты для limit параметров |

## Ожидаемый результат

- `rti_summary` на session_id=21: **< 1 сек** вместо 2+ часов
- `rti_slow` на session_id=21: **< 1 сек** (100 записей вместо 160K)
- `rti_errors` на session_id=21: **< 1 сек** (100 записей вместо 194K)
- `rti_tree` на session_id=21: **< 5 сек** (recursive CTE, max 5000 узлов)
- Парсинг 5.5 ГБ файла: batch UPDATE вместо 1.45M индивидуальных — ускорение в ~10-50x

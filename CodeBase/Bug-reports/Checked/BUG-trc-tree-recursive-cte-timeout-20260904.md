# Bug Report: `codebase_trc_tree` — timeout 60s из-за неоптимального recursive CTE

## Summary

MCP-инструмент `codebase_trc_tree` при вызове с `session_id` и фильтром по `procedure` регулярно завершается с ошибкой `context deadline exceeded` (таймаут `query_timeout_sec = 60`). Причина — неоптимальный recursive CTE в `LoadEventsForTree`, который сканирует все события сессии и выполняет избыточные JOIN на каждом шаге рекурсии.

Пример ошибки из лога:

```
profile=trc tool=codebase_trc_tree args=procedure="FCD_CCred_MassProcessByFOActID_MT" duration=1m0.056s status=error error="context deadline exceeded"
```

---

## Environment

- Tool: `codebase_trc_tree` (MCP, profile=trc)
- TRC-файл: `C:\NT\FA#\7.2GIT\logs\RMS-3965352\Трейс после оптимизации покупки 6.trc`
- Session ID: 11
- SPID: 95
- Procedure: `FCD_CCred_MassProcessByFOActID_MT`
- CodeBase: актуальная сборка

---

## Reproduction Steps

1. Спарсить TRC-файл через `codebase_trc_parse` → получить `session_id` (например, 11).
2. Вызвать `codebase_trc_tree` с `session_id=11`, `spid=95`, `procedure="FCD_CCred_MassProcessByFOActID_MT"`.
3. Запрос завершается с ошибкой `context deadline exceeded` через 60 секунд.

### Лог

```
profile=trc tool=codebase_trc_parse args=file_path="...Трейс после оптимизации покупки 6.trc" duration=4.729s status=success
profile=trc tool=codebase_trc_events args=procedure="FCD_CCred_MassProcessByFOActID_MT" duration=457ms status=success
profile=trc tool=codebase_trc_tree args=procedure="FCD_CCred_MassProcessByFOActID_MT" duration=1m0.061s status=error error="context deadline exceeded"
profile=trc tool=codebase_trc_tree args=procedure="FCD_CCred_MassProcessByFOActID_MT" duration=1m0.069s status=error error="context deadline exceeded"
profile=trc tool=codebase_trc_tree args=procedure="FCD_CCred_MassProcessByFOActID_MT" duration=1m0.066s status=error error="context deadline exceeded"
```

---

## Expected Result

`codebase_trc_tree` возвращает дерево вызовов для указанной процедуры за разумное время (< 5 секунд для сессии с сотнями тысяч событий).

---

## Actual Result

Таймаут 60 секунд на каждом вызове. Инструмент фактически нефункционален для больших трейсов при работе с `session_id`.

---

## Root Cause Analysis

### Проблема 1 — `numbered` CTE сканирует ВСЕ события сессии без фильтра по SPID

**Файл:** `internal/trc/store.go`, функция `LoadEventsForTree`, строка 730

```sql
WITH RECURSIVE numbered AS (
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM trc_events WHERE session_id = $1
)
```

`numbered` нумерует **все** события сессии, а не только события нужного SPID. Если в сессии 500K событий на 50 SPID'ов, а SPID 95 имеет 10K — `numbered` всё равно материализует и сортирует все 500K строк.

### Проблема 2 — `parent_id` хранит 1-based offset, а не реальный `id`

**Файл:** `internal/trc/store.go`, строка 107

```go
parentID = ev.ParentID + 1 // 1-based offset within session
```

`ComputeParentIDs` (`internal/trc/tree.go:173`) вычисляет `ParentID` как индекс в срезе `events` (0-based). При сохранении добавляется +1, чтобы получить 1-based offset. Это сделано потому что `id` генерируется БД (`BIGSERIAL`), и в момент вычисления `ParentID` реальный `id` ещё неизвестен.

Из-за этого recursive CTE не может напрямую JOIN `c.parent_id = t.id` — нужен промежуточный `numbered` CTE для маппинга offset → id на каждом шаге рекурсии.

### Проблема 3 — избыточный JOIN `nc` в рекурсивной части

**Файл:** `internal/trc/store.go`, строка 744

```sql
JOIN numbered nc ON c.id = nc.id   -- ← ВСЕГДА TRUE, т.к. c уже фильтруется по session_id
```

Этот JOIN проверяет, что `c.id` присутствует в `numbered`. Но `numbered` содержит все события сессии, а `c` уже отфильтрован по `c.session_id = $1`. JOIN **всегда выполняется** и добавляет O(M) hash probe на каждом шаге рекурсии для пустой проверки, где M = общее число событий сессии.

### Полный текст проблемного запроса

```sql
WITH RECURSIVE numbered AS (
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM trc_events WHERE session_id = $1
),
tree AS (
    SELECT e.event_class, e.event_name, e.procedure, e.duration_ms,
           e.params, e.columns, e.parent_id, e.depth, e.id, 1 AS tree_depth
    FROM trc_events e
    JOIN numbered n ON e.id = n.id
    WHERE e.session_id = $1 AND e.spid = $2 AND e.procedure = $4
    UNION ALL
    SELECT c.event_class, c.event_name, c.procedure, c.duration_ms,
           c.params, c.columns, c.parent_id, c.depth, c.id, t.tree_depth + 1
    FROM trc_events c
    JOIN numbered nc ON c.id = nc.id           -- ← избыточный JOIN
    JOIN numbered np ON c.parent_id = np.rn    -- ← маппинг offset→id
    JOIN tree t ON np.id = t.id
    WHERE c.session_id = $1 AND c.spid = $2 AND ($3 = 0 OR t.tree_depth < $3)
)
SELECT event_class, event_name, procedure, duration_ms, params, columns,
       parent_id, depth
FROM tree
```

### Сложность

Каждый шаг рекурсии:
- Сканирует `trc_events c` по `(session_id, spid)` — использует индекс `idx_trc_events_session_spid`, OK
- JOIN с `numbered nc` — hash join, O(M) где M = **все** события сессии
- JOIN с `numbered np` — hash join, O(M)
- JOIN с `tree t` — O(K) где K = узлы дерева на текущем уровне

Итого: **O(D × M)** hash probes, где D = глубина дерева, M = общее число событий сессии. Для большой сессии (500K событий, глубина 20) — миллионы операций хеширования.

### Доступные индексы

```
idx_trc_events_session_spid     ON trc_events(session_id, spid)
idx_trc_events_session_proc     ON trc_events(session_id, procedure)
idx_trc_events_session_parent   ON trc_events(session_id, parent_id)
```

Индексы есть, но `numbered` CTE не может их использовать — `row_number() OVER (ORDER BY id)` требует полной сортировки.

---

## Impact

- `codebase_trc_tree` с `session_id` **не работает** для больших трейсов (Timeout 60s).
- Инструмент `codebase_trc_events` (без дерева) работает быстро (457ms) — значит проблема именно в CTE, а не в объёме данных.
- Пользователи не могут получить дерево вызовов для анализа производительности TRC-трейсов через MCP.

---

## Suggested Fix

### Вариант 1 (минимальный, быстрый фикс): убрать избыточный JOIN + отфильтровать `numbered` по SPID

Убрать `JOIN numbered nc ON c.id = nc.id` (всегда true) и добавить SPID-фильтр в `numbered`:

```sql
WITH RECURSIVE numbered AS (
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM trc_events WHERE session_id = $1 AND spid = $2
),
tree AS (
    ... anchor ...
    UNION ALL
    SELECT c.*, t.tree_depth + 1
    FROM trc_events c
    JOIN numbered np ON c.parent_id = np.rn
    JOIN tree t ON np.id = t.id
    WHERE c.session_id = $1 AND c.spid = $2 AND ($3 = 0 OR t.tree_depth < $3)
)
```

**Важно:** `parent_id` — это offset в рамках **всей сессии**, а не в рамках SPID. Фильтрация `numbered` по SPID сломает маппинг, если parent и child на разных SPID. Нужно предварительно проверить, что все parent-child пары всегда на одном SPID (скорее всего так и есть, т.к. `ComputeParentIDs` группирует по SPID).

### Вариант 2 (правильный, более инвазивный): хранить реальный `id` вместо offset

После `COPY IN` выполнить `UPDATE` для маппинга offset → id:

```sql
UPDATE trc_events e
SET parent_id = n.id
FROM (
    SELECT id, row_number() OVER (ORDER BY id) AS rn
    FROM trc_events WHERE session_id = $1
) n
WHERE e.session_id = $1 AND e.parent_id = n.rn
```

После этого recursive CTE упрощается до:

```sql
WITH RECURSIVE tree AS (
    SELECT e.*, 1 AS tree_depth
    FROM trc_events e
    WHERE e.session_id = $1 AND e.spid = $2 AND e.procedure = $4
    UNION ALL
    SELECT c.*, t.tree_depth + 1
    FROM trc_events c
    JOIN tree t ON c.parent_id = t.id
    WHERE c.session_id = $1 AND c.spid = $2 AND ($3 = 0 OR t.tree_depth < $3)
)
```

Никакого `numbered` CTE, прямой JOIN по индексу `idx_trc_events_session_parent`.

**Минус:** дополнительный `UPDATE` после вставки (один раз при парсинге, не при каждом запросе дерева).

### Вариант 3 (компромиссный): materialize `numbered` как temp table с индексом

```sql
CREATE TEMP TABLE tmp_numbered AS
SELECT id, row_number() OVER (ORDER BY id) AS rn
FROM trc_events WHERE session_id = $1;

CREATE INDEX ON tmp_numbered(rn);

WITH RECURSIVE tree AS (
    ... JOIN tmp_numbered np ON c.parent_id = np.rn ...
)
```

Temp table с индексом на `rn` делает lookup O(log N) вместо O(N) hash probe.

---

## Тесты

- `TestLoadEventsForTree_LargeSession_NoTimeout` — создать сессию с 100K+ событий на нескольких SPID, убедиться что `LoadEventsForTree` с фильтром по SPID выполняется < 5 секунд.
- `TestLoadEventsForTree_ProcedureFilter` — убедиться что фильтр по `procedure` в anchor CTE корректно ограничивает дерево.
- `TestLoadEventsForTree_ParentChildSameSPID` — проверить что все parent-child пары на одном SPID (для Варианта 1).

---

## Связанные файлы

1. **`internal/trc/store.go`** — функция `LoadEventsForTree` (строки 695-777), функция `insertTRCEvents` (строки 65-141, сохранение `parent_id` как offset)
2. **`internal/trc/tree.go`** — функция `ComputeParentIDs` (строки 173-248, вычисление `ParentID` как индекса в срезе)
3. **`internal/trcsvc/runtime.go`** — функция `ExecuteTree` (строки 200-230, вызов `LoadEventsForTree`)
4. **`internal/store/db_schema.go`** — схема `trc_events` (строки 518-550), индексы (строки 671-676)

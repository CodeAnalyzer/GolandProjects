## Context

`codebase_trc_tree` при работе с сохранёнными сессиями (session_id > 0) использует recursive CTE в `LoadEventsForTree` (`internal/trc/store.go:730`). Текущий CTE требует `numbered` CTE с `row_number() OVER (ORDER BY id)` для маппинга `parent_id` (1-based offset) → реальный `id` строки на каждом шаге рекурсии. Это создаёт O(D×M) hash probes (D = глубина дерева, M = все события сессии), что приводит к таймауту 60s на сессиях с 500K+ событий.

`ComputeParentIDs` (`internal/trc/tree.go:173`) вычисляет `ParentID` как 0-based индекс в срезе `events`. При сохранении (`store.go:107`) добавляется +1 для 1-based offset. Реальный `id` (BIGSERIAL) неизвестен в момент вычисления.

См. proposal.md — Why для мотивации.

## Goals / Non-Goals

**Goals:**
- Устранить таймаут `codebase_trc_tree` для больших сессий (< 5s для 500K событий)
- Хранить `parent_id` как реальный `id` строки, чтобы recursive CTE использовал прямой JOIN по индексу
- Сохранить корректность дерева вызовов (Starting/Completed пары, fallback для Completed-only)

**Non-Goals:**
- Изменение `ComputeParentIDs` — алгоритм вычисления ParentID как индекса в срезе не меняется
- Изменение файлового режима (без БД) — `buildSPIDTree` работает в памяти без изменений
- Оптимизация других TRC-запросов (events, errors, slow, procedures)

## Decisions

### Decision 1: Go-маппинг parent_id после COPY IN

**Выбор:** После завершения `COPY IN` загружаем `id` строк, маппим offset→id в Go, затем batch UPDATE через temp table с JOIN по PK.

**Альтернативы:**
- *SQL UPDATE с row_number() self-join* — изначально реализовано, но непредсказуемый план: PostgreSQL может выбрать nested loop O(N²) вместо hash join, т.к. `rn` — вычисляемое поле без индекса. На 40K событий — таймаут 300с.
- *Temp table + CREATE INDEX ON _map(rn)* — компромисс, но всё ещё зависит от планировщика.
- *INSERT ... RETURNING id вместо COPY IN* — COPY IN в 5-10x быстрее, потеря неприемлема.

**Алгоритм (3 шага, O(N)):**
1. `SELECT id FROM trc_events WHERE session_id = $1 ORDER BY id` → `ids[]` (index scan по `idx_trc_events_session_spid`)
2. Go: `realParentID = ids[ev.ParentID]` для каждого события с `ParentID >= 0`
3. `COPY IN` во temp table `(id, parent_id)` + `UPDATE trc_events e SET parent_id = t.parent_id FROM temp t WHERE e.id = t.id` — JOIN по PK, всегда index scan

Для 400K событий: ~5-7с (SELECT ~2с + Go-маппинг мгновенно + COPY+UPDATE ~3-5с).

### Decision 2: Упрощение recursive CTE в LoadEventsForTree

**Выбор:** Полностью убрать `numbered` CTE. Recursive CTE использует прямой `JOIN tree t ON c.parent_id = t.id`.

**Новый запрос:**
```sql
WITH RECURSIVE tree AS (
    SELECT e.event_class, e.event_name, e.procedure, e.duration_ms,
           e.params, e.columns, e.parent_id, e.depth, e.id, 1 AS tree_depth
    FROM trc_events e
    WHERE e.session_id = $1 AND e.spid = $2 AND e.parent_id IS NULL
      AND (e.event_name LIKE '%Starting' OR e.event_name LIKE '%Completed')
    UNION ALL
    SELECT c.event_class, c.event_name, c.procedure, c.duration_ms,
           c.params, c.columns, c.parent_id, c.depth, c.id, t.tree_depth + 1
    FROM trc_events c
    JOIN tree t ON c.parent_id = t.id
    WHERE c.session_id = $1 AND c.spid = $2 AND ($3 = 0 OR t.tree_depth < $3)
)
SELECT event_class, event_name, procedure, duration_ms, params, columns,
       parent_id, depth
FROM tree
```

Сложность: O(D × log N) через индекс `idx_trc_events_session_parent` (session_id, parent_id).

### Decision 3: Миграция существующих сессий

**Выбор:** Не выполнять автоматическую миграцию. Существующие сессии имеют `parent_id` как offset — `LoadEventsForTree` для них будет возвращать некорректное дерево. Пользователи SHALL пересоздать сессии (reparse) для корректной работы.

**Альтернатива:** Автоматический детект старого формата (например, проверка `parent_id > max(id)` для session) и on-the-fly миграция. Минус: усложняет код, добавляет overhead на каждый запрос. Непрактично — TRC-сессии легко пересоздаются.

## Risks / Trade-offs

- **[Risk] Go-маппинг увеличивает время парсинга** → Mitigation: 3 шага O(N), JOIN по PK гарантирует index scan. Для 400K событий — ~5-7с. Общее время парсинга увеличивается на <10%.
- **[Risk] Существующие сессии сломаются** → Mitigation: TRC-сессии легко пересоздаются (`trc parse`). Документировать в release notes. Альтернатива: добавить миграцию в `InitSchema` (один UPDATE для всех сессий), но это рискованно для больших БД.
- **[Risk] parent_id = 0 для root событий** → Mitigation: `ComputeParentIDs` устанавливает `ParentID = -1` для root, при сохранении `parentID = nil` (NULL в БД). UPDATE не затрагивает NULL. Корректно.

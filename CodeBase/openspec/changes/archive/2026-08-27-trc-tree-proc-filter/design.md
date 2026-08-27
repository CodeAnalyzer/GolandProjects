## Context

`ExecuteTree` (`internal/trcsvc/runtime.go:201-229`) имеет два режима:

1. **Серверный** (session_id > 0, БД доступна): `LoadEventsForTree` (`internal/trc/store.go:693-753`) выполняет recursive CTE, загружая события одного SPID. CTE anchor: `WHERE parent_id IS NULL`, recursive part спускается по `parent_id`. Результат — `[]TRCEvent`, из которых `BuildTrees` строит дерево.
2. **Файловый** (без БД): `resolveSession` загружает все события, `BuildTreesWithDepth` строит лес по всем SPID.

Фильтрация по процедуре должна работать в обоих режимах. CLI и MCP используют общий service-слой (`trcsvc.ExecuteTree`), поэтому изменение в `TreeParams` + `ExecuteTree` покрывает оба транспорта.

## Goals / Non-Goals

**Goals:**
- Фильтрация дерева по имени процедуры в обоих режимах (серверный и файловый)
- Минимальное изменение существующего API: новое поле `Procedure` в `TreeParams`, опциональный параметр в CLI/MCP
- Серверная фильтрация в CTE — не загружать лишние события из БД

**Non-Goals:**
- Частичный (partial) match по имени процедуры — только exact match (как `--proc` в `trc events`)
- Фильтрация по procedure в `trc events` — уже существует (`--proc` в `cmd/trc.go:372`)
- Изменение формата вывода дерева

## Decisions

### Decision 1: Серверная фильтрация в CTE (не пост-фильтрация)

**Выбор:** Модифицировать `LoadEventsForTree` — добавить параметр `procedure string`. Если `procedure != ""`, anchor CTE ищет `e.procedure = $procedure` вместо `e.parent_id IS NULL`. Recursive part не меняется (спускается по parent_id от anchor).

**Альтернатива:** Пост-фильтрация — загрузить весь SPID, построить дерево, отфильтровать в памяти. Отвергнута: при больших трейсах (сотни тысяч событий в SPID) загружает и строит ненужные данные.

**SQL-изменение anchor:**
```sql
-- Было:
WHERE e.session_id = $1 AND e.spid = $2 AND e.parent_id IS NULL
-- Стало (при procedure != ""):
WHERE e.session_id = $1 AND e.spid = $2 AND e.procedure = $4
```

Параметр `$4` — `procedure` (пустая строка = старое поведение). Используется `CASE` или динамическое построение query в Go (как уже делается для `maxNodes` через `LIMIT`).

### Decision 2: Фильтрация в памяти для файлового режима

**Выбор:** Новая функция `FilterTreesByProcedure(trees map[int][]*TRCTreeNode, procedure string) map[int][]*TRCTreeNode` в `internal/trc/tree.go`. Обходит каждое дерево, находит узлы с `node.Start.Procedure == procedure`, возвращает новые деревья с этими узлами как корнями.

**Альтернатива:** Фильтровать события до `BuildTrees`. Отвергнута: нарушит parent_id/depth, т.к. события отфильтрованы, но parent_id ссылается на удалённые. Пост-фильтрация готового дерева проще и корректнее.

### Decision 3: SPID auto-select при procedure без --spid

При серверном режиме без `--spid` `LoadEventsForTree` автовыбирает SPID с наибольшим числом root-событий. Если `procedure` задан, автовыбор должен учитывать процедуру: выбрать SPID с наибольшим числом событий `procedure = $procedure`. Иначе можно попасть на SPID, где нужной процедуры нет.

**Реализация:** Если `procedure != ""` и `spid <= 0`, отдельный query:
```sql
SELECT spid FROM trc_events
WHERE session_id = $1 AND procedure = $2 AND spid IS NOT NULL
GROUP BY spid ORDER BY count(*) DESC LIMIT 1
```

### Decision 4: Общий service-слой

`TreeParams` расширяется полем `Procedure string`. `ExecuteTree` передаёт его в `LoadEventsForTree` (серверный режим) или в `FilterTreesByProcedure` (файловый режим). CLI и MCP просто заполняют `TreeParams.Procedure` — дублирования нет.

## Risks / Trade-offs

- **[Несколько вызовов одной процедуры в одном SPID]** → `FilterTreesByProcedure` возвращает все найденные поддеревья (multi-root), не одно. Это корректно: процедура может вызываться несколько раз.
- **[Procedure в Starting vs Completed]** → `TRCTreeNode.Start.Procedure` — это процедура из Starting-события. У Completed-события procedure может отличаться (редко). Фильтрация по `Start.Procedure` — ожидаемое поведение (как в RTI `--proc`).
- **[CTE performance]** → anchor по `procedure` вместо `parent_id IS NULL` может быть медленнее без индекса на `(session_id, procedure, spid)`. Существующий составной индекс `idx_trc_events_session_id` (или аналог) должен покрывать. Проверить через `EXPLAIN`.

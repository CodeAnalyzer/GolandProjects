## Context

`buildSPIDTree` (`internal/trc/tree.go:173-206`) строит дерево вызовов по SPID через стек Starting/Completed пар. Ветка `default` (строки 201-203) прикрепляет любые non-Starting/Completed события как отдельные узлы через `attach()`, которые становятся корневыми, когда стек пуст. Это включает diagnostic-события (SP:Recompile, SQL:StmtRecompile, Audit Login/Logout и др.), создавая шум на верхнем уровне дерева.

`LoadEventsForTree` (`internal/trc/store.go:695-777`) использует recursive CTE с anchor `parent_id IS NULL`. Diagnostic-события без родителя попадают в anchor и становятся корнями дерева.

`ComputeParentIDs` (`internal/trc/tree.go:112-171`) — аналогичная логика для in-memory вычисления parent_id перед сохранением в БД. Diagnostic-события в ветке `default` (строки 162-168) не push'атся в стек, но получают parent_id от текущего top. Когда стек пуст — parent_id остаётся -1 (корень).

## Goals / Non-Goals

**Goals:**
- Корневые узлы TRC call tree — только Starting/Completed события
- Diagnostic-события внутри вызова сохраняются как дети
- Фильтрация работает в обоих путях: DB (LoadEventsForTree) и in-memory (buildSPIDTree)
- Без перепарса уже сохранённых сессий

**Non-Goals:**
- Полное удаление diagnostic-событий из результатов (они остаются в поддеревьях)
- Изменение `ComputeParentIDs` — parent_id в БД уже корректен (diagnostic не является родителем Starting/Completed)
- Изменение API MCP-инструмента `codebase_trc_tree` или CLI `trc tree`
- Фильтрация diagnostic-событий в других командах (`trc events`, `trc errors`, `trc slow`)

## Decisions

### Decision 1: Фильтрация в SQL anchor CTE (DB путь)

Добавить условие `(e.event_name LIKE '%Starting' OR e.event_name LIKE '%Completed')` в `anchorWhere` в `LoadEventsForTree`, когда `procedure` не задан. Когда `procedure` задан — anchor уже фильтрует по `procedure = $4`, дополнительная фильтрация не нужна (процедурные события — всегда Starting/Completed).

**Альтернатива:** Фильтровать все события в recursive part через `WHERE event_name LIKE ...`. Отвергнута — это исключило бы diagnostic-события из поддеревьев, а они могут быть полезны для контекста.

### Decision 2: Guard в buildSPIDTree (in-memory путь)

В ветке `default` (строки 201-203) добавить проверку: если стек пуст — `continue` (пропустить событие). Если стек не пуст — `attach` как ребёнок (существующее поведение).

```
default:
    if len(stack) == 0 {
        continue  // diagnostic без родителя — не корень
    }
    attach(&TRCTreeNode{Start: ev})
```

**Альтернатива:** Фильтровать events перед вызовом `BuildTreesWithDepth`. Отвергнута — теряется привязка diagnostic-событий к их родительскому фрейму.

### Decision 3: ComputeParentIDs не изменяется

`ComputeParentIDs` уже работает корректно: diagnostic-события не push'ятся в стек и не становятся родителями Starting/Completed. Их parent_id = -1 когда стек пуст — это допустимо, фильтрация в `LoadEventsForTree` anchor CTE исключит их из дерева. Менять `ComputeParentIDs` не нужно.

## Risks / Trade-offs

- **[Risk] Diagnostic-события без родителя теряются из дерева** → Acceptable: событие без контекста вызова бесполезно для анализа иерархии. Команды `trc events`/`trc errors` остаются без фильтрации для полноты.
- **[Risk] LIKE '%Starting' на больших таблицах** → Mitigation: `trc_events` уже индексирована по `(session_id, spid)`, anchor CTE фильтрует по ним первым. LIKE применяется только к строкам одного SPID одной сессии.
- **[Risk] Новые типы событий с другим суффиксом** → Low: SQL Server Profiler использует только Starting/Completed пары для call-flow событий. Диагностические события имеют другие суффиксы (Recompile, Logout, Login).

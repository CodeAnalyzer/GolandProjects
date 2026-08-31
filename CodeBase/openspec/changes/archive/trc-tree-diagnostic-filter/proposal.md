## Why

TRC call tree (`codebase_trc_tree`) включает diagnostic-события (SP:Recompile, SQL:StmtRecompile, Audit Login/Logout и др.) как корневые узлы, когда нет открытого Starting/Completed фрейма. Это создаёт шум на верхнем уровне дерева: вместо чистой иерархии вызовов `exec Proc_A → exec Proc_B` пользователь видит десятки неподходящих корневых узлов. 643-страничный пагинированный ответ для одного SPID — симптом проблемы.

## What Changes

- `LoadEventsForTree` (DB путь): anchor CTE фильтрует корневые узлы — только события с `event_name` заканчивающимся на `Starting` или `Completed` могут быть корнями дерева
- `buildSPIDTree` (in-memory путь): non-Starting/Completed события прикрепляются как дети текущего открытого фрейма, но **не становятся корневыми узлами** когда стек пуст
- `ComputeParentIDs`: аналогично — diagnostic-события без открытого родителя получают `ParentID = -1`, но не порождают корневые узлы в дереве при загрузке из БД
- Поведение diagnostic-событий **внутри** вызова не меняется — они остаются в поддереве как дети

## Capabilities

### New Capabilities

_Нет новых capabilities._

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: фильтрация diagnostic-событий из корневого уровня call tree

## Impact

- `internal/trc/tree.go` — `buildSPIDTree`, `ComputeParentIDs`
- `internal/trc/store.go` — `LoadEventsForTree` SQL query
- `internal/trc/tree_test.go` — обновление/добавление тестов
- `internal/trcsvc/runtime.go` — не требует изменений (делегирует в `trc.BuildTrees`/`LoadEventsForTree`)
- MCP tool `codebase_trc_tree` — без изменения API, меняется только содержимое ответа
- Совместимость: уже сохранённые сессии в БД не требуют перепарса — parent_id корректен, фильтрация применяется при загрузке

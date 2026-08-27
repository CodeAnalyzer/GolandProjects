## Why

Команда `trc tree` строит полный лес вызовов по всем SPID (или по одному SPID через `--spid`), но не позволяет построить поддерево от конкретной процедуры. Это ограничивает анализ: если нужно изучить вызовы внутри одной процедуры (например, `MyProc`), приходится просматривать всё дерево вручную. RTI уже имеет `--proc` для `rti tree` — TRC не имеет аналога.

## What Changes

- Добавить фильтрацию дерева TRC по имени процедуры через параметр `Procedure` в `TreeParams` (`internal/trcsvc/types.go`)
- В `ExecuteTree` (`internal/trcsvc/runtime.go`): передача `Procedure` в `LoadEventsForTree` при серверном режиме; вызов `FilterTreesByProcedure` при файловом режиме
- CLI: добавить флаг `--proc` для `trc tree` (`cmd/trc.go`)
- MCP: добавить параметр `procedure` в инструмент `codebase_trc_tree` (`internal/mcp/registry.go`)
- При серверном режиме (session_id > 0): фильтрация выполняется **в SQL** — recursive CTE в `LoadEventsForTree` модифицируется так, что anchor ищет события с `procedure = $procedure` (вместо `parent_id IS NULL`), и recursive part спускается только от них. Это загружает из БД только поддерево нужной процедуры, а не весь SPID
- При файловом режиме (без БД): дерево строится по всем событиям, затем фильтруется по процедуре в памяти (нет CTE, нет БД)

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: добавляется фильтрация дерева вызовов по имени процедуры — новое требование и сценарии

## Impact

- `internal/trcsvc/types.go` — новое поле `Procedure` в `TreeParams`
- `internal/trcsvc/runtime.go` — логика фильтрации дерева в `ExecuteTree`
- `internal/trc/store.go` — модификация `LoadEventsForTree`: параметр `procedure`, серверная фильтрация в recursive CTE (anchor по `procedure` вместо `parent_id IS NULL`)
- `internal/trc/tree.go` — новая функция `FilterTreesByProcedure` (для файлового режима: поиск узлов по процедуре и возврат их поддеревьев)
- `cmd/trc.go` — флаг `--proc` для `trc tree`
- `internal/mcp/registry.go` — параметр `procedure` в `codebase_trc_tree`
- `internal/trcsvc/runtime_test.go` — тесты фильтрации (серверный и файловый режимы)

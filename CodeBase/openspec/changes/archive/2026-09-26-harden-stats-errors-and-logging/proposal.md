## Why

Три независимых дефекта эксплуатационного качества, подтверждённых на реальном индексе:

1. **`stats` считает всё заново на каждый вызов.** `GetStats` (`internal/store/db_stats.go:13`) выполняет 41 запрос (`COUNT(*)` по 36 таблицам + `COUNT(*) FROM files` с 13 `FILTER`); на реальном индексе это **14.8s warm** (`relations` ~3.8M, `query_fragments` 1.7M, `files` 144k), а MCP-вызов `codebase_stats` уходит в таймаут. При этом счётчики меняются только при `init`/`update`.
2. **`query procedure` падает на отсутствующей процедуре.** `GetProcedureResult` (`internal/query/query_sql.go:508`) и `GetProcedureDetails` (`:479`) возвращают `sql.ErrNoRows` как ошибку — в `WORK-LOG` 9 записей `query failed: sql: no rows in result set`. Это противоречит требованию «Поведение при пустых результатах entity queries» (пустой результат вместо ошибки).
3. **Логи MCP непригодны для анализа.** `formatToolArgs` (`internal/mcp/server.go:189`) пишет **ровно один** аргумент: по приоритетному списку `name/procedure/text/...`, иначе первый ключ `map` (порядок в Go случаен). Плюс `text`/`sql` не маскируются, хотя спека `mcp-server/mcp-transport-tools` уже требует sanitized аргументы. Из-за этого нельзя сгруппировать вызовы по реальным аргументам (например, «одинаковые» `args=limit="10"` давали разброс 44ms…2821ms).

## What Changes

- **stats — снапшот.** Счётчики вычисляются один раз по завершении `init`/`update` и сохраняются; команда `stats` и MCP-инструмент `codebase_stats` читают сохранённый снапшот без пересчёта. Живой подсчёт остаётся фолбэком, когда снапшота ещё нет (пустая/свежая БД).
- **procedure — пустой результат.** Отсутствующая процедура возвращает пустой результат (`count: 0`, без ошибки); форма ответа приводится к массиву 0/1 элемента.
- **logging — канонический формат.** Логируются **все** аргументы вызова, отсортированные по имени (`args=key:value,key:value`), приватные (`text`, `sql`) маскируются.
- **BREAKING** для `query procedure` / `codebase_query_procedure`: «не найдено» больше не ошибка, а пустой результат; форма элемента — массив 0/1 (ранее одиночный объект).

## Capabilities

### New Capabilities

- (нет)

### Modified Capabilities

- `infrastructure/encoding-cli`: требование «Stats через systemsvc» — статистика читается из снапшота, обновляемого `init`/`update`, без пересчёта 41 `COUNT(*)`; фолбэк на живой подсчёт при отсутствии снапшота.
- `infrastructure/database-schema`: хранение снапшота статистики (новая таблица/колонка, инициализация в `InitSchema`).
- `query/entity-queries`: требования «Детали SQL-процедуры» и «Поведение при пустых результатах entity queries» — отсутствие процедуры возвращает пустой результат, а не ошибку.
- `mcp-server/mcp-transport-tools`: требование «Логирование tool-вызовов» — канонический формат всех аргументов и маскировка `text`/`sql`.

## Impact

- `internal/store/db_stats.go` — `GetStats` читает снапшот; новая запись снапшота (`SaveStatsSnapshot`).
- `internal/store/db_schema.go` — объект хранения снапшота в `InitSchema`.
- `internal/systemsvc/runtime.go` — `ExecuteStats` (CLI + MCP) работает со снапшотом.
- `cmd/init.go`, `cmd/update.go` — запись снапшота по завершении индексации.
- `internal/query/query_sql.go` — `GetProcedureResult`/`GetProcedureDetails` обрабатывают `sql.ErrNoRows`.
- `cmd/query_commands.go`, `internal/mcp/registry.go` — `query procedure`/`codebase_query_procedure` отдают пустой результат.
- `internal/mcp/server.go` — `formatToolArgs` (все аргументы, сортировка, маскировка).
- Тесты: `internal/store`, `internal/systemsvc`, `internal/query`, `internal/mcp`.

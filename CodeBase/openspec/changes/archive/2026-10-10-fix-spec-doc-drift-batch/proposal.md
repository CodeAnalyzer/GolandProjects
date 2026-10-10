# Proposal: fix-spec-doc-drift-batch

## Why

По итогам аудита «29 спек ↔ код» (BUG-spec-doc-drift-batch-20261009, все 7 пунктов подтверждены на коде 2026-10-10) набралась пачка расхождений класса LOW: тексты спек зафиксировали устаревшие счётчики, неверную атрибуцию вызовов, неверную структуру выдачи и неверное имя функции. Поведение системы корректно, но спеки — «источник истины» — систематически лгут: пользователь или LLM-агент, ориентирующийся на «55 инструментов» или «94 записи», делает ложные выводы. Пока дрейф маленький, его дешевле закрыть одной волной.

## What Changes

- **rti-parsing**: убрать устаревший счётчик «94 записи» из требования «Справочник Module ID → Product Name» (фактически 438) и из Related code/Notes.
- **mcp-transport-tools**: добавить `codebase_query_desc_search` в список Query-инструментов (28→29) и `codebase_query_spec_config` в список Spec-инструментов (6→7); заменить «все 55 инструментов» на формулировку без счётчика; убрать устаревшие приблизительные счётчики из сценариев профилей (rti ~17, query ~30, trc ~14, review ~5).
- **mcp-pagination**: перенести атрибуцию `SetPaginationTTL` с `RunStdio` на bootstrap CLI (`cmd/root.go`, вызов при загрузке конфигурации до входа в `RunStdio`); атрибуция `globalPages`/GC-loop за `RunStdio` остаётся (она верна); поправить Notes «меняет TTL для уже созданного store» (TTL устанавливается до создания store).
- **spec-queries**: «результат группируется по capability» → «плоский список упоминаний, упорядоченный по имени capability и номеру строки» (реализация — плоский `Hits`); задокументировать алиасы направлений `outgoing`/`incoming` в требовании spec_deps.
- **api-macros-t01**: атрибуция `indexAPIMacros` — определение в `internal/indexer/indexer.go`, вызывается из `indexer_sql_pas.go`; в Notes зафиксировать, что `macro_type` в БД хранится в lowercase.
- **trc-storage**: Related code — `ParseToDB` → `ParseFileToDB` (правка секции Related code, вне дельта-механизма).
- **Код (единственная правка)**: `cmd/query_spec.go:70` — Use-string `--direction outgoing|incoming` → `--direction depends_on|depended_by` (алиасы `outgoing|incoming` сохраняются в реализации; help-текст, поведение не меняется).

Без изменения поведения системы: 6 правок текста спек + 1 правка CLI help-строки.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `rti-analysis/rti-parsing`: требование «Справочник Module ID → Product Name» — убрать счётчик записей справочника
- `mcp-server/mcp-transport-tools`: требования «Query инструменты» (+ `codebase_query_desc_search`), «Профильная регистрация инструментов» (счётчики инструментов в сценариях), «Spec инструменты» (+ `codebase_query_spec_config`)
- `mcp-server/mcp-pagination`: требование «Динамическое применение TTL из конфига» — атрибуция вызова `SetPaginationTTL` (bootstrap CLI, не `RunStdio`)
- `query/spec-queries`: требования «Спеки по коду (spec_by_code)» — плоская структура результата; «Зависимости capability (spec_deps)» — документирование алиасов направлений
- `indexing/api-macros-t01`: требование «Relations из API-макросов (implements_contract, publishes_event, executes_contract)» — корректная атрибуция `indexAPIMacros`

## Impact

- **Спеки**: `openspec/specs/rti-analysis/rti-parsing/spec.md`, `openspec/specs/mcp-server/mcp-transport-tools/spec.md`, `openspec/specs/mcp-server/mcp-pagination/spec.md`, `openspec/specs/query/spec-queries/spec.md`, `openspec/specs/indexing/api-macros-t01/spec.md` (дельты) + `openspec/specs/trc-analysis/trc-storage/spec.md` (только Related code — прямая правка).
- **Код**: `cmd/query_spec.go` (строка 70, Use-string) — правка help-текста, поведение CLI/MCP не меняется.
- **Потребители**: без влияния — структура данных `SpecByCodeResult` (плоский `Hits`) не меняется, набор MCP-инструментов не меняется, алиасы `outgoing|incoming` продолжают приниматься.
- **Примечание**: правки секций `Related code`/`Notes` (rti-parsing, mcp-pagination, trc-storage, api-macros-t01) не покрываются дельта-механизмом (дельты оперируют только требованиями) и применяются прямыми правками основных спек в рамках tasks.

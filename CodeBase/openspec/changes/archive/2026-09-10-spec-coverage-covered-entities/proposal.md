## Why

Инструмент `codebase_query_spec_coverage` в текущем виде возвращает бессмысленные метрики: числитель — сущности одной capability, знаменатель — все сущности продукта (3/2903). Процент покрытия не нужен для навигации; вместо него нужен обратный к `spec_by_code` инструмент — перечень покрытых спеками код-сущностей, сгруппированных по capability.

## What Changes

- **BREAKING**: `codebase_query_spec_coverage` меняет контракт: вместо метрик покрытия (api_total, api_covered, code_total, code_listed, проценты) возвращает перечень покрытых код-сущностей, сгруппированных по capability.
- **BREAKING**: Параметр `product` становится обязательным; `name` (capability slug) — опциональным (нет `name` → все capability продукта; есть `name` → одна capability).
- Добавлен параметр `kind` (опциональный) — фильтр по типу сущности: `api_contract | sql_procedure | sql_table | dfm_form | smf_instrument | pas_method | js_function | report_form`.
- Удалён параметр `mode` (saved/gaps). Saved-метрики и gaps больше не возвращаются этим инструментом.
- Ответ: дерево `capabilities[]` с `covered[]` (записи `{name, kind}`), дедуплицированные по `name|kind` внутри capability.
- CLI `codebase query spec coverage`: `--product` required, `--name` optional, `--kind` optional, `--mode` удалён.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-queries`: требование «Покрытие кода спеками (spec_coverage)» переписано — инструмент возвращает перечень покрытых код-сущностей, сгруппированных по capability, с фильтром по типу; метрики покрытия и gaps удалены.

## Impact

- **`internal/specsvc/specsvc.go`** — `SpecCoverageResult`, `ExecuteSpecCoverage`, `computeSpecCoverage`: переделка структуры и логики; `SpecCoverageGap` остаётся (для будущего `spec_gaps`).
- **`cmd/query_spec.go`** — `querySpecCoverageCmd`: флаги `--product` (required), `--name` (optional), `--kind` (optional); `--mode` удалён.
- **`internal/mcp/registry.go`** — `codebase_query_spec_coverage`: input schema (product required, name/kind optional, mode убран) и handler.
- **`openspec/specs/query/spec-queries/spec.md`** — требование «Покрытие кода спеками» переписано.

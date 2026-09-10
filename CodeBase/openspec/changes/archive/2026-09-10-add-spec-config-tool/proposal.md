## Why

Индексатор сохраняет профиль продукта (`spec_configs`: layout, язык, стиль cross-ref, наличие changes/audit/ADR) и иерархию capabilities (`spec_capabilities.parent_id`), но ни одно из шести существующих spec-инструментов не возвращает эти данные. Пользователь, открывающий новый продукт, работает вслепую: не знает конвенций спек и структуры дерева capabilities. Параллельно `spec_usecase` возвращает пустой результат при отсутствии usecase вместо ошибки `ErrSpecNotFound` — это нарушает единообразие с `spec_deps` и `spec_history`.

## What Changes

- **Новый MCP-инструмент** `codebase_query_spec_config`: по имени продукта возвращает профиль (`spec_configs` — 12 полей), статистику (счётчики capabilities/requirements/scenarios/usecases/changes) и опциональное дерево иерархии capabilities (`parent_id`) с глубиной раскрытия и `children_count` на каждом узле.
- **Исправление** `codebase_query_spec_usecase`: при отсутствии usecase возвращать `ErrSpecNotFound` вместо пустого результата — единообразие с другими spec-инструментами.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-queries`: добавляется требование «Профиль продукта и иерархия capabilities (spec_config)» и модифицируется требование «Usecase-слой (spec_usecase)» — поведение при отсутствии usecase меняется с пустого результата на `ErrSpecNotFound`.

## Impact

- `internal/specsvc/specsvc.go` — новая функция `ExecuteSpecConfig`, модификация `ExecuteSpecUsecase` (возврат `ErrSpecNotFound`).
- `internal/mcp/registry.go` — регистрация нового инструмента `codebase_query_spec_config`.
- `internal/store/db_lookup_spec.go` — новый lookup для профиля и иерархии.
- `cmd/root.go` — зеркальная CLI-подкоманда `codebase query spec config`.
- Существующие тесты `spec_usecase` — обновление ожиданий (пустой результат → ошибка).

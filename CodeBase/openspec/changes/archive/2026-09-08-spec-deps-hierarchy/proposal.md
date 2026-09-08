## Why

Парсер `depends_on_capability` пропускает реальные связи между спецификациями из-за двух проблем: (1) регэксп "Связан с доменами" требует двоеточие, которое часто отсутствует в реальных spec-файлах, (2) отсутствует маркер для паттерна "поддомену/поддоменам: `slug1`, `slug2`". Кроме того, иерархия capabilities (parent → child) хранится в БД, но не используется при построении зависимостей — упоминание родительского домена не создаёт неявных зависимостей на его поддомены, и наоборот.

## What Changes

- Регэксп `reLinkedDomains` делает двоеточие опциональным (`:` → `:?`)
- Добавляется 6-й маркер `reSubdomains` для паттерна "поддомену/поддоменам: `slug1`, `slug2`"
- `LoadAllSpecCapabilitiesForDeps` загружает `parent_id` для построения иерархии
- После извлечения прямых зависимостей раскрываются неявные связи по иерархии в обоих направлениях (parent → children, child → parent) с `confidence: "hierarchy"`

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/relations-postprocessing`: извлечение `depends_on_capability` расширяется новыми маркерами и раскрытием иерархии capabilities

## Impact

- `internal/indexer/indexer_postprocess_spec_deps.go` — новые маркеры, функция `expandHierarchyDeps`, изменение `resolveSlug`/`slugToID` для учёта `parent_id`
- `internal/store/db_lookup_spec_deps.go` — `LoadAllSpecCapabilitiesForDeps` добавляет `COALESCE(parent_id, 0)` в SELECT
- `internal/model/` — `SpecCapability` получает поле `ParentID int64`
- `codebase query spec deps` — результаты будут содержать больше зависимостей (неявные иерархические)

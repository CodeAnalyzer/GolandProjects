## Context

Парсер `depends_on_capability` (`extractCapabilityDeps` в `indexer_postprocess_spec_deps.go`) извлекает связи между capabilities из текста spec-файлов. Иерархия capabilities уже хранится в `spec_capabilities.parent_id` — `ensureSpecCapabilityHierarchy` создаёт промежуточные контейнеры и устанавливает parent-child связи. Однако `LoadAllSpecCapabilitiesForDeps` не загружает `parent_id`, и `extractCapabilityDeps` не использует иерархию при построении зависимостей.

## Goals / Non-Goals

**Goals:**
- Распознавать "Связан с доменами" без двоеточия
- Распознавать "поддомену/поддоменам: `slug1`, `slug2`" как маркер зависимости
- Раскрывать иерархию: parent → children (вниз) и child → parent (вверх)
- Дедупликация: прямая связь имеет приоритет над иерархической

**Non-Goals:**
- Транзитивное раскрытие иерархии (parent → child → grandchild) — только один уровень
- Изменение `codebase query spec deps` — запрос уже читает все `depends_on_capability` relations
- Изменение формата `relations` — используется тот же `relation_type = 'depends_on_capability'`

## Decisions

### 1. Загрузка parent_id в LoadAllSpecCapabilitiesForDeps

Добавить `COALESCE(parent_id, 0)` в SELECT. Альтернатива — отдельный запрос для иерархии — отклонена: лишний round-trip к БД при наличии данных в той же таблице.

### 2. Построение иерархических map'ов в postProcessSpecDependencies

После загрузки caps строить два map'а:
- `childrenOf map[int64][]int64` — parent_id → список child IDs
- `parentOf map[int64]int64` — child_id → parent_id (0 если нет)

Это in-memory структуры, построенные один раз за O(n).

### 3. Функция expandHierarchyDeps

Новая функция принимает `[]*model.Relation` + иерархические map'ы, возвращает расширенный список. Для каждой прямой зависимости:
- **Вниз**: если target имеет детей, добавить relation на каждого ребёнка (`confidence: "hierarchy"`)
- **Вверх**: если target имеет родителя, добавить relation на родителя (`confidence: "hierarchy"`)

Дедупликация через существующий `depSeen` map — прямая связь уже там, иерархическая проверяет перед добавлением.

### 4. Маркер reSubdomains

Регэксп: `(?i)поддомен(?:у|ам|а|ов)?\s*:?\s*(.+)` — покрывает "поддомену", "поддоменам", "поддомена", "поддоменов". Двоеточие опционально. Slug'и извлекаются через `reBacktickSlugs` (переиспользуется от маркера "Связан с доменами").

### 5. Опциональное двоеточие в reLinkedDomains

Изменение `:\s*` → `:?\s*` — минимальное, обратно совместимое. Существующие spec-файлы с двоеточием продолжают работать.

### 6. Поле ParentID в model.SpecCapability

Добавить `ParentID int64` в структуру. Альтернатива — отдельная структура `SpecCapabilityWithHierarchy` — отклонена: избыточный тип для одного поля.

## Risks / Trade-offs

- **[False positives в reSubdomains]** — слово "поддомен" может встретиться в другом контексте → Mitigation: `reBacktickSlugs` требует backtick-обёрнутые slug'и, `resolveSlug` проверяет существование в `slugToID` — двойной фильтр
- **[Избыточные иерархические связи]** — при глубокой иерархии (3+ уровня) раскрытие только на 1 уровень может пропустить связи → Acceptable: complexity vs. coverage trade-off, можно расширить позже
- **[Производительность]** — `expandHierarchyDeps` O(n*m) где n — relations, m — среднее число детей → Acceptable: общее число capabilities невелико (тысячи, не миллионы)

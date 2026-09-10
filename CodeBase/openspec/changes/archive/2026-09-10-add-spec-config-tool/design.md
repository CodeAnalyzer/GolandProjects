## Context

Существующие spec-инструменты (`spec_search`, `spec_by_code`, `spec_deps`, `spec_history`, `spec_usecase`, `spec_coverage`) все capability-центричны или entity-центричны. Нет продукта-центричного инструмента: профиль продукта (`spec_configs` — 12 полей, детектируемых постпроцессором `indexer_postprocess_spec_profile.go`) и иерархия capabilities (`spec_capabilities.parent_id`, строится в `indexer_postprocess_spec_deps.go`) недостижимы через MCP.

Параллельно `spec_usecase` возвращает `ErrSpecNotFound` в коде (`specsvc.go:424`), но описание инструмента в registry (`registry.go:1063`) обещает «honest empty result» — рассинхрон описания и поведения.

## Goals / Non-Goals

**Goals:**
- Новый инструмент `codebase_query_spec_config` — профиль + статистика + опциональная иерархия
- Исправление описания `codebase_query_spec_usecase` в registry — убрать обещание «honest empty result»
- Зеркальная CLI-подкоманда `codebase query spec config`

**Non-Goals:**
- Возвращение метрик покрытия из Notes (`api_total`, `api_covered`, `code_total`, `code_listed`) — отдельный change
- Возвращение rich-полей usecase (`business_value`, `architecture`, `data_schema`, `page_id`) — отдельный change
- Возвращение файловых путей для spec-сущностей — отдельный change
- Изменение поведения `spec_usecase` в коде (он уже возвращает `ErrSpecNotFound`) — только описание в registry

## Decisions

### 1. depth=2 по умолчанию, children_count всегда

**Решение**: `include_hierarchy=true`, `depth=2` по умолчанию. На каждом узле — `children_count` (общее количество прямых детей), `children` — массив раскрытый до `depth` (null на листьях или при достижении лимита).

**Обоснование**: Замеры на реальных данных:
- fa-reports: 7 корней → 139 детей → 1748 внуков. depth=2 даёт 146 узлов (~25 KB), depth=3 даёт 1894 (~200+ KB).
- fa-cards: 73 узла, плоская структура. depth=2 покрывает всё дерево.

**Альтернатива**: плоский список с `parent_id` вместо дерева. Отвергнут — неконсистентно с `spec_deps`, который возвращает nested.

### 2. is_container по пустым title/purpose/notes

**Решение**: `is_container = true` если `title = '' AND purpose IS NULL AND notes IS NULL`. Контейнерные capabilities создаются `GetOrCreateSpecCapabilityContainer` без этих полей.

**Альтернатива**: отдельный флаг в БД. Отвергнут — требует миграции схемы, а эвристика надёжна (реальные spec.md всегда имеют хотя бы title).

### 3. Статистика одним запросом

**Решение**: Один запрос с `UNION ALL` по `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_changes` с фильтром по `spec_config_id` (через join с `ds_products`).

**Обоснование**: 5 отдельных `COUNT(*)` — 5 round-trips. Один `UNION ALL` — 1 round-trip, тот же объём данных.

### 4. Lookup продукта через ds_products

**Решение**: `JOIN ds_products dp ON dp.id = sc.ds_product_id WHERE dp.product_name = $1`. Если `ds_product_id IS NULL` у spec_config — продукт недостижим (такая ситуация не встречается на реальных данных).

**Альтернатива**: fallback на `spec_configs.product_name`. Отвергнут — все существующие инструменты фильтруют через `ds_products`, консистентность важнее edge-case покрытия.

### 5. Иерархия — рекурсивный CTE

**Решение**: Один `WITH RECURSIVE` запрос выбирает все capabilities продукта с `parent_id` и `title`. Дерево строится в Go (map[parent_id][]children), как `buildDepTree` в `specsvc.go`. `children_count` считается из той же выборки.

**Обоснование**: Рекурсивный CTE в PostgreSQL эффективен для дерева глубины 3. Построение дерева в Go даёт контроль над `depth` и форматом вывода.

### 6. Структуры данных

```go
type SpecConfigResult struct {
    Profile   SpecConfigProfile   `json:"profile"`
    Stats     SpecConfigStats     `json:"stats"`
    Hierarchy []SpecConfigNode    `json:"hierarchy,omitempty"`
}

type SpecConfigProfile struct {
    ProductName     string `json:"product_name"`
    SchemaName      string `json:"schema_name"`
    RootDir         string `json:"root_dir"`
    UsecaseLayout   string `json:"usecase_layout"`
    IDStyle         string `json:"id_style"`
    CrossRefStyle   string `json:"cross_ref_style"`
    NormativeLang   string `json:"normative_lang"`
    Traceability    string `json:"traceability"`
    HasChanges      bool   `json:"has_changes"`
    HasAudit        bool   `json:"has_audit"`
    HasADR          bool   `json:"has_adr"`
    CoverageMetrics bool   `json:"coverage_metrics"`
    ContextText     string `json:"context_text"`
}

type SpecConfigStats struct {
    Capabilities int `json:"capabilities"`
    Requirements  int `json:"requirements"`
    Scenarios     int `json:"scenarios"`
    Usecases      int `json:"usecases"`
    Changes       int `json:"changes"`
}

type SpecConfigNode struct {
    CapabilityName string           `json:"capability_name"`
    Title          string           `json:"title"`
    IsContainer    bool             `json:"is_container"`
    ChildrenCount  int              `json:"children_count"`
    Children       []SpecConfigNode `json:"children,omitempty"`
}
```

### 7. Файлы для изменения

| Файл | Изменение |
|---|---|
| `internal/specsvc/specsvc.go` | `ExecuteSpecConfig` + структуры |
| `internal/store/db_lookup_spec.go` | `LoadSpecConfigProfile`, `LoadSpecConfigStats`, `LoadSpecConfigHierarchy` |
| `internal/mcp/registry.go` | Регистрация `codebase_query_spec_config`, исправление описания `spec_usecase` |
| `cmd/query_spec.go` | CLI-подкоманда `query spec config` |

## Risks / Trade-offs

- **[fa-reports при depth=2: 146 узлов]** → Управляемо (~25 KB). При depth=3 — 1894 узлов, но это явный выбор пользователя.
- **[Контейнерные узлы без title]** → `is_container` эвристика может ложно сработать на spec.md с пустым title. Снижение риска: реальные spec.md всегда имеют `## Purpose` и хотя бы title из `#`.
- **[spec_usecase: только описание в registry]** → Код уже возвращает `ErrSpecNotFound`. Меняем только строку описания, поведение не меняется. Существующие тесты не затронуты.
- **[file_path на узлах иерархии намеренно опущен]** → Путь к `spec.md` реконструируется клиентом из `profile.root_dir` + `capability_name` (`{root_dir}/specs/{capability_name}/spec.md`) для неконтейнерных узлов; `is_container = true` сигнализирует об отсутствии файла. Добавление `file_path` на каждый узел дублировало бы `root_dir` N раз (для fa-reports — 1894 × ~80 байт ≈ 150 KB). Для других spec-инструментов (`spec_search`, `spec_deps`, `spec_history`), где `root_dir` недоступен в ответе, `file_path` был бы ценен — это отдельный change.

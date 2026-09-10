## Context

Текущая реализация `ExecuteSpecCoverage` (`specsvc.go:471-593`) принимает обязательный `capabilityName` и возвращает saved-метрики (`api_total`, `api_covered`, `code_total`, `code_listed`) либо gaps. `computeSpecCoverage` (`specsvc.go:595-642`) считает per-capability числитель относительно всего продукта — бессмысленная метрика. Связь capability ↔ код — много-ко-многим через `relations(references_code)` от источников `spec_capability`/`spec_requirement`/`spec_scenario` к код-сущностям. Постпроцессор (`indexer_postprocess_spec.go`) резолвит `spec_code_mentions` в `relations`. Пагинация MCP-ответов уже есть (`mcp-pagination`, ~8000 байт на чанк).

## Goals / Non-Goals

**Goals:**
- Новый контракт `codebase_query_spec_coverage`: `product` (required) + `name` (optional) + `kind` (optional) → перечень покрытых сущностей, сгруппированных по capability.
- Один SQL-запрос, возвращающий строки `(capability_id, capability_name, title, entity_name, entity_kind)` с дедупликацией по `name|kind` внутри capability.
- CLI и MCP-инструмент используют одну функцию `ExecuteSpecCoverage` с новым контрактом.

**Non-Goals:**
- Метрики покрытия (проценты, saved-значения) — убираются.
- Режим gaps — убирается из этого инструмента (возможно отдельный `codebase_query_spec_gaps` в будущем).
- Изменение постпроцессора или схемы БД — не требуется.

## Decisions

### 1. Один SQL-запрос с UNION ALL по типам сущностей

Резолвить имена сущностей по `target_type` через UNION ALL — по одной ветке на тип (как в существующем gaps-запросе `specsvc.go:531-561`):

```sql
WITH target_capabilities AS (
  SELECT id, capability_name, title
  FROM spec_capabilities
  WHERE ds_product_id = $1
    AND ($2 = '' OR LOWER(capability_name) = LOWER($2))
), capability_sources AS (
  SELECT sc.id AS cap_id, 'spec_capability'::text AS source_type, sc.id AS source_id
  FROM target_capabilities sc
  UNION ALL
  SELECT req.capability_id, 'spec_requirement', req.id
  FROM spec_requirements req
  JOIN target_capabilities sc ON sc.id = req.capability_id
  UNION ALL
  SELECT req.capability_id, 'spec_scenario', s.id
  FROM spec_scenarios s
  JOIN spec_requirements req ON req.id = s.requirement_id
  JOIN target_capabilities sc ON sc.id = req.capability_id
), covered AS (
  SELECT DISTINCT cs.cap_id, r.target_type, r.target_id
  FROM capability_sources cs
  JOIN relations r ON r.source_type = cs.source_type AND r.source_id = cs.source_id
  WHERE r.relation_type = 'references_code'
    AND ($3 = '' OR r.target_type = $3)
)
SELECT c.cap_id, c.capability_name, c.title, e.entity_name, e.entity_kind
FROM covered c
JOIN LATERAL (
  SELECT contract_name AS entity_name, 'api_contract' AS entity_kind FROM api_contracts WHERE id = c.target_id
  UNION ALL SELECT proc_name, 'sql_procedure' FROM sql_procedures WHERE id = c.target_id
  ... -- по одной ветке на тип
) e ON e.entity_kind IS NOT NULL
```

**Альтернатива**: `LEFT JOIN` к каждой таблице по `target_type` (как в `ExecuteSpecByCode`). Менее читаемо при 8 типах. UNION ALL в LATERAL — компактнее и не плодит NULL-колонки.

### 2. Дедупликация в Go, не в SQL

SQL возвращает строки; Go собирает `map[cap_id] → capability` с `map[name|kind] → struct{}` для дедупликации. Это проще чем `DISTINCT ON` в SQL и даёт контроль над порядком (сортировка по `kind, name`).

### 3. Структура ответа

```go
type SpecCoverageResult struct {
    Product      string                   `json:"product"`
    Capabilities []SpecCoverageCapability `json:"capabilities"`
}
type SpecCoverageCapability struct {
    CapabilityName string                 `json:"capability_name"`
    Title          string                 `json:"title"`
    Covered        []SpecCoverageEntity   `json:"covered"`
}
type SpecCoverageEntity struct {
    Name string `json:"name"`
    Kind string `json:"kind"`
}
```

`SpecCoverageGap` остаётся в коде (для будущего `spec_gaps`), но из ответа убирается.

### 4. `product` → `ds_product_id` резолвится в SQL

`spec_capabilities.ds_product_id` уже связан с `ds_products.id`. Запрос фильтрует по `ds_product_id` через подзапрос `ds_products WHERE product_name = $1`, либо принимает `ds_product_id` напрямую. Для простоты — join через `ds_products` в `target_capabilities`.

## Risks / Trade-offs

- **[BREAKING контракт]** Параметр `product` становится required, `name` — optional, `mode` удалён. Существующие вызовы с `name` без `product` перестанут работать. → Mitigation: это намеренное изменение; инструмент перепроектируется.
- **[Размер ответа]** Продукт-level запрос может вернуть сотни записей. → Mitigation: автоматическая пагинация MCP (`mcp-pagination`) уже работает; ответ разбивается на чанки ~8000 байт.
- **[LATERAL UNION ALL]** 8 веток в LATERAL на каждую covered-строку. Для больших продуктов (тысячи покрытых сущностей) — нагрузка. → Mitigation: `kind` фильтр сужает запрос; `target_type` индексируется в `relations`.

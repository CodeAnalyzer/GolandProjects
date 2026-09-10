# Bug Report: spec_coverage считает покрытие одной capability относительно всего продукта вместо продукт-level агрегации

**Дата:** 2026-09-08
**Файл:** `internal/specsvc/specsvc.go` (`computeSpecCoverage`, строки 588–638)
**Версия CodeBase:** 0.9.1
**Статус:** Не исправлено

## Summary

`codebase_query_spec_coverage` при вызове для конкретной capability возвращает бессмысленную метрику: числитель — это сущности, на которые ссылается **одна** capability, а знаменатель — **все** сущности продукта. Результат выглядит как «3/2903» (0.1%) вместо ожидаемого продукт-level покрытия с разбивкой по capability.

Спека (`openspec/specs/query/spec-queries/spec.md:49-56`) требует: при вызове **с фильтром продукта** возвращать покрытие **всего продукта** (например, 55/69) **с разбивкой по capability**. Реализация считает per-capability относительно всего продукта — метрика не имеет смысла.

---

## Environment

- CodeBase 0.9.1, MCP-инструмент `codebase_query_spec_coverage`
- Продукт: `fa-contracts`, capability: `consumer-event`

---

## Reproduction Steps

1. Вызвать `codebase_query_spec_coverage` с `name = "consumer-event"`, `product = "fa-contracts"`.
2. Получить результат: `api_total = 2903, api_covered = 3, code_total = 246129, code_listed = 8`.

## Expected Result

Метрика покрытия продукта: `api_covered` = количество уникальных API-контрактов, на которые ссылается **хотя бы одна** capability продукта `fa-contracts`; `api_total` = 2903. С разбивкой по capability внутри.

## Actual Result

`api_covered` = 3 — только API-контракты, на которые ссылается **одна** capability `consumer-event`. Знаменатель — весь продукт (2903). Метрика: 3/2903 ≈ 0.1% — не отражает реальное покрытие продукта спеками.

---

## Root Cause Analysis

### 1. Спека требует продукт-level агрегацию

`openspec/specs/query/spec-queries/spec.md:49-56`:

> Система SHALL **по продукту или capability** возвращать метрики покрытия …

Сценарий:

> **GIVEN** продукт с 69 API-сервисами и спеками, разрешившими 55 упоминаний контрактов
> **WHEN** вызывается `codebase_query_spec_coverage` **с фильтром продукта**
> **THEN** возвращено покрытие **55/69 с разбивкой по capability**

«55» — это сумма по **всем** capability продукта, не по одной.

### 2. Реализация ограничивает числитель одной capability

`computeSpecCoverage` (`specsvc.go:588-638`), CTE `capability_sources`:

```sql
capability_sources AS (
    SELECT 'spec_capability'::text AS source_type, $1::bigint AS source_id
    UNION ALL SELECT 'spec_requirement', req.id FROM spec_requirements req WHERE req.capability_id = $1
    UNION ALL
    SELECT 'spec_scenario', s.id FROM spec_scenarios s
    JOIN spec_requirements req ON req.id = s.requirement_id WHERE req.capability_id = $1
)
```

`$1` — это `capabilityID` одной запрошенной capability. `covered_entities` джойнит `relations` только с sources этой одной capability. Знаменатель `product_entities` включает все сущности продукта.

### 3. Saved-метрики тоже per-capability

Колонки `api_total`, `api_covered`, `code_total`, `code_listed` в `spec_capabilities` заполняются постпроцессором профилирования (`indexer_postprocess_spec_profile.go`). Однако в схеме БД (`db_schema.go:612`) они описаны как `покрытие из Notes, NULL = считать из relations` — то есть это saved-значения для **одной** capability, не продукта.

---

## Impact

- **Средний:** метрика `spec_coverage` для конкретной capability возвращает бессмысленные числа (доля одной capability во всём продукте).
- Продукт-level покрытие (как описано в спеке) невозможно получить текущим API.
- Пользователи не могут оценить, какая доля кода продукта покрыта спеками в целом.

---

## Suggested Fix

### Вариант: продукт-level агрегация с разбивкой по capability

1. Добавить режим продукта: при вызове `codebase_query_spec_coverage` с `product = "fa-contracts"` (без `name`) — считать `covered_entities` по **всем** capability продукта (через `spec_capabilities WHERE ds_product_id = ...`), а не по одной.
2. Возвращать разбивку: для каждой capability продукта — её вклад в покрытие (сколько API/кода она покрывает).
3. При вызове с `name` — вернуть покрытие **этой** capability относительно **сущностей, которые она упоминает** (а не всего продукта), либо явно пометить как per-capability.

### Файлы для изменения

1. **`internal/specsvc/specsvc.go`** — `ExecuteSpecCoverage` (строки 466–638): добавить продукт-level режим.
2. **`internal/specsvc/specsvc.go`** — `computeSpecCoverage` (строки 588–638): расширить `capability_sources` до всех capability продукта при продукт-level запросе.
3. **`internal/specsvc/specsvc.go`** — `SpecCoverageResult`: добавить поле `[]SpecCoveragePerCapability` для разбивки.

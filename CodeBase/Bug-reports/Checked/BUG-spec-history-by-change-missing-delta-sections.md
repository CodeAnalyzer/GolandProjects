# Bug Report: spec_history по change не возвращает delta-секции (ADDED/MODIFIED/REMOVED) — только список capability

**Дата:** 2026-09-08
**Файл:** `internal/specsvc/specsvc.go` (`executeSpecHistoryByChange`, строки 714–744)
**Версия CodeBase:** 0.9.1
**Статус:** Исправлено

## Summary

`codebase_query_spec_history` при вызове по change (параметр `change`) возвращает только список затронутых capability без delta-секций (ADDED/MODIFIED/REMOVED). Поле `changes` в JSON-ответе — `null`. Спека требует delta-секции с именами и телами затронутых требований; их отсутствие — особый случай (skip_specs), а не норма.

---

## Environment

- CodeBase 0.9.0, MCP-инструмент `codebase_query_spec_history`
- Change: `2026-09-02-rms-3965352-parallel-accrual-purchase` (archived, продукт fa-contracts)

---

## Reproduction Steps

1. Вызвать `codebase_query_spec_history` с `change = "2026-09-02-rms-3965352-parallel-accrual-purchase"`.
2. Получить результат:

```json
{
  "change_name": "2026-09-02-rms-3965352-parallel-accrual-purchase",
  "status": "archived",
  "changes": null,
  "capabilities": [
    {
      "capability_id": 177903,
      "capability_name": "consumer-cession/purchase",
      "title": "Покупка портфеля кредитов (цессия)",
      "product": "fa-contracts"
    }
  ]
}
```

## Expected Result

`changes` содержит delta-секции: `section` = "ADDED", `requirement_name` = "Параллельная обработка начислений при покупке портфеля", `body_text` = тело требования. Для change с `skip_specs: true` — пометка «связь извлечена из proposal, delta-требований нет».

## Actual Result

`changes` = `null`. Возвращён только список capability без delta-секций.

---

## Root Cause Analysis

### 1. Спека требует delta-секции в обоих режимах

`openspec/specs/query/spec-queries/spec.md:63-75`:

> Система SHALL по slug capability возвращать хронологию изменений: … для каждого — статус, артефакты и **delta-секции (ADDED/MODIFIED/REMOVED) с именами и телами затронутых требований**. **Обратный запрос — по change его затронутые capability.**

Сценарий «Change со skip_specs» (строки 72-75):

> **THEN** возвращена связь с capability `CORE/data` **с пометкой, что связь извлечена из proposal (delta-требований нет)**

Фраза «delta-требований нет» — **особый случай** (skip_specs). В обычном режиме delta-требования должны быть.

### 2. executeSpecHistoryByChange не запрашивает delta

`specsvc.go:720-729`:

```sql
SELECT DISTINCT sc.change_name, sc.status, c.id, c.capability_name, c.title, COALESCE(dp.product_name, '')
FROM relations r
JOIN spec_changes sc ON r.source_type = 'spec_change' AND sc.id = r.source_id
JOIN spec_capabilities c ON r.target_type = 'spec_capability' AND c.id = r.target_id
LEFT JOIN ds_products dp ON dp.id = c.ds_product_id
WHERE r.relation_type = 'change_modifies'
  AND LOWER(sc.change_name) = LOWER($1)
  AND ($2 = '' OR dp.product_name = $2)
ORDER BY c.capability_name, c.id
```

Нет джойна к `spec_change_delta`. Запрос возвращает только capability.

### 3. Обратный режим (по capability) — delta есть

`ExecuteSpecHistory` (`specsvc.go:682-695`) — режим по `name` capability:

```sql
LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)
```

Здесь delta-секции запрашиваются и возвращаются. Асимметрия: по capability — delta есть, по change — нет.

### 4. Поле Changes без omitempty

`SpecHistoryResult` (`specsvc.go:132`):

```go
Changes []SpecHistoryEntry `json:"changes"` // без omitempty
```

`nil`-слайс сериализуется как `null`, а не отсутствует в JSON.

---

## Impact

- **Средний:** при запросе по change пользователь не видит, какие требования были добавлены/изменены/удалены — только имена затронутых capability. Приходится читать файл change вручную.
- Спека явно требует delta-секции в обоих режимах.
- `skip_specs`-сценарий не реализован (нет пометки «извлечено из proposal»).

---

## Suggested Fix

### Добавить delta-секции в executeSpecHistoryByChange

1. В SQL `executeSpecHistoryByChange` добавить `LEFT JOIN spec_change_delta d ON d.change_id = sc.id AND LOWER(d.capability_slug) = LOWER(c.capability_name)`.
2. Возвращать delta-секции (section, requirement_name, body_text) для каждой capability.
3. Для change с `skip_specs: true` (delta-секций нет) — возвращать пометку в ответе (например, `source = "proposal"`).
4. Инициализировать `Changes: make([]SpecHistoryEntry, 0)` вместо `nil` (или добавить `omitempty` в тег).

### Файлы для изменения

1. **`internal/specsvc/specsvc.go`** — `executeSpecHistoryByChange` (строки 714–744): добавить джойн `spec_change_delta`, заполнить `Changes`.
2. **`internal/specsvc/specsvc.go`** — `SpecHistoryResult.Changes` (строка 132): добавить `omitempty` или инициализировать пустым слайсом.
3. **`internal/specsvc/specsvc.go`** — `SpecHistoryCapability`: добавить поля для delta-секций или использовать существующую структуру `SpecHistoryEntry`.

## Why

`codebase_trc_tree` — единственный MCP-инструмент для построения дерева вызовов TRC-трейса — таймаутит (60s) на сессиях с сотнями тысяч событий. Причина: `parent_id` хранится как 1-based offset в рамках сессии, а не реальный `id` строки, что требует `numbered` CTE с `row_number()` на каждом запросе. Recursive CTE выполняет O(D×M) hash probes (D = глубина дерева, M = все события сессии), делая инструмент нефункциональным для больших трейсов.

## What Changes

- После `COPY IN` событий в `trc_events` выполняется Go-маппинг `parent_id` из offset → реальный `id` строки: `SELECT id` → Go-маппинг → `COPY IN` в temp table → `UPDATE ... JOIN` по PK
- `LoadEventsForTree` упрощается: `numbered` CTE полностью удаляется, recursive CTE использует прямой `JOIN tree t ON c.parent_id = t.id` по индексу `idx_trc_events_session_parent`
- Сложность запроса снижается с O(D×M) до O(D×log N)
- **BREAKING**: ранее сохранённые сессии имеют `parent_id` как offset — требуется миграция (UPDATE для существующих сессий) или пересоздание

## Capabilities

### New Capabilities

_(нет)_

### Modified Capabilities

- `trc-analysis/trc-aggregation-tree`: `parent_id` в `trc_events` хранит реальный `id` родительской строки вместо 1-based offset; `LoadEventsForTree` использует прямой JOIN по индексу вместо `numbered` CTE

## Impact

- **`internal/trc/store.go`** — `insertTRCEvents`: замена SQL UPDATE с `row_number()` на Go-маппинг (SELECT id → Go map → COPY IN temp table → UPDATE JOIN по PK); `LoadEventsForTree`: упрощение recursive CTE
- **`internal/trc/tree.go`** — `ComputeParentIDs`: без изменений (вычисляет offset как раньше), маппинг выполняется в Go после COPY IN
- **`internal/store/db_schema.go`** — индекс `idx_trc_events_session_parent` уже существует, используется напрямую
- **Миграция** — для существующих БД: пересоздание сессий (reparse), т.к. старый SQL UPDATE с `row_number()` непредсказуем на больших сессиях
- **Производительность** — парсинг 400K событий: Go-маппинг ~5-7с (JOIN по PK, O(N)); запрос дерева: ~60s → <1s (ожидаемо)

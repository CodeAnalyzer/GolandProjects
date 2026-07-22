# Оптимизация prune/delete для TRC и RTI сессий

Оптимизация удаления больших объёмов данных (18.7M строк / 28 Гб) в `trc_events` и `rti_calls` через TRUNCATE для полной очистки, batch-удаление для частичной, удаление избыточных индексов и авто-VACUUM после prune.

## Контекст проблемы

Текущие `PruneSessions` и `DeleteSession` (и для TRC, и для RTI) используют одиночный `DELETE FROM <sessions> WHERE ...`, полагаясь на `ON DELETE CASCADE`. При удалении сессии с миллионами дочерних строк PostgreSQL выполняет каскадное удаление в одной транзакции, обновляя 11+ индексов на каждую строку — это занимает минуты и создаёт огромное давление на WAL.

## Затронутые файлы

- `internal/trc/store.go` — `DeleteSession`, `PruneSessions`
- `internal/rti/store.go` — `DeleteSession`, `PruneSessions`
- `internal/store/db_schema.go` — удаление избыточных индексов
- `cmd/trc.go` — `runTRCPrune`, `runTRCDelete` (прогресс-вывод)
- `cmd/rti.go` — `runRTIPrune`, `runRTIDelete` (прогресс-вывод)
- `internal/mcp/registry.go` — валидация `keep_last=0` для MCP tools
- `internal/trc/store_test.go` — новые тесты
- `internal/rti/store_test.go` — новые тесты (или `enrich_test.go` если store_test отсутствует)

## План (6 шагов)

### Шаг 1: TRUNCATE при keep_last=0 (TRC + RTI)

Когда `keepLast == 0` — удалить все сессии. Вместо `DELETE` использовать `TRUNCATE trc_events, trc_sessions RESTART IDENTITY CASCADE` (и аналогично для RTI: `TRUNCATE rti_client_events, rti_blog_tables, rti_blog_blocks, rti_checkpoints, rti_params, rti_calls, rti_sessions RESTART IDENTITY CASCADE`). TRUNCATE не трогает индексы построчно — выполняется за секунды независимо от объёма данных.

**Изменения:**
- `internal/trc/store.go` — в `PruneSessions`: если `keepLast == 0`, выполнить `TRUNCATE trc_events, trc_sessions RESTART IDENTITY CASCADE`, вернуть количество удалённых сессий (предварительно `SELECT count(*)`).
- `internal/rti/store.go` — то же для RTI-таблиц.
- `cmd/trc.go` — разрешить `trcKeepLast == 0` (уже разрешено, валидация `>= 0`).
- `cmd/rti.go` — проверить, что `rtiKeepLast == 0` разрешён (если нет — добавить).
- `internal/mcp/registry.go` — убрать проверку `keepLast <= 0` → `keepLast < 0` для обоих prune-инструментов (0 = удалить все).

### Шаг 2: Batch-удаление для keep_last > 0 (TRC + RTI)

Когда `keepLast > 0`, удаляются только некоторые сессии. Вместо одного CASCADE-DELETE:

1. Найти ID сессий на удаление: `SELECT id FROM trc_sessions WHERE id NOT IN (SELECT id ... ORDER BY parsed_at DESC LIMIT $1)`
2. Для каждой удаляемой сессии — удалить events батчами по 50K:
   ```sql
   DELETE FROM trc_events WHERE session_id = $1 AND id IN (
       SELECT id FROM trc_events WHERE session_id = $1 LIMIT 50000
   )
   ```
   Повторять пока `RowsAffected > 0`.
3. После очистки events — `DELETE FROM trc_sessions WHERE id = $1` (CASCADE уже нечего удалять).

Аналогично для RTI: батчить по `rti_calls` (через `rti_params` и `rti_checkpoints` CASCADE), затем `rti_blog_blocks`/`rti_blog_tables` (по `session_id`), затем `rti_client_events` (по `session_id`), затем `rti_sessions`.

**Изменения:**
- `internal/trc/store.go` — переписать `PruneSessions` для batch-режима при `keepLast > 0`.
- `internal/rti/store.go` — переписать `PruneSessions` аналогично.
- Добавить константу `batchDeleteSize = 50000`.

### Шаг 3: Batch-удаление для DeleteSession по ID (TRC + RTI)

`DeleteSession` тоже страдает от CASCADE при больших сессиях. Применить тот же подход: сначала батч-удаление дочерних таблиц, потом удаление session.

**Изменения:**
- `internal/trc/store.go` — переписать `DeleteSession`: батчить `trc_events` по `session_id`, затем `DELETE FROM trc_sessions`.
- `internal/rti/store.go` — переписать `DeleteSession`: батчить `rti_calls` (CASCADE → params/checkpoints), затем blog-таблицы, затем client_events, затем session.

### Шаг 4: Удаление избыточных индексов на trc_events

Анализ использования: все запросы к `trc_events` в `internal/trc/store.go` и `internal/mcp/registry.go` **всегда** фильтруют по `session_id`. Standalone-индексы без `session_id` не используются ни одним запросом.

**Удалить:**
- `idx_trc_events_session_id` — дублируется составными `idx_trc_events_session_spid`, `idx_trc_events_session_proc`, `idx_trc_events_session_duration`, `idx_trc_events_session_parent`, `idx_trc_events_session_event_name` (все начинаются с `session_id`).
- `idx_trc_events_procedure` — дублируется `idx_trc_events_session_proc`.
- `idx_trc_events_duration_ms` — дублируется `idx_trc_events_session_duration`.
- `idx_trc_events_spid` — дублируется `idx_trc_events_session_spid`.
- `idx_trc_events_event_sequence` — не используется ни в одном запросе (нет фильтра по `event_sequence` без `session_id`).

**Оставить** (используются или могут быть полезны для tree-CTE):
- `idx_trc_events_session_spid` — tree loading + SPID-фильтр
- `idx_trc_events_session_proc` — LoadEventsByProcedure, LoadProceduresAggregated
- `idx_trc_events_session_duration` — LoadSlowEvents
- `idx_trc_events_session_parent` — tree CTE
- `idx_trc_events_session_error` — LoadErrorEvents
- `idx_trc_events_session_event_name` — event_name фильтр

**Аналогично для rti_calls** — удалить:
- `idx_rti_calls_session_id` — дублируется составными `idx_rti_calls_session_proc`, `idx_rti_calls_session_elapsed`, `idx_rti_calls_session_retval`, `idx_rti_calls_session_entertime`.
- `idx_rti_calls_procedure` — дублируется `idx_rti_calls_session_proc`.
- `idx_rti_calls_elapsed_ms` — дублируется `idx_rti_calls_session_elapsed`.
- `idx_rti_calls_parent_id` — не используется без session_id; tree строится через CTE с `session_id` фильтром.

**Изменения:**
- `internal/store/db_schema.go` — убрать CREATE INDEX для удалённых индексов. Добавить `DROP INDEX IF EXISTS` для существующих БД.

**Итого:** -5 индексов на trc_events (11→6), -4 индекса на rti_calls (8→4). Каждый удалённый индекс = ~9% ускорения DELETE.

### Шаг 5: VACUUM ANALYZE после prune

После массового удаления таблицы и индексы раздуты. Добавить вызов `VACUUM ANALYZE` после prune (и только после prune, не после delete-одной-сессии).

**Важно:** `VACUUM` нельзя выполнить внутри транзакции. `store.DB` обёртка над `sql.DB` — `db.Exec("VACUUM ANALYZE trc_events, trc_sessions")` сработает, т.к. `sql.DB.Exec` не открывает явную транзакцию для одиночного оператора.

**Изменения:**
- `internal/trc/store.go` — в конце `PruneSessions` (после успешного удаления) вызвать `db.Exec("VACUUM ANALYZE trc_events, trc_sessions")`, игнорировать ошибку (не критично).
- `internal/rti/store.go` — то же для RTI-таблиц.
- Не делать VACUUM в `DeleteSession` (удаление одной сессии — не требует).

### Шаг 6: Тесты

- **TRC store tests:**
  - `TestPruneSessions_TruncateAll` — keepLast=0 → TRUNCATE, все сессии удалены, events пусты.
  - `TestPruneSessions_BatchDelete` — keepLast=1 с 3 сессиями → batch-удаление, 2 удалены, 1 осталась.
  - `TestDeleteSession_BatchDelete` — удаление одной сессии с events → батч-удаление.
  - `TestPruneSessions_VacuumNoError` — VACUUM не вызывает панику (ошибка игнорируется).

- **RTI store tests:**
  - `TestPruneSessions_TruncateAll` — keepLast=0 → TRUNCATE.
  - `TestPruneSessions_BatchDelete` — keepLast=1 с 3 сессиями.
  - `TestDeleteSession_BatchDelete` — удаление одной сессии с calls/params/checkpoints.

- **MCP registry tests:**
  - Обновить тесты для `keep_last=0` (теперь валидно).

## Порядок выполнения

1. Шаг 1 (TRUNCATE) → сборка + тесты
2. Шаг 2 (batch prune) → сборка + тесты
3. Шаг 3 (batch delete) → сборка + тесты
4. Шаг 4 (индексы) → сборка + тесты
5. Шаг 5 (VACUUM) → сборка + тесты
6. Шаг 6 (тесты) — выполняется вместе с каждым шагом

## Ожидаемый результат

| Сценарий | Было | Стало |
|---|---|---|
| `prune --keep-last 0` (18.7M events) | 5m55s | < 5s (TRUNCATE) |
| `prune --keep-last N` (частичное) | ~5m (CASCADE) | ~1-2m (batch + меньше индексов) |
| `delete --session ID` (большая сессия) | ~5m (CASCADE) | ~1-2m (batch) |
| Индексов на trc_events | 11 | 6 |
| Индексов на rti_calls | 8 | 4 |

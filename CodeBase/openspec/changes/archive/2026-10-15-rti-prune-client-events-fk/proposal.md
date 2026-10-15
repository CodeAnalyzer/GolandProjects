## Why

`rti prune --keep-last N` и `rti delete` падают с ошибкой FK на сессиях, где клиентские события связаны с серверными вызовами: `rti_client_events.server_call_id REFERENCES rti_calls(id)` объявлен без `ON DELETE` (db_schema.go:666), а оба места удаления чистят `rti_calls` раньше `rti_client_events` (store.go:639, store.go:752). Первая же сессия с коррелированными событиями (HRTI) обрушивает prune; результат частично применён.

## What Changes

- Порядок batch-удаления в `DeleteSession` и `PruneSessions`: `rti_client_events` удаляются **до** `rti_calls` (остальной порядок без изменений).
- FK `rti_client_events.server_call_id` получает `ON DELETE SET NULL`: в `CREATE TABLE` для свежих БД и через идемпотентную миграцию (guard по `pg_constraint.confdeltype`) для существующих БД.
- Тесты: unit + integration на удаление/prune сессии с коррелированными клиентскими событиями; тест миграции FK на «старой» схеме.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `rti-analysis/rti-storage`: удаление/prune сессии с коррелированными `rti_client_events` (непустой `server_call_id`) выполняются без ошибки FK; уточнён порядок batch-удаления (client_events раньше calls).
- `infrastructure/database-schema`: FK `rti_client_events.server_call_id` объявляется с `ON DELETE SET NULL`; миграция на существующих БД идемпотентна и выполняется без верификации пользователем.

## Impact

- `internal/rti/store.go` — `DeleteSession`, `PruneSessions` (перестановка порядка батчей).
- `internal/store/db_schema.go` — DDL `rti_client_events` (CREATE TABLE) + guard-миграция DO-блоком для существующих БД.
- Тесты: `internal/rti` (unit/integration), `internal/store` (миграция схемы).
- CLI/MCP-контракты не меняются (`rti prune`, `rti delete`, `codebase_rti_prune`, `codebase_rti_delete`); поведение — прежний успех вместо ошибки.
- Совместимость: однократная миграция FK (DROP+ADD, ACCESS EXCLUSIVE на `rti_client_events`, валидация FK-сканом) срабатывает один раз при первом прогоне схемы после обновления; последующие прогоны — no-op по guard-условию.

# Bug Report: `rti prune`/`rti delete` падают на FK `rti_client_events.server_call_id` — порядок batch-удаления и FK без правила удаления

**Дата:** 2026-10-15
**Файл:** `internal/rti/store.go` (`DeleteSession`, ~строка 639; `PruneSessions`, ~строка 752); `internal/store/db_schema.go` (~строка 666, DDL `rti_client_events`); CLI `rti prune` / `rti delete`; MCP `codebase_rti_prune` / `codebase_rti_delete`
**Версия CodeBase:** 0.9.3 (build 1596 на момент обнаружения)
**Статус:** Решено (2026-10-15). Исправлено в change [`rti-prune-client-events-fk`](../../openspec/changes/archive/2026-10-15-rti-prune-client-events-fk/) (заархивирован 2026-10-15; вариант C: перестановка порядка удаления + FK `ON DELETE SET NULL` с guard-миграрацией). Проверено integration-тестами и E2E на живой БД.
**Обнаружено:** вручную на рабочем ПК при рутинной очистке RTI-сессий (`C:\NT\FA#\7.2GIT> codebase rti prune --keep-last 5`).

## Summary

`codebase rti prune --keep-last 5` падает с ошибкой нарушения внешнего ключа на первой же сессии, у которой клиентские события связаны с серверными вызовами через `server_call_id` (HRTI-корреляция клиент ↔ серверный вызов):

```
Error: batch delete rti_calls for session 16: pq: UPDATE или DELETE в таблице "rti_calls"
нарушает ограничение внешнего ключа "rti_client_events_server_call_id_fkey" таблицы "rti_client_events"
```

Критичность: **HIGH** (storage-команды `rti prune`/`rti delete` неработоспособны на сессиях с коррелированными клиентскими событиями; результат prune частично применён — сессии, обработанные до упавшей, уже удалены).

---

## Environment

- CodeBase 0.9.3 (build 1596), CLI `rti prune` / `rti delete` (через `rtisvc.ExecutePrune` / `ExecuteDelete`)
- Живая БД `codebase`@localhost:5435 с RTI-сессиями, среди которых есть HRTI-сессии с клиентскими событиями (`rti_client_events.server_call_id IS NOT NULL`)

---

## Reproduction Steps

1. Сохранить RTI-сессию с клиентскими событиями, у которых `server_call_id` ссылается на вызовы этой сессии (HRTI-лог: клиентское событие инициировало серверный вызов).
2. Выполнить `codebase rti prune --keep-last 5` (или `codebase rti delete --session <id>`).
3. Наблюдать ошибку FK при batch-удалении `rti_calls`.

Минимальный SQL-репро (не требует RTI-файла):

```sql
INSERT INTO rti_sessions (file_path, file_size, total_calls, client_events_count)
VALUES ('repro.rti', 10, 1, 1) RETURNING id;  -- пусть id = :sid
INSERT INTO rti_calls (session_id, procedure, enter_line, exit_line, elapsed_ms, nest_level, module_id, tran_count, begin_cnt, spid)
VALUES (:sid, 'P', 1, 2, 1, 0, 0, 0, 0, 1) RETURNING id;  -- пусть id = :call_id
INSERT INTO rti_client_events (session_id, pid, seq_no, line_no, kind, server_call_id)
VALUES (:sid, 100, 1, 1, 'sql_block', :call_id);
-- затем: codebase rti delete --session :sid  →  Error: batch delete rti_calls: pq: ... нарушает ограничение внешнего ключа
```

## Expected Result

Сессия и все связанные записи (calls, params, checkpoints, blog_blocks, blog_tables, client_events) удаляются без ошибок; prune оставляет последние N сессий.

## Actual Result

Ошибка нарушения FK `rti_client_events_server_call_id_fkey` при удалении `rti_calls`; команда падает, результат частично применён (сессии до упавшей в цикле prune уже удалены).

---

## Root Cause Analysis

Два сопряжённых дефекта:

1. **Схема.** FK `rti_client_events.server_call_id → rti_calls(id)` объявлен **без правила удаления** (`db_schema.go`, `NO ACTION` по умолчанию) — единственная ссылка на `rti_calls` без `ON DELETE` (все остальные — `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables` — `ON DELETE CASCADE`).
2. **Порядок удаления.** `DeleteSession` и `PruneSessions` чистят таблицы батчами по 50000 в порядке `rti_calls → rti_client_events → ...`. Пока `server_call_id` у всех событий NULL (сессии без HRTI-корреляции) — порядок не проявлялся. Как только у сессии есть событие с непустым `server_call_id`, удаление вызова на шаге 1 нарушает «голый» FK.

`server_call_id` всегда ссылается на вызов той же сессии (корреляция выполняется в рамках одного parse-цикла), т.е. проблема локальна для удаляемой сессии.

TRUNCATE-путь (`prune --keep-last 0`) не затронут — `TRUNCATE ... CASCADE` обрабатывает FK в одном statement.

---

## Impact

- **HIGH:** `rti prune --keep-last N` (N>0) и `rti delete` неработоспособны на сессиях с коррелированными клиентскими событиями; затронуты CLI и MCP (`codebase_rti_prune`, `codebase_rti_delete`).
- Prune не атомарен: до падения часть сессий уже удалена (безопасно — повторный запуск добирает хвост).
- TRC-хранилище не затронуто (`trc_events` не ссылается на вызовы).

---

## Suggested Fix

Вариант C (реализован):

1. **Порядок** — в `DeleteSession` и `PruneSessions` удалять `rti_client_events` **до** `rti_calls` (работает при любом состоянии FK, включая непромигрированные БД).
2. **Схема** — FK получает `ON DELETE SET NULL`: в `CREATE TABLE` (свежие БД) и через guard-нутый DO-блок миграции (существующие БД): пересоздание только при `pg_constraint.confdeltype = 'a'` (legacy NO ACTION), на уже мигрированной схеме — no-op. `SET NULL`, а не `CASCADE`: событие принадлежит сессии, а не вызову; при удалении вызова событие сохраняется с обнулённой ссылкой.

---

## Verification

1. `TestClientEventsFK_FreshSchemaIsSetNull`, `TestClientEventsFK_MigratesLegacyConstraint`, `TestClientEventsFK_DeleteCallNullsReference` (`internal/store`, `-tags integration`) — PASS.
2. `TestDeletePruneWithCorrelatedClientEvents`, `TestDeletePruneWithLegacyClientEventsFK` (`internal/rti`, `-tags integration`) — PASS, включая delete/prune на намеренно деградированном legacy-FK.
3. Полный прогон: `go build/vet/test ./...`, `go test -tags integration ./internal/store/... ./internal/rti/...` — ok.
4. E2E на живой БД `codebase`@localhost:5435 (2026-10-15):
   - FK был `confdeltype='a'` (legacy), 0 сессий;
   - посеяны 6 сессий с коррелированными событиями → `codebase rti prune --keep-last 5` → «Deleted 1 session(s), kept last 5», `count(*)=5` (порядок работает на legacy FK без миграции);
   - `codebase health` (инициализация схемы) → FK стал `confdeltype='n'` (SET NULL);
   - `codebase rti prune --keep-last 0` → TRUNCATE-путь, зачистка probe-сессий.

---

## Resolution (2026-10-15)

Исправлено в change **[`rti-prune-client-events-fk`](../../openspec/changes/archive/2026-10-15-rti-prune-client-events-fk/)** (заархивирован 2026-10-15); дельты применены к `openspec/specs/rti-analysis/rti-storage/spec.md` и `openspec/specs/infrastructure/database-schema/spec.md`.

- `internal/rti/store.go`: в `DeleteSession` и `PruneSessions` batch-цикл `rti_client_events` переставлен перед `rti_calls` (остальной порядок без изменений).
- `internal/store/db_schema.go`: DDL `rti_client_events` — `server_call_id BIGINT REFERENCES rti_calls(id) ON DELETE SET NULL`; guard-нутый DO-блок миграции для существующих БД (одноразовый, срабатывает только при legacy `NO ACTION`).
- Тесты: `internal/store/rti_client_events_fk_migration_integration_test.go` (3 теста), расширение `internal/rti/store_integration_test.go` (2 теста + helpers).

Совместимость: старый бинарник на мигрированной схеме не падает (удаление вызова обнуляет `server_call_id` ещё живых событий); новый бинарник на legacy-схеме работает благодаря перестановке порядка.

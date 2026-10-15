## Context

FK `rti_client_events.server_call_id → rti_calls(id)` объявлен без правила удаления (db_schema.go:666), при этом все остальные ссылки на `rti_calls` (`rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`) — `ON DELETE CASCADE`. `DeleteSession` и `PruneSessions` чистят таблицы батчами по 50000 в порядке `rti_calls → rti_client_events → blog_* → session` (store.go:639, store.go:752). У сессий с коррелированными клиентскими событиями (HRTI: событие инициировало серверный вызов, `server_call_id` не NULL) удаление calls нарушает FK → prune падает, результат частично применён.

Схема инициализируется идемпотентными DDL-стейтментами (`InitSchemaCtx`) на множестве команд (`init`, `update`, `query`, `stats`, `health`, `review`) — миграция выполняется при каждом прогоне и обязана быть no-op на уже мигрированной схеме. Конкурентные прогоны схемы уже защищены (см. `schema_version_integration_test.go`), миграция должна вписаться в этот паттерн.

## Goals / Non-Goals

**Goals:**

- `rti delete` / `rti prune` успешно удаляют сессии с коррелированными клиентскими событиями.
- Устранение ошибки на уровне схемы: FK получает `ON DELETE SET NULL`, семантика — «событие переживает удаление вызова, ссылка обнуляется».
- Идемпотентная автоматическая миграция существующих БД без участия пользователя.
- Корректность удаления независимо от состояния FK (старый `NO ACTION` или новый `SET NULL`).

**Non-Goals:**

- Не меняем TRC-хранилище (`trc_events` не ссылается на вызовы, проблема не воспроизводится).
- Не вводим framework версионированных миграций — остаёмся в паттерне inline идемпотентных DDL.
- Не меняем путь сохранения сессий и контракты CLI/MCP.
- `keep-last 0` (TRUNCATE … CASCADE) не трогаем — корректен.

## Decisions

### D1. Порядок удаления: `rti_client_events` раньше `rti_calls`

В `DeleteSession` и `PruneSessions` переставить batch-цикл client_events перед batch-циклом calls; порядок остальных таблиц не меняется (`calls → blog_blocks → blog_tables → session` после перестановки: `client_events → calls → blog_blocks → blog_tables → session`).

Почему: работает при любом состоянии FK (включая ещё не мигрированную БД), нулевая стоимость. Альтернатива — полагаться только на `SET NULL` FK — отвергнута: до первого прогона схемы на БД остаётся старый `NO ACTION` FK, и удаление calls будет молча обнулять ссылки (лишняя работа) либо падать (старый FK).

`server_call_id` всегда ссылается на вызов той же сессии (корреляция выполняется в рамках одного parse), перекрёстных ссылок между сессиями нет — перестановка в пределах цикла одной сессии безопасна.

### D2. FK → `ON DELETE SET NULL`, не CASCADE

- `CASCADE` удалял бы клиентское событие вместе с вызовом — событие принадлежит сессии, а не вызову; при точечном удалении вызовов событие должно сохраниться.
- `SET NULL` сохраняет строку события, обнуляя только корреляцию. Колонка nullable — конфликтов нет.

### D3. Миграция: guard-нутый DO-блок в списке DDL

В `CREATE TABLE rti_client_events` для свежих БД: `server_call_id BIGINT REFERENCES rti_calls(id) ON DELETE SET NULL` (имя ограничения останется дефолтным `rti_client_events_server_call_id_fkey`).

Для существующих БД — после CREATE TABLE / индексов выполняется DO-блок:

```sql
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE c.conname = 'rti_client_events_server_call_id_fkey'
          AND t.relname = 'rti_client_events'
          AND c.confdeltype = 'a'  -- NO ACTION — старый вариант
    ) THEN
        ALTER TABLE rti_client_events
            DROP CONSTRAINT rti_client_events_server_call_id_fkey;
        ALTER TABLE rti_client_events
            ADD CONSTRAINT rti_client_events_server_call_id_fkey
            FOREIGN KEY (server_call_id) REFERENCES rti_calls(id)
            ON DELETE SET NULL;
    END IF;
END $$;
```

Почему guard по `confdeltype`: схема прогоняется часто; без guard каждый прогон платил бы DROP+ADD (ACCESS EXCLUSIVE на `rti_client_events` + валидация FK сканом). Guard делает миграцию одноразовой: срабатывает только при старом `NO ACTION`-варианте, на свежей и уже мигрированной схеме — no-op.

Альтернативы отвергнуты:
- `ADD CONSTRAINT IF NOT EXISTS` — не поддерживается PostgreSQL для ограничений.
- Версионированные миграции через `schema_migrations` — смена паттерна схемы, больше объёма, чем требует фикс.
- `NOT VALID` при ADD — не нужен: `rti_client_events` по объёму на порядки меньше `rti_calls` (событий на сессию тысячи, не миллионы), полная валидация дешевле усложнения.

DO-блок — один стейтмент для `ExecContext` (dollar-quoting совместим с lib/pq), вписывается в существующий цикл DDL-стейтментов.

### D4. Тесты

- Integration (`internal/rti`): сессия + вызов + клиентское событие с `server_call_id` → `DeleteSession` и `PruneSessions` завершаются без ошибки, строки удалены. Проверить и на старом FK (`NO ACTION`, пересоздать руками перед удалением) — порядок D1 работает без миграции.
- Integration (`internal/store`): миграция — создать старый FK вручную, прогнать `InitSchemaCtx`, убедиться `confdeltype='n'` (SET NULL); повторный прогон — no-op (констрейнт не пересоздаётся, имя/oid неизменны); на свежей схеме FK сразу `SET NULL`.
- Унит-покрытие SQL-логики не требуется — поведение целиком в БД.

## Risks / Trade-offs

- [Однократный ACCESS EXCLUSIVE на `rti_client_events` при миграции; конкуренция с длинной транзакцией → таймаут] → Миграция выполняется на старте команды, окно короткое (DROP+ADD на таблице клиентских событий); при конкурентных прогонах схемы действует существующая сериализация инициализации.
- [Потеря корреляции при удалении отдельного вызова (SET NULL)] → Намеренная семантика D2; в сценариях delete/prune сессии события всё равно удаляются целиком (порядок D1 удаляет их первыми).
- [Старые бинарники с новой схемой] → Старый порядок + `SET NULL` FK не падает: удаление вызова обнуляет `server_call_id` ещё живых событий, затем события удаляются своим батчем. Обратная совместимость сохранена.
- [Частично применённый prune у пользователя (сессии до 16-й удалены)] → После фикса повторный `prune --keep-last 5` доберёт оставшиеся; идемпотентность цикла prune не требует восстановления.

## Migration Plan

1. Деплой нового бинарника; любой запуск, инициализирующий схему (`init`, `update`, `query`, …), применит DO-блок миграции автоматически.
2. Откат: вернуть старый бинарник — схема с `SET NULL` FK остаётся рабочей (см. риски), ручных действий не требуется.

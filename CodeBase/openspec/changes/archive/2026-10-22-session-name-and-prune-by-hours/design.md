## Context

См. `proposal.md` — Why. Текущее устройство, важное для реализации:

- `rti_sessions`/`trc_sessions` создаются в `internal/store/db_schema.go`; аддитивные изменения схемы (`file_hash`, `parser_version`) уже делаются через `ALTER TABLE ... IF NOT EXISTS` внутри общего списка `InitSchemaCtx`, без изменения `CurrentSchemaVersion`.
- `InitSchemaCtx` выполняется командами `health`/`init`/`update`/`stats`/`query spec`, но не командами `rti`/`trc`.
- `rti parse` сначала ищет reuse по `(file_hash, parser_version)`; `trc parse` делает то же, затем `ParseFileToDB` создаёт строку сессии до стриминга событий.
- Удаление и prune уже реализованы батчами по 50000, но логика удаления дочерних таблиц продублирована между `DeleteSession` и `PruneSessions`.

## Goals / Non-Goals

**Goals:**

- Единая модель имени сессии для RTI и TRC: колонка `session_name`, значение по умолчанию из имени файла, явное переопределение в CLI/MCP.
- Единые правила delete-by-name и prune-by-hours для обоих анализаторов.
- Сохранение обратной совместимости CLI/MCP-контрактов и работы старых бинарников с новой схемой.

**Non-Goals:**

- Уникальность имени сессии и замена `session_id` именем как идентификатором.
- Поиск/фильтрация списка сессий по имени (кроме удаления).
- Изменение формата reuse-ответа и алгоритма хеширования.
- Автоматическое переименование legacy-сессий при каждом чтении (только backfill в схеме).

## Decisions

### 1. Колонка `session_name` с `DEFAULT ''`

```sql
ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS session_name TEXT NOT NULL DEFAULT '';
ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS session_name TEXT NOT NULL DEFAULT '';
UPDATE rti_sessions SET session_name = substring(file_path from '[^/\\]+$')
 WHERE session_name = '';
UPDATE trc_sessions SET session_name = substring(file_path from '[^/\\]+$')
 WHERE session_name = '';
```

- `NOT NULL DEFAULT ''` вместо nullable: старый бинарник/MCP-процесс, вставляющий сессию без имени, не падает и получает пустую строку.
- Backfill идемпотентен (`WHERE session_name = ''`) и безопасен при повторном `InitSchemaCtx`.
- `CurrentSchemaVersion` не меняется: все statements аддитивны и уже выполняются при каждом `InitSchemaCtx` (прецедент — `file_hash`/`parser_version`).
- Индекс по `session_name` не добавляется: таблица сессий мала (десятки–сотни строк), удаление по имени — редкая операция. При необходимости индекс можно добавить позже без изменения поведения.

### 2. Имя по умолчанию вычисляется в Go

`filepath.Base(filePath)` (с расширением) передаётся в `SaveSession`/`ParseFileToDB`. Это исключает расхождение между ОС и SQL-извлечением имени и покрывает Windows-пути с обратными слэшами. SQL-backfill нужен только для legacy-строк.

### 3. Reuse с явным именем переименовывает сессию

- `ExecuteParse` получает `ParseOptions{Name string}`; пустая строка означает «имя не задано».
- При reuse: если `Name != ""` и отличается от текущего, вызывается `SetSessionName(ctx, db, id, name)`; иначе имя не трогается.
- Альтернативы: игнорировать имя при reuse (ломает ожидание пользователя), считать имя частью ключа кэша (создаёт лишние сессии и ломает reuse) — отклонены.

### 4. Store API: общий helper удаления

В `internal/rti/store.go` и `internal/trc/store.go`:

- `SaveSession(..., sessionName string)` — имя пишется в INSERT.
- `SetSessionName(ctx, db, sessionID, name)` — используется reuse-веткой.
- `FindSessionIDsByName(ctx, db, name) ([]int64, error)` — точное совпадение `session_name`.
- `PruneSessionsOlderThan(ctx, db, cutoff time.Time) (int64, error)` — выборка `parsed_at < cutoff`.
- Внутренний `deleteSessionIDs(ctx, db, ids []int64) error` — вынос существующих батч-циклов; `DeleteSession` = `deleteSessionIDs([]int64{id})`, prune по ID/имени/времени используют его же.

Порядок удаления сохраняется: для RTI сначала `rti_client_events`, затем `rti_calls` (CASCADE на params/checkpoints), затем blog-таблицы, затем сессии. Для TRC — `trc_events`, затем `trc_sessions`. Это устраняет дублирование и гарантирует одинаковое поведение для ID, имени и времени.

### 5. Сервисные параметры

```go
type ParseOptions struct { Name string }
type DeleteParams struct { SessionID int64; Name string }
type PruneParams struct { KeepLast *int; KeepHours *int }
```

- `DeleteParams`: ровно один из `SessionID > 0` / `Name != ""`; иначе ошибка. Возвращаемый `DeleteResult` дополняется `DeletedCount int` и `SessionIDs []int64`; для одиночного удаления заполняется существующий `SessionID`.
- `PruneParams` с указателями: `nil` означает «аргумент не передан». Если оба `nil` — сохраняется текущее поведение `keep_last = 0` (полная очистка). Если оба заданы — ошибка. `KeepHours != nil && *KeepHours <= 0` — ошибка.
- Cutoff вычисляется в сервисе (`time.Now().Add(-N*time.Hour)`) и передаётся в store как значение; это делает поведение тестируемым и позволяет вернуть его в `PruneResult.Cutoff`.
- `PruneResult` дополняется `KeptHours *int` и `Cutoff *time.Time`; `KeptLast` остаётся для обратной совместимости.

На CLI наличие флага определяется через `cmd.Flags().Changed(...)`, на MCP — через наличие ключа в `args` (а не через нулевое значение), чтобы отличить `keep_last=0` от «не задан».

### 6. MCP-контракт

- `codebase_{rti,trc}_parse`: optional `name` (string).
- `codebase_{rti,trc}_delete`: `session_id` (int) или `name` (string), взаимоисключающие; ошибка, если заданы оба или ни одного.
- `codebase_{rti,trc}_prune`: `keep_last` или `keep_hours`, взаимоисключающие; `keep_hours > 0`.
- `list`/`summary` отдают `session_name` автоматически через модели сессий.
- Существующие имена аргументов не меняются; новые аргументы опциональны.

### 7. JSON-поле

В `RTISession`/`TRCSession` используется `session_name` (как `file_path`/`file_size`), в CLI-флаге — `--name`, в MCP — `name`. Это сохраняет единый стиль доменных моделей и короткий аргумент на границе.

## Risks / Trade-offs

- **Имя не уникально** → delete-by-name удаляет все совпадения. Это осознанное решение; результат возвращает `deleted_count` и `session_ids`, чтобы операция была наблюдаемой. Альтернатива (требовать уникальность) сломала бы повторные сессии после bump `parser_version`.
- **Опасность случайного массового удаления по общему имени** → CLI/MCP возвращают список ID; для точечного удаления остаётся `--session`/`session_id`.
- **Backfill по регулярке для Windows-путей** → покрывается `[^/\\]+$`; проверяется интеграционным тестом на путь с `\`.
- **Смешанные версии бинарников** → `DEFAULT ''` не ломает старые INSERT; сессии, созданные старым бинарником после миграции, получат пустое имя и не будут найдены по имени, но останутся доступны по ID.
- **Расхождение часов Go и PostgreSQL** → cutoff вычисляется в Go и передаётся в SQL; семантика `parsed_at < cutoff` эквивалентна `NOW() - N hours` на момент вызова. Для тестов это удобнее, чем `NOW()` внутри запроса.
- **Refactor удаления затрагивает проверенную логику FK** → helper сохраняет порядок таблиц и размер батча; существующие интеграционные тесты на correlated client events и legacy FK должны остаться зелёными.
- **TRC стриминговый INSERT создаёт сессию до парсинга** → имя передаётся в `ParseFileToDB`; при ошибке разбора сессия остаётся с `file_hash IS NULL` и не попадает в reuse, как и раньше.

## Migration Plan

1. Добавить колонки и backfill в `InitSchemaCtx`; проверить идемпотентность повторным запуском.
2. Обновить модели и store-функции, затем сервисный слой, CLI и MCP.
3. Прогнать unit/integration-тесты; отдельно проверить backfill на legacy-строке и delete-by-name с дубликатами.
4. Применить схему к рабочей БД через `codebase health` (команды `rti`/`trc` сами схему не инициализируют).
5. Пересобрать `bin/CodeBase.exe`; для новых MCP-аргументов перезапустить запущенные MCP-серверы.

Rollback: код можно откатить на предыдущую версию — лишняя колонка с `DEFAULT ''` старым бинарникам не мешает. Удаление колонки не требуется и выполняется только вручную при необходимости.

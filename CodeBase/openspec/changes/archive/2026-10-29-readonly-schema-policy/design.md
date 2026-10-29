## Context

`InitSchemaCtx` (`internal/store/db_schema.go:14`) выполняет весь список из ~120 DDL-выражений при каждом вызове: один tx, advisory-lock `pg_advisory_xact_lock(hashtextextended('codebase/schema-init',0))` (`db_schema.go:1203`) берётся до первого statement'а и держится до COMMIT. `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` берёт ACCESS EXCLUSIVE даже как no-op. Причина дедлока и полный разбор — `Bug-reports/BUG-query-deadlock-init-schema-ddl-20261026.md`; мотивация — proposal.md.

Проверенная карта вызовов (факт, не предположение):

- **Schema owner (DDL легитимен):** `cmd/init.go:98`, `cmd/update.go:55`.
- **Read-only с полным DDL (дефект):** `internal/querysvc/runtime.go:101,133`, `cmd/query_spec_exec.go:24`, `cmd/stats.go:218`, `cmd/health.go:72`, `internal/reviewsvc/runtime.go:41`.
- **Уже без DDL:** MCP (`internal/mcp/server.go:76` — `CheckSchemaVersion`), `cmd/rti.go`/`cmd/trc.go`/`cmd/dsinfo.go` — `openDB()` вообще без проверки схемы (на пустой БД — криптичное `relation does not exist`).

Готовые примитивы: `CheckSchemaVersion` (`internal/store/db_schema_version.go`) уже возвращает различимые `ErrSchemaNotInitialized` (42P01) / `ErrSchemaUpdateRequired`; существует интеграционный тест `schema_version_integration_test.go`. Вечный backfill `UPDATE files` — легаси: продакшн-INSERT индексатора уже пишет `is_generated` (`internal/indexer/indexer.go:1776`), предикат (`rel_path ~* '/UPLOAD/'`, `extension = 't01'`) статичен для файла. В `cmd/root.go` machine-readable ветки пишут error-envelope и делают `return nil`; при этом `SilenceErrors=true` уже установлен (`root.go:66-69`) — возврат исходной ошибки не породит дубля на stderr.

## Goals / Non-Goals

**Goals:**

- В установившемся режиме (актуальная схема) ни read-only команды, ни `init`/`update` не выполняют DDL и не берут advisory-lock — класс дедлоков AccessShare ↔ AccessExclusive исчезает, старт CLI теряет 1–4 с накладных расходов.
- Единая политика: DDL — только `init`/`update`; все остальные команды (CLI и MCP) проверяют версию и дают различимую диагностику.
- Ошибки видимы: командный лог отражает фактический результат, exit code ненулевой при ошибке в любом режиме.
- Backfill `files.is_generated` — однократная именованная миграция.

**Non-Goals:**

- Полноценный миграционный фреймворк (down-миграции, версионный журнал отдельных DDL) — остаётся идемпотентный список + маркер версии.
- Устранение конкуренции «`update` идёт параллельно с долгим SELECT» — DDL vs выполняющийся запрос остаётся пользовательским сценарием (сегодня он не хуже).
- Изменение формата JSON-envelope, авторетрай при `deadlock detected`.
- Деградированный режим `health`/`stats` на устаревшей схеме — решение: единая жёсткая ошибка (см. D4).

## Decisions

### D1. Fast-path в `InitSchemaCtx` = переиспользование `CheckSchemaVersion`

В начале `InitSchemaCtx`: `if err := db.CheckSchemaVersion(ctx); err == nil { return nil }`. Rich-ошибки CheckSchemaVersion уже различают «нет схемы» / «нужен update» — дублировать логику отдельным `to_regclass`-запросом не нужно. Проверка выполняется на пуловом соединении вне транзакции: в установившемся режиме не берётся ни advisory-lock, ни один DDL-стейтмент. Альтернатива «проверять уже под advisory-lock» отклонена: сохраняет латентность и конкуренцию за лок при каждом запуске.

### D2. Double-check маркера внутри tx после advisory-lock

После `pg_advisory_xact_lock` — повторный `SELECT EXISTS(... schema_migrations WHERE version = CurrentSchemaVersion)` на tx-соединении; при актуальном маркере — rollback и `return nil`. Защищает гонку двух одновременных миграций (оба процесса увидели устаревшую версию до fast-path, взяли лок по очереди): второй процесс не повторяет 1–4-секундный проход. Альтернатива «полагаться на `IF NOT EXISTS`» корректна по результату, но не убирает повторный DDL-проход и нагрузку на каталог.

### D3. Bump `CurrentSchemaVersion` v2 → v3 в том же изменении

Существующая БД однократно проходит идемпотентный список при первом `codebase update` (пишется маркер v3, применяются именованные миграции). До этого read-only команды возвращают `ErrSchemaUpdateRequired` — одноразовая гонка «query мигрирует сам» исключена конструкцией, а не ретраем. Отклонённая альтернатива из баг-репорта: только fast-path без CheckSchemaVersion на read-путях — в установившемся режиме дедлок исчезает, но окно гонки при каждом bump'е остаётся.

### D4. Read-пути: замена вызова, без переобёртки ошибки

Шесть точек из карты вызовов: `db.InitSchema()` → `db.CheckSchemaVersion(ctx)`. Ошибка версии НЕ оборачивается в `errs.ErrSchemaInit` (термин «инициализация» вводит в заблуждение — попытки инициализации не было) — пробрасывается как есть: store-ошибки уже человекочитаемы и содержат подсказку «run codebase update». Для `querysvc.Execute`/`ExecuteWithProfile` ошибка маппится в возвращаемую обёртку команды без `ErrSchemaInit`. `ctx` в командах уже доступен (root context с signal handling).

### D5. rti/trc/dsinfo — helper в cmd-слое

`openDB()` возвращает `(*store.DB, nil-при-ошибке)` и не может донести типизированную ошибку — 29 call-site'ов проверяют `db == nil`. Вводится helper (например, `openDBReady(ctx) (*store.DB, error)`: `NewDB` + `CheckSchemaVersion`), call-site'ы rti/trc/dsinfo переключаются на него механически с пробросом ошибки в ответ команды. Проверка живёт в cmd, потому что именно там открывается соединение; сервисные слои (`rtisvc`/`trcsvc`) принимают готовый `*store.DB` и не меняются. Альтернатива «проверять в каждом Execute* сервисов» — размазывание политики по слоям, отклонена.

### D6. Лог + exit code: `return nil` → `return err` в JSON-ветках `root.go`

В четырёх machine-readable ветках `Execute()` после записи error-envelope возвращается исходная ошибка вместо `nil`. Deferred-логгер (`logCommandExecution`) читает named return → `status=error` с текстом; `main.go` уже делает `os.Exit(1)`. Дубль ошибки на stderr не появляется, т.к. `SilenceErrors/SilenceUsage` уже включены для machine-readable режимов. Отдельная переменная `execErr` из баг-репорта не требуется. Отклонённая альтернатива: логировать исходную ошибку, возвращая `nil` — фиксирует только лог и сохраняет ломающий exit-code контракт (решение В1 — наоборот).

### D7. Backfill — именованная миграция `files_is_generated_backfill_v1`

`UPDATE files SET is_generated = TRUE WHERE (rel_path ~* '/UPLOAD/' OR extension = 't01') AND NOT is_generated` выносится из вечного списка в `applyMigrationTx` (вызывается после основного списка — колонка к этому моменту гарантированно добавлена). Свежая БД: UPDATE на пустом наборе + маркер; существующая: однократное обновление ~91 строки из 202k; последующие прогоны — маркер, ноль сканирований. Потери поведения нет (см. Context: индексатор заполняет флаг при вставке).

### D8. Дисциплина версии схемы

Правило «изменение DDL-списка ⇒ bump `CurrentSchemaVersion`» фиксируется: комментарий-контрактом у константы, пунктом в AGENTS.md, тестами D8a/D8b из tasks (свежая БД — полный список и маркер; актуальная БД — ноль DDL). Тест-инвариант дедлока: открытая транзакция держит AccessShare на `files` → `InitSchemaCtx` на актуальной версии возвращается до её фиксации (guard-таймаут) — это прямое кодирование сценария из баг-репорта.

## Risks / Trade-offs

- [BREAKING: exit code 1 при ошибке в `--json`/`--ndjson`/`--summary`] → скрипты с проверкой `$LASTEXITCODE` потребуют правки. Mitigation: пункт в release notes/README (machine-readable контракт), формат envelope не меняется — парсинг-скрипты не затронуты.
- [После апгрейда read-only команды не работают до первого `codebase update`] → осознанный UX (ясная ошибка с подсказкой вместо тихой миграции); шаг зафиксирован в Migration Plan; альтернатива-автомиграция отклонена в баг-репорте.
- [TOCTOU fast-path: версия проверена, схема изменилась перед основным запросом] → обновление схемы — редкая ручная операция; конфликт возможен только при совпадении с `update` и сегодня проявляется жёстче. Принято.
- [Забытый bump при будущих правках DDL → новое DDL молча не применяется на существующих БД] → D8: комментарий-контракт, правило в AGENTS.md, тесты свежей/актуальной БД; ошибка проявится тестом «актуальная БД выполняет ноль DDL» только при честном bump — риск остаточный, осознанный.
- [Тест «не блокируется под AccessShare» flake на перегруженном CI] → guard-таймаут с внятным сообщением; блокировка-детектор вместо замера времени.
- [Латентность первого `update` после bump не меняется] → полный проход остаётся (это и есть механизм миграции); цель — убрать его из установившегося режима.

## Migration Plan

1. Реализация + интеграционные тесты; сборка `CodeBase.exe`.
2. На рабочей БД однократно `codebase update`: полный DDL-проход v2→v3 + backfill-миграция + миграции `applyMigrationTx`.
3. Smoke из баг-репорта: 10 параллельных пар `query procedure` × `query callers` — без `deadlock detected`; `pg_stat_database.deadlocks` не растёт; замер latency (ожидание: с ~3 с до <0.5 с); `query --json` при ошибке → exit 1, лог `status=error`.
4. Rollback: предыдущий бинарник работает с БД, имеющей маркер v3 (маркер просто не проверяется старым кодом, схема совместима) — возвращается прежнее поведение (вечный DDL-проход), без повреждения данных.

## Open Questions

Нет — все вилки (exit code, скоуп rti/trc/dsinfo, политика health/stats, размещение лог-фикса в спеках) закрыты в explore-фазе (В1–В4).

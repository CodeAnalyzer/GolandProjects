# Proposal: readonly-schema-policy

## Why

Каждая read-only CLI-команда (`query`, `stats`, `health`, `review`, `rti`, `trc`, `dsinfo`) при старте выполняет полный DDL-проход `InitSchemaCtx`: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` берёт ACCESS EXCLUSIVE и держит его до COMMIT, а advisory-lock сериализует только сами InitSchema, но не последующие SELECT-ы. При двух параллельных CLI-вызовах возникает классический цикл AccessShare ↔ AccessExclusive → `pq: deadlock detected` (спорадические отказы 1 из 6 прогонов; см. `Bug-reports/BUG-query-deadlock-init-schema-ddl-20261026.md`). Побочно: 1–4 с latency на каждый запуск (включая seq-scan `UPDATE files` по 202k строк) и DDL-нагрузка на системный каталог при каждом чтении индекса. Сопутствующий дефект: в machine-readable режиме (`--json`/`--ndjson`/`--summary`) ошибка конвертируется в JSON-envelope с возвратом `nil`, поэтому командный лог пишет `status=success`, exit code = 0 — реальный deadlock невидим ни в логе, ни в коде возврата.

## What Changes

- **Политика «проверяй, а не создавай»**: read-only пути (`querysvc.Execute`, `querysvc.ExecuteWithProfile`, `cmd/query_spec_exec.go`, `cmd/stats.go`, `cmd/health.go`, `reviewsvc`) заменяют `InitSchema()` на read-only `CheckSchemaVersion(ctx)`; на пустой/устаревшей схеме — различимая ошибка `ErrSchemaNotInitialized`/`ErrSchemaUpdateRequired` с подсказкой «run codebase update», без попытки тихой миграции.
- **rti/trc/dsinfo получают проверку версии схемы** (сегодня — никаких проверок, криптичный `relation does not exist` на пустой БД): через единый helper в cmd-слое вместо копипасты по 29 call-site'ам `openDB()`.
- **Fast-path в `InitSchemaCtx`** (для `init`/`update`): если маркер актуальной версии присутствует — немедленный `return nil` без advisory-lock и без единого DDL-выражения; double-check версии внутри транзакции после взятия advisory-lock (защита от гонки двух одновременных миграций).
- **Bump `CurrentSchemaVersion` v2 → v3**: существующая БД один раз проходит идемпотентный DDL-список при очередном `codebase update` и дальше в установившемся режиме не выполняет DDL вообще.
- **Вечный backfill `UPDATE files SET is_generated ...` вынесен в именованную one-time миграцию** `files_is_generated_backfill_v1` — даже при будущих bump'ах 202k-строчный seq-scan не повторяется.
- **Правило дисциплины схемы**: любое изменение DDL-списка требует bump `CurrentSchemaVersion` (иначе новое DDL молча не применится на существующих БД); фиксируется тестом и комментарием.
- **BREAKING: ошибки в machine-readable режиме возвращают ненулевой exit code** (было 0) и фиксируются в командном логе как `status=error` с текстом ошибки. JSON-envelope с `success:false` сохраняется; печать дубля ошибки в stderr подавляется существующим `SilenceErrors`.

## Capabilities

### New Capabilities

- (нет)

### Modified Capabilities

- `infrastructure/database-schema`: MODIFIED «Идемпотентная инициализация схемы» — fast-path по маркеру версии + double-check под advisory-lock; ADDED «Read-only команды не выполняют DDL» — политика проверки версии вместо создания схемы; MODIFIED «Признак is_generated в схеме файлов» — backfill как именованная one-time миграция.
- `infrastructure/encoding-cli`: MODIFIED «Логирование CLI-команд» — статус/ошибка в командном логе отражают фактический результат во всех режимах вывода; ADDED «Код завершения при ошибке в machine-readable режиме» — ненулевой exit code при ошибке (BREAKING), envelope без дубля ошибки в stderr. (Уточнение решения «новая capability command-logging»: командное логирование уже спек-ано в `encoding-cli` — требование «Логирование CLI-команд»; фиксы кладутся туда, чтобы не дублировать одну функцию в двух спеках.)

## Impact

- **Код**: `internal/store/db_schema.go` (fast-path, double-check, версия, миграция backfill), `internal/store/db_schema_version.go` (без изменений, переиспользуется), `internal/querysvc/runtime.go`, `internal/reviewsvc/runtime.go`, `cmd/query_spec_exec.go`, `cmd/stats.go`, `cmd/health.go`, `cmd/rti.go`, `cmd/trc.go`, `cmd/dsinfo.go` (helper проверки), `cmd/root.go` (возврат ошибки в JSON-ветках).
- **Совместимость**: **BREAKING** для скриптов, полагающихся на exit code 0 при ошибке в `--json`/`--ndjson`/`--summary`; после обновления бинарника существующая БД требует однократного `codebase update` (до него read-only команды возвращают понятную ошибку `ErrSchemaUpdateRequired`). MCP-сервер не затронут (уже использует `CheckSchemaVersion`, инструменты идут через `ExecuteWith`).
- **Тесты**: новые интеграционные тесты fast-path, политики read-only, регресс-тест блокировки (AccessShare на `files` не блокирует InitSchema на актуальной версии); smoke-матрица из баг-репорта (10 параллельных прогонов без deadlock, замер latency).

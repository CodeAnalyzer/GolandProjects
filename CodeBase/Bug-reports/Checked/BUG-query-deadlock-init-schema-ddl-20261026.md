# Bug Report: deadlock Postgres в параллельных query-командах — InitSchema выполняет DDL на каждом read-only запуске

**Дата:** 2026-10-26
**Файл:** `internal/querysvc/runtime.go:50`, `cmd/query_spec_exec.go:24`, `internal/store/db_schema.go:14-95,116,422,846,1203`; сопутствующий дефект логирования — `cmd/root.go:58-107`, `main.go:16-18`
**Версия CodeBase:** 0.9.4 (build 1640)
**Статус:** Реализовано в рамках openspec\changes\archive\2026-10-29-readonly-schema-policy

## Summary

Каждая CLI-команда `query` (и другие read-only команды) перед выполнением запроса вызывает `db.InitSchema()`, который безусловно прогоняет весь DDL-список: `CREATE TABLE/INDEX IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, `ALTER TABLE ... SET NOT NULL`, `DROP INDEX IF EXISTS`, а также тяжёлый `UPDATE files ...`. `ALTER TABLE` берёт **ACCESS EXCLUSIVE** на таблицу и держит его до COMMIT транзакции инициализации. Advisory-lock (`pg_advisory_xact_lock('codebase/schema-init')`, `db_schema.go:1203`) сериализует только сами InitSchema, но **не** последующие SELECT.

Итог: при двух параллельных CLI-вызовах один процесс ещё в DDL, второй уже в SELECT — возникает классический цикл блокировок:

- процесс A (SELECT) держит AccessShare на `sql_procedures` (или `spec_configs`), но ждёт AccessShare на `files`;
- процесс B (InitSchema) уже выполнил `ALTER TABLE files ADD COLUMN ...` (держит AccessExclusive на `files`) и ждёт AccessExclusive на `sql_procedures`.

Через `deadlock_timeout=1s` PostgreSQL убивает одну транзакцию → `pq: deadlock detected`. Побочно: из-за DDL каждый запрос тратит 1–4 с на старте, а `UPDATE files` (seq-scan 202 282 строк, обновляется 91) выполняется при каждом запуске.

Критичность: **MEDIUM** (спорадические отказы CLI-команд `query`/`spec` при параллельной работе + деградация latency; MCP не затронут — см. п. 5 Root Cause).

**Сопутствующий дефект:** в machine-readable режиме (`--json`/`--ndjson`/`--summary`) ошибка команды превращается в JSON-конверт, `Execute()` возвращает `nil`, поэтому лог пишет `status=success error=""`, а exit code = 0. Реальный deadlock в логе CodeBase не виден вообще.

---

## Environment

- CodeBase 0.9.4 (build 1640), Windows, Go 1.25
- PostgreSQL 18.1 (Windows-служба `postgresql-X64-18`, порт 5435), БД `codebase`
- Индекс FA: 202 282 файла, 17 релизных линий (fa-contracts develop + 11-01-6 + 11-01-7, fa-cards develop + 11-01-9 и др.)
- `log_min_messages=warning`, `log_lock_waits=off`, `deadlock_timeout=1s`
- Серверный лог: `C:\Program Files\PostgreSQL\18\data\log\postgresql-2026-10-26_000000.log`

---

## Reproduction Steps

1. Запустить две read-only команды одновременно:
   ```powershell
   .\CodeBase.exe query procedure --name SettlementOrder_OnlineAuthorize --json
   .\CodeBase.exe query callers --procedure CRD_MassAuthorize_CheckFlags --limit 10 --json
   ```
2. Повторять пары 5–10 раз.

Наблюдение: в 1 из 6 прогонов `query callers` вернул

```json
{"success":false,"format_version":"1.0","command":"query callers",
 "error":{"code":"query_failed","message":"query failed: pq: deadlock detected"}}
```

при exit code 0 и записи в `codebase_<date>.log`:

```
command="codebase query callers ... --json" duration=2.5s status=success error=""
```

Живой захват `pg_locks` в момент гонки (инструментированный прогон):

```
pid=12804 (query callers, SELECT ...)   wait AccessShareLock      on public.files          blocked_by=[21492]
pid=21492 (query procedure, InitSchema) wait AccessExclusiveLock  on public.sql_procedures blocked_by=[12804]
```

## Expected Result

- Параллельные read-only запросы не конкурируют за DDL-блокировки: `query`/`desc-search`/`spec *`/`health`/`stats` не выполняют DDL.
- Ошибка запроса отражается в командном логе как `status=error` с текстом ошибки.

## Actual Result

- Периодические `pq: deadlock detected` (серверный лог: 4 записи за 2026-10-26, из них 2 до эксперимента и 2 в ходе воспроизведения).
- `pg_stat_database.deadlocks` = 2 на момент первого замера ровно соответствует двум серверным записям до эксперимента.
- Лог CodeBase рапортует `status=success`, exit code 0.

---

## Evidence (серверный лог PostgreSQL)

### 1. 2026-10-26 17:32:19.244, `:2828` — `query procedure` × `InitSchema`

```
ERROR: deadlock detected
DETAIL: Process 12212 waits for AccessShareLock on relation 2613692; blocked by process 34236.
        Process 34236 waits for AccessExclusiveLock on relation 2613748; blocked by process 12212.
Process 12212: SELECT sp.id, ... FROM sql_procedures sp
               JOIN files f ON sp.file_id = f.id
               LEFT JOIN ds_products p ON p.id = f.ds_product_id
               LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
               WHERE sp.proc_name = $1
               ORDER BY (NOT f.is_generated) DESC, (COALESCE(br.is_default, FALSE)) DESC, sp.id DESC
               LIMIT 1
Process 34236: ALTER TABLE sql_procedures ADD COLUMN IF NOT EXISTS description TEXT
```

OID по `pg_class`: 2613692 = `files`, 2613748 = `sql_procedures`.

### 2. 2026-10-26 13:37:39.524, `:2632` — `query spec config` × `InitSchema`

```
ERROR: deadlock detected
DETAIL: Process 21256 waits for AccessShareLock on relation 2613692; blocked by process 15300.
        Process 15300 waits for AccessExclusiveLock on relation 2614753; blocked by process 21256.
Process 21256: SELECT sc.id, COALESCE(sc.root_dir,''), COALESCE(br.branch_key,''), COALESCE(br.is_default,FALSE)
               FROM spec_configs sc
               JOIN ds_products dp ON dp.id = sc.ds_product_id
               JOIN files f ON f.id = sc.file_id
               LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
               WHERE dp.product_name = $1
               ORDER BY COALESCE(br.is_default,FALSE) DESC, sc.id
Process 15300: ALTER TABLE spec_configs ADD COLUMN IF NOT EXISTS root_dir TEXT
```

OID 2614753 = `spec_configs`. Обе «горячие» формы запросов — из среза
`branch-aware-lookup` (`GetProcedureResult`, `LoadSpecConfigProfileByProduct`);
сам механизм столкновения существует независимо от веточных доработок.

### 3. 18:30:25 и 18:34:30 — воспроизведение (та же пара команд), DETAIL аналогичен п. 1.

---

## Root Cause Analysis

### 1. Read-only команды выполняют полный DDL

- `cmd/query_execution.go:25` → `querysvc.Execute` → `internal/querysvc/runtime.go:50`: `db.InitSchema()`.
- `cmd/query_spec_exec.go:24` — то же для spec-команд.
- `InitSchemaCtx` (`internal/store/db_schema.go:14`) выполняет весь список `statements` **при каждом вызове**, без проверки `schema_migrations`:

  - `db_schema.go:85` — `ALTER TABLE files ADD COLUMN IF NOT EXISTS ds_product_branch_id ...`
  - `db_schema.go:92-93` — `UPDATE files SET is_generated = TRUE WHERE ... AND NOT is_generated` (seq-scan всего `files`);
  - `db_schema.go:116-117` — `ALTER TABLE sql_procedures ADD COLUMN description/search_vector`;
  - `db_schema.go:332-354` — `ALTER TABLE pas_classes/pas_methods/pas_fields ... SET NOT NULL`;
  - `db_schema.go:1173-1188` — серия `DROP INDEX IF EXISTS`;
  - `db_schema.go:1203` — `pg_advisory_xact_lock('codebase/schema-init')`.

  `ALTER TABLE` берёт ACCESS EXCLUSIVE даже когда действие — no-op (`IF NOT EXISTS` при существующей колонке); блокировка держится до COMMIT всей транзакции инициализации.

### 2. Advisory-lock защищает только писателей схемы

`pg_advisory_xact_lock` сериализует две InitSchema между собой, но SELECT-ы его не берут. Сценарий:

1. Процесс A выполнил InitSchema, отпустил advisory-lock, начал SELECT (держит AccessShare на `sql_procedures`, затем Join'ом идёт в `files`).
2. Процесс B получил advisory-lock и начал DDL-проходку: уже взял AccessExclusive на `files`, теперь ждёт AccessExclusive на `sql_procedures`.
3. A ждёт AccessShare на `files` → цикл → deadlock.

### 3. Окно гонки расширено веточными доработками (не логический регресс)

- Срез A (`branch-aware-indexing`) добавил в вечно-выполняемый список `ALTER TABLE files`, `CREATE TABLE ds_products_branch`, индексы и `UPDATE files` — DDL-транзакция стала дольше держать ACCESS EXCLUSIVE на `files`.
- Срез B (`branch-aware-lookup`) добавил в горячие запросы join'ы `sql_procedures → files → ds_products → ds_products_branch` и `spec_configs → ds_products → files → ds_products_branch` — запросы стали дольше держать AccessShare; обе формы фигурируют в логах дедлоков.
- Механизм существовал и до срезов (старый `GetProcedureDetails` тоже шёл `sql_procedures → files`), но окно было меньше.

### 4. Сопутствующий дефект: JSON-режим скрывает ошибку от лога и от exit code

`cmd/root.go:80-105`: при `--json/--ndjson/--summary` ошибка уходит в `writeQueryErrorResponse` (JSON с `success:false`), после чего `Execute()` возвращает `nil`. Deferred-логгер (`cmd/root.go:63-65`, `logCommandExecution`) видит `nil` → `status=success error=""`; `main.go:16-18` не вызывает `os.Exit(1)`. В итоге лог фиксирует только ошибки Cobra, а все machine-readable ошибки (включая этот deadlock) невидимы.

### 5. Несогласованность путей: MCP уже работает правильно

MCP-сервер при открытии БД вызывает read-only `db.CheckSchemaVersion(ctx)` (`internal/mcp/server.go:76`, реализация `internal/store/db_schema_version.go:15`) и DDL не выполняет; вызовы инструментов идут через `querysvc.ExecuteWith` на уже открытом соединении (`internal/mcp/registry.go:1596-1603` — `Execute` с `InitSchema` только в fallback-ветке `db == nil`, в MCP не используемой). CLI query — единственный «горячий» путь, который делает DDL на каждый запуск.

---

## Impact

- **MEDIUM:** спорадические отказы CLI-команд `query`/`desc-search`/`spec *` при параллельной работе (два CLI-процесса или CLI + внешний клиент к БД); ошибка не видна в логе CodeBase и не отражается в exit code. MCP-сервер не затронут: инструменты работают через `ExecuteWith` без `InitSchema`.
- Латентность каждого CLI read-only запуска +1–4 с (полный DDL-проход + seq-scan `UPDATE files` 202k строк).
- Риск для `health`/`stats`/`review`, которые тоже вызывают `InitSchema` и могут попасть под тот же цикл.
- Данные индекса не повреждаются: жертва дедлока — транзакция инициализации или SELECT, ретрай успешен.

---

## Suggested Fix

### A. Убрать DDL из штатного пути read-only команд (основное)

1. **Read-only пути проверяют версию, а не создают схему:**
   - `internal/querysvc/runtime.go:50` — `db.InitSchema()` → `db.CheckSchemaVersion(ctx)`;
   - `cmd/query_spec_exec.go:24` — то же;
   - аналогично `cmd/stats.go:218`, `cmd/health.go:72`, `internal/reviewsvc/runtime.go:41`.
   - При устаревшей/пустой схеме — понятная ошибка `ErrSchemaUpdateRequired`/`ErrSchemaNotInitialized` («run codebase update»), а не тихая попытка миграции.
2. **Fast-path в `InitSchemaCtx`** (для `init`/`update`, чтобы не гонять DDL и `UPDATE files` на каждом запуске): до взятия advisory-lock проверить `to_regclass('schema_migrations')` и наличие `CurrentSchemaVersion`; если версия актуальна — сразу `return nil`. Advisory-lock брать до повторной проверки версии (double-check), затем statements.
3. **Поднять `CurrentSchemaVersion` до `codebase_schema_v3`** (`internal/store/db_schema.go:9`), чтобы существующая БД один раз прошла идемпотентный список и дальше DDL не выполняла. Первую проходку выполнить через `update` (единственный writer), а не через query — иначе одноразовая гонка сохранится.
4. **Вынести `UPDATE files SET is_generated ...` в именованную миграцию** (`files_is_generated_backfill_v1` через `applyMigrationTx`), чтобы даже при будущих bump'ах не сканировать 202k строк.
5. **Зафиксировать правило:** любое изменение списка DDL требует bump `CurrentSchemaVersion` (комментарий в `db_schema.go` + AGENTS.md). Без этого новое DDL молча не применится на существующих БД. Тест: свежая БД → полная схема создаётся; текущая БД → `InitSchemaCtx` не выполняет ни одного DDL.

### B. Починить логирование ошибок

`cmd/root.go`: сохранять исходную ошибку выполнения в отдельную переменную и логировать именно её, даже если в JSON-режиме возвращается `nil`:

```go
execErr := rootCmd.ExecuteContext(ctx)
defer func() { logCommandExecution(startedAt, args, execErr) }()
```

Отдельным решением (требует согласования, может сломать скрипты): должен ли `--json` при ошибке возвращать ненулевой exit code. Сейчас возвращает 0.

### C. Тесты и проверка

1. Integration-тест: при актуальном `schema_migrations` `InitSchemaCtx` не берёт блокировок и не блокируется, пока другая транзакция держит AccessShare на `files`.
2. Тест свежей БД: первый `InitSchema` создаёт схему и пишет маркер версии.
3. Регресс: `go build ./...`, `go vet ./...`, `go test ./...`, интеграционные (store/query/specsvc/indexer).
4. Smoke: 10 прогонов двух параллельных query без deadlock; замер падения latency (ожидание: с ~3 с до <0.5 с); `pg_stat_database.deadlocks` не растёт.
5. Пересборка `CodeBase.exe` и проверка, что лог при ошибке содержит `status=error` с текстом.

### Отклонённые альтернативы

- **Только fast-path без `CheckSchemaVersion` в read-only путях:** в установившемся режиме дедлок исчезает, но одноразовая гонка при bump версии остаётся.
- **Shared advisory-lock на всё время чтения (exclusive — на InitSchema):** надёжно, но требует обёртки всех read-path и выделенного соединения; latency DDL при апгрейде остаётся.
- **Убрать `InitSchema` из read-only команд без версионной проверки:** запрос на пустой/старой БД падает «relation does not exist» без внятной диагностики.

---

## Related

- `Modifications/Code-review-branch-aware-lookup.md` — независимый code review среза B.
- `Bug-reports/Анализ-deadlock-detected.md` — исходный анализ инцидента (серверный лог, OID, сценарий гонки).
- `openspec/changes/branch-aware-lookup/design.md` (D4) — веточные join'ы, участвующие в горячих запросах.

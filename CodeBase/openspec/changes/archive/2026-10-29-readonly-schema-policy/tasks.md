## 1. Store: fast-path и версия схемы

- [x] 1.1 `InitSchemaCtx` (`internal/store/db_schema.go`): в начале fast-path — `CheckSchemaVersion(ctx)` без ошибки → `return nil` (без advisory-lock и DDL). Verify: интеграционный тест — на БД с актуальным маркером `InitSchemaCtx` возвращается до фиксации параллельной транзакции, держащей AccessShare на `files` (guard-таймаут); тест `go test ./internal/store/ -run TestInitSchema` зелёный
- [x] 1.2 Double-check: после `pg_advisory_xact_lock` повторная проверка маркера в tx; актуален → rollback, `return nil` без DDL. Verify: расширение `schema_version_integration_test.go` — два конкурентных `InitSchemaCtx` на неактуальной схеме оба успешны, маркер записан один раз
- [x] 1.3 Bump `CurrentSchemaVersion` `codebase_schema_v2` → `codebase_schema_v3`; комментарий-контракт у константы: «изменение DDL-списка ⇒ bump версии». Verify: тест свежей БД — полный список применён, маркер v3 в `schema_migrations` (`TestCheckSchemaVersion_CurrentDatabase` зелёный)
- [x] 1.4 Вечный `UPDATE files SET is_generated ...` вынесен из списка statements в `applyMigrationTx("files_is_generated_backfill_v1", ...)` после основного списка. Verify: тест — после первого `InitSchemaCtx` маркер `files_is_generated_backfill_v1` записан, копии `*/UPLOAD/` и `*.t01` помечены; повторный прогон — данные не меняются (маркер, no-op)

## 2. Read-only пути → проверка версии

- [x] 2.1 `internal/querysvc/runtime.go:101` (`ExecuteWithProfile`) и `:133` (`Execute`): `db.InitSchema()` → `db.CheckSchemaVersion(ctx)`, ошибка версии пробрасывается без обёртки `errs.ErrSchemaInit`. Verify: интеграционный тест — `query` на пустой БД → `ErrSchemaNotInitialized`, на устаревшей (маркер v2) → `ErrSchemaUpdateRequired`; объекты схемы не созданы
- [x] 2.2 `cmd/query_spec_exec.go:24` — та же замена. Verify: интеграционный тест spec-команды на пустой/устаревшей БД возвращает различимую ошибку с подсказкой `codebase update`
- [x] 2.3 `cmd/stats.go:218` и `cmd/health.go:72` — та же замена (жёсткая ошибка без деградации, решение В3). Verify: `go build` + тест stats/health на пустой БД → различимая ошибка; на актуальной — прежний вывод
- [x] 2.4 `internal/reviewsvc/runtime.go:41` — та же замена. Verify: интеграционный тест review на пустой БД → `ErrSchemaNotInitialized`
- [x] 2.5 Регресс-тест политики: инвариант «читающая транзакция с AccessShare на `files` не блокирует `InitSchemaCtx` на актуальной версии» (`TestInitSchema_FastPathNotBlockedByReader`); полная матрица «10 параллельных пар без deadlock» — в smoke 6.3. Verify: инвариант-тест зелёный; smoke 6.3 — 10 пар без `deadlock detected`

## 3. rti/trc/dsinfo — helper проверки (решение В2)

- [x] 3.1 Helper в cmd-слое (например `openDBReady(ctx) (*store.DB, error)`): `NewDB` + `CheckSchemaVersion`, типизированный проброс ошибки вместо `nil`. Verify: `go build`; тест helper на пустой БД возвращает `ErrSchemaNotInitialized`
- [x] 3.2 Переключить call-site'ы `cmd/rti.go` (12), `cmd/trc.go` (12), `cmd/dsinfo.go` (5) на helper с пробросом ошибки в ответ команды (CLI и machine-readable). Verify: `rti list`/`trc list`/`dsinfo list` на пустой БД дают «schema not initialized … run codebase init», а не `relation does not exist`; на актуальной схеме — прежнее поведение

## 4. Лог и exit code (решение В1, В4)

- [x] 4.1 `cmd/root.go` `Execute()`: в четырёх machine-readable ветках после записи error-envelope `return err` вместо `return nil` (дубль на stderr исключён включённым `SilenceErrors`; deferred-логгер читает named return). Verify: тест/ручной прогон `query procedure --name NoSuchProc --json` с недоступной БД — exit code 1, stdout — envelope `success:false`, stderr без текста ошибки, `codebase_*.log` содержит `status=error` с текстом
- [x] 4.2 Негативная проверка контракта успеха: успешная `query --json` → exit 0, `status=success`. Verify: ручной прогон + существующие тесты envelope зелёные

## 5. Документация

- [x] 5.1 `AGENTS.md`: правило «любое изменение DDL-списка `db_schema.go` требует bump `CurrentSchemaVersion`» в раздел про схему/store. Verify: текст присутствует
- [x] 5.2 `README.md`: machine-readable контракт — ненулевой exit code при ошибке (**BREAKING**, решение В1); после обновления бинарника однократный `codebase update` до первого read-only запуска. Verify: разделы присутствуют

## 6. Регресс и smoke

- [x] 6.1 `go build ./...`, `go vet ./...`, `go test ./...`. Verify: все зелёные
- [x] 6.2 Интеграционные наборы: `go test ./internal/store/ ./internal/query/ ./internal/specsvc/ ./internal/indexer/ -tags integration` (теги — как в проекте). Verify: зелёные, включая новые тесты 1.1–2.5
- [x] 6.3 Пересборка `CodeBase.exe` с бампом `buildNumber = "1650"` (и `version` при необходимости — политика нумерации сборок в `cmd/root.go`) и smoke на рабочей БД: один `codebase update` (переход v2→v3 + backfill-миграция); затем 10 параллельных пар `query procedure` × `query callers --json` — ни одного `deadlock detected`, `pg_stat_database.deadlocks` не растёт; замер latency старта query (ожидание < 0.5 с, было ~3 с); `codebase_*.log` при искусственной ошибке содержит `status=error`. Verify: наблюдения зафиксированы в tasks-комментарии/итоге; banner показывает build 1650

## Итог smoke (6.3, 2026-10-29)

- Баннер: `CodeBase 0.9.4 build 1650`.
- `codebase update`: 334s, Changes 0, Errors 0 — переход v2→v3 (одноразовый DDL-проход + `files_is_generated_backfill_v1`).
- Latency `query procedure --json`: 599ms (первый, прогрев) → **138–142ms** установившейся режим (было ~3 с).
- 10 параллельных пар `query procedure` × `query callers --json`: 20/20 конвертов `success:true`, stderr пуст, `deadlock detected` — 0; `pg_stat_database.deadlocks` 7 → 7 (не растёт).
- Ошибка (`--dsinfo 999`): exit 1, stdout — envelope `success:false`, stderr — 0 байт, лог `status=error error="dsinfo profile 999 not found"`.
- Успех: exit 0, лог `status=success`.

## 7. Фиксы code review (code-review-readonly-schema-policy.md)

- [x] 7.1 [B1] Подсказка `run codebase init` / `run codebase update` в самой `CheckSchemaVersion` (единая точка); MCP-обёртка без дублирования. Verify: querysvc/mcp-интеграционные тесты проверяют Contains подсказки; TestPrepareMCPDatabase_* зелёные
- [x] 7.2 [B2] Различимые коды schema_not_initialized / schema_update_required в classifyQueryError/classifyStatsError/classifyHealthError/classifyReviewError/classifyDsinfoError; мёртвые ветки errs.ErrSchemaInit убраны. Verify: TestClassify* с новыми sentinels зелёные
- [x] 7.3 [Н1] dsinfo-конверт консолидирован на root-уровень (writeDsinfoErrorResponse + detectDsinfoCommandName): ошибки уровня Cobra в dsinfo --json больше не теряются молча; handleDsInfoError удалён (14 call-site'ов → возврат ошибки). Verify: TestWriteDsinfoErrorResponse, TestDetectDsinfoCommandName, TestDsInfoJSONErrorEnvelope
- [x] 7.4 [Н2] Комментарии db_schema.go актуализированы: полный список выполняется только на переходах версии схемы; data-фиксы — ремонты легаси-БД (no-op на свежей). Verify: текст присутствует
- [x] 7.5 [Н3] Double-check в InitSchemaCtx пробрасывает scanErr/checkErr (отмена контекста не уводит к DDL молча). Verify: go build/vet, suite зелёные
- [x] 7.6 [Н4] ctx прокинут: executeStats/executeHealth/specsvcExecute+runSpecCommand (7 sites)/querysvc.ExecuteCtx+executeQueryPlain; Execute(run) — deprecated-обёртка. Verify: go vet, suite зелёные
- [x] 7.7 [Н6] openDBOptional: offline-fallback только для ErrDBConnect/ErrConfigNotLoaded; schema-ошибки — жёстко; прочие сбои проверки — наружу. Verify: TestOpenDBOptional_SchemaErrorIsHard зелёный
- [x] 7.8 [Н5] Тест stats/health на пустой БД добавлен (TestExecuteStats/Health_SchemaNotInitialized); 2.5 переформулирован под фактический инвариант-тест; подсказка assert-ится в querysvc-тестах. Verify: cmd/querysvc integration зелёные
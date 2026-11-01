# Code Review: change `relations-incremental-consistency`

- **Дата:** 2026-11-01
- **Ревьюер:** opencode (только анализ, код не менялся)
- **Объём:** `internal/store/db_reconcile_relations.go` (+unit/integration-тесты), `internal/indexer/indexer_relations_reconcile.go`, `internal/indexer/reconcile_relations_integration_test.go`, `internal/indexer/runner.go`, `README.md`, `cmd/root.go`, `openspec/config.yaml`, дельта-спека; отчёт прогона `Tests/RECONCILE_INCREMENTAL_RESULTS_2026-11-01.md` и логи `Tests/reconcile-run/`.

**Вердикт: ГОТОВ К АРХИВАЦИИ** (после уточнения отчёта прогона — замечание F1).

Проверено воспроизведением:

- `go build ./...`, `go vet ./...` — чисто;
- юнит-тесты `go test ./internal/store/ ./internal/model/ ./internal/indexer/` — ok;
- integration-тесты живьём на PG localhost:5435 — **8 store + 12 indexer, все PASS**;
- `openspec validate --all --strict` — **34 passed / 0 failed**.

## 1. Соответствие design → код

| Решение | Реализация | Оценка |
|---|---|---|
| D1 порядок фаз | `runner.go:316–348`: workers → reconcile → `DeleteFilesByPathsExcept`/`DeleteFilesByPaths` → postprocessing | Точно по design |
| D2 purge 17 типов, без source-remap | `db_reconcile_relations.go:25–43` — ровно 17 binding'ов; юнит-тест сверяет с `model.RelationEntityTypes`; косвенные (report_field/report_param/vb_function) через `DELETE ... USING report_forms` | Корректно |
| D3 remap 3 типов по identity | max(id) в пределах `files.path`, `LOWER(name)` [+kind] — эквивалент резолверов внутри path (оговорка D3 соблюдена) | Корректно |
| D4 удаление остатков | Критерий по old file_id, не `NOT EXISTS` — старые строки на этом шаге ещё живы | Корректно |
| D5 частичный сбой | `reconcileOK=false` блокирует **оба** удаления → оба поколения живы, резолверы берут max id, safety-net доводит | Корректно |
| D6 наблюдаемость | Счётчики purge/remap/unresolved по типам; нулевой прогон печатает нули | Подтверждено логами |
| D7 изоляция | join по `files.path`; тест 4.9 (branchB не задет) | Корректно |

## 2. Сильные стороны

- **Safety-net** (`runner.go:189`, до `GetLatestFilesByRootPath`) — `existing` уже без дублей; best-effort с ретраем следующим прогоном; сценарий crash-window покрыт `TestReconcile_SafetyNetCleansCrashWindow`.
- **No-op без запросов**: ранний выход в `reconcileRelations:88` и `GetOldFileGenerationIDs:62` — подтверждено (update №4: 0 мс, 0 запросов).
- **Порядок purge → remap → delete** не даёт воскреснуть рёбрам purged-источников (тест 4.13); тест идемпотентности — счётчики второго вызова 0/0/0.
- **`new_id <> old_id`** в remap (`db_reconcile_relations.go:283`) корректно отсекает «само-совпадение» при переименовании сущности: max(id) по path находит старую строку, ребро уходит в `deleted_unresolved`. Монотонность serial гарантирует, что max — строка нового поколения (включая crash-window с двумя старыми поколениями).
- Чанкинг 500/1000, счётчики через `RowsAffected`; индексы `idx_relations_source_type_id` / `idx_relations_target_type_id` (db_schema.go:1121–1122) покрывают DELETE.
- **Без DDL**: `git diff` не трогает `db_schema.go` / `CurrentSchemaVersion`; README-раздел точно соответствует коду; build 1658→1661.
- Все 13 сценариев задач 4.1–4.13 имеют тесты; property-тест «incremental == full rebuild» на id-агностичном identity-наборе с честно задокументированным Non-Goal по sql_table-occurrence; проверка дублей полного кортежа.

## 3. Замечания

### F1 — Средняя (отчёт/фикстура CLI-прогона, не код)

XML-контракт в CLI-прогоне **не участвовал вовсе**: дефолтные `IncludePatterns` не содержат `*.xml` (`internal/config/config.go:377–381`), а `Tests/reconcile-run/codebase.test.toml` не задаёт `include_patterns`. Следствия по логам прогона:

- init: `XML files: 0`, `SQL files: 37` — XML не обходился; на диске 38 файлов (7 SQL + 1 XML + 30 gen), в отчёте «37 файлов» с XML в перечне;
- update №1: `Files updated: 3`, `old_file_generations=4` (3 modified + 1 removed) — правка «описание FCD_X_Get.xml» не дала эффекта, вопреки формулировке «Изменены»;
- семейства `implements_contract`/`executes_contract` в CLI-прогоне отсутствуют (check-init: ровно 4 семейства, 104 = 6+35+32+31) — remap контрактов в CLI не проверялся.

Сценарий при этом покрыт интеграционным тестом `TestReconcile_ContractFileModified` (прошёл живьём). Цифры в отчёте достоверны (сверены с `update*.log`, `check-*.txt`, `scan-update.txt`; 57/233 = 24,5%), но нарратив вводит в заблуждение.

**Рекомендация:** добавить `include_patterns = ["*.sql", "*.xml"]` в `Tests/reconcile-run/codebase.test.toml` и перепрогнать update №1, либо скорректировать разделы «Фикстура»/«update №1» отчёта с пометкой, что контрактная ветка проверена интеграционным тестом.

### F2 — Низкая

`FindDuplicateFileGenerations` (`db_reconcile_relations.go:108–114`): LIKE-паттерн строится без экранирования `%`/`_` в rootPath — подчёркивание в корне совпадёт с любым символом (возможны ложноположительные sibling-пути). Деградации корректности нет (пути с >1 поколениями подлежат чистке в любом случае), но scope safety-net шире заявленного. Плюс seq scan `files` (~623k строк на FA) на каждом update — консистентно с существующим `GetLatestFilesByRootPath`, DDL сознательно не тронут.

### F3 — Инфо

Подзапрос remap видит и старые строки (не ограничен новым поколением) — работает благодаря монотонности id и фильтру `new_id <> old_id`; неявно, заслуживает комментария.

### F4 — Инфо

Порог 50 мс в `TestReconcile_NoopUpdateZeroCounters` — теоретический флак на нагруженной машине; локально стабилен.

## 4. Состояние change

- Открыта только задача 5.2 (синк дельты при архивации); карта доменов в `openspec/config.yaml` уже добавлена — опережение, не противоречит.
- После `/opsx-archive` обязателен контрольный `openspec validate --all --strict`.

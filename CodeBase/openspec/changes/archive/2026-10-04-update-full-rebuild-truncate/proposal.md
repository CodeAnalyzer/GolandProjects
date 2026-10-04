# Proposal: update-full-rebuild-truncate

## Why

`codebase update --modified=false` сейчас выполняет полный перепрох через update-ветку: каждый файл вставляется новой записью, а старые сущности удаляются каскадными DELETE по ~40 таблицам. Замеры на FA (145 341 файлов) показали 4ч39м, повторный прогон — 47 тыс. файлов за ~2.5ч (прерван как непрактичный), тогда как `init` на чистой БД — 1ч35м (только INSERT, без per-file DELETE). Ручной обход (чистая БД + `init`) работает, но теряет RTI/TRC-сессии и историю сканов.

## What Changes

- `codebase update --modified=false` становится полной пересборкой индекса: `InitSchema` → `TRUNCATE` таблиц кодовой базы → запуск штатного init-пайплайна (walk без pre-filter → парсинг → batch insert → постобработка → LSA). Update-ветка остаётся только для `--modified=true`.
- Новый store-метод `ResetCodebaseTables(ctx)`: один `TRUNCATE TABLE ... CASCADE` по 52 таблицам кодовой базы. Сохраняются: RTI-таблицы (`rti_*`), TRC-таблицы (`trc_*`), `schema_migrations`, `scan_runs` (история сканов), `stats_snapshot` — усекается (кэш, обновляется в конце прогона).
- Перед пайплайном удаляются LSA-sidecar файлы (`spec_lsa_model.bin`, `spec_lsa_state.json`) — пересоздаются ретрейном.
- Прогресс-строка пересборки помечается меткой `rebuild` (вместо унаследованной `init`).
- Уточнена справка флага: `--modified=false = full rebuild of codebase index; RTI/TRC sessions and scan history are kept`.
- Не меняется: семантика `--modified=true` (дефолт) с pre-filter по mtime+size; RTI/TRC storage (`rti_prune`/`trc_prune`).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/file-walking`: требование «Pre-filter по fingerprint (mtime + size)» — сценарий `--modified=false` переописан: помимо отключения pre-filter и hash-сравнения, прогон выполняется как полная пересборка (TRUNCATE + init-пайплайн), а не по per-file update-ветке. Новое требование «Полная пересборка индекса (--modified=false)»: сброс таблиц, удаление LSA-sidecar, делегация в init-пайплайн, сохранение RTI/TRC-сессий и истории scan_runs, метка прогресса `rebuild`, доигрывание прерванной пересборки через `--modified=true`.
- `infrastructure/database-schema`: новое требование «Сброс таблиц кодовой базы (ResetCodebaseTables)» — один `TRUNCATE ... CASCADE` по таблицам кодовой базы с явным перечнем сохраняемых (`rti_*`, `trc_*`, `schema_migrations`, `scan_runs`).

## Impact

- `internal/store/db_schema.go` (или новый `db_reset.go`) — `ResetCodebaseTables(ctx)`.
- `internal/indexer/runner.go` — ветка в `UpdateCtx` при `onlyModified == false`: сброс → удаление sidecar → делегация в init-пайплайн; выделение внутреннего пайплайна с параметром метки прогресса (`init` / `rebuild`).
- `cmd/update.go` — текст справки флага `--modified`.
- Тесты: `internal/store` (ResetCodebaseTables: таблицы пусты, RTI/TRC/scan_runs живы), `internal/indexer` (делегация, метка rebuild).
- Поведение: во время пересборки (~1.5ч на FA) индекс пуст, запросы возвращают пустые результаты; прерванная пересборка возобновляется повторным запуском `update --modified=false` (полный рестарт; инкрементальный run сущности восстанавливает, но не relations-граф).

# Оптимизация пайплайна `codebase init`/`update` — устранение узких мест

План поэтапной оптимизации: 6 фаз, от дешёвых локальных фиксов к архитектурным. После каждой фазы — сборка + тесты + явный акцепт пользователя.

Предыстория: план `init-pipeline-perf-640dcd.md` (реализован) устранил N+1 в per-file резолвах и распараллелил пост-обработку. Данный план закрывает следующий уровень проблем: микро-транзакции, дубли SELECT-back, многократное чтение файлов, последовательный saveFile.

---

## Фаза 0: Инструментирование — тайминги по стадиям

**Цель:** получить фактические замеры до оптимизации, чтобы подтвердить приоритеты и измерить эффект каждой фазы.

**Файлы:**
- `internal/indexer/runner.go` — добавить замеры времени по стадиям: walk+hash, saveFile (суммарно), processFile (суммарно), post-processing (по каждой из 4 задач), deleted-files cleanup (для update). Вывод в финальную статистику.
- `internal/model/` (ScanStats или отдельная структура TimingStats) — поля длительностей.
- `cmd/init.go`, `cmd/update.go` — вывод таймингов в консоль после статистики.

**Критерий акцепта:** `codebase update` на реальной базе печатает разложение времени по стадиям.

---

## Фаза 1: Дешёвые локальные фиксы

### 1.1. Кэш ds_products в saveFile

**Проблема:** `GetOrCreateDSProductIDByName` (`internal/store/db_products.go:21-27`) — INSERT..ON CONFLICT DO UPDATE на **каждый** файл (перезапись `updated_at` → dead tuples). Продуктов — десятки, файлов — десятки тысяч.

**Файлы:**
- `internal/indexer/indexer.go` (`saveFile`, строки 1362-1394) — in-memory map `productName → id` в `Indexer` с mutex; промах кэша → один upsert, попадание → 0 round-trips.
- `internal/store/db_products.go` — `ON CONFLICT DO NOTHING` + fallback SELECT (не трогать строку при повторе). Опционально: метод `LoadDSProductIDs()` для предзагрузки всей таблицы при старте init/update.

### 1.2. Пул соединений под параллелизм

**Проблема:** `db.SetMaxIdleConns(5)` (`internal/store/db.go:114-116`) при `parallel > 5` → постоянное закрытие/открытие соединений (TCP + auth на батч).

**Файлы:**
- `internal/store/db.go` — `SetMaxIdleConns` = `SetMaxOpenConns` (25), либо параметр из конфига. `SetConnMaxLifetime` оставить.
- Опционально `internal/config/config.go` — `max_open_conns`/`max_idle_conns` в `[database]`.

### 1.3. Убрать дубли SELECT-back в parseSQLLikeFile

**Проблема:** `internal/indexer/indexer_sql_pas.go`:
- `FindSQLIndexDefinitionIDsByFile` вызывается дважды (строки 209 и 244) для одного файла.
- `buildSQLProcedureTableRelations` (`indexer_relations.go:248-256`) заново запрашивает `FindSQLProcedureIDsByFile` + `FindSQLTableIDsByFileAndLine`, которые уже загружены на строках 278-289.

**Файлы:**
- `internal/indexer/indexer_sql_pas.go` — переиспользовать `indexIDs` из первого вызова для index fields.
- `internal/indexer/indexer_relations.go` — `buildSQLProcedureTableRelations` принимает готовые `procedureIDs`/`tableIDs` map'ы вместо fileID (сигнатура меняется, вызовы обновить). Тесты в `indexer_relations_helpers_test.go` / `indexer_postprocess_sql_calls_test.go` адаптировать.

**Критерий акцепта:** build + `go test ./internal/indexer/... ./internal/store/...` PASS; -3 round-trip на SQL-файл.

---

## Фаза 2: Чтение файла один раз

**Проблема:** SQL-файл читается с диска 3–5 раз:
1. `computeHash` в walker (`internal/fswalk/fswalk.go:197-210`)
2. `sqlparser.ParseFile`
3. `apimacro.ParseFile` (`indexer.go:1204`)
4. retcode-прескрининг (`indexer_sql_pas.go:381`, повторный `ReadFile`)
5. у T01 — `buildT01GeneratedSubscriberRelations` (`indexer_sql_pas.go:703`)

**Файлы:**
- `internal/fswalk/fswalk.go` — walker возвращает `Content []byte` (сырые байты) в FileInfo; хэш считается из уже прочитанных байт. Опция конфига на случай памяти: файлы 5NT маленькие, риск минимален.
- `internal/encoding/` — API чтения из байт (уже есть конвертеры CP866/CP1251 → UTF-8, добавить `Decode(content []byte, enc string) string`, если нет).
- Парсеры sql/h/apimacro/retcode — перейти на `ParseContent([]byte|string)` там, где сейчас только `ParseFile` (у sql/h `ParseContent` уже есть). Все остальные парсеры (pas/js/smf/dfm/tpr/rpt/dsxml) — минимум перевести на чтение из FileInfo.Content.
- `internal/indexer/*` — processFile и все вызовы используют контент из FileInfo; `isLineInsideMacroDefinition`, retcode, T01 — работают с тем же контентом.

**Критерий акцепта:** один файл = одно чтение с диска во всём пайплайне (проверка code review + тесты PASS).

---

## Фаза 3: Одна транзакция на файл

**Проблема:** на один SQL-файл ~7 COPY-батчей, каждый — своя транзакция (`withCopyInTx`, `internal/store/db_tx.go`). Десятки тысяч файлов → сотни тысяч BEGIN/PREPARE/COMMIT с fsync.

**Файлы:**
- `internal/store/db_tx.go` — helper `WithTx(fn)` + изменение всех `BatchInsert*` (db_insert_sql.go, db_insert_pas.go, db_insert_h.go, db_insert_j.go, db_insert_dfm.go, db_insert_reports.go, db_insert_retcode.go, api_store.go): принимать `*sql.Tx` (вариант `BatchInsert*Tx`) — если tx передан, не открывать свою транзакцию. pq.CopyIn поддерживает несколько COPY в одной транзакции последовательно.
- `internal/indexer/indexer_sql_pas.go`, `internal/indexer/indexer.go` (parseHFile/parseJSFile/parseDFMFile/...) — обернуть все вставки одного файла в одну tx. SELECT-back lookups внутри той же tx (видят свои вставки — корректность сохраняется).

**Риск:** длинные транзакции на большие файлы — приемлемо (объём на файл мал). Взаимоблокировки между воркерами маловероятны: разные file_id, вставки только INSERT/COPY.

**Критерий акцепта:** build + тесты PASS; замер init/update на тестовом дереве — снижение времени стадии processFile.

---

## Фаза 4: Глобальный резолв query_fragment relations

**Проблема:** `buildQueryFragmentRelations` (`internal/indexer/indexer_relations.go:559-670`) на **каждый файл** делает `FindLatestSQLTableIDsByNames`/`FindLatestSQLProcedureIDsByNames` по таблицам, растущим с индексом → квадратичный рост стоимости init. Аналогично per-file `FindLatestSQLProcedureIDsByNames` в `buildJSProcedureCallRelations`, `buildT01GeneratedSubscriberRelations`, `indexAPIMacros`.

**Решение:** распространить паттерн `pendingSQLCalls` (уже работает для calls_procedure, `indexer_postprocess_sql_calls.go`):
- `internal/indexer/indexer.go` — структуры `PendingFragmentRelations` (fileID, fragments), накапливаются в Indexer с mutex.
- `internal/indexer/indexer_relations.go` — `buildQueryFragmentRelations` разделить: per-file часть (parent-relations, они локальны) остаётся в воркере; глобальные резолвы (table/proc references) — накопление в pending.
- `internal/indexer/indexer_postprocess_sql_calls.go` (или новый `indexer_postprocess_fragments.go`) — новая пост-обработка: один `FindLatestSQLTableIDsByNames` + один `FindLatestSQLProcedureIDsByNames` на **все** уникальные имена всех файлов → параллельная сборка relations (паттерн `buildSQLProcedureCallRelationsParallel`) → `saveRelations`. Добавить в `runPostProcessingParallel` (runner.go) пятой горутиной.
- To same фазе: `buildJSProcedureCallRelations`, `buildT01GeneratedSubscriberRelations`, `indexAPIMacros` proc-resolve — перевести на те же глобальные map'ы (передать в пост-процесс или предзагрузить один раз перед воркерами — см. риск).

**Риск:** в update-режиме глобальные map'ы по именам содержат "последние" id — семантика `DISTINCT ON ... ORDER BY id DESC` сохраняется, т.к. запрос тот же, просто один раз.

**Критерий акцепта:** число round-trips на файл с фрагментами падает с ~3 глобальных резолвов до 0; regression: итоговое число relations в БД после полного init не меняется (сравнить stats.Relations / запросом count).

---

## Фаза 5: Batch-операции в update и пост-процессе

### 5.1. Batch DELETE изменённых/удалённых файлов

**Проблема:** `runner.go:152-157` — `DeleteFilesByPathExcept` на каждый изменённый файл последовательно в feeder-горутине (каскад по ~20 таблицам); `runner.go:181-191` — цикл `DeleteFilesByPath` для удалённых.

**Файлы:**
- `internal/store/db_files.go` — `DeleteFilesByPaths(paths []string)` / `DeleteFilesByPathsExcept(paths []string, keepID int64)` с `WHERE path = ANY($1)` чанками по 500.
- `internal/indexer/runner.go` — feeder собирает изменённые пути в срез, удаление батчами; удалённые файлы — одним батч-вызовом.

### 5.2. Batch UPDATE в postProcessPASPending

**Проблема:** `internal/indexer/indexer_postprocess_pas.go:110-186` — по одному UPDATE на класс/метод/поле (`UpdatePASClassDFMForm`, `UpdatePASMethodClass`, `UpdatePASFieldClass`, `UpdatePASFieldDFMComponent`).

**Файлы:**
- `internal/store/db_lookup_pas.go` — batch-варианты через `UPDATE ... FROM (SELECT * FROM unnest($1::bigint[], $2::bigint[])) AS t(id, class_id) WHERE ...`.
- `internal/indexer/indexer_postprocess_pas.go` — собрать пары (id, classID) в срезы, один batch-UPDATE на тип сущности (чанки по 1000).

**Критерий акцепта:** build + тесты PASS; update с сотнями изменённых файлов — заметное снижение времени стадии cleanup.

---

## Фаза 6 (опционально, по результатам замеров): кросс-файловый COPY-aggregator

**Когда делать:** только если после Фаз 1–5 стадия processFile остаётся доминирующей по таймингам Фазы 0.

**Суть:** воркеры только парсят и складывают сущности в память (chan или срезы под mutex); отдельные writer-горутины по типам сущностей сливают большие COPY-пачки. ID-резолв через "вставил → один SELECT по диапазону file_id" или генерацию id вне БД.

**Риски:** серьёзная переделка порядка insert/lookup (SELECT-back паттерн ломается), память на больших деревьях. Не начинать без подтверждённых замеров.

---

## Проверка после каждой фазы

```bash
cd d:\GITHUB\GolandProjects\CodeBase
go build ./...
go vet ./...
go test ./internal/indexer/... ./internal/store/... ./internal/encoding/... ./internal/fswalk/...
```

Плюс после Фаз 3–5 — прогон `codebase update` на реальной БД и сравнение таймингов стадий с базовой линией Фазы 0.

## Затронутые файлы (сводно)

| Файл | Фазы |
|------|------|
| `internal/indexer/runner.go` | 0, 1.1, 4, 5.1 |
| `internal/indexer/indexer.go` | 1.1, 2, 3, 4 |
| `internal/indexer/indexer_sql_pas.go` | 1.3, 2, 3, 4 |
| `internal/indexer/indexer_relations.go` | 1.3, 4 |
| `internal/indexer/indexer_postprocess_pas.go` | 5.2 |
| `internal/indexer/indexer_postprocess_*.go` | 4 |
| `internal/store/db.go` | 1.2 |
| `internal/store/db_products.go` | 1.1 |
| `internal/store/db_tx.go` | 3 |
| `internal/store/db_insert_*.go`, `api_store.go` | 3 |
| `internal/store/db_files.go` | 5.1 |
| `internal/store/db_lookup_pas.go` | 5.2 |
| `internal/fswalk/fswalk.go` | 2 |
| `internal/encoding/*`, парсеры | 2 |
| `cmd/init.go`, `cmd/update.go` | 0 |

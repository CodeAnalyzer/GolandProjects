# Reuse сессии при повторном парсинге того же файла (`rti parse` / `trc parse`)

Повторный парсинг одного и того же файла сейчас всегда создаёт новую сессию и
заново прогоняет весь конвейер разбора. Предлагается сохранять SHA-256 содержимого
файла и build парсера в строке сессии: если такой файл уже разобран этим же
билдом — просто вернуть `session_id` существующей сессии, не читая и не разбирая
файл повторно. Перепарсить принудительно — `delete` + `parse` (отдельного флага
`--force` не вводим).

## Контекст проблемы

Дедупликации нет ни на одном уровне: ни `file_path`, ни хеш/`mtime`/`size` не
проверяются, сессия создаётся безусловным `INSERT ... RETURNING id` в таблицу с
`BIGSERIAL PRIMARY KEY`.

**Текущие пути кода:**

| Звено | Файл:строка | Что делает |
|---|---|---|
| RTI CLI | `cmd/rti.go:127-145` | `runRTIParse` → `rtisvc.ExecuteParse`, печатает summary, `Saved session: N` |
| RTI сервис | `internal/rtisvc/runtime.go:68-97` | `rti.ParseFile` + `rti.SaveSession` |
| RTI запись | `internal/rti/store.go:17-63` | `INSERT INTO rti_sessions (...) RETURNING id` (стр. 20-26), затем батчи calls/params/checkpoints/blog/client-events |
| TRC CLI | `cmd/trc.go:160-179` | `runTRCParse` → `trcsvc.ExecuteParse` |
| TRC сервис | `internal/trcsvc/runtime.go:51-74` | `trc.ParseFileToDB` |
| TRC запись | `internal/trc/parse_to_db.go:29-197` | `INSERT INTO trc_sessions` (стр. 62-66) до парсинга (нужна для FK), потоковые батчи по 50k, финальный `UPDATE` header/total_events (стр. 185-194) |
| Схема | `internal/store/db_schema.go:532-542` (rti), `601-613` (trc) | нет `file_hash`, нет `UNIQUE`; есть лишь `idx_*_sessions_parsed_at` |

**Последствия:** каждый повторный `parse` = полный разбор + полный дубль данных в
БД. Для реальных логов из `Tests/` это минуты работы и гигабайты:

| Файл | Размер | SHA-256 (тёплый кэш) | SHA-256 (холодный) |
|---|---|---|---|
| `ABS8E_abs_e1_SRV.rti` | 77 МБ | 0.26 с | — |
| `nbki.trc` | 212 МБ | 0.34 с | 1.1 с |
| `SPCompletedOnly.trc` | 335 МБ | ~0.55 с | ~1.7 с |
| `nbki.xml` | 754 МБ | ~1.2 с | ~3.7 с |

Хеш читает файл один раз последовательно (~600 МБ/с на тёплом кэше), парсинг же
декодирует каждое событие и вставляет миллионы строк — соотношение ~1:50…1:200.
Дополнительно RTI уже читает файл целиком в память (`internal/rti/parser.go:77`).

**Режим `--file` без `session_id`** (`resolveSession` в `rtisvc/runtime.go:17` и
`trcsvc/runtime.go:17`) парсит файл в памяти и сессию не создаёт — его изменения
не касаются.

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/store/db_schema.go` | `ALTER TABLE ... ADD COLUMN IF NOT EXISTS file_hash/parser_build` для обеих таблиц + partial unique indexes |
| `internal/buildinfo/buildinfo.go` | **новый** пакет: `Version`, `Build` — единый источник для `cmd` и сервисов |
| `cmd/root.go` | `version`/`buildNumber` берутся из `buildinfo` (строки 26-27) |
| `internal/util/hash.go` | **новый**: `FileHash(path string) (string, error)` — SHA-256 hex (по образцу `internal/fswalk/fswalk.go:369-373`) |
| `internal/rti/store.go` | `FindSessionByHash`, `SetSessionHash` |
| `internal/rtisvc/runtime.go` | reuse в `ExecuteParse` (hit → `LoadSummary`, miss → parse/save/hash) |
| `internal/trc/store.go` | `FindSessionByHash`, `SetSessionHash` |
| `internal/trcsvc/runtime.go` | reuse в `ExecuteParse` (hit → `GetSession` для `total_events`) |
| `internal/rtisvc/runtime_test.go` | reuse без БД не ломается; при nil-db hash не считается |
| `internal/trcsvc/runtime_test.go` | то же |
| `internal/rti/store_integration_test.go` | find/set hash, повторный parse → тот же ID |
| `internal/trc/store_integration_test.go` | то же |
| `README.md` | разделы RTI/TRC: повторный `parse` того же файла возвращает существующую сессию |

## План (5 шагов)

### Шаг 1: Схема — `file_hash` + `parser_build`

В `internal/store/db_schema.go` (список `statements`, после `CREATE TABLE`
соответствующих таблиц; стиль как у `ALTER TABLE trc_sessions ADD COLUMN IF NOT
EXISTS source_format ...`, стр. 614):

```sql
ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS file_hash TEXT
ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS parser_build TEXT
ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS file_hash TEXT
ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS parser_build TEXT

CREATE UNIQUE INDEX IF NOT EXISTS idx_rti_sessions_file_hash
    ON rti_sessions(file_hash) WHERE file_hash IS NOT NULL
CREATE UNIQUE INDEX IF NOT EXISTS idx_trc_sessions_file_hash
    ON trc_sessions(file_hash) WHERE file_hash IS NOT NULL
```

- `file_hash` — hex SHA-256 (64 символа), `NULL` у legacy-сессий и у сессий,
  разбор которых не завершился.
- `parser_build` — build бинарника, которым сессия разобрана (`1609` и т. п.).
- Partial unique index: `NULL` не конфликтуют, старые строки не мешают.

### Шаг 2: `internal/buildinfo` + `internal/util/hash.go`

Версия/build сейчас живут только в `cmd` (`cmd/root.go:26-27`), а нужны в
`rtisvc`/`trcsvc`; `internal` не может импортировать `cmd`. Выносим в листовой
пакет:

```go
// internal/buildinfo/buildinfo.go
package buildinfo

var (
    Version = "0.9.3"
    Build   = "1609"
)
```

`cmd/root.go`:

```go
version     = buildinfo.Version
buildNumber = buildinfo.Build
```

Хеш-хелпер:

```go
// internal/util/hash.go
func FileHash(path string) (string, error) // sha256 → hex
```

Реализация — `os.Open` + `io.Copy(sha256.New(), f)` + `hex.EncodeToString`;
RTI-путь можно позже оптимизировать (файл уже прочитан в `ParseFile`), но в
первой версии — отдельный проход: 0.3–1.2 с на реальных логах.

### Шаг 3: RTI — поиск по хешу и запись хеша последним

`internal/rti/store.go`:

```go
// FindSessionByHash возвращает ID сессии для (hash, build) или 0.
func FindSessionByHash(ctx context.Context, db *store.DB, hash, build string) (int64, error)
// SELECT id FROM rti_sessions WHERE file_hash = $1 AND parser_build = $2 LIMIT 1

// SetSessionHash проставляет хеш успешно разобранной сессии.
func SetSessionHash(ctx context.Context, db *store.DB, sessionID int64, hash, build string) error
// UPDATE rti_sessions SET file_hash = $2, parser_build = $3 WHERE id = $1
```

`internal/rtisvc/runtime.go`, `ExecuteParse`, ветка `db != nil`:

```
h, err := util.FileHash(filePath)            // ошибка чтения → обычная ошибка parse
if id, _ := rti.FindSessionByHash(ctx, db, h, buildinfo.Build); id > 0 {
    summary, err := rti.LoadSummary(ctx, db, id)   // store.go:1229, top-10 slow
    return &ParseResult{SessionID: id, TotalCalls: summary.TotalCalls, Summary: *summary}, nil
}
result := rti.ParseFile(filePath)
id := rti.SaveSession(...)
rti.SetSessionHash(ctx, db, id, h, buildinfo.Build)   // последним шагом
```

Почему хеш пишется последним: `SaveSession` не транзакционен (падение на шаге 3
оставляет `rti_calls` без `rti_params`). Если хеш не записан — прерванная сессия
не будет переиспользована, повторный `parse` создаст новую.

### Шаг 4: TRC — то же, с учётом «сессия создаётся до парсинга»

`internal/trc/store.go` — `FindSessionByHash` / `SetSessionHash` (аналогично RTI).

`internal/trcsvc/runtime.go`, `ExecuteParse`, ветка `db != nil`:

```
h := util.FileHash(filePath)
if id, _ := trc.FindSessionByHash(ctx, db, h, buildinfo.Build); id > 0 {
    session, err := trc.GetSession(ctx, db, id)    // store.go:343
    return &ParseResult{SessionID: id, TotalEvents: session.TotalEvents}, nil
}
sessionID, total, err := trc.ParseFileToDB(ctx, filePath, db)
trc.SetSessionHash(ctx, db, sessionID, h, buildinfo.Build)   // после успешного парсинга
```

`ParseFileToDB` не меняется: сессия по-прежнему вставляется до парсинга (нужна
для FK `trc_events.session_id`), но `file_hash` в INSERT **не** пишется — только
финальным `UPDATE`. При ошибке парсинга функция возвращает `sessionID` вместе с
ошибкой, хеш остаётся `NULL` → сессия не попадает в кэш (и подчищается `prune`).

### Шаг 5: Тесты

- **Интеграционные (`integration` tag, реальный Postgres):**
  - `TestParseFileToDB_SecondParseReusesSession` — два `ExecuteParse` одного
    файла → одинаковый `session_id`, число событий в `trc_events` не удвоилось.
  - `TestRTI_SaveSession_SecondParseReusesSession` — то же для RTI
    (`rti_calls` не удвоились, `LoadSummary` совпадает).
  - `TestFindSessionByHash_BuildMismatch` — тот же файл, другой `parser_build` →
    новая сессия.
  - `TestFindSessionByHash_LegacyNullHash` — сессия без хеша не находится и не
    блокирует вставку новой (partial index).
  - `TestSetSessionHash_UniqueConflict` — второй `SetSessionHash` с тем же хешем
    даёт unique violation → `ExecuteParse` возвращает ID победителя, «лишняя»
    сессия остаётся с `NULL` (чистится `prune`).
  - `TestDeleteThenParseCreatesNewSession` — `delete` + `parse` → новый ID
    (сброс кэша удалением строки).
- **Юнит-тесты:** `util.FileHash` (детерминизм, hex-длина 64, ошибка на
  несуществующем файле); `ExecuteParse` при `db == nil` не считает хеш и
  возвращает `Warning` как раньше.

## Порядок выполнения

1. Шаг 1 (схема) → `go build ./...`, проверить `InitSchemaCtx` на живой БД.
2. Шаг 2 (`buildinfo`, `util.FileHash`) → сборка, `codebase --version` без
   изменений.
3. Шаг 3 (RTI) → сборка + интеграционные тесты.
4. Шаг 4 (TRC) → сборка + интеграционные тесты.
5. Шаг 5 (тесты) + smoke на реальных файлах из `Tests/`.
6. Обновить `README.md`; отдельным OpenSpec-изменением добавить требование про
   reuse в `rti-analysis/rti-storage` и `trc-analysis/trc-storage`.

## Проверка

1. `go build ./...`, `go vet ./internal/rti/... ./internal/trc/... ./internal/rtisvc/... ./internal/trcsvc/... ./internal/util/... ./internal/buildinfo/... ./cmd/...`
2. `go test ./internal/rti/... ./internal/trc/... ./internal/rtisvc/... ./internal/trcsvc/... ./internal/util/... -count=1`
3. `go test -tags integration ./internal/rti/... ./internal/trc/... -count=1`
4. CLI smoke (живая БД):
   - `codebase trc parse Tests\nbki.trc` → новый ID, время = разбор;
   - повторно тот же файл → **тот же ID**, время < 2 с, `Total events` совпадает;
   - `codebase trc delete -s <ID>` + `codebase trc parse Tests\nbki.trc` → новый ID;
   - `UPDATE trc_sessions SET parser_build='0' WHERE id=<ID>` + `parse` → новый ID;
   - аналогично `codebase rti parse Tests\ABS8E_abs_e1_SRV.rti` (summary и
     `total_calls` при reuse совпадают с первым прогоном).
5. MCP smoke: `codebase_trc_parse` / `codebase_rti_parse` дважды → одинаковый
   `session_id` в ответе.

## Совместимость

- Формат ответа `ParseResult` (CLI JSON и MCP) **не меняется**: при reuse
  возвращаются те же `session_id`, `total_calls`/`summary` (RTI) и
  `total_events` (TRC). Пометки «reused» нет — reuse молчаливый (осознанное
  решение).
- CLI печатает `Saved session: N` и при reuse — текст не меняем.
- Legacy-сессии (`file_hash IS NULL`) не переиспользуются, но и не мешают:
  первое повторное разборное — обычный `parse`.
- `file_path` переиспользованной сессии остаётся первым встретившимся путём
  (одинаковые байты из другого каталога → тот же `session_id`).
- `delete`/`prune` не меняются: хеш живёт в строке сессии, поэтому удаление
  сессии автоматически сбрасывает кэш.
- Спеки: `rti-storage`/`trc-storage` сейчас описывают «сохранять результат и
  возвращать session_id» — reuse не противоречит, но должен быть описан явно
  (дельты отдельным change'ом).

## Риски

- **`parser_build` в ключе → переразбор после каждого релиза.** Один и тот же
  лог после каждого bump'а билда даёт новую сессию (+212 МБ…+2 ГБ в БД на файл).
  Это цена того, что улучшения парсера долетают до старых файлов. Если рост БД
  станет проблемой — заменить build на отдельный ручной `parser_version`
  (`rti=1`, `trc=1`, инкремент только при изменении логики разбора).
- **TOCTOU при растущем файле.** Хеш считается до парсинга: если лог дописывается,
  сохранённый хеш соответствует состоянию «до», а в сессии — больше событий.
  Следующий `parse` вырастет → другой хеш → новая сессия (лишней работы нет,
  просто нет reuse). Обратный случай (файл заменили между хешированием и
  разбором) теоретически даёт сессию с чужим хешем — практически исключён для
  логов, но стоит помнить.
- **Гонка двух параллельных `parse`.** Оба считают хеш, оба не находят сессию,
  оба парсят. Второй `SetSessionHash` упадёт на unique-индексе → предлагается
  вернуть ID победителя, а «лишнюю» сессию оставить с `NULL` (её удалит `prune`).
  Без unique-индекса возможны две сессии с одинаковым хешем.
- **Лишнее чтение файла.** RTI: `FileHash` + `os.ReadFile` в `ParseFile` = два
  прохода (0.26 с + 0.15 с на 77 МБ — приемлемо). TRC: хеш-проход + streaming
  парсинг. При желании RTI можно позже складывать хеш из уже прочитанного буфера.
- **Гибридный случай «тот же файл, другой build, но идентичные данные»** —
  сессия-дубль. Осознанно: простота важнее экономии.

## Ожидаемый результат

| Сценарий | Было | Стало |
|---|---|---|
| `trc parse nbki.trc` (повторно) | новый ID, минуты, +212 МБ в БД | тот же ID, < 2 с, +0 строк |
| `rti parse ABS8E…rti` (повторно) | новый ID, полный разбор | тот же ID, < 1 с, summary из `LoadSummary` |
| `delete` + `parse` | новый ID | новый ID (кэш сброшен удалением строки) |
| `parse` после bump билда | новый ID | новый ID (build в ключе) |
| Прерванный парсинг | мусорная сессия, видна в `list` | мусорная сессия с `NULL` хешем, в кэш не попадает |

## Реализовано (2026-10-20)

OpenSpec change: `openspec/changes/parse-session-reuse-by-file-hash/`
(дельты `rti-analysis/rti-storage`, `trc-analysis/trc-storage`).

Отличия от исходного плана (по итогам исследования кода и реализации):

1. **Композитный unique index `(file_hash, parser_version)`** вместо
   `UNIQUE(file_hash)`. Исходный вариант давал невосстановимый unique
   violation при смене версии парсера: поиск по `(hash, build)` промахивался,
   полный разбор завершался, а запись хеша конфликтовала со старой сессией
   (повторный поиск по новой версии тоже пуст). Композитный ключ совпадает с
   ключом поиска: гонка при равной версии → violation → восстановимо; инкремент
   версии → сосуществование сессий.
2. **Доменные `rti.ParserVersion` / `trc.ParserVersion`** (константы в
   `internal/rti/parser_version.go`, `internal/trc/parser_version.go`,
   инкремент при изменении логики разбора) вместо `buildinfo.Build`: build
   меняется на каждый релиз и обнулял бы весь кэш. Пакет `internal/buildinfo`
   не создавался.
3. **Явный `DeleteSession` проигравшей сессии** при конфликте вместо «её
   удалит prune»: prune хранит N свежих по `parsed_at`, а проигравшая — самая
   свежая. При недоступности победителя — один повтор `SetSessionHash`.
4. **Сериализация параллельных TRC-разборов** (выявлена pre-existing гонка при
   реализации): `getBaseID = MAX(id)+1` не атомарен, параллельные
   `ParseFileToDB` сталкивались на `trc_events_pkey`. `ParseFileToDB` берёт
   session-level `pg_advisory_lock` по абсолютному пути файла на выделенном
   соединении (паттерн advisory-локов кодовой базы, адаптированный под длинную
   операцию). RTI не подвержен (BIGSERIAL).
5. `store.IsUniqueViolation` (SQLSTATE 23505, `internal/store/db_errors.go`) —
   общий детект для обоих сервисов.

Верификация: unit + integration (reuse, версия, legacy-NULL, unique violation,
конфликт с удалением проигравшего, прерванный разбор) — зелёные; CLI smoke на
живой БД: `nbki.trc` 58.4с → 1.01с (тот же ID, 547 508 событий), `delete`+
`parse` → новый ID; RTI `ABS8E…rti` reuse 0.97с, сводка совпадает; MCP
`codebase_trc_parse` дважды → одинаковый `session_id`.

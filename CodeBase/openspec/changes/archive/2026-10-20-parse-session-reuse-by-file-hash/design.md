# Design: Reuse сессии при повторном парсинге того же файла

## Context

Источник — `Modifications/parse-session-reuse-by-file-hash-d7a1c3.md`; все ссылки
на код верифицированы по состоянию на 2026-10-20 (build 1609). Текущее состояние:

- `rtisvc.ExecuteParse` (`internal/rtisvc/runtime.go:68-97`): безусловный
  `ParseFile` → `SaveSession` → новый `session_id` при каждом вызове.
- `trcsvc.ExecuteParse` (`internal/trcsvc/runtime.go:51-74`): то же через
  `trc.ParseFileToDB`.
- `rti.SaveSession` (`internal/rti/store.go:17-63`) нетранзакционен: 7
  последовательных INSERT-батчей; падение в середине оставляет частичные данные.
- `trc.ParseFileToDB` (`internal/trc/parse_to_db.go`): сессия вставляется ДО
  парсинга (строки 62-66, нужна для FK `trc_events.session_id`), финальный
  UPDATE header/total_events (строки 185-194); при ошибке парсинга сессия
  остаётся (возвращается вместе с ошибкой).
- Схема (`internal/store/db_schema.go:532-542`, `601-613`): нет ни хеша, ни
  уникальности; стиль миграций — `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
  (образец: строка 614).
- Версия/build живут только в `cmd/root.go:26-27`; `internal/*` не может
  импортировать `cmd`.
- `rti.LoadSummary` (`store.go:1229`) собирает полную сводку из БД, включая
  top-10 медленных вызовов; `trc.GetSession` (`store.go:343`) отдаёт
  `total_events` после финального UPDATE — эти функции дают reuse-ответ,
  эквивалентный первому парсингу.
- Единая точка входа parse: CLI (`cmd/rti.go:131`, `cmd/trc.go:164`) и MCP
  (`internal/mcp/registry.go:815`, `1015`) → `ExecuteParse` — reuse
  распространяется на оба транспорта без правок.

## Goals / Non-Goals

**Goals:**

- Повторный `parse` того же файла тем же парсером — `< 2 с`, `+0` строк в БД,
  тот же `session_id`.
- Прерванные и legacy-сессии никогда не переиспользуются.
- Параллельные `parse` одного файла сходятся к одной сессии без мусора в БД.
- Изменения логики парсера (инкремент версии) вызывают контролируемый
  переразбор, не требующий ручной чистки.

**Non-Goals:**

- Флаг `--force` (перепарсинг = `delete` + `parse`).
- Оптимизация двойного чтения RTI-файла (хеш из уже прочитанного буфера).
- Сборка мусора прерванных сессий (существующее поведение `list`/`prune` не
  меняется).
- Изменение формата ответа `ParseResult` (CLI JSON и MCP) и текста CLI
  `Saved session: N`.

## Decisions

### 1. Ключ reuse — пара (file_hash, parser_version), композитный unique index

Индекс обязан совпадать с ключом поиска. В исходном документе был разрыв:
`UNIQUE(file_hash)` при поиске `WHERE file_hash = $1 AND parser_build = $2` —
смена версии парсера давала miss поиска, полный разбор и **невосстановимый**
unique violation при записи хеша (повторный поиск по новой версии тоже пуст).
Композитный индекс устраняет разрыв:

```sql
ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS file_hash TEXT
ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS parser_version INTEGER
ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS file_hash TEXT
ALTER TABLE trc_sessions ADD COLUMN IF NOT EXISTS parser_version INTEGER

CREATE UNIQUE INDEX IF NOT EXISTS idx_rti_sessions_file_hash
    ON rti_sessions(file_hash, parser_version) WHERE file_hash IS NOT NULL
CREATE UNIQUE INDEX IF NOT EXISTS idx_trc_sessions_file_hash
    ON trc_sessions(file_hash, parser_version) WHERE file_hash IS NOT NULL
```

Свойства: partial index терпит legacy-NULL; гонка при одинаковой версии →
violation → восстановимо повторным поиском; инкремент версии → ключи разные,
сессии сосуществуют (это и есть желаемый переразбор). Альтернатива
«хеш-только индекс + build вне ключа» отклонена: реюз устаревшего разбора.

### 2. Доменные parser_version вместо build бинарника

`internal/rti/parser_version.go` → `const ParserVersion = 1`,
`internal/trc/parser_version.go` → `const ParserVersion = 1`. Инкремент —
вручную, только при изменении логики разбора соответствующего парсера
(комментарий в файле обязует).

Почему не `buildinfo.Build` (вариант из исходного документа): build
инкрементируется на каждое изменение CodeBase (1609 — уже третье за октябрь),
парсер — редко. Build в ключе обнулял бы весь кэш на каждый релиз (+минуты
разбора и ГБ на каждый файл) ради улучшений, которых для конкретного парсера
не было. Побочный плюс: пакет `internal/buildinfo` вообще не нужен — константа
живёт в доменном пакете, который сервисы уже импортируют.

### 3. Хеш проставляется последним шагом

RTI: `FileHash` → `FindSessionByHash` → (miss) `ParseFile` → `SaveSession` →
`SetSessionHash`. TRC: `FileHash` → `FindSessionByHash` → (miss)
`ParseFileToDB` (хеш в INSERT не пишется) → `SetSessionHash`.

Так прерванная сессия (`SaveSession` упал на батче 4; TRC-парсинг упал после
INSERT сессии) остаётся с `file_hash = NULL` и не попадает в кэш. Колонка
заполняется только для гарантированно полных сессий.

### 4. Конфликт: победитель + явный DeleteSession проигравшего

`SetSessionHash` упирается в unique violation только при гонке двух полных
разборов одного файла (одинаковая версия — иначе ключи не пересекаются).
Обработка в `ExecuteParse`:

```
SetSessionHash(loserID) → unique violation
  → FindSessionByHash(hash, version) повторно
  → найден победитель winnerID:
        DeleteSession(loserID)          // явная чистка, не «пусть prune уберёт»
        → вернуть результат по winnerID (LoadSummary / GetSession)
  → не найден (победитель удалён конкурентно):
        повторить SetSessionHash один раз; при повторном отказе — ошибка
```

Отказ от формулировки исходного документа «лишнюю сессию удалит prune»:
`PruneSessions` хранит N свежих по `parsed_at`, а проигравший — самая свежая
сессия в момент конфликта, т. е. prune сохранил бы мусор с миллионами строк.
Явный `DeleteSession` (batch-удаление уже реализовано) дешевле и детерминирован.

### 5. util.FileHash — отдельный последовательный проход

`internal/util/hash.go`: `os.Open` + `io.Copy(sha256.New(), f)` → hex
(образец — `computeHashBytes` в `internal/fswalk/fswalk.go:369-373`, но для
пути, а не `[]byte`). Стоимость: 0.26–1.2 с на реальных логах из `Tests/`
против минут разбора (соотношение ~1:50…1:200). Ошибка чтения → обычная
ошибка `parse` (файл недоступен в любом случае).

### 6. Reuse молчаливый

`ParseResult` не расширяется: RTI — `LoadSummary` (эквивалент первой сводки),
TRC — `GetSession.TotalEvents`. CLI продолжает печатать `Saved session: N`;
агент MCP видит стабильный `session_id` при повторном вызове — это и есть
наблюдаемый контракт reuse.

### 7. Сериализация параллельных TRC-разборов (advisory lock по пути файла)

Конкурентный тест выявил pre-existing гонку: `getBaseID` выделяет явные id для
COPY IN как `COALESCE(MAX(id),0)+1` без блокировки — два параллельных
`ParseFileToDB` получают одинаковый base и падают на `trc_events_pkey`.
RTI не подвержен (батчи через `INSERT` с BIGSERIAL).

Решение — устоявшийся в кодовой базе паттерн advisory-локов (InitSchemaCtx,
spec-upsert), адаптированный под длинную операцию: `ParseFileToDB` берёт
выделенное соединение из пула и держит **session-level** `pg_advisory_lock`
(ключ `hashtextextended(абсолютный путь)`) на весь стриминг; unlock — тем же
соединением, при отмене ctx/сбое лок отпускается закрытием соединения.
Следствия: разборы одного файла выстраиваются в очередь — дождавшийся второй
парсинг доходит до `SetSessionHash`, упирается в unique violation и
разрешается решением 4 (сценарий «оба успешны, сходятся к победителю»
становится детерминированным); разборы разных файлов идут параллельно.
`getBaseID`/`syncSequence` под локом не меняются. Отвергнутые альтернативы:
`setval(nextval()+N)` одним statement — не concurrency-safe (`setval` не
сериализуется с чужими `nextval`); карта `позиция→id` с чанковым
резервированием — параллелизм того же файла сохраняет, но требует переделки
аффинной формулы `id = base + EventIndex` / `parent_id = base + ParentID` в
`insertTRCEvents`; вероятность параллельного разбора одного файла без
сериализации сочтена несущественной.

## Risks / Trade-offs

- [TOCTOU растущего лога] Хеш считается до разбора; дописанный файл даёт хеш
  «до» и, возможно, больше событий в сессии → mitigation: следующий parse
  видит другой хеш и создаёт новую сессию (нет некорректного reuse того же
  содержимого; максимум — один холостой полный разбор).
- [Забытый инкремент ParserVersion после правки парсера] → устаревший reuse →
  mitigation: обязующий комментарий в `parser_version.go`; инкремент — пункт
  чек-листа задач на любые правки `internal/rti/parser*.go`,
  `internal/trc/parser*.go` (зафиксировано в Notes основных спек при архиве).
- [file_path reuse-сессии — первый встретившийся путь] Одинаковые байты из
  другого каталога дают тот же `session_id` с чужим путём в списке → принято
  (identity по содержимому — фича).
- [Двойное чтение RTI-файла] `FileHash` + `os.ReadFile` в `ParseFile` ≈ 0.4 с
  на 77 МБ → принято; оптимизация (хеш из буфера `ParseFile`) — вне scope.
- [Unique violation при массовой миграции] Индекс создаётся на таблице, где
  дублей хешей нет (колонка новая, везде NULL) → конфликт невозможен на
  миграции, только в runtime-гонке.

## Migration Plan

Миграция — часть `InitSchemaCtx` (statements в `internal/store/db_schema.go`
после CREATE TABLE соответствующих таблиц, стиль строки 614): идемпотентна,
применяется при первом запуске нового бинарника, не требует даунтайма (БД
однопользовательская, локальный стенд). Legacy-сессии остаются с `NULL` — не
переиспользуются, не мешают. Откат: `DROP INDEX idx_*_sessions_file_hash` +
`DROP COLUMN file_hash, parser_version` (поведение деградирует к «всегда новый
parse», данные не теряются).

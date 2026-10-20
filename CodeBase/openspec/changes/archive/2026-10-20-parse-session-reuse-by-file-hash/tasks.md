# Tasks: parse-session-reuse-by-file-hash

## 1. Схема и хелперы

- [x] 1.1 Добавить в `internal/store/db_schema.go` (statements, после CREATE TABLE `rti_sessions`/`trc_sessions`): `ALTER TABLE ... ADD COLUMN IF NOT EXISTS file_hash TEXT` + `parser_version INTEGER` для обеих таблиц и partial unique indexes `idx_rti_sessions_file_hash` / `idx_trc_sessions_file_hash` на `(file_hash, parser_version) WHERE file_hash IS NOT NULL` — по design.md, решение 1. Проверить: `go build ./...`; дополнить `TestInitSchema_IdempotentAndHasRequiredObjects` (`internal/store/schema_integration_test.go`) — новые индексы в `requiredIndexes`, колонки `file_hash`/`parser_version` обеих таблиц через `information_schema.columns`; `go test -tags integration ./internal/store/ -run TestInitSchema -count=1` применяет схему к живой БД (psql на стенде нет — тест и есть способ её коснуться) и проходит, повторный запуск — без ошибок.
- [x] 1.2 Создать `internal/util/hash.go` — `FileHash(path string) (string, error)` (SHA-256 → hex, `os.Open` + `io.Copy`). Юнит-тесты в `internal/util/hash_test.go`: детерминизм (два вызова — одинаковый результат), длина hex 64, ошибка на несуществующем файле; `go test ./internal/util/ -count=1` зелёный.
- [x] 1.3 Создать `internal/rti/parser_version.go` (`const ParserVersion = 1`) и `internal/trc/parser_version.go` (`const ParserVersion = 1`) с обязующим комментарием: инкремент при любом изменении логики разбора соответствующего парсера (design.md, решение 2). Проверить: `go build ./...`.

## 2. RTI: reuse в store и сервисе

- [x] 2.1 Реализовать в `internal/rti/store.go`: `FindSessionByHash(ctx, db, hash string, parserVersion int) (int64, error)` (`SELECT id ... WHERE file_hash=$1 AND parser_version=$2 LIMIT 1`, no rows → 0, nil) и `SetSessionHash(ctx, db, sessionID, hash, parserVersion)` (UPDATE). Проверить: `go build ./internal/rti/`.
- [x] 2.2 В `internal/rtisvc/runtime.go` `ExecuteParse` (ветка `db != nil`): `util.FileHash` → `FindSessionByHash` → hit: `rti.LoadSummary` → тот же `ParseResult`; miss: `ParseFile` → `SaveSession` → `SetSessionHash` последним шагом (design.md, решение 3). Конфликт unique violation в `SetSessionHash`: повторный `FindSessionByHash` → победитель: `DeleteSession`(проигравшая) + результат победителя; не найден: один повтор `SetSessionHash`, затем ошибка (решение 4). Формат `ParseResult` не меняется. Юнит-тест в `internal/rtisvc/runtime_test.go`: `db == nil` → хеш не считается, `Warning` как раньше.
- [x] 2.3 Интеграционные тесты (`internal/rti/store_integration_test.go`, тег `integration`): повторный `ExecuteParse` того же файла → тот же `session_id`, `rti_calls` не удвоились, `LoadSummary` совпадает; другая `parser_version` (UPDATE колонки) → новая сессия, старая остаётся; legacy-сессия без хеша не находится и не блокирует вставку; `SetSessionHash` с чужим хешем → unique violation; конфликт → ID победителя и удалённая проигравшая; `delete` + `parse` → новый ID. `go test -tags integration ./internal/rti/ -count=1` зелёный.

## 3. TRC: reuse в store и сервисе

- [x] 3.1 Реализовать в `internal/trc/store.go`: `FindSessionByHash` / `SetSessionHash` (аналогично 2.1). Проверить: `go build ./internal/trc/`.
- [x] 3.2 В `internal/trcsvc/runtime.go` `ExecuteParse` (ветка `db != nil`): `util.FileHash` → `FindSessionByHash` → hit: `trc.GetSession` → `ParseResult{SessionID, TotalEvents}`; miss: `ParseFileToDB` (хеш в INSERT не пишется) → `SetSessionHash` после успешного парсинга; обработка конфликта как в 2.2. `ParseFileToDB` дорабатывается: session-level `pg_advisory_lock` по абсолютному пути файла на выделенном из пула соединении — сериализация параллельных разборов одного файла (закрывает гонку неатомарного `getBaseID` = `MAX(id)+1` → PK `trc_events_pkey`, выявленную конкурентным тестом 3.3); `getBaseID`/`syncSequence` не меняются. Юнит-тест в `internal/trcsvc/runtime_test.go`: `db == nil` → без хеша, `Warning` как раньше.
- [x] 3.3 Интеграционные тесты (`internal/trc/store_integration_test.go`, тег `integration`): повторный `ExecuteParse` → тот же `session_id`, событий не удвоилось; версия/хеш → новая сессия; прерванный разбор (сессия без хеша) не переиспользуется; конфликт → победитель + удалённая проигравшая; `delete` + `parse` → новый ID. `go test -tags integration ./internal/trc/ -count=1` зелёный.

## 4. Сборка и проверки

- [x] 4.1 `go build ./...`, `go vet ./internal/rti/... ./internal/trc/... ./internal/rtisvc/... ./internal/trcsvc/... ./internal/util/... ./internal/store/... ./cmd/...`, `go test ./internal/rti/... ./internal/trc/... ./internal/rtisvc/... ./internal/trcsvc/... ./internal/util/... -count=1` — без ошибок.
- [x] 4.2 CLI smoke на живой БД с реальными файлами из `Tests/`: `codebase trc parse Tests\nbki.trc` → новый ID (полное время); повторно → тот же ID, < 2 c, `Total events` совпадает; `trc delete` + `parse` → новый ID; аналогично `codebase rti parse Tests\ABS8E_abs_e1_SRV.rti` (reuse-сводка совпадает с первым прогоном). Сценарий «другая версия парсера → новая сессия» проверяется integration-тестами 2.3/3.3 (SQL на живой БД исполняем только из Go-тестов — psql на стенде нет).
- [x] 4.3 MCP smoke: после пересборки бинарника `codebase_trc_parse` / `codebase_rti_parse` дважды → одинаковый `session_id` в ответах (запущенные MCP-серверы требуют рестарта с новым CodeBase.exe).

## 5. Документация и финализация

- [x] 5.1 README.md: в разделах RTI и TRC описать reuse (повторный `parse` того же файла возвращает существующую сессию по SHA-256 + версии парсера; перепарсинг — `delete` + `parse`).
- [x] 5.2 Обновить `Modifications/parse-session-reuse-by-file-hash-d7a1c3.md`: статус «реализовано», отражены отличия от исходного плана (композитный индекс `(file_hash, parser_version)`, доменные `ParserVersion` вместо `buildinfo`, явный `DeleteSession` проигравшего) со ссылкой на change.
- [x] 5.3 Bump `buildNumber` до 1612 в `cmd/root.go`, пересобрать CodeBase.exe (`go build -o bin/CodeBase.exe .`), проверить `codebase --version`.

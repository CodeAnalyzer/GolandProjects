# Задачи: счётчик использования сессий

## 1. Схема БД

- [x] 1.1 В `internal/store/db_schema.go` добавить идемпотентные `ALTER TABLE rti_sessions ADD COLUMN IF NOT EXISTS use_count INTEGER NOT NULL DEFAULT 0` и то же для `trc_sessions`; проверить применением через `codebase health` (SELECT колонки из psql)
- [x] 1.2 Убедиться, что существующий partial unique index `(file_hash, parser_version)` не затронут и `use_count` не входит ни в один индекс (EXPLAIN list-запроса без нового скана)

## 2. Доменные store (RTI)

- [x] 2.1 `internal/rti/store.go`: `SetSessionHash` инкрементирует `use_count` тем же statement (`SET file_hash, parser_version, use_count = use_count + 1`); unit-проверка не требуется (SQL), покрытие интеграционным тестом 3.1
- [x] 2.2 `internal/rti/store.go`: новый `AcquireSessionByHash(ctx, db, fileHash, parserVersion) (sessionID, useCount int64, err)` — атомарный `UPDATE ... SET use_count = use_count + 1 WHERE file_hash = $1 AND parser_version = $2 RETURNING id, use_count` (0 строк → 0, nil); заменить им `FindSessionByHash` в parse-пути (или удалить FindSessionByHash, если потребителей не осталось — проверить rg)
- [x] 2.3 `internal/rti/store.go`: новый `ReleaseSession(ctx, db, sessionID) (remaining int, decremented bool, err)` — `UPDATE ... SET use_count = use_count - 1 WHERE id = $1 AND use_count > 1 RETURNING use_count`; 0 строк → проверить существование сессии (`GetSession`) и вернуть «not found» либо «окончательное удаление нужно»
- [x] 2.4 `internal/rti/store.go`: `ListSessions`/`GetSession` — добавить `use_count` в SELECT и в структуру `RTISession` (JSON-поле `use_count`); CLI/MCP получают поле автоматически

## 3. Доменные store (TRC)

- [x] 3.1 `internal/trc/store.go`: те же правки, что 2.1–2.4 (`SetSessionHash` + инкремент, `AcquireSessionByHash`, `ReleaseSession`, `use_count` в `ListSessions`/`GetSession`/`TRCSession`)

## 4. Сервисный слой

- [x] 4.1 `internal/rtisvc/types.go`: `ParseResult.UseCount int` (json `use_count`, без omitempty); `DeleteParams.Hard bool`; `DeleteResult` — `Decremented bool`, `DecrementedIDs []int64`, `UseCount int` по design.md D5
- [x] 4.2 `internal/rtisvc/runtime.go` `executeParseDB`: reuse-ветка → `AcquireSessionByHash`, пробросить `useCount` в `parseResultFromSession`-результат; fresh-ветка — `UseCount: 1`; конфликт → winner через `AcquireSessionByHash` (победитель получает +1), `DeleteSession(loser)` без изменений (хеш NULL, счётчик 0); fallback без БД — `UseCount: 0`
- [x] 4.3 `internal/rtisvc/runtime.go` `ExecuteDelete`: без `Hard` → `ReleaseSession`: декремент → `DeleteResult{Deleted:false, Decremented:true, UseCount:remaining}`; иначе `DeleteSession` + прежние поля; с `Hard` → всегда `DeleteSession`; по имени — независимая обработка каждого совпадения, агрегация `SessionIDs` (удалённые) и `DecrementedIDs`
- [x] 4.4 `internal/trcsvc/types.go` + `runtime.go`: зеркально 4.1–4.3 для TRC
- [x] 4.5 Проверить `parseResultFromSession` (rtisvc) и TRC-аналог: `use_count` сессии возвращается в ParseResult при reuse (данные уже в `RTISession`/`TRCSession` после 2.4/3.1, либо получить из acquire)

## 5. CLI

- [x] 5.1 `cmd/rti.go`: флаг `--hard` у `rti delete` → `DeleteParams.Hard`; человекочитаемый вывод различает «Deleted session N» / «Session N: use_count 3 → 2 (kept; use --hard to remove)»; `rti list` выводит use_count; JSON-режим (`--json`) прогоняет новые поля автоматически
- [x] 5.2 `cmd/trc.go`: зеркально 5.1 для `trc delete --hard` и `trc list`
- [x] 5.3 `rti parse`/`trc parse` человекочитаемый вывод: строка `Use count: N` рядом с `Saved session:` (и при reuse)

## 6. MCP

- [x] 6.1 `internal/mcp/registry.go`: `codebase_rti_delete`/`codebase_trc_delete` — опциональный boolean-параметр `hard`, обновлённые Description (декремент/окончательное удаление, поля ответа)
- [x] 6.2 Проверить, что ответы parse/list/delete серилизуются с новыми полями (`use_count`, `decremented`, `decremented_ids`) без правок пагинации/конверта

## 7. Тесты

- [x] 7.1 RTI store_integration: `TestAcquireReleaseSessionUse` — fresh `SetSessionHash` → 1; два acquire → 3; `ReleaseSession` → 2, сессия жива; release при 1 → удалить `DeleteSession`, `GetSession` — not found; legacy (use_count 0, hash NULL) → release-семантика = окончательное удаление
- [x] 7.2 TRC store_integration: зеркально 7.1 (`_TRC`)
- [x] 7.3 rtisvc runtime_integration: parse fresh → `UseCount=1`; parse повторно → `UseCount=2`, строки не растут; delete при 2 → `{Deleted:false, Decremented:true, UseCount:1}`; delete повторно → `Deleted:true`, сессии нет; `delete --hard` (params) при 2 → `Deleted:true`; parse после декремента → reuse того же id с `UseCount=2`; конфликт `ConcurrentSameFileConverges` → winner `UseCount=2`
- [x] 7.4 trcsvc runtime_integration: зеркально 7.3 (`_TRC`)
- [x] 7.5 Прогнать `go test ./internal/rti/... ./internal/trc/... ./internal/rtisvc/... ./internal/trcsvc/...` (unit + integration) — зелёные
- [x] 7.6 Smoke вручную: parse файла из `Tests/` дважды → use_count 2 в `list` и ответе parse; delete → kept; delete --hard → удалена; prune --keep-hours затрагивает общую сессию

## 8. Документация и финализация

- [x] 8.1 README.md: разделы RTI/TRC — parse (поле use_count), delete (декремент/`--hard`, новый формат ответа), list (колонка use_count), prune (игнорирует счётчик); заменить упоминания «delete + parse = перепарсинг» на `--hard`
- [x] 8.2 `go build ./...` без ошибок; `go vet ./...` чисто
- [x] 8.3 `openspec validate parse-session-use-count --strict` — без ошибок

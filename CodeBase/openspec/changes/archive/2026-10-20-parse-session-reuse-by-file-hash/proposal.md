# Reuse сессии при повторном парсинге того же файла

## Why

Повторный `rti parse` / `trc parse` одного и того же файла всегда создаёт новую
сессию и заново прогоняет весь конвейер разбора: для реальных логов из `Tests/`
(77–754 МБ) это минуты работы и гигабайты дублей в БД. Дедупликации нет ни на
одном уровне — ни `file_path`, ни хеш/`mtime`/`size` не проверяются. Источник
предложения: `Modifications/parse-session-reuse-by-file-hash-d7a1c3.md`
(исследовано, верифицировано по коду 2026-10-20; найдены и учтены два уточнения —
см. design.md).

## What Changes

- Повторный `parse` того же файла (SHA-256 содержимого + версия парсера)
  возвращает существующий `session_id` без повторного разбора и без новых
  строк в БД; формат ответа CLI/MCP не меняется (reuse молчаливый).
- В `rti_sessions` и `trc_sessions` добавляются колонки `file_hash` (hex
  SHA-256) и `parser_version` + partial unique index `(file_hash,
  parser_version) WHERE file_hash IS NOT NULL`.
- Версия парсера — доменные константы `rti.ParserVersion` и
  `trc.ParserVersion`, инкрементируемые вручную только при изменении логики
  разбора соответствующего парсера (а не build бинарника).
- Хеш проставляется последним шагом после успешного сохранения: прерванные
  и legacy-сессии (`file_hash IS NULL`) не переиспользуются.
- При конфликте уникальности (параллельный `parse` того же файла) проигравшая
  сессия явно удаляется (`DeleteSession`), вызывающая сторона получает
  `session_id` победителя.
- Принудительный перепарсинг — `delete` + `parse`; отдельный флаг `--force`
  не вводится.
- Новые `FindSessionByHash` / `SetSessionHash` в `internal/rti/store.go` и
  `internal/trc/store.go`; `internal/util/hash.go` — хелпер `FileHash`.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `rti-analysis/rti-storage`: добавляется требование reuse сессии при
  повторном парсинге того же файла по (file_hash, parser_version)
- `trc-analysis/trc-storage`: то же для TRC-сессий

## Impact

- **Схема БД**: `internal/store/db_schema.go` — `ALTER TABLE ... ADD COLUMN
  IF NOT EXISTS file_hash/parser_version` для `rti_sessions`/`trc_sessions` +
  два partial unique index (миграция идемпотентна, legacy-строки не мешают).
- **Доменные пакеты**: `internal/rti/` (parser_version.go, store.go),
  `internal/trc/` (parser_version.go, store.go); `internal/util/hash.go`
  (новый).
- **Сервисный слой**: `internal/rtisvc/runtime.go` и
  `internal/trcsvc/runtime.go` — reuse-ветка в `ExecuteParse`; единая точка
  входа, поэтому CLI (`cmd/rti.go`, `cmd/trc.go`) и MCP (`registry.go`)
  получают поведение автоматически без правок.
- **Не затрагивается**: режим `--file` без `session_id` (`resolveSession`
  сессий не создаёт), формат `ParseResult` (CLI JSON и MCP), `delete`/`prune`
  (сброс кэша удалением строки), пакет `buildinfo` не создаётся.
- **Совместимость**: legacy-сессии без хеша не переиспользуются, но и не
  блокируют вставку (partial index); повторный разбор после инкремента
  `ParserVersion` даёт новую сессию (старые строки остаются).
- **Документация**: README.md — разделы RTI/TRC.

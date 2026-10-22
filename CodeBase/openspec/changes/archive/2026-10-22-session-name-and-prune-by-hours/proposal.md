## Why

Сейчас сессии RTI/TRC идентифицируются только числовым `session_id` и путём к файлу, поэтому в `list` и при удалении приходится оперировать неудобными идентификаторами, а `prune` умеет только «оставить последние N», из-за чего нельзя освободить место, сохранив свежие сессии по времени. Нужно дать сессиям человекочитаемое имя по умолчанию (имя разобранного файла), возможность задать его при парсинге, удалять по имени и очищать сессии старше заданного числа часов.

## What Changes

- В `rti_sessions` и `trc_sessions` добавляется колонка `session_name TEXT NOT NULL DEFAULT ''`. Для новых сессий по умолчанию записывается имя файла (`filepath.Base`, с расширением), для legacy-сессий выполняется идемпотентный backfill из `file_path`.
- `codebase rti parse` и `codebase trc parse` получают опциональный флаг `--name`; MCP-инструменты `codebase_rti_parse` / `codebase_trc_parse` — опциональный аргумент `name`.
- При reuse по `(file_hash, parser_version)`: если имя передано явно, существующая сессия переименовывается (`UPDATE session_name`); если не передано — сохранённое имя не меняется.
- `codebase rti delete` / `codebase trc delete` принимают `--session` **или** `--name` (взаимоисключающе). Удаление по имени удаляет **все** совпадающие сессии и возвращает их ID и количество. MCP delete принимает `session_id` **или** `name`.
- `codebase rti prune` / `codebase trc prune` получают `--keep-hours N` (N > 0) как альтернативу `--keep-last`. Флаги взаимоисключающие; отсчёт по `parsed_at`; удаляются сессии с `parsed_at < NOW() - N hours`. TRUNCATE-оптимизация сохраняется только для режима `keep_last == 0` без `keep_hours`. MCP prune получает `keep_hours`.
- `PruneResult` дополняется `kept_hours` и `cutoff`, поле `kept_last` сохраняется.
- `rti list` / `trc list` и модели сессий (`RTISession`, `TRCSession`) в JSON отдают `session_name`.
- Существующие вызовы без новых аргументов сохраняют текущее поведение; старые MCP-клиенты и старые бинарники не ломаются (колонка имеет `DEFAULT ''`).

## Capabilities

### New Capabilities

_Нет._

### Modified Capabilities

- `rti-analysis/rti-storage`: сохранение сессии с именем, reuse с переименованием, список с именем, удаление по ID или имени, prune по keep-last или keep-hours.
- `trc-analysis/trc-storage`: те же изменения для TRC-сессий.
- `mcp-server/mcp-transport-tools`: новые опциональные аргументы `name` (parse/delete) и `keep_hours` (prune) у RTI/TRC-инструментов.

## Impact

- **Схема БД**: `ALTER TABLE rti_sessions/trc_sessions ADD COLUMN IF NOT EXISTS session_name`; backfill в `InitSchemaCtx`; `CurrentSchemaVersion` не меняется (аддитивная идемпотентная миграция, как с `file_hash`).
- **Код**:
  - `internal/store/db_schema.go` — колонки и backfill.
  - `internal/rti/{model,store}.go`, `internal/trc/{model,store,parse_to_db}.go` — модель, INSERT, SELECT, `SetSessionName`, выборки по имени/времени, общий batch-delete helper.
  - `internal/rtisvc/{types,runtime}.go`, `internal/trcsvc/{types,runtime}.go` — параметры и результаты parse/delete/prune.
  - `cmd/rti.go`, `cmd/trc.go` — флаги `--name`, `--name`/`--session`, `--keep-hours`, вывод имени в `list`.
  - `internal/mcp/registry.go` — схемы и обработчики RTI/TRC parse/delete/prune.
- **Тесты**: сигнатуры `SaveSession`/`ParseFileToDB` затронут существующие интеграционные тесты; добавляются тесты на default name, backfill, rename при reuse, delete-by-name с дубликатами, prune-by-hours.
- **Документация/спеки**: `README.md`, дельты `rti-storage`, `trc-storage`, `mcp-transport-tools`.

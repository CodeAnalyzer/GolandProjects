# TRC Storage

## Purpose

Управление сессиями парсинга TRC-файлов: сохранение в БД для повторного анализа, список сессий, удаление, очистка старых сессий с batch-обработкой для больших объёмов данных.

## Requirements

### Requirement: Сохранение сессии в БД

Система SHALL сохранять результат парсинга .trc/.xml/.xel файла в БД (trc_sessions, trc_events) и возвращать session_id для повторного анализа.

#### Scenario: Парсинг с сохранением

- **GIVEN** .trc файл `trace.trc`
- **WHEN** выполняется `codebase trc parse trace.trc`
- **THEN** данные сохранены в БД и возвращён session_id с указанием total_events

#### Scenario: Загрузка из сессии

- **GIVEN** ранее сохранённая сессия с id 42
- **WHEN** выполняется `codebase trc summary --session 42`
- **THEN** сводка загружена из БД без повторного парсинга файла

### Requirement: Список сессий

Система SHALL предоставлять команду `trc list` для вывода списка сохранённых сессий, упорядоченных по убыванию даты, с указанием session_id, file_path, total_events, file_size.

#### Scenario: Список с лимитом

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase trc list --limit 10`
- **THEN** возвращены последние 10 сессий

### Requirement: Удаление сессии

Система SHALL предоставлять команду `trc delete` для удаления сессии с каскадным удалением всех связанных событий.

#### Scenario: Удаление сессии

- **GIVEN** сохранённая сессия с id 42
- **WHEN** выполняется `codebase trc delete --session 42`
- **THEN** сессия и все связанные события удалены

### Requirement: Очистка старых сессий

Система SHALL предоставлять команду `trc prune` для удаления старых сессий с оставлением последних N, с batch-обработкой (по 50000 записей) для больших объёмов данных и VACUUM ANALYZE после очистки.

#### Scenario: Очистка с оставлением 5 сессий

- **GIVEN** 20 сохранённых сессий
- **WHEN** выполняется `codebase trc prune --keep-last 5`
- **THEN** удалены 15 старых сессий, оставлены 5 последних

#### Scenario: Полная очистка

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase trc prune --keep-last 0`
- **THEN** все сессии удалены через TRUNCATE ... RESTART IDENTITY CASCADE

### Requirement: Batch-удаление для больших объёмов

Система SHALL удалять записи batch-ами по 50000 session_id за раз (сначала дочерние таблицы, затем session), для предотвращения переполнения при удалении сессий с миллионами событий.

#### Scenario: Удаление большой сессии

- **GIVEN** сессия с 18 млн событий в trc_events
- **WHEN** выполняется `codebase trc delete --session 42`
- **THEN** записи удаляются batch-ами по 50000, без переполнения

### Requirement: Fallback при недоступной БД

Система SHALL поддерживать режим работы без подключения к БД: при недоступной БД команды анализа TRC работают с файлом напрямую в памяти, без сохранения сессии. Команда `trc parse` в этом случае возвращает результат с `session_id = 0` и непустым полем `Warning` (значение `"database unavailable, session not saved"`), а не падает с ошибкой. Аналитические команды (`trc summary`, `trc events`, `trc procedures`, `trc tree`, `trc errors`, `trc slow`) при недоступной БД и указанном `--file` парсят файл в памяти и возвращают результат без сохранения. Команды `trc list`, `trc delete`, `trc prune` требуют наличия БД и возвращают ошибку `"database not available"`, если БД недоступна.

#### Scenario: Парсинг при недоступной БД

- **GIVEN** .trc файл `trace.trc` и конфигурация без подключения к БД (или БД недоступна)
- **WHEN** выполняется `codebase trc parse trace.trc`
- **THEN** файл успешно парсится в памяти
- **AND** возвращается результат с `session_id = 0`, непустым `Warning = "database unavailable, session not saved"` и заполненным `TotalEvents`
- **AND** команда не завершается ошибкой

#### Scenario: Парсинг при доступной БД

- **GIVEN** .trc файл `trace.trc` и доступное подключение к БД
- **WHEN** выполняется `codebase trc parse trace.trc`
- **THEN** файл парсится и сохраняется в БД
- **AND** возвращается результат с `session_id > 0` и пустым `Warning`

#### Scenario: Анализ из файла при недоступной БД

- **GIVEN** .trc файл `trace.trc` и недоступная БД
- **WHEN** выполняется `codebase trc summary --file trace.trc`
- **THEN** файл парсится в памяти, возвращается сводка без сохранения сессии в БД

#### Scenario: Storage-команды требуют БД

- **GIVEN** недоступная БД
- **WHEN** выполняется `codebase trc list` (или `trc delete --session 42`, или `trc prune --keep-last 5`)
- **THEN** команда завершается ошибкой `"database not available"`

## Related code

- `internal/trc/store.go` — `SaveSession`, `LoadEvents`, `ListSessions`, `DeleteSession`, `PruneSessions`
- `internal/trc/parse_to_db.go` — `ParseToDB`, связка парсинг + сохранение в БД (используется при доступной БД)
- `internal/trcsvc/runtime.go` — execution-слой для CLI (`cmd/trc.go`) и MCP: `ExecuteParse` (с веткой fallback при `db == nil`), `ExecuteList`/`ExecuteDelete`/`ExecutePrune` (требуют БД), `ExecuteSummary`/`ExecuteEvents`/`ExecuteProcedures`/`ExecuteTree`/`ExecuteErrors`/`ExecuteSlow` (работают из файла при недоступной БД через `resolveSession`)
- `internal/store/db_schema.go` — таблицы `trc_sessions`, `trc_events`
- `cmd/trc.go` — CLI commands `trc parse`, `trc list`, `trc delete`, `trc prune`

## Notes

- `keep_last = 0` валидно и означает полную очистку через TRUNCATE
- `keep_last < 0` отклоняется с ошибкой валидации
- VACUUM ANALYZE выполняется после prune (ошибка игнорируется — не критично)
- trc_events содержит JSONB-поля params и columns для гибкого хранения декодированных данных
- Execution-слой `internal/trcsvc` — общая точка входа для CLI и MCP, устраняющая дублирование оркестрации (см. также домен `mcp-server`). Поведение storage-команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

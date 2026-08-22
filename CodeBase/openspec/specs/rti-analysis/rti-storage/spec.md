# RTI Storage

## Purpose

Управление сессиями парсинга RTI-логов: сохранение в БД для повторного анализа, список сессий, удаление, очистка старых сессий с batch-обработкой для больших объёмов данных.

## Requirements

### Requirement: Сохранение сессии в БД

Система SHALL сохранять результат парсинга RTI-файла в БД (rti_sessions, rti_calls, rti_params, rti_checkpoints, rti_blog_blocks, rti_blog_tables, rti_client_events) и возвращать session_id для повторного анализа.

#### Scenario: Парсинг с сохранением

- **GIVEN** RTI-файл `trace.rti`
- **WHEN** выполняется `codebase rti parse trace.rti`
- **THEN** данные сохранены в БД и возвращён session_id

#### Scenario: Загрузка из сессии

- **GIVEN** ранее сохранённая сессия с id 42
- **WHEN** выполняется `codebase rti summary --session 42`
- **THEN** сводка загружена из БД без повторного парсинга файла

### Requirement: Список сессий

Система SHALL предоставлять команду `rti list` для вывода списка сохранённых сессий, упорядоченных по убыванию даты, с указанием session_id, file_path, количества вызовов и ошибок.

#### Scenario: Список с лимитом

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase rti list --limit 10`
- **THEN** возвращены последние 10 сессий

### Requirement: Удаление сессии

Система SHALL предоставлять команду `rti delete` для удаления сессии с каскадным удалением всех связанных записей (calls, params, checkpoints, blog_blocks, blog_tables, client_events).

#### Scenario: Удаление сессии

- **GIVEN** сохранённая сессия с id 42
- **WHEN** выполняется `codebase rti delete --session 42`
- **THEN** сессия и все связанные записи удалены

### Requirement: Очистка старых сессий

Система SHALL предоставлять команду `rti prune` для удаления старых сессий с оставлением последних N, с batch-обработкой (по 50000 записей) для больших объёмов данных и VACUUM ANALYZE после очистки.

#### Scenario: Очистка с оставлением 5 сессий

- **GIVEN** 20 сохранённых сессий
- **WHEN** выполняется `codebase rti prune --keep-last 5`
- **THEN** удалены 15 старых сессий, оставлены 5 последних

#### Scenario: Полная очистка

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase rti prune --keep-last 0`
- **THEN** все сессии удалены через TRUNCATE ... RESTART IDENTITY CASCADE

### Requirement: Batch-удаление для больших объёмов

Система SHALL удалять записи batch-ами по 50000 session_id за раз (сначала дочерние таблицы, затем session), для предотвращения переполнения при удалении сессий с миллионами записей.

#### Scenario: Удаление большой сессии

- **GIVEN** сессия с 18 млн записей в rti_calls
- **WHEN** выполняется `codebase rti delete --session 42`
- **THEN** записи удаляются batch-ами по 50000, без переполнения

### Requirement: Fallback при недоступной БД

Система SHALL поддерживать режим работы без подключения к БД: при недоступной БД команды анализа RTI работают с файлом напрямую в памяти, без сохранения сессии. Команда `rti parse` в этом случае возвращает результат с `session_id = 0` и непустым полем `Warning` (значение `"database unavailable, session not saved"`), а не падает с ошибкой. Аналитические команды (`rti summary`, `rti tree`, `rti errors`, `rti slow`, `rti details`, `rti blog`, `rti client-tree`, `rti timeline`) при недоступной БД и указанном `--file` парсят файл в памяти и возвращают результат без сохранения. Команды `rti list`, `rti delete`, `rti prune` требуют наличия БД и возвращают ошибку `"database not available"`, если БД недоступна.

#### Scenario: Парсинг при недоступной БД

- **GIVEN** RTI-файл `trace.rti` и конфигурация без подключения к БД (или БД недоступна)
- **WHEN** выполняется `codebase rti parse trace.rti`
- **THEN** файл успешно парсится в памяти
- **AND** возвращается результат с `session_id = 0`, непустым `Warning = "database unavailable, session not saved"` и заполненной сводкой `TotalCalls`
- **AND** команда не завершается ошибкой

#### Scenario: Парсинг при доступной БД

- **GIVEN** RTI-файл `trace.rti` и доступное подключение к БД
- **WHEN** выполняется `codebase rti parse trace.rti`
- **THEN** файл парсится и сохраняется в БД
- **AND** возвращается результат с `session_id > 0` и пустым `Warning`

#### Scenario: Анализ из файла при недоступной БД

- **GIVEN** RTI-файл `trace.rti` и недоступная БД
- **WHEN** выполняется `codebase rti summary --file trace.rti`
- **THEN** файл парсится в памяти, возвращается сводка без сохранения сессии в БД

#### Scenario: Storage-команды требуют БД

- **GIVEN** недоступная БД
- **WHEN** выполняется `codebase rti list` (или `rti delete --session 42`, или `rti prune --keep-last 5`)
- **THEN** команда завершается ошибкой `"database not available"`

## Related code

- `internal/rti/store.go` — `SaveSession`, `LoadCalls`, `LoadClientEvents`, `ListSessions`, `DeleteSession`, `PruneSessions`
- `internal/rtisvc/runtime.go` — execution-слой для CLI (`cmd/rti.go`) и MCP: `ExecuteParse` (с веткой fallback при `db == nil`), `ExecuteList`/`ExecuteDelete`/`ExecutePrune` (требуют БД), `ExecuteSummary`/`ExecuteTree`/`ExecuteErrors`/`ExecuteSlow`/`ExecuteDetails`/`ExecuteBlog`/`ExecuteClientTree`/`ExecuteTimeline` (работают из файла при недоступной БД через `resolveSession`)
- `internal/store/db_schema.go` — таблицы `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`
- `cmd/rti.go` — CLI commands `rti parse`, `rti list`, `rti delete`, `rti prune`

## Notes

- `keep_last = 0` валидно и означает полную очистку через TRUNCATE
- `keep_last < 0` отклоняется с ошибкой валидации
- VACUUM ANALYZE выполняется после prune (ошибка игнорируется — не критично)
- Batch-удаление: сначала дочерние таблицы батчами по session_id, потом session (CASCADE уже нечего удалять)
- Execution-слой `internal/rtisvc` — общая точка входа для CLI и MCP, устраняющая дублирование оркестрации (см. также домен `mcp-server`). Поведение storage-команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

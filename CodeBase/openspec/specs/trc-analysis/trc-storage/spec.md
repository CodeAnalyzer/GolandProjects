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

### Requirement: Reuse сессии при повторном парсинге того же файла

Система SHALL при выполнении `trc parse` файла, содержимое которого (SHA-256) уже разобрано текущей версией TRC-парсера, возвращать `session_id` существующей сессии без повторного разбора файла и без добавления строк в БД. Ответ при reuse SHALL иметь тот же формат, что и при первом парсинге (`session_id`, `total_events`), без пометки о reuse. Поведение SHALL быть единым для CLI и MCP. Ключ кэша — пара (хеш содержимого, версия парсера): после инкремента версии парсера повторный парсинг того же файла SHALL создавать новую сессию. Сессии без хеша (созданные до введения reuse или с прерванным разбором) не переиспользуются и не мешают созданию новых. Принудительный перепарсинг выполняется через `trc delete` + `trc parse`.

#### Scenario: Повторный парсинг того же файла

- **GIVEN** .trc файл `trace.trc`, уже разобранный и сохранённый в сессию 42
- **WHEN** выполняется `codebase trc parse trace.trc` повторно
- **THEN** возвращён тот же `session_id = 42` с `total_events` первого парсинга
- **AND** новые строки в `trc_events` не добавлены

#### Scenario: Reuse через MCP

- **GIVEN** .trc файл, уже разобранный ранее текущей версией парсера
- **WHEN** инструмент `codebase_trc_parse` вызывается с тем же `file_path`
- **THEN** ответ содержит тот же `session_id`, что и предыдущий вызов

#### Scenario: Изменённое содержимое файла

- **GIVEN** .trc файл `trace.trc`, ранее сохранённый в сессию 42
- **WHEN** содержимое файла изменилось и выполняется `codebase trc parse trace.trc`
- **THEN** создана новая сессия с новым `session_id` и полным разбором

#### Scenario: Удаление сессии сбрасывает кэш

- **GIVEN** сохранённая сессия 42 для файла `trace.trc`
- **WHEN** выполняется `codebase trc delete --session 42`, затем `codebase trc parse trace.trc`
- **THEN** создана новая сессия с новым `session_id > 42` без reuse

#### Scenario: Инкремент версии парсера

- **GIVEN** файл разобран версией парсера N и сохранён в сессию 42
- **WHEN** версия парсера инкрементирована до N+1 и выполняется `trc parse` того же файла
- **THEN** создана новая сессия с новым `session_id`
- **AND** сессия 42 остаётся в БД

#### Scenario: Конфликт параллельных парсингов одного файла

- **GIVEN** файл `trace.trc` ещё не разобран
- **WHEN** два `trc parse` одного файла выполняются параллельно
- **THEN** оба вызова завершаются успешно с одинаковым `session_id` сессии-победителя
- **AND** сессия проигравшего парсинга удалена из БД

#### Scenario: Прерванный разбор не переиспользуется

- **GIVEN** предыдущий `trc parse` завершился ошибкой в середине разбора (сессия-обрывок сохранена без хеша)
- **WHEN** выполняется повторный `trc parse` того же файла
- **THEN** выполняется полный разбор и создаётся новая сессия
- **AND** сессия-обрывок не переиспользуется

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

- `internal/trc/store.go` — `SaveSession`, `LoadEvents`, `ListSessions`, `DeleteSession`, `PruneSessions`, `FindSessionByHash`, `SetSessionHash` (reuse по `(file_hash, parser_version)`)
- `internal/trc/parser_version.go` — `ParserVersion` (инкремент при изменении логики разбора; входит в ключ reuse)
- `internal/util/hash.go` — `FileHash` (SHA-256 содержимого файла)
- `internal/trc/parse_to_db.go` — `ParseFileToDB`, связка парсинг + сохранение в БД (используется при доступной БД); сериализация параллельных разборов одного файла session-level advisory lock по абсолютному пути
- `internal/trcsvc/runtime.go` — execution-слой для CLI (`cmd/trc.go`) и MCP: `ExecuteParse` (reuse-ветка + fallback при `db == nil`, конфликт unique violation → победитель + удаление проигравшей), `ExecuteList`/`ExecuteDelete`/`ExecutePrune` (требуют БД), `ExecuteSummary`/`ExecuteEvents`/`ExecuteProcedures`/`ExecuteTree`/`ExecuteErrors`/`ExecuteSlow` (работают из файла при недоступной БД через `resolveSession`)
- `internal/store/db_schema.go` — таблицы `trc_sessions` (колонки `file_hash`/`parser_version`, partial unique index `(file_hash, parser_version)`), `trc_events`
- `cmd/trc.go` — CLI commands `trc parse`, `trc list`, `trc delete`, `trc prune`

## Notes

- `keep_last = 0` валидно и означает полную очистку через TRUNCATE
- `keep_last < 0` отклоняется с ошибкой валидации
- VACUUM ANALYZE выполняется после prune (ошибка игнорируется — не критично)
- trc_events содержит JSONB-поля params и columns для гибкого хранения декодированных данных
- Reuse молчаливый: формат ответа не меняется, CLI печатает `Saved session: N` и при reuse; хеш проставляется последним шагом после успешного сохранения — сессия вставляется до парсинга (нужна для FK `trc_events.session_id`), при ошибке разбора `file_hash` остаётся NULL и сессия в кэш не попадает
- Execution-слой `internal/trcsvc` — общая точка входа для CLI и MCP, устраняющая дублирование оркестрации (см. также домен `mcp-server`). Поведение storage-команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

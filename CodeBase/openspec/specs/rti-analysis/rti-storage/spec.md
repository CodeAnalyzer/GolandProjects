# RTI Storage

## Purpose

Управление сессиями парсинга RTI-логов: сохранение в БД для повторного анализа, список сессий, удаление, очистка старых сессий с batch-обработкой для больших объёмов данных.

## Requirements

### Requirement: Сохранение сессии в БД

Система SHALL сохранять результат парсинга RTI-файла в БД (rti_sessions, rti_calls, rti_params, rti_checkpoints, rti_blog_blocks, rti_blog_tables, rti_client_events) и возвращать session_id для повторного анализа. Каждая сессия SHALL иметь `session_name`: по умолчанию — имя разобранного файла (`filepath.Base` с расширением), при явно заданном `--name` в CLI или `name` в MCP — переданное значение. Для legacy-сессий с пустым `session_name` при инициализации схемы SHALL выполняться идемпотентный backfill из `file_path`.

#### Scenario: Парсинг с сохранением

- **GIVEN** RTI-файл `trace.rti`
- **WHEN** выполняется `codebase rti parse trace.rti`
- **THEN** данные сохранены в БД и возвращён session_id
- **AND** сессия сохранена с `session_name = "trace.rti"`

#### Scenario: Загрузка из сессии

- **GIVEN** ранее сохранённая сессия с id 42
- **WHEN** выполняется `codebase rti summary --session 42`
- **THEN** сводка загружена из БД без повторного парсинга файла

#### Scenario: Явно заданное имя сессии в CLI

- **GIVEN** RTI-файл `trace.rti`
- **WHEN** выполняется `codebase rti parse trace.rti --name "Проблемный расчёт"`
- **THEN** сессия сохранена с `session_name = "Проблемный расчёт"`

#### Scenario: Явно заданное имя сессии через MCP

- **GIVEN** RTI-файл и запущенный MCP-сервер
- **WHEN** `codebase_rti_parse` вызывается с `file_path` и `name = "Проблемный расчёт"`
- **THEN** возвращён `session_id` сессии с `session_name = "Проблемный расчёт"`

#### Scenario: Backfill имени legacy-сессии

- **GIVEN** сохранённая до изменения сессия с `file_path = "D:\\logs\\trace.rti"` и пустым `session_name`
- **WHEN** выполняется инициализация схемы новой версией CodeBase
- **THEN** `session_name` сессии заполнен значением `trace.rti`

### Requirement: Reuse сессии при повторном парсинге того же файла

Система SHALL при выполнении `rti parse` файла, содержимое которого (SHA-256) уже разобрано текущей версией RTI-парсера, возвращать `session_id` существующей сессии без повторного разбора файла и без добавления строк в БД. Ответ при reuse SHALL иметь тот же формат, что и при первом парсинге (`session_id`, `total_calls`, `summary`, включая топ медленных вызовов), без пометки о reuse. Поведение SHALL быть единым для CLI и MCP. Ключ кэша — пара (хеш содержимого, версия парсера): после инкремента версии парсера повторный парсинг того же файла SHALL создавать новую сессию. Сессии без хеша (созданные до введения reuse или прерванные) не переиспользуются и не мешают созданию новых. При reuse с явно переданным именем сессии система SHALL обновить `session_name` найденной сессии; при отсутствии явного имени сохранённое имя SHALL NOT изменяться. Принудительный перепарсинг выполняется через `rti delete` + `rti parse`.

#### Scenario: Повторный парсинг того же файла

- **GIVEN** RTI-файл `trace.rti`, уже разобранный и сохранённый в сессию 42
- **WHEN** выполняется `codebase rti parse trace.rti` повторно
- **THEN** возвращён тот же `session_id = 42` со сводкой первого парсинга
- **AND** новые строки в таблицы сессии не добавлены

#### Scenario: Reuse через MCP

- **GIVEN** RTI-файл, уже разобранный ранее текущей версией парсера
- **WHEN** инструмент `codebase_rti_parse` вызывается с тем же `file_path`
- **THEN** ответ содержит тот же `session_id`, что и предыдущий вызов

#### Scenario: Переименование при reuse

- **GIVEN** RTI-файл `trace.rti`, уже разобранный в сессию 42 с `session_name = "trace.rti"`
- **WHEN** выполняется `codebase rti parse trace.rti --name "Повторный анализ"`
- **THEN** возвращён тот же `session_id = 42`
- **AND** `session_name` сессии 42 изменён на `"Повторный анализ"`

#### Scenario: Reuse без явного имени не меняет имя

- **GIVEN** RTI-файл, уже разобранный в сессию 42 с `session_name = "Повторный анализ"`
- **WHEN** выполняется `codebase rti parse trace.rti` без `--name`
- **THEN** возвращён тот же `session_id = 42`
- **AND** `session_name` сессии 42 остался `"Повторный анализ"`

#### Scenario: Изменённое содержимое файла

- **GIVEN** RTI-файл `trace.rti`, ранее сохранённый в сессию 42
- **WHEN** содержимое файла изменилось и выполняется `codebase rti parse trace.rti`
- **THEN** создана новая сессия с новым `session_id` и полным разбором

#### Scenario: Удаление сессии сбрасывает кэш

- **GIVEN** сохранённая сессия 42 для файла `trace.rti`
- **WHEN** выполняется `codebase rti delete --session 42`, затем `codebase rti parse trace.rti`
- **THEN** создана новая сессия с новым `session_id > 42` без reuse

#### Scenario: Инкремент версии парсера

- **GIVEN** файл разобран версией парсера N и сохранён в сессию 42
- **WHEN** версия парсера инкрементирована до N+1 и выполняется `rti parse` того же файла
- **THEN** создана новая сессия с новым `session_id`
- **AND** сессия 42 остаётся в БД

#### Scenario: Конфликт параллельных парсингов одного файла

- **GIVEN** файл `trace.rti` ещё не разобран
- **WHEN** два `rti parse` одного файла выполняются параллельно
- **THEN** оба вызова завершаются успешно с одинаковым `session_id` сессии-победителя
- **AND** сессия проигравшего парсинга удалена из БД

#### Scenario: Legacy-сессия без хеша

- **GIVEN** сессия 42, сохранённая до введения reuse (без хеша содержимого)
- **WHEN** выполняется `rti parse` того же файла
- **THEN** создана новая сессия с полным разбором
- **AND** сессия 42 не мешает сохранению новой

### Requirement: Список сессий

Система SHALL предоставлять команду `rti list` для вывода списка сохранённых сессий, упорядоченных по убыванию даты, с указанием session_id, session_name, file_path, количества вызовов и ошибок.

#### Scenario: Список с лимитом

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase rti list --limit 10`
- **THEN** возвращены последние 10 сессий

#### Scenario: Имя сессии в списке

- **GIVEN** сохранённая сессия с `session_name = "trace.rti"`
- **WHEN** выполняется `codebase rti list`
- **THEN** в выводе и JSON-ответе присутствует `session_name = "trace.rti"`

### Requirement: Удаление сессии

Система SHALL предоставлять команду `rti delete` для удаления сессии с каскадным удалением всех связанных записей (calls, params, checkpoints, blog_blocks, blog_tables, client_events), включая сессии с клиентскими событиями, связанными с серверными вызовами через `server_call_id`. Удаление SHALL выполняться по `--session` (ID) либо по `--name` (точное совпадение `session_name`); параметры SHALL быть взаимоисключающими, и хотя бы один из них MUST быть задан. Удаление по имени SHALL удалять все совпадающие сессии и возвращать количество и список ID удалённых сессий. Если сессии с указанным именем не найдены, команда SHALL завершаться ошибкой.

#### Scenario: Удаление сессии

- **GIVEN** сохранённая сессия с id 42
- **WHEN** выполняется `codebase rti delete --session 42`
- **THEN** сессия и все связанные записи удалены

#### Scenario: Удаление сессии по имени

- **GIVEN** сохранённая сессия 42 с `session_name = "trace.rti"`
- **WHEN** выполняется `codebase rti delete --name "trace.rti"`
- **THEN** сессия 42 и все связанные записи удалены
- **AND** результат содержит `deleted_count = 1` и `session_id = 42`

#### Scenario: Удаление по имени при дубликатах

- **GIVEN** сессии 42 и 57 с одинаковым `session_name = "trace.rti"`
- **WHEN** выполняется `codebase rti delete --name "trace.rti"`
- **THEN** удалены обе сессии и все связанные записи
- **AND** результат содержит `deleted_count = 2` и список ID `[42, 57]`

#### Scenario: Взаимоисключающие параметры удаления

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** одновременно заданы `--session`/`session_id` и `--name`/`name`
- **THEN** команда завершается ошибкой о взаимоисключающих параметрах

#### Scenario: Удаление по несуществующему имени

- **GIVEN** отсутствует сессия с `session_name = "missing.rti"`
- **WHEN** выполняется `codebase rti delete --name "missing.rti"`
- **THEN** команда завершается ошибкой о ненайденной сессии

#### Scenario: Удаление сессии с коррелированными клиентскими событиями

- **GIVEN** сохранённая сессия с клиентскими событиями, у которых `server_call_id` ссылается на вызовы этой сессии
- **WHEN** выполняется `codebase rti delete --session 42`
- **THEN** сессия и все связанные записи (включая client_events и вызовы) удалены
- **AND** команда завершается без ошибки нарушения внешнего ключа

### Requirement: Очистка старых сессий

Система SHALL предоставлять команду `rti prune` для удаления старых сессий в одном из двух взаимоисключающих режимов: с оставлением последних N (`--keep-last`) либо с оставлением нетронутыми сессий за последние N часов (`--keep-hours`). Режим `--keep-hours` SHALL удалять сессии с `parsed_at < NOW() - N hours`, значение N MUST быть больше нуля. Batch-обработка (по 50000 записей) для больших объёмов данных и VACUUM ANALYZE после очистки SHALL выполняться в обоих режимах. TRUNCATE-оптимизация SHALL применяться только при `--keep-last 0` без `--keep-hours`. Результат SHALL содержать `deleted_count`, `kept_last`, `kept_hours` и `cutoff`. Очистка SHALL выполняться без ошибок внешнего ключа на сессиях с клиентскими событиями, связанными с серверными вызовами через `server_call_id`.

#### Scenario: Очистка с оставлением 5 сессий

- **GIVEN** 20 сохранённых сессий
- **WHEN** выполняется `codebase rti prune --keep-last 5`
- **THEN** удалены 15 старых сессий, оставлены 5 последних

#### Scenario: Очистка с оставлением сессий за последние часы

- **GIVEN** сессии с `parsed_at` 1 час назад, 23 часа назад и 25 часов назад
- **WHEN** выполняется `codebase rti prune --keep-hours 24`
- **THEN** удалена только сессия с `parsed_at` 25 часов назад
- **AND** сессии за последние 24 часа остались нетронутыми
- **AND** результат содержит `deleted_count = 1`, `kept_hours = 24` и `cutoff`

#### Scenario: Полная очистка

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase rti prune --keep-last 0`
- **THEN** все сессии удалены через TRUNCATE ... RESTART IDENTITY CASCADE

#### Scenario: Взаимоисключающие режимы очистки

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** одновременно заданы `--keep-last`/`keep_last` и `--keep-hours`/`keep_hours`
- **THEN** команда завершается ошибкой о взаимоисключающих параметрах

#### Scenario: Некорректное значение keep-hours

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** выполняется prune с `--keep-hours 0` или отрицательным значением
- **THEN** команда завершается ошибкой валидации, не удаляя сессии

#### Scenario: Очистка при наличии коррелированных клиентских событий

- **GIVEN** сохранённые сессии, среди которых есть сессии с клиентскими событиями, у которых `server_call_id` ссылается на вызовы той же сессии
- **WHEN** выполняется `codebase rti prune --keep-last 5`
- **THEN** все подлежащие удалению сессии удалены, включая сессии с коррелированными клиентскими событиями
- **AND** команда завершается без ошибки нарушения внешнего ключа

### Requirement: Batch-удаление для больших объёмов

Система SHALL удалять записи batch-ами по 50000 session_id за раз (сначала дочерние таблицы, затем session), для предотвращения переполнения при удалении сессий с миллионами записей. Порядок дочерних таблиц SHALL учитывать ссылку `rti_client_events.server_call_id → rti_calls(id)`: клиентские события удаляются до вызовов.

#### Scenario: Удаление большой сессии

- **GIVEN** сессия с 18 млн записей в rti_calls
- **WHEN** выполняется `codebase rti delete --session 42`
- **THEN** записи удаляются batch-ами по 50000, без переполнения

#### Scenario: Порядок удаления при живой ссылке server_call_id

- **GIVEN** сессия, в которой клиентские события ссылаются через `server_call_id` на вызовы сессии, и FK не имеет правила удаления
- **WHEN** выполняется batch-удаление сессии (`rti delete` или `rti prune`)
- **THEN** клиентские события удаляются до вызовов rti_calls
- **AND** удаление завершается без ошибки нарушения внешнего ключа

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

- `internal/rti/store.go` — `SaveSession`, `LoadCalls`, `LoadClientEvents`, `ListSessions`, `DeleteSession`, `PruneSessions`, `FindSessionByHash`, `SetSessionHash` (reuse по `(file_hash, parser_version)`)
- `internal/rti/parser_version.go` — `ParserVersion` (инкремент при изменении логики разбора; входит в ключ reuse)
- `internal/util/hash.go` — `FileHash` (SHA-256 содержимого файла)
- `internal/rtisvc/runtime.go` — execution-слой для CLI (`cmd/rti.go`) и MCP: `ExecuteParse` (reuse-ветка + fallback при `db == nil`, конфликт unique violation → победитель + удаление проигравшей), `ExecuteList`/`ExecuteDelete`/`ExecutePrune` (требуют БД), `ExecuteSummary`/`ExecuteTree`/`ExecuteErrors`/`ExecuteSlow`/`ExecuteDetails`/`ExecuteBlog`/`ExecuteClientTree`/`ExecuteTimeline` (работают из файла при недоступной БД через `resolveSession`)
- `internal/store/db_schema.go` — таблицы `rti_sessions` (колонки `file_hash`/`parser_version`, partial unique index `(file_hash, parser_version)`), `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`
- `cmd/rti.go` — CLI commands `rti parse`, `rti list`, `rti delete`, `rti prune`

## Notes

- `keep_last = 0` валидно и означает полную очистку через TRUNCATE
- `keep_last < 0` отклоняется с ошибкой валидации
- VACUUM ANALYZE выполняется после prune (ошибка игнорируется — не критично)
- Batch-удаление: сначала дочерние таблицы батчами по session_id, потом session (CASCADE уже нечего удалять)
- Reuse молчаливый: формат ответа не меняется, CLI печатает `Saved session: N` и при reuse; хеш проставляется последним шагом после успешного сохранения — прерванные сессии остаются с `file_hash IS NULL` и в кэш не попадают
- Execution-слой `internal/rtisvc` — общая точка входа для CLI и MCP, устраняющая дублирование оркестрации (см. также домен `mcp-server`). Поведение storage-команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

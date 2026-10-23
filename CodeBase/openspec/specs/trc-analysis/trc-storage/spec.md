# TRC Storage

## Purpose

Управление сессиями парсинга TRC-файлов: сохранение в БД для повторного анализа, список сессий, удаление, очистка старых сессий с batch-обработкой для больших объёмов данных.

## Requirements

### Requirement: Сохранение сессии в БД

Система SHALL сохранять результат парсинга .trc/.xml/.xel файла в БД (trc_sessions, trc_events) и возвращать session_id для повторного анализа. Каждая сессия SHALL иметь `session_name`: по умолчанию — имя разобранного файла (`filepath.Base` с расширением), при явно заданном `--name` в CLI или `name` в MCP — переданное значение. Для legacy-сессий с пустым `session_name` при инициализации схемы SHALL выполняться идемпотентный backfill из `file_path`.

#### Scenario: Парсинг с сохранением

- **GIVEN** .trc файл `trace.trc`
- **WHEN** выполняется `codebase trc parse trace.trc`
- **THEN** данные сохранены в БД и возвращён session_id с указанием total_events
- **AND** сессия сохранена с `session_name = "trace.trc"`

#### Scenario: Загрузка из сессии

- **GIVEN** ранее сохранённая сессия с id 42
- **WHEN** выполняется `codebase trc summary --session 42`
- **THEN** сводка загружена из БД без повторного парсинга файла

#### Scenario: Явно заданное имя сессии в CLI

- **GIVEN** .trc файл `trace.trc`
- **WHEN** выполняется `codebase trc parse trace.trc --name "Проблемный SPID"`
- **THEN** сессия сохранена с `session_name = "Проблемный SPID"`

#### Scenario: Явно заданное имя сессии через MCP

- **GIVEN** .trc файл и запущенный MCP-сервер
- **WHEN** `codebase_trc_parse` вызывается с `file_path` и `name = "Проблемный SPID"`
- **THEN** возвращён `session_id` сессии с `session_name = "Проблемный SPID"`

#### Scenario: Backfill имени legacy-сессии

- **GIVEN** сохранённая до изменения сессия с `file_path = "D:\\logs\\trace.trc"` и пустым `session_name`
- **WHEN** выполняется инициализация схемы новой версией CodeBase
- **THEN** `session_name` сессии заполнен значением `trace.trc`

### Requirement: Reuse сессии при повторном парсинге того же файла

Система SHALL при выполнении `trc parse` файла, содержимое которого (SHA-256) уже разобрано текущей версией TRC-парсера, возвращать `session_id` существующей сессии без повторного разбора файла и без добавления строк в БД. Ответ при reuse SHALL иметь тот же формат, что и при первом парсинге (`session_id`, `total_events`, и `use_count`), без отдельной пометки о reuse. Поведение SHALL быть единым для CLI и MCP. Ключ кэша — пара (хеш содержимого, версия парсера): после инкремента версии парсера повторный парсинг того же файла SHALL создавать новую сессию. Сессии без хеша (созданные до введения reuse или с прерванным разбором) не переиспользуются и не мешают созданию новых. При reuse с явно переданным именем сессии система SHALL обновить `session_name` найденной сессии; при отсутствии явного имени сохранённое имя SHALL NOT изменяться.

Каждая сессия SHALL иметь счётчик использования `use_count`. Первый успешный парсинг файла SHALL проставлять `use_count = 1` атомарно вместе с записью хеша; каждый reuse SHALL атомарно инкрементировать `use_count`, а ответ SHALL содержать актуальное значение поля `use_count`. При конфликте параллельных парсингов одного файла `use_count` сессии-победителя SHALL быть инкрементирован (оба вызова становятся держателями). Принудительный перепарсинг выполняется через `trc delete --hard` + `trc parse`.

#### Scenario: Повторный парсинг того же файла

- **GIVEN** .trc файл `trace.trc`, уже разобранный и сохранённый в сессию 42
- **WHEN** выполняется `codebase trc parse trace.trc` повторно
- **THEN** возвращён тот же `session_id = 42` с `total_events` первого парсинга
- **AND** ответ содержит `use_count = 2`
- **AND** новые строки в `trc_events` не добавлены

#### Scenario: Reuse через MCP

- **GIVEN** .trc файл, уже разобранный ранее текущей версией парсера
- **WHEN** инструмент `codebase_trc_parse` вызывается с тем же `file_path`
- **THEN** ответ содержит тот же `session_id`, что и предыдущий вызов
- **AND** ответ содержит актуальный `use_count`

#### Scenario: Первый парсинг инициализирует счётчик

- **GIVEN** .trc файл `trace.trc` ещё не разбирался текущей версией парсера
- **WHEN** выполняется `codebase trc parse trace.trc`
- **THEN** создана новая сессия, ответ содержит `use_count = 1`

#### Scenario: Серия повторных парсингов наращивает счётчик

- **GIVEN** .trc файл `trace.trc`, разобранный дважды (сессия 42, `use_count = 2`)
- **WHEN** выполняется `codebase trc parse trace.trc` третий раз
- **THEN** возвращён `session_id = 42` с `use_count = 3`
- **AND** новые строки в `trc_events` не добавлены

#### Scenario: Переименование при reuse

- **GIVEN** .trc файл `trace.trc`, уже разобранный в сессию 42 с `session_name = "trace.trc"`
- **WHEN** выполняется `codebase trc parse trace.trc --name "Повторный анализ"`
- **THEN** возвращён тот же `session_id = 42`
- **AND** `session_name` сессии 42 изменён на `"Повторный анализ"`

#### Scenario: Reuse без явного имени не меняет имя

- **GIVEN** .trc файл, уже разобранный в сессию 42 с `session_name = "Повторный анализ"`
- **WHEN** выполняется `codebase trc parse trace.trc` без `--name`
- **THEN** возвращён тот же `session_id = 42`
- **AND** `session_name` сессии 42 остался `"Повторный анализ"`

#### Scenario: Изменённое содержимое файла

- **GIVEN** .trc файл `trace.trc`, ранее сохранённый в сессию 42
- **WHEN** содержимое файла изменилось и выполняется `codebase trc parse trace.trc`
- **THEN** создана новая сессия с новым `session_id`, полным разбором и `use_count = 1`

#### Scenario: Удаление сессии сбрасывает кэш

- **GIVEN** сохранённая сессия 42 для файла `trace.trc` с `use_count = 1` (единственный держатель — окончательное удаление)
- **WHEN** выполняется `codebase trc delete --session 42`, затем `codebase trc parse trace.trc`
- **THEN** создана новая сессия с новым `session_id > 42` без reuse

#### Scenario: Общая сессия после декремента переиспользуется

- **GIVEN** сохранённая сессия 42 для файла `trace.trc` с `use_count = 2`
- **WHEN** выполняется `codebase trc delete --session 42` (счётчик декрементирован до 1), затем `codebase trc parse trace.trc`
- **THEN** возвращён тот же `session_id = 42` без повторного разбора
- **AND** `use_count` сессии 42 снова равен 2

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
- **AND** `use_count` сессии-победителя равен 2

#### Scenario: Прерванный разбор не переиспользуется

- **GIVEN** предыдущий `trc parse` завершился ошибкой в середине разбора (сессия-обрывок сохранена без хеша)
- **WHEN** выполняется повторный `trc parse` того же файла
- **THEN** выполняется полный разбор и создаётся новая сессия
- **AND** сессия-обрывок не переиспользуется

### Requirement: Список сессий

Система SHALL предоставлять команду `trc list` для вывода списка сохранённых сессий, упорядоченных по убыванию даты, с указанием session_id, session_name, file_path, total_events, file_size, а также счётчика использования `use_count`.

#### Scenario: Список с лимитом

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase trc list --limit 10`
- **THEN** возвращены последние 10 сессий

#### Scenario: Имя сессии в списке

- **GIVEN** сохранённая сессия с `session_name = "trace.trc"`
- **WHEN** выполняется `codebase trc list`
- **THEN** в выводе и JSON-ответе присутствует `session_name = "trace.trc"`

#### Scenario: Счётчик использования в списке

- **GIVEN** сессия 42, переиспользованная дважды после первого парсинга (`use_count = 3`)
- **WHEN** выполняется `codebase trc list`
- **THEN** в выводе и JSON-ответе у сессии 42 присутствует `use_count = 3`

### Requirement: Удаление сессии

Система SHALL предоставлять команду `trc delete` для удаления сессии с каскадным удалением всех связанных событий. Удаление SHALL выполняться по `--session` (ID) либо по `--name` (точное совпадение `session_name`); параметры SHALL быть взаимоисключающими, и хотя бы один из них MUST быть задан.

Без флага `--hard` удаление SHALL учитывать счётчик использования: при `use_count > 1` система SHALL атомарно декрементировать `use_count`, сохранив сессию и все связанные события (сессия остаётся переиспользуемой по хешу); при `use_count <= 1` — выполнять окончательное каскадное удаление. С флагом `--hard` в CLI или `hard = true` в MCP система SHALL выполнять окончательное удаление независимо от счётчика. Ответ SHALL разделять исходы: окончательно удалённые сессии (`deleted`, `deleted_count`, `session_ids`) и декрементированные (`decremented`, `decremented_ids`, актуальный `use_count`). Удаление по имени SHALL обрабатывать каждую совпавшую сессию независимо по тем же правилам счётчика и возвращать оба списка. Если сессии с указанным именем не найдены, команда SHALL завершаться ошибкой.

#### Scenario: Удаление сессии

- **GIVEN** сохранённая сессия с id 42 и `use_count = 1` (последний держатель)
- **WHEN** выполняется `codebase trc delete --session 42`
- **THEN** сессия и все связанные события удалены окончательно
- **AND** ответ содержит `deleted = true`

#### Scenario: Удаление сессии по имени

- **GIVEN** сохранённая сессия 42 с `session_name = "trace.trc"` и `use_count = 1`
- **WHEN** выполняется `codebase trc delete --name "trace.trc"`
- **THEN** сессия 42 и все связанные события удалены окончательно
- **AND** результат содержит `deleted_count = 1` и `session_id = 42`

#### Scenario: Удаление по имени при дубликатах

- **GIVEN** сессии 42 и 57 с одинаковым `session_name = "trace.trc"`, обе с `use_count = 1`
- **WHEN** выполняется `codebase trc delete --name "trace.trc"`
- **THEN** удалены обе сессии и все связанные события
- **AND** результат содержит `deleted_count = 2` и список ID `[42, 57]`

#### Scenario: Декремент счётчика общей сессии

- **GIVEN** сохранённая сессия 42 с `use_count = 3`
- **WHEN** выполняется `codebase trc delete --session 42`
- **THEN** сессия 42 и все связанные события остаются в БД
- **AND** `use_count` сессии 42 равен 2
- **AND** ответ содержит `deleted = false`, `decremented = true` и `use_count = 2`

#### Scenario: Принудительное удаление общей сессии

- **GIVEN** сохранённая сессия 42 с `use_count = 3`
- **WHEN** выполняется `codebase trc delete --session 42 --hard`
- **THEN** сессия 42 и все связанные события удалены окончательно
- **AND** ответ содержит `deleted = true`

#### Scenario: Удаление по имени со смешанными исходами

- **GIVEN** сессии 42 (`use_count = 2`) и 57 (`use_count = 1`) с одинаковым `session_name = "trace.trc"`
- **WHEN** выполняется `codebase trc delete --name "trace.trc"`
- **THEN** сессия 57 удалена окончательно, сессия 42 декрементирована до 1 и осталась в БД
- **AND** результат содержит список окончательно удалённых `[57]` и список декрементированных `[42]`

#### Scenario: Взаимоисключающие параметры удаления

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** одновременно заданы `--session`/`session_id` и `--name`/`name`
- **THEN** команда завершается ошибкой о взаимоисключающих параметрах

#### Scenario: Удаление по несуществующему имени

- **GIVEN** отсутствует сессия с `session_name = "missing.trc"`
- **WHEN** выполняется `codebase trc delete --name "missing.trc"`
- **THEN** команда завершается ошибкой о ненайденной сессии

### Requirement: Очистка старых сессий

Система SHALL предоставлять команду `trc prune` для удаления старых сессий в одном из двух взаимоисключающих режимов: с оставлением последних N (`--keep-last`) либо с оставлением нетронутыми сессий за последние N часов (`--keep-hours`). Режим `--keep-hours` SHALL удалять сессии с `parsed_at < NOW() - N hours`, значение N MUST быть больше нуля. Batch-обработка (по 50000 записей) для больших объёмов данных и VACUUM ANALYZE после очистки SHALL выполняться в обоих режимах. TRUNCATE-оптимизация SHALL применяться только при `--keep-last 0` без `--keep-hours`. Результат SHALL содержать `deleted_count`, `kept_last`, `kept_hours` и `cutoff`. Очистка SHALL игнорировать счётчик использования: подпадающие под условие удаления сессии удаляются окончательно независимо от `use_count`.

#### Scenario: Очистка с оставлением 5 сессий

- **GIVEN** 20 сохранённых сессий
- **WHEN** выполняется `codebase trc prune --keep-last 5`
- **THEN** удалены 15 старых сессий, оставлены 5 последних

#### Scenario: Очистка с оставлением сессий за последние часы

- **GIVEN** сессии с `parsed_at` 1 час назад, 23 часа назад и 25 часов назад
- **WHEN** выполняется `codebase trc prune --keep-hours 24`
- **THEN** удалена только сессия с `parsed_at` 25 часов назад
- **AND** сессии за последние 24 часа остались нетронутыми
- **AND** результат содержит `deleted_count = 1`, `kept_hours = 24` и `cutoff`

#### Scenario: Очистка игнорирует счётчик использования

- **GIVEN** сессия с `parsed_at` 25 часов назад и `use_count = 5`
- **WHEN** выполняется `codebase trc prune --keep-hours 24`
- **THEN** сессия удалена окончательно вместе со всеми связанными событиями

#### Scenario: Полная очистка

- **GIVEN** несколько сохранённых сессий
- **WHEN** выполняется `codebase trc prune --keep-last 0`
- **THEN** все сессии удалены через TRUNCATE ... RESTART IDENTITY CASCADE

#### Scenario: Взаимоисключающие режимы очистки

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** одновременно заданы `--keep-last`/`keep_last` и `--keep-hours`/`keep_hours`
- **THEN** команда завершается ошибкой о взаимоисключающих параметрах

#### Scenario: Некорректное значение keep-hours

- **GIVEN** запущенный CLI или MCP-сервер
- **WHEN** выполняется prune с `--keep-hours 0` или отрицательным значением
- **THEN** команда завершается ошибкой валидации, не удаляя сессии

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

- `internal/trc/store.go` — `SaveSession`, `LoadEvents`, `ListSessions`, `DeleteSession`, `PruneSessions`, `AcquireSessionByHash` (атомарный find+increment по `(file_hash, parser_version)`), `SetSessionHash` (хеш + `use_count = 1` одним statement), `ReleaseSession` (декремент при `use_count > 1`)
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
- Reuse молчаливый: отдельной пометки о reuse нет; хеш проставляется последним шагом после успешного сохранения (в одном statement с `use_count = 1`) — сессия вставляется до парсинга (нужна для FK `trc_events.session_id`), при ошибке разбора `file_hash` остаётся NULL и сессия в кэш не попадает
- Счётчик использования `use_count` (refcount без идентичности держателей): каждый успешный parse — +1 (включая победителя конфликта параллельных разборов), `delete` без `--hard` при `use_count > 1` декрементирует, `delete --hard` удаляет окончательно, `prune` игнорирует счётчик; legacy-сессии без хеша (`use_count = 0`) ведут себя как раньше; утечка анонимной ссылки (держатель не сделал delete) ограничивается `prune`
- Execution-слой `internal/trcsvc` — общая точка входа для CLI и MCP, устраняющая дублирование оркестрации (см. также домен `mcp-server`). Поведение storage-команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.

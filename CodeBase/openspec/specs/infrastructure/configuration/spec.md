# Configuration

## Purpose

Конфигурация CodeBase через `codebase.toml`: настройки БД, индексатора, query лимитов, RTI/TRC параметров, MCP-сервера, логирования. Все параметры имеют значения по умолчанию.

## Requirements

### Requirement: Конфигурационный файл

Система SHALL использовать `codebase.toml` в качестве конфигурационного файла, искомого рядом с executable (не в текущем рабочем каталоге), с возможностью переопределения пути через флаг `--config`.

#### Scenario: Поиск конфига рядом с executable

- **GIVEN** executable `codebase.exe` в каталоге `D:\Tools\` и `codebase.toml` в том же каталоге
- **WHEN** выполняется `codebase init` без флага `--config`
- **THEN** конфигурация загружена из `D:\Tools\codebase.toml`

#### Scenario: Явный путь к конфигу

- **GIVEN** конфигурационный файл в нестандартном расположении
- **WHEN** выполняется `codebase init --config /path/to/custom.toml`
- **THEN** конфигурация загружена из указанного пути

### Requirement: Настройки БД

Система SHALL поддерживать настройку подключения к PostgreSQL через секцию `[database]` с параметрами host, port, database, user, password, sslmode, connect_timeout, max_open_conns, max_idle_conns, conn_max_lifetime.

#### Scenario: Подключение к БД

- **GIVEN** конфигурация с `[database] host = "localhost", port = 5435, database = "codebase"`
- **WHEN** выполняется любая команда, требующая БД
- **THEN** подключение установлено с указанными параметрами

### Requirement: Настройки индексатора

Система SHALL поддерживать настройку индексатора через секцию `[indexer]` с параметрами parallel, batch_size, batch_insert_size, progress_interval_ms, include_patterns, exclude_patterns.

#### Scenario: Кастомные patterns

- **GIVEN** конфигурация с `include_patterns = ["*.sql", "*.t01"]`
- **WHEN** выполняется `codebase init`
- **THEN** индексируются только `.sql` и `.t01` файлы

### Requirement: Настройки query лимитов

Система SHALL поддерживать настройку лимитов query через секцию `[query]` с параметрами default_limit (по умолчанию 100) и max_limit (по умолчанию 1000).

#### Scenario: Кастомный лимит

- **GIVEN** конфигурация с `[query] default_limit = 50`
- **WHEN** выполняется `query symbol --name MyProc` без флага `--limit`
- **THEN** возвращено не более 50 результатов

### Requirement: Настройки лексического покрытия LSA-запросов

Система SHALL поддерживать настройку минимального лексического покрытия
запроса для semantic-слоя через параметр `lsa_min_query_coverage` в секциях
`[spec]` и `[desc_lsa]` конфигурации `codebase.toml`. Параметр SHALL иметь три
состояния: отсутствует — применяется значение по умолчанию 0.6; явный `0` —
проверка покрытия отключена; значение меньше 0 или больше 1 — ошибка загрузки
конфигурации с указанием имени параметра. Секции настраиваются независимо:
`[spec]` действует на `spec_search`, `[desc_lsa]` — на `desc_search`.

#### Scenario: Параметр отсутствует

- **GIVEN** конфигурация без `lsa_min_query_coverage` в секциях `[spec]` и
  `[desc_lsa]`
- **WHEN** загружается конфигурация
- **THEN** для обоих поисков применяется порог покрытия 0.6

#### Scenario: Явный ноль отключает проверку

- **GIVEN** конфигурация с `[spec] lsa_min_query_coverage = 0`
- **WHEN** выполняется `spec_search` со смешанным запросом
- **THEN** гейт покрытия не применяется, значение `0` не заменяется дефолтом

#### Scenario: Значение вне диапазона отклоняется

- **GIVEN** конфигурация с `[desc_lsa] lsa_min_query_coverage = 1.5`
- **WHEN** загружается конфигурация
- **THEN** загрузка завершается ошибкой с упоминанием `lsa_min_query_coverage`
- **AND** поиск не продолжает работу со значением по умолчанию

#### Scenario: Секции настраиваются независимо

- **GIVEN** конфигурация с `[spec] lsa_min_query_coverage = 0.5` и
  `[desc_lsa] lsa_min_query_coverage = 0.7`
- **WHEN** выполняются `spec_search` и `desc_search` с одинаковым запросом
  покрытия 0.6
- **THEN** `spec_search` гейт не применяет (0.6 не ниже 0.5), а `desc_search`
  подавляет semantic-слой (0.6 ниже 0.7)

### Requirement: Настройки RTI и TRC

Система SHALL поддерживать настройку RTI-анализатора через секцию `[rti]` (slow_threshold_ms, top_slow_count, parse_timeout_sec) и TRC-анализатора через секцию `[trc]` (slow_threshold_ms, max_enrich_workers, min_procs_for_parallel_enrich, parse_timeout_sec). Параметр `parse_timeout_sec` задаёт таймаут в секундах для MCP-инструментов `codebase_rti_parse` и `codebase_trc_parse` соответственно и SHALL иметь три состояния: параметр отсутствует в конфигурации — применяется значение по умолчанию 300; явный `0` — таймаут отключён (вызов без ограничения по времени); явное отрицательное значение — ошибка загрузки конфигурации с указанием имени параметра.

#### Scenario: Кастомный порог медленности RTI

- **GIVEN** конфигурация с `[rti] slow_threshold_ms = 500`
- **WHEN** выполняется `codebase rti slow file.rti` без `--slow-ms`
- **THEN** порог медленности 500 мс

#### Scenario: Кастомный parse-таймаут TRC

- **GIVEN** конфигурация с `[trc] parse_timeout_sec = 600`
- **WHEN** MCP-сервер загружает конфиг и вызывается `codebase_trc_parse`
- **THEN** таймаут парсинга установлен в 600 секунд

#### Scenario: Кастомный parse-таймаут RTI

- **GIVEN** конфигурация с `[rti] parse_timeout_sec = 120`
- **WHEN** MCP-сервер загружает конфиг и вызывается `codebase_rti_parse`
- **THEN** таймаут парсинга установлен в 120 секунд

#### Scenario: Дефолтный parse-таймаут при отсутствии параметра

- **GIVEN** конфигурация без `parse_timeout_sec` в секциях `[rti]` и `[trc]`
- **WHEN** MCP-сервер загружает конфиг
- **THEN** `parse_timeout_sec` = 300 для обеих секций (значение по умолчанию)

#### Scenario: Явный ноль отключает parse-таймаут

- **GIVEN** конфигурация с `[trc] parse_timeout_sec = 0`
- **WHEN** MCP-сервер загружает конфиг
- **THEN** значение `0` сохраняется как явная настройка (не заменяется дефолтом)
- **AND** при вызове `codebase_trc_parse` ограничение времени не применяется

#### Scenario: Отрицательный parse-таймаут отклоняется

- **GIVEN** конфигурация с `[trc] parse_timeout_sec = -5`
- **WHEN** загружается конфигурация
- **THEN** загрузка завершается ошибкой с упоминанием `parse_timeout_sec`
- **AND** сервер/команда не продолжает работу со значением по умолчанию

### Requirement: Настройки MCP

Система SHALL поддерживать настройку MCP-сервера через секцию `[mcp]` с параметрами pagination_chunk_size, pagination_ttl, regexp_cache_max_entries, query_timeout_sec, review_timeout_sec, warmup_enabled и warmup_timeout_sec. Параметры `query_timeout_sec` (по умолчанию 30), `review_timeout_sec` (по умолчанию 120) и `warmup_timeout_sec` (по умолчанию 60) SHALL иметь три состояния: параметр отсутствует — применяется значение по умолчанию; явный `0` — таймаут отключён (для `warmup_timeout_sec` — без ограничения времени probe); явное отрицательное значение — ошибка загрузки конфигурации с указанием имени параметра. Параметр `warmup_enabled` (по умолчанию включён) SHALL иметь три состояния: отсутствует — включён; явный `true` — включён; явный `false` — probe не запускается. `query_timeout_sec` применяется как `context.WithTimeout` в `registerSDKCoreTools` ко всем инструментам по умолчанию, КРОМЕ `codebase_review_sql` (использует `review_timeout_sec`) и `codebase_trc_parse`/`codebase_rti_parse` (используют `parse_timeout_sec` из соответствующих секций `[trc]`/`[rti]`, см. «Настройки RTI и TRC»). `warmup_enabled`/`warmup_timeout_sec` управляют фоновым warm-up probe при старте сервера (см. `mcp-server/mcp-transport-tools`, требование «Warm-up probe при старте сервера»). `pagination_chunk_size`/`pagination_ttl` инициализируют `globalPages` и `SetPaginationTTL` (см. `mcp-pagination`). `regexp_cache_max_entries` применяется через `util.SetRegexpCacheMaxEntries` (см. «Bounded LRU кэш regexp»).

#### Scenario: Кастомный размер чанка

- **GIVEN** конфигурация с `[mcp] pagination_chunk_size = 4000`
- **WHEN** MCP-ответ превышает 4000 байт
- **THEN** ответ разбит на чанки по 4000 байт

#### Scenario: Таймаут query-инструмента

- **GIVEN** конфигурация с `[mcp] query_timeout_sec = 10`
- **WHEN** MCP-инструмент `codebase_query_*` выполняется дольше 10 секунд
- **THEN** вызов прерывается по таймауту

#### Scenario: Кастомный review-таймаут

- **GIVEN** конфигурация с `[mcp] review_timeout_sec = 60`
- **WHEN** вызывается `codebase_review_sql` и обработка длится дольше 60 секунд
- **THEN** вызов прерывается по таймауту

#### Scenario: Query-таймаут не применяется к parse-инструментам

- **GIVEN** конфигурация с `[mcp] query_timeout_sec = 10` и `[trc] parse_timeout_sec = 300`
- **WHEN** вызывается `codebase_trc_parse` и парсинг занимает 60 секунд
- **THEN** вызов не прерывается по query-таймауту 10с, используется parse_timeout_sec = 300с

#### Scenario: Явный ноль отключает query-таймаут

- **GIVEN** конфигурация с `[mcp] query_timeout_sec = 0`
- **WHEN** MCP-сервер загружает конфиг
- **THEN** значение `0` сохраняется как явная настройка (не заменяется дефолтом 30)
- **AND** не-parse инструменты выполняются без ограничения по времени

#### Scenario: Отрицательный query-таймаут отклоняется

- **GIVEN** конфигурация с `[mcp] review_timeout_sec = -1`
- **WHEN** загружается конфигурация
- **THEN** загрузка завершается ошибкой с упоминанием `review_timeout_sec`
- **AND** сервер не стартует со значением по умолчанию

#### Scenario: Warm-up включён по умолчанию

- **GIVEN** конфигурация без параметров `[mcp] warmup_enabled` и `[mcp] warmup_timeout_sec`
- **WHEN** MCP-сервер загружает конфиг
- **THEN** warm-up probe включён с таймаутом 60 секунд

#### Scenario: Warm-up отключается явно

- **GIVEN** конфигурация с `[mcp] warmup_enabled = false`
- **WHEN** MCP-сервер стартует
- **THEN** warm-up probe не запускается
- **AND** поведение сервера идентично состоянию до доработки

#### Scenario: Кастомный таймаут warm-up

- **GIVEN** конфигурация с `[mcp] warmup_timeout_sec = 120`
- **WHEN** warm-up probe выполняется дольше 120 секунд суммарно
- **THEN** оставшиеся пробы прекращаются по таймауту

#### Scenario: Явный ноль отключает таймаут warm-up

- **GIVEN** конфигурация с `[mcp] warmup_timeout_sec = 0`
- **WHEN** warm-up probe выполняется
- **THEN** пробы выполняются без ограничения по времени

#### Scenario: Отрицательный warm-таймаут отклоняется

- **GIVEN** конфигурация с `[mcp] warmup_timeout_sec = -5`
- **WHEN** загружается конфигурация
- **THEN** загрузка завершается ошибкой с упоминанием `warmup_timeout_sec`

### Requirement: Bounded LRU кэш regexp

Система SHALL предоставлять bounded LRU-кэш скомпилированных regexp-ов (`util.CachedRegexp(pattern)`) с настраиваемым лимитом записей через `[mcp] regexp_cache_max_entries` (по умолчанию 2048). При превышении лимита вытесняются наименее недавно использованные записи (LRU eviction через двусвязный список). Применение лимита из конфига выполняется через `util.SetRegexpCacheMaxEntries(max)` при загрузке конфигурации; при уменьшении лимита лишние записи вытесняются немедленно. Кэш потокобезопасен (`sync.Mutex`). Доработка «global-state-eviction» — устраняет неограниченный рост глобального состояния при долгоживущих процессах (MCP-сервер).

#### Scenario: Кэш с лимитом по умолчанию

- **GIVEN** конфигурация без `[mcp] regexp_cache_max_entries`
- **WHEN** загружается конфиг
- **THEN** `util.SetRegexpCacheMaxEntries(2048)` применяется, лимит кэша 2048

#### Scenario: Кастомный лимит кэша

- **GIVEN** конфигурация с `[mcp] regexp_cache_max_entries = 512`
- **WHEN** загружается конфиг и вызывается `util.SetRegexpCacheMaxEntries(512)`
- **THEN** при достижении 512 записей новые вытесняют LRU-записи

#### Scenario: Повторные запросы того же pattern-а не компилируются заново

- **GIVEN** в кэше есть скомпилированный regexp для `pattern = "^MyProc$"`
- **WHEN** повторно вызывается `util.CachedRegexp("^MyProc$")`
- **THEN** запись перемещается в начало LRU (most recently used), новая компиляция не выполняется

### Requirement: Настройки логирования

Система SHALL поддерживать настройку логирования через секцию `[logging]` с параметром command_enabled (по умолчанию true) для логирования CLI-команд в файл `codebase_YYYYMMDD.log`.

#### Scenario: Отключение логирования

- **GIVEN** конфигурация с `[logging] command_enabled = false`
- **WHEN** выполняется любая CLI-команда
- **THEN** логирование в файл не выполняется

### Requirement: Значения по умолчанию

Система SHALL предоставлять значения по умолчанию для всех параметров конфигурации, так что старый `codebase.toml` без новых секций работает без изменений.

#### Scenario: Минимальный конфиг

- **GIVEN** конфигурация только с `root_path` и `[database]`
- **WHEN** выполняется `codebase init`
- **THEN** используются значения по умолчанию для indexer, query, rti, trc, mcp, logging

## Related code

- `internal/config/config.go` — `Config`, `LoadConfig`, `MCPConfig.QueryTimeoutSec`, `MCPConfig.ReviewTimeoutSec`, `MCPConfig.RegexpCacheMaxEntries`, `MCPConfig.PaginationChunkSize`, `MCPConfig.PaginationTTL`, `RTIConfig.ParseTimeoutSec`, `TRCConfig.ParseTimeoutSec`, `SpecConfig.LSAMinQueryCoverage`, `DescLSAConfig.LSAMinQueryCoverage`
- `internal/config/config_test.go` — тесты конфигурации
- `internal/util/regexp_cache.go` — `CachedRegexp`, `SetRegexpCacheMaxEntries`, `globalRegexpCache`, LRU eviction
- `internal/store/db.go` — `FormatDSN`, `quoteDSNValue` (применяется при `NewDB` из `[database]` секции)
- `internal/mcp/server.go` — применение `QueryTimeoutSec`/`ReviewTimeoutSec` в `registerSDKCoreTools`
- `internal/mcp/pagination.go` — `SetPaginationTTL` (применяется в `RunStdio`)
- `cmd/root.go` — загрузка конфига, флаг `--config`

## Notes

- Конфиг ищется рядом с executable, а не в текущем рабочем каталоге
- Все параметры имеют значения по умолчанию — старый конфиг без новых секций работает
- DSN строится через `FormatDSN(cfg)` с экранированием значений через `quoteDSNValue` (см. `database-schema`) — критично для паролей со спецсимволами
- Путь к проекту (`root_path`) может быть переопределён флагом `--path`
- Bounded LRU кэш regexp (`regexp_cache_max_entries`, доработка «global-state-eviction») — единственный настраиваемый bounded-кэш в проекте; предотвращает неограниченный рост глобального состояния при долгоживущем MCP-сервере

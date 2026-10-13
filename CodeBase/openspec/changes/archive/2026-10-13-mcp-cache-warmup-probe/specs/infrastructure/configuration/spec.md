## MODIFIED Requirements

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

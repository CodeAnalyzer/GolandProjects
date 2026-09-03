## MODIFIED Requirements

### Requirement: Настройки RTI и TRC

Система SHALL поддерживать настройку RTI-анализатора через секцию `[rti]` (slow_threshold_ms, top_slow_count, parse_timeout_sec) и TRC-анализатора через секцию `[trc]` (slow_threshold_ms, max_enrich_workers, min_procs_for_parallel_enrich, parse_timeout_sec). Параметр `parse_timeout_sec` (по умолчанию 300) задаёт таймаут в секундах для MCP-инструментов `codebase_rti_parse` и `codebase_trc_parse` соответственно; `0` означает отсутствие таймаута.

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

### Requirement: Настройки MCP

Система SHALL поддерживать настройку MCP-сервера через секцию `[mcp]` с параметрами pagination_chunk_size, pagination_ttl, regexp_cache_max_entries, query_timeout_sec (по умолчанию 30) и review_timeout_sec (по умолчанию 120). `query_timeout_sec` применяется как `context.WithTimeout` в `registerSDKCoreTools` ко всем инструментам по умолчанию, КРОМЕ `codebase_review_sql` (использует `review_timeout_sec`) и `codebase_trc_parse`/`codebase_rti_parse` (используют `parse_timeout_sec` из соответствующих секций `[trc]`/`[rti]`, см. «Настройки RTI и TRC»). `pagination_chunk_size`/`pagination_ttl` инициализируют `globalPages` и `SetPaginationTTL` (см. `mcp-pagination`). `regexp_cache_max_entries` применяется через `util.SetRegexpCacheMaxEntries` (см. «Bounded LRU кэш regexp»).

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

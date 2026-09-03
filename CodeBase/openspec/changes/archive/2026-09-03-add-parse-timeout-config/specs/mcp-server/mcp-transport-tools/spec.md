## MODIFIED Requirements

### Requirement: Таймауты tool-вызовов

Система SHALL применять per-tool таймаут через `context.WithTimeout` поверх контекста запроса. Таймаут определяется по имени инструмента:

- `codebase_review_sql` — `review_timeout_sec` из секции `[mcp]` (по умолчанию 120с)
- `codebase_trc_parse` — `parse_timeout_sec` из секции `[trc]` (по умолчанию 300с)
- `codebase_rti_parse` — `parse_timeout_sec` из секции `[rti]` (по умолчанию 300с)
- Все остальные инструменты — `query_timeout_sec` из секции `[mcp]` (по умолчанию 30с)

Значения читаются из соответствующих секций конфигурации `codebase.toml`. При `timeout = 0` таймаут не применяется (вызов выполняется без ограничения). При истечении таймаута инструмент возвращает structured JSON-ошибку контекста отмены.

#### Scenario: Query-вызов укладывается в таймаут

- **GIVEN** запущенный MCP-сервер и `query_timeout_sec = 30`
- **WHEN** вызывается `codebase_query_symbol` и обработка занимает 2 секунды
- **THEN** инструмент возвращает успешный результат

#### Scenario: Превышение таймаута review

- **GIVEN** запущенный MCP-сервер и `review_timeout_sec = 120`
- **WHEN** вызывается `codebase_review_sql` и обработка занимает дольше 120 секунд
- **THEN** контекст отменён, инструмент возвращает structured JSON-ошибку

#### Scenario: TRC parse укладывается в таймаут

- **GIVEN** запущенный MCP-сервер и `[trc] parse_timeout_sec = 300`
- **WHEN** вызывается `codebase_trc_parse` с файлом 1.4 ГБ и парсинг занимает 90 секунд
- **THEN** инструмент возвращает успешный результат с session_id и total_events

#### Scenario: RTI parse укладывается в таймаут

- **GIVEN** запущенный MCP-сервер и `[rti] parse_timeout_sec = 300`
- **WHEN** вызывается `codebase_rti_parse` с файлом 44 МБ и парсинг занимает 5 секунд
- **THEN** инструмент возвращает успешный результат с session_id и summary

#### Scenario: Превышение таймаута TRC parse

- **GIVEN** запущенный MCP-сервер и `[trc] parse_timeout_sec = 60`
- **WHEN** вызывается `codebase_trc_parse` с файлом 5 ГБ и парсинг занимает дольше 60 секунд
- **THEN** контекст отменён, инструмент возвращает structured JSON-ошибку

#### Scenario: Таймаут отключён

- **GIVEN** конфигурация с `[trc] parse_timeout_sec = 0`
- **WHEN** вызывается `codebase_trc_parse`
- **THEN** `context.WithTimeout` не создаётся, вызов выполняется без ограничения по времени

#### Scenario: Не-parse TRC инструмент использует query-таймаут

- **GIVEN** запущенный MCP-сервер и `query_timeout_sec = 30`, `[trc] parse_timeout_sec = 300`
- **WHEN** вызывается `codebase_trc_summary` с `session_id = 42`
- **THEN** применяется таймаут 30с (query_timeout_sec), не 300с (parse_timeout_sec)

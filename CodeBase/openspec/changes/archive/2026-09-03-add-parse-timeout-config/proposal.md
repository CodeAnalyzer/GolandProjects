## Why

MCP-инструменты `codebase_trc_parse` и `codebase_rti_parse` падают по таймауту при парсинге больших файлов (1+ ГБ .trc, 44+ МБ .rti). Корневая причина: `query_timeout_sec` (30с) задумывался для лёгких query-инструментов, но реально применяется как глобальный дефолт ко всем MCP-вызовам, кроме `codebase_review_sql`. Парсинг 1.4 ГБ .trc занимает ~60-90с и обрывается на 30-й секунде — контекст отменяется, транзакция падает с *"transaction has already been committed or rolled back"*.

## What Changes

- Добавлены параметры `parse_timeout_sec` в секции `[rti]` и `[trc]` конфигурации `codebase.toml` (по умолчанию 300с, `0` = без таймаута)
- `registerSDKCoreTools` в `server.go` использует `switch` для маршрутизации таймаута: `codebase_trc_parse` → `[trc] parse_timeout_sec`, `codebase_rti_parse` → `[rti] parse_timeout_sec`, `codebase_review_sql` → `[mcp] review_timeout_sec`, остальные → `[mcp] query_timeout_sec`
- `RTIConfig` и `TRCConfig` в `config.go` дополнены полем `ParseTimeoutSec int` с дефолтом 300
- `CreateDefault` в `config.go` включает `ParseTimeoutSec: 300` для обеих секций
- Остальные TRC/RTI инструменты (tree, events, slow, errors, summary, details, blog, timeline, client_tree, list, delete, prune) продолжают использовать `query_timeout_sec` — это DB-запросы, 30с достаточно

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `mcp-server/mcp-transport-tools`: Requirement «Таймауты tool-вызовов» расширяется — добавляются отдельные таймауты для `codebase_trc_parse` и `codebase_rti_parse`, источаемые из `[trc]` и `[rti]` секций конфига
- `infrastructure/configuration`: Requirement «Настройки RTI и TRC» расширяется — добавляется параметр `parse_timeout_sec` в обе секции; Requirement «Настройки MCP» уточняет, что `query_timeout_sec` более не применяется к parse-инструментам

## Impact

- `internal/config/config.go` — `RTIConfig.ParseTimeoutSec`, `TRCConfig.ParseTimeoutSec`, дефолты в `Load()` и `CreateDefault()`
- `internal/mcp/server.go` — `registerSDKCoreTools`: замена `if` на `switch` для маршрутизации таймаута
- `codebase.toml` — новые параметры в `[rti]` и `[trc]` секциях
- Обратно совместимо: старый `codebase.toml` без новых параметров получает дефолт 300с
- CLI путь (`codebase trc parse` / `codebase rti parse`) не затронут — использует `context.Background()` без таймаута

# MCP Transport and Tools

## Purpose

MCP-сервер (Model Context Protocol) поверх stdio JSON-RPC транспорта: регистрация инструментов (tools), диспетчеризация вызовов, возврат доменных данных. Инструменты реализованы поверх внутреннего сервисного слоя, без вызова Cobra-команд.

## Requirements

### Requirement: Stdio JSON-RPC транспорт

Система SHALL запускать MCP-сервер через `codebase mcp` с использованием stdio для JSON-RPC транспорта, зарезервировав stdout для протокола (без баннера и лишнего текстового вывода).

#### Scenario: Запуск MCP-сервера

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp`
- **THEN** MCP-сервер запускается в stdio-режиме и обрабатывает JSON-RPC запросы

### Requirement: Базовые инструменты

Система SHALL предоставлять базовые MCP-инструменты: `codebase_ping` (проверка живости), `codebase_health` (проверка готовности БД и индекса), `codebase_stats` (статистика индекса).

#### Scenario: Ping

- **GIVEN** запущенный MCP-сервер
- **WHEN** вызывается `codebase_ping`
- **THEN** возвращён ответ, подтверждающий живость сервера

#### Scenario: Health check

- **GIVEN** запущенный MCP-сервер с подключённой БД
- **WHEN** вызывается `codebase_health`
- **THEN** возвращён статус готовности: config, database, schema, index readiness

### Requirement: Query инструменты

Система SHALL предоставлять MCP-инструменты `codebase_query_*` для всех query-подкоманд CLI: `codebase_query_symbol`, `codebase_query_table`, `codebase_query_table_schema`, `codebase_query_table_index`, `codebase_query_procedure`, `codebase_query_callers`, `codebase_query_method`, `codebase_query_methods`, `codebase_query_sql_fragment`, `codebase_query_form`, `codebase_query_form_component`, `codebase_query_report_form`, `codebase_query_report_field`, `codebase_query_report_param`, `codebase_query_vb_function`, `codebase_query_js_function`, `codebase_query_smf_instrument`, `codebase_query_smf_type`, `codebase_query_api_contract`, `codebase_query_api_table`, `codebase_query_api_param`, `codebase_query_api_table_index`, `codebase_query_api_impl`, `codebase_query_api_publishers`, `codebase_query_api_consumers`, `codebase_query_relations`, `codebase_query_inspect`, `codebase_query_retcode`.

#### Scenario: Query symbol через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированный проект
- **WHEN** вызывается `codebase_query_symbol` с `name = "MyProc"`
- **THEN** возвращены данные символа в формате чистых доменных данных (без CLI envelope)

### Requirement: RTI инструменты

Система SHALL предоставлять MCP-инструменты для RTI-анализа: `codebase_rti_parse`, `codebase_rti_list`, `codebase_rti_summary`, `codebase_rti_tree`, `codebase_rti_errors`, `codebase_rti_slow`, `codebase_rti_details`, `codebase_rti_blog`, `codebase_rti_client_tree`, `codebase_rti_timeline`, `codebase_rti_delete`, `codebase_rti_prune`.

#### Scenario: RTI summary через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая RTI-сессия с id 42
- **WHEN** вызывается `codebase_rti_summary` с `session_id = 42`
- **THEN** возвращена сводка сессии в формате доменных данных

### Requirement: TRC инструменты

Система SHALL предоставлять MCP-инструменты для TRC-анализа: `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events`, `codebase_trc_procedures`, `codebase_trc_tree`, `codebase_trc_errors`, `codebase_trc_slow`, `codebase_trc_delete`, `codebase_trc_prune`.

#### Scenario: TRC procedures через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** вызывается `codebase_trc_procedures` с `session_id = 42`
- **THEN** возвращена агрегация по процедурам с enrichment

### Requirement: Review инструмент

Система SHALL предоставлять MCP-инструмент `codebase_review_sql` для статического анализа SQL-файла с настраиваемым набором правил.

#### Scenario: Review через MCP

- **GIVEN** запущенный MCP-сервер и SQL-файл
- **WHEN** вызывается `codebase_review_sql` с `file_path` и опциональными `rules`
- **THEN** возвращены findings в формате доменных данных

### Requirement: Контракт формата MCP vs CLI

Система SHALL возвращать через MCP-инструменты чистые доменные данные (например `{ "count": N, "items": [...] }`), а через CLI — JSON envelope (`success`, `format_version`, `command`, `meta`, ...).

#### Scenario: MCP возвращает чистые данные

- **GIVEN** запущенный MCP-сервер
- **WHEN** вызывается любой MCP-инструмент
- **THEN** ответ не содержит `success`, `format_version`, `command` — только доменные данные

### Requirement: Обработка ошибок в MCP-инструментах

Система SHALL возвращать ошибки MCP-инструментов в structured JSON формате, без panic, с подавлением banner/output noise в stdout.

#### Scenario: БД недоступна

- **GIVEN** запущенный MCP-сервер, БД недоступна
- **WHEN** вызывается `codebase_query_symbol`
- **THEN** возвращена ошибка в JSON формате без panic
- **AND** stdout не содержит лишнего текстового вывода

#### Scenario: Неверные параметры

- **GIVEN** запущенный MCP-сервер
- **WHEN** вызывается `codebase_rti_details` без обязательного параметра `procedure`
- **THEN** возвращена ошибка с описанием недостающего параметра

#### Scenario: Файл не найден (RTI/TRC parse)

- **GIVEN** запущенный MCP-сервер
- **WHEN** вызывается `codebase_rti_parse` с несуществующим `file_path`
- **THEN** возвращена ошибка с описанием отсутствия файла

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

### Requirement: Логирование tool-вызовов

Система SHALL логировать каждый tool-вызов через `logMCPToolCall`, если при запуске MCP-сервера передан non-nil `*log.Logger`. Запись содержит: имя инструмента (`tool`), sanitized аргументы (`args`, с приватными полями типа `text`/`sql` замаскированными), длительность (`duration` и `duration_ms`), статус (`success`/`error`) и текст ошибки (`error`). Если logger равен `nil` — логирование отключено.

#### Scenario: Успешный вызов залогирован

- **GIVEN** MCP-сервер, запущенный с переданным logger
- **WHEN** выполняется успешный tool-вызов
- **THEN** в логе запись вида `tool=codebase_query_symbol args=name:MyProc duration=12ms duration_ms=12 status=success error=""`

#### Scenario: Ошибка залогирована

- **GIVEN** MCP-сервер, запущенный с переданным logger
- **WHEN** tool-вызов завершается ошибкой
- **THEN** в логе запись со `status=error` и текстом ошибки (whitespace-нормализованным через `strings.Fields`)

#### Scenario: Логирование отключено

- **GIVEN** MCP-сервер, запущенный без logger (`logger = nil`)
- **WHEN** выполняется любой tool-вызов
- **THEN** логирование не производится

### Requirement: Context propagation в tool handler

Система SHALL передавать `context.Context` в каждый зарегистрированный tool-handler (`tool.Handler(ctx, args)`), что позволяет сервисному слою (`querysvc`, `reviewsvc`, `rtisvc`, `trcsvc`) реагировать на отмену: ctx-таймаут tool-а, shutdown MCP-сервера, клиентскую отмену. Контекст таймаута (см. «Таймауты tool-вызовов») оборачивает исходный контекст SDK-запроса. `systemsvc` (`ExecuteHealth`, `ExecuteStats`) на текущий момент использует `context.Background()` и не реагирует на отмену ctx tool-а — это известное ограничение (health/stats — дешёвые синхронные операции).

#### Scenario: Сервисный слой получает контекст

- **GIVEN** MCP-сервер и tool `codebase_query_symbol`
- **WHEN** выполняется вызов
- **THEN** `querysvc.Execute*` получает `ctx`, производный от SDK-контекста и (если задан) таймаута
- **AND** при отмене ctx сервисный слой прекращает длительные операции

### Requirement: Профильная регистрация инструментов

Система SHALL поддерживать необязательный флаг `--profile` на команде `codebase mcp`. Без флага регистрируются все доступные инструменты (текущее поведение). При указании `--profile=<name>` регистрируется только подмножество инструментов, релевантное профилю, плюс базовые (`codebase_ping`, `codebase_health`, `codebase_stats`, `codebase_read_more`).

#### Scenario: Запуск без профиля — все инструменты

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp` без флага `--profile`
- **THEN** MCP-сервер регистрирует все 55 инструментов
- **AND** `tools/list` response идентичен текущему поведению

#### Scenario: Запуск с профилем rti

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=rti`
- **THEN** MCP-сервер регистрирует только базовые + RTI инструменты (~17)
- **AND** `tools/list` response не содержит query, trc, review инструменты

#### Scenario: Запуск с профилем query

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=query`
- **THEN** MCP-сервер регистрирует только базовые + query инструменты (~30)
- **AND** `tools/list` response не содержит rti, trc, review инструменты

#### Scenario: Запуск с профилем trc

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=trc`
- **THEN** MCP-сервер регистрирует только базовые + TRC инструменты (~14)
- **AND** `tools/list` response не содержит query, rti, review инструменты

#### Scenario: Запуск с профилем review

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=review`
- **THEN** MCP-сервер регистрирует только базовые + review инструмент (~5)
- **AND** `tools/list` response не содержит query, rti, trc инструменты

#### Scenario: Неизвестный профиль

- **GIVEN** сконфигурированный проект с БД
- **WHEN** выполняется `codebase mcp --profile=unknown`
- **THEN** возвращается ошибка с перечислением доступных профилей: `query`, `rti`, `trc`, `review`
- **AND** MCP-сервер не запускается

### Requirement: Профиль в логировании tool-вызовов

Система SHALL добавлять поле `profile` в каждую запись лога `logMCPToolCall`. При запуске без `--profile` пишется `profile=all`. При запуске с профилем пишется `profile=<name>`. Это позволяет отличить записи от разных MCP-серверов при одновременном запуске нескольких профилей.

#### Scenario: Лог с профилем

- **GIVEN** MCP-сервер, запущенный с `--profile=rti` и переданным logger
- **WHEN** выполняется tool-вызов `codebase_rti_parse`
- **THEN** в логе запись содержит `profile=rti` вместе с `tool`, `args`, `duration`, `status`

#### Scenario: Лог без профиля

- **GIVEN** MCP-сервер, запущенный без `--profile` и с переданным logger
- **WHEN** выполняется tool-вызов `codebase_query_symbol`
- **THEN** в логе запись содержит `profile=all`

### Requirement: Контракт outputSchema и structuredContent

Система SHALL регистрировать MCP-инструменты без декларации `outputSchema`: результат каждого инструмента — text-only (`content` с одним `TextContent`; для ошибок дополнительно `isError = true`). Декларация `outputSchema` без возвращаемого `structuredContent` запрещена (нарушение спецификации MCP 2025-06-18: *"If a tool declares an output schema, the tool MUST return structuredContent that validates against the schema"*). Если в будущем инструмент станет декларировать `outputSchema`, он MUST возвращать `structuredContent`, валидируемый по объявленной схеме, во всех результатах (включая isError-результаты).

#### Scenario: tools/list без outputSchema

- **GIVEN** запущенный MCP-сервер с любым профилем (или без профиля)
- **WHEN** клиент запрашивает `tools/list`
- **THEN** ни один зарегистрированный инструмент не содержит поле `outputSchema`
- **AND** каждый инструмент описывает только `name`, `description`, `inputSchema`

#### Scenario: Text-only результат принимается валидирующим клиентом

- **GIVEN** MCP-клиент, валидирующий контракт outputSchema/structuredContent (например opencode)
- **WHEN** вызывается любой инструмент, например `codebase_ping`
- **THEN** результат содержит `content` (один `TextContent`) и не содержит `structuredContent`
- **AND** клиент принимает результат без ошибки JSON-RPC `-32600`

#### Scenario: Ошибка инструмента без structuredContent

- **GIVEN** запущенный MCP-сервер
- **WHEN** tool-вызов завершается ошибкой (например `codebase_rti_parse` с несуществующим `file_path`)
- **THEN** результат содержит `content` (текст ошибки) и `isError = true`, без `structuredContent`
- **AND** клиент принимает isError-результат без протокольной ошибки

#### Scenario: Пагинированный и raw-ответ остаются text-only

- **GIVEN** запущенный MCP-сервер и ответ инструмента, превышающий лимит пагинации
- **WHEN** вызывается инструмент (например `codebase_query_callers` с большим результатом) или `codebase_read_more`
- **THEN** результат содержит текстовый чанк в `content` без `structuredContent`
- **AND** инструмент не декларирует `outputSchema`

## Related code

- `internal/mcp/server.go` — `RunStdio`, `registerSDKCoreTools` (обёртка handler-а с таймаутом и логированием), `logMCPToolCall`, `formatToolArgs`, `sdkToolPagedResult`, `decodeSDKToolArgs`
- `internal/mcp/registry.go` — registry всех MCP tools (query, RTI, TRC, review, health, stats), `toolHandler(ctx, args)` сигнатура, `buildToolRegistryForProfile`, `profileToolSets`, `ValidProfiles`
- `internal/mcp/tools.go` — вспомогательные типы для tool definitions
- `internal/mcp/types.go` — внутренние типы MCP
- `cmd/mcp.go` — CLI command `mcp`, конструирование logger для `RunStdio`
- `internal/querysvc/` — runtime для query (CLI + MCP), потребляет ctx
- `internal/systemsvc/` — runtime для health/stats (CLI + MCP), потребляет ctx
- `internal/reviewsvc/` — runtime для review (CLI + MCP), потребляет ctx
- `internal/rtisvc/` — runtime для RTI (CLI + MCP), потребляет ctx
- `internal/trcsvc/` — runtime для TRC (CLI + MCP), потребляет ctx
- `internal/config/config.go` — `MCPConfig.QueryTimeoutSec`, `MCPConfig.ReviewTimeoutSec`, `RTIConfig.ParseTimeoutSec`, `TRCConfig.ParseTimeoutSec`

## Notes

- MCP-сервер использует `github.com/modelcontextprotocol/go-sdk/mcp` SDK
- stdout зарезервирован под JSON-RPC транспорт — без баннера; logger пишет в stderr
- Инструменты реализованы поверх сервисного слоя (querysvc, systemsvc, reviewsvc, rtisvc, trcsvc), без вызова Cobra-команд — это устраняет дублирование оркестрации между CLI и MCP
- MCP tools возвращают чистые доменные данные; CLI сохраняет JSON envelope
- Ошибки БД, неверные параметры и отсутствующие файлы возвращаются как structured JSON errors, не как panic
- `internal/mcp/handlers.go` — пустой файл (только `package mcp`), не используется (мёртвый код, не удалялся)

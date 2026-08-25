# MCP Tool Profiles — Delta

## ADDED Requirements

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

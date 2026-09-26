## MODIFIED Requirements

### Requirement: Логирование tool-вызовов

Система SHALL логировать каждый tool-вызов через `logMCPToolCall`, если при запуске MCP-сервера передан non-nil `*log.Logger`. Запись содержит: имя инструмента (`tool`), sanitized аргументы (`args` — все аргументы вызова, отсортированные по имени, в формате `key:value` через запятую; значения полей `text` и `sql` заменены маской), длительность (`duration` и `duration_ms`), статус (`success`/`error`) и текст ошибки (`error`). Если logger равен `nil` — логирование отключено.

#### Scenario: Успешный вызов залогирован

- **GIVEN** MCP-сервер, запущенный с переданным logger
- **WHEN** выполняется успешный вызов `codebase_query_symbol` с аргументами `name=MyProc`, `type=sql_procedure`, `like=false`
- **THEN** в логе запись вида `tool=codebase_query_symbol args=like:false,name:MyProc,type:sql_procedure duration=12ms duration_ms=12 status=success error=""`

#### Scenario: Приватные аргументы замаскированы

- **GIVEN** MCP-сервер, запущенный с переданным logger
- **WHEN** tool-вызов содержит аргумент `text` (например, фрагмент SQL)
- **THEN** в логе значение `text` заменено маской (`text:***`)
- **AND** фактическое содержимое аргумента в лог не попадает

#### Scenario: Ошибка залогирована

- **GIVEN** MCP-сервер, запущенный с переданным logger
- **WHEN** tool-вызов завершается ошибкой
- **THEN** в логе запись со `status=error` и текстом ошибки (whitespace-нормализованным через `strings.Fields`)

#### Scenario: Логирование отключено

- **GIVEN** MCP-сервер, запущенный без logger (`logger = nil`)
- **WHEN** выполняется любой tool-вызов
- **THEN** логирование не производится

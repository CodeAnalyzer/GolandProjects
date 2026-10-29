## MODIFIED Requirements

### Requirement: Логирование CLI-команд

Система SHALL логировать все CLI-команды в файл `codebase_YYYYMMDD.log` (один файл на день) с информацией: started_at, command, duration, status, error. Логирование включено по умолчанию. Статус и текст ошибки SHALL отражать фактический результат выполнения команды во всех режимах вывода, включая machine-readable (`--json`, `--ndjson`, `--summary`), где ошибка дополнительно конвертируется в структурированный envelope: отказ команды SHALL фиксироваться записью `status=error` с однострочным текстом ошибки, успешное выполнение — записью `status=success`.

#### Scenario: Логирование команды

- **GIVEN** включённое логирование (`logging.command_enabled = true`)
- **WHEN** выполняется `codebase init`
- **THEN** в файл `codebase_YYYYMMDD.log` записана информация о команде: started_at, command, duration, status

#### Scenario: Ошибка в machine-readable режиме фиксируется в логе

- **GIVEN** включённое логирование и режим `--json`
- **WHEN** команда завершается ошибкой (например, `pq: deadlock detected` при запросе к БД)
- **THEN** в лог записана `status=error` с однострочным текстом исходной ошибки
- **AND** запись `status=success` с пустым текстом ошибки не создана

## ADDED Requirements

### Requirement: Код завершения при ошибке в machine-readable режиме

Команда SHALL завершаться с ненулевым кодом возврата при ошибке независимо от режима вывода. В machine-readable режиме (`--json`, `--ndjson`, `--summary`) ошибка SHALL доставляться структурированным envelope (`success: false`), а текст ошибки SHALL NOT дублироваться в stderr. Успешное выполнение SHALL возвращать нулевой код возврата во всех режимах.

#### Scenario: Ошибка в JSON-режиме

- **GIVEN** выполняется команда в режиме `--json`
- **WHEN** команда завершается ошибкой (например, ошибка подключения к БД)
- **THEN** код возврата процесса ненулевой
- **AND** stdout содержит валидный envelope с `success: false` и описанием ошибки
- **AND** текст ошибки не продублирован в stderr

#### Scenario: Успех в JSON-режиме

- **GIVEN** выполняется команда в режиме `--json`
- **WHEN** команда завершается успешно
- **THEN** код возврата процесса нулевой
- **AND** stdout содержит валидный envelope с `success: true`

#### Scenario: Ошибка в текстовом режиме

- **GIVEN** выполняется команда без machine-readable флагов
- **WHEN** команда завершается ошибкой
- **THEN** код возврата процесса ненулевой
- **AND** сообщение об ошибке выведено пользователю

## Related code

- `cmd/root.go` — `Execute`: возврат исходной ошибки в machine-readable ветках после записи error-envelope; deferred-логгер `logCommandExecution`
- `main.go` — `os.Exit(1)` при ненулевой ошибке
- `cmd/query_response.go` (и аналоги `writeStatsErrorResponse`/`writeHealthErrorResponse`/`writeReviewErrorResponse`) — structured error envelope

## Notes

- Требование «Логирование CLI-команд» существует в этой capability; изменение фиксирует контракт «лог отражает фактический результат» для machine-readable режимов (дефект из `Bug-reports/BUG-query-deadlock-init-schema-ddl-20261026.md`, п. 4 — deadlock выглядел как `status=success`).
- Смена контракта exit code для machine-readable режима — **BREAKING** для скриптов, полагавшихся на нулевой код возврата при ошибке; формат envelope не меняется.

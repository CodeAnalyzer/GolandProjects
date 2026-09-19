# MCP Transport and Tools

## Purpose

MCP-сервер (Model Context Protocol) поверх stdio JSON-RPC транспорта: регистрация инструментов (tools), диспетчеризация вызовов, возврат доменных данных. Инструменты реализованы поверх внутреннего сервисного слоя, без вызова Cobra-команд.

## Requirements

### Requirement: Stdio JSON-RPC транспорт

Система SHALL запускать MCP-сервер через `codebase mcp` с использованием stdio для JSON-RPC транспорта, зарезервировав stdout для протокола (без баннера и лишнего текстового вывода). До запуска транспорта MCP startup MUST выполнить read-only проверку совместимости схемы и MUST NOT создавать, изменять или удалять объекты БД. При отсутствующей или несовместимой схеме процесс SHALL завершиться с диагностикой в журнале команд, содержащей ожидаемую и обнаруженную версию, когда она доступна, и указание выполнить предусмотренную CLI-команду инициализации или обновления.

#### Scenario: Запуск MCP-сервера

- **GIVEN** сконфигурированный проект с доступной БД и совместимой версией схемы
- **WHEN** выполняется `codebase mcp`
- **THEN** MCP-сервер запускается в stdio-режиме и обрабатывает JSON-RPC запросы
- **AND** startup не выполняет DDL

#### Scenario: Параллельный запуск профильных MCP-серверов

- **GIVEN** схема БД совместима с текущей версией CodeBase
- **WHEN** MCP-клиент одновременно запускает профили `query`, `rti`, `trc` и `review`
- **THEN** все профильные процессы успешно проходят read-only проверку
- **AND** все процессы запускают stdio-транспорт без DDL-конфликтов

#### Scenario: Запуск MCP-сервера без подготовленной схемы

- **GIVEN** БД доступна, но схема CodeBase отсутствует
- **WHEN** выполняется `codebase mcp`
- **THEN** MCP-процесс завершается до начала обслуживания инструментов
- **AND** журнал команд содержит причину и указание выполнить CLI-команду инициализации
- **AND** startup не создаёт схему автоматически

#### Scenario: Запуск MCP-сервера с устаревшей схемой

- **GIVEN** БД содержит схему без текущего маркера совместимости
- **WHEN** выполняется `codebase mcp`
- **THEN** MCP-процесс завершается до начала обслуживания инструментов
- **AND** журнал команд содержит ожидаемую и обнаруженную версию, когда она доступна, и указание выполнить CLI-команду обновления
- **AND** startup не применяет миграции автоматически

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

Система SHALL предоставлять MCP-инструменты для TRC-анализа: `codebase_trc_parse`, `codebase_trc_list`, `codebase_trc_summary`, `codebase_trc_events`, `codebase_trc_procedures`, `codebase_trc_compare_procedures`, `codebase_trc_spids`, `codebase_trc_tree`, `codebase_trc_errors`, `codebase_trc_slow`, `codebase_trc_delete`, `codebase_trc_prune`. Каждый TRC-обработчик SHALL возвращать клиенту ошибку разбора переданного optional-аргумента вместо подстановки его нулевого значения.

`codebase_trc_events` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `time_from`, `time_to` (RFC3339), `min_duration_ms`, `after_id`, `format=full|short` и SHALL возвращать полное `filtered_count` до `limit`, фактический `returned_count`, `has_more` и `next_after_id`. Существующие скалярные `spid` и `event_name` SHALL сохраняться у `codebase_trc_events` только как legacy-алиасы и нормализоваться в массивы из одного элемента; одновременная передача legacy-алиаса и соответствующего массива SHALL возвращать ошибку.

`codebase_trc_procedures` SHALL принимать параметры `spids` (integer[]), `event_names` (string[]), `top`, `sort_by` и `group_by_spid`; скалярные alias-параметры для него SHALL NOT вводиться. `codebase_trc_procedures` SHALL возвращать enrichment и по умолчанию (без `event_names`) агрегировать только завершённые вызовы `SP:Completed`.

`codebase_trc_compare_procedures` SHALL принимать параметры `focus_spid`, `compare_spids` (integer[]), `event_names` (string[]), `top` (default 20, max 100) и `sort_by` по контракту сравнения focus/peer SPID; скалярные alias-параметры SHALL NOT вводиться. `codebase_trc_spids` SHALL принимать параметры `spids` (integer[]), `time_from`, `time_to` (RFC3339), `sort_by` и `limit`. Оба инструмента SHALL возвращать предупреждения `warnings` по своим контрактам, включая предупреждение о недоступности метрик длительности при `max(duration_ms) = 0` по сессии.

Array-параметры SHALL приниматься только массивами соответствующего типа со строгой валидацией каждого элемента: ошибка SHALL содержать имя аргумента и индекс некорректного элемента; пустые строки, дробные значения и переполнение SHALL NOT игнорироваться.

#### Scenario: TRC procedures через MCP

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** вызывается `codebase_trc_procedures` с `session_id = 42`
- **THEN** возвращена агрегация завершённых вызовов процедур с enrichment

#### Scenario: Некорректный optional-аргумент TRC-инструмента

- **GIVEN** запущенный MCP-сервер
- **WHEN** TRC-инструмент вызывается с optional-аргументом неверного типа
- **THEN** инструмент возвращает ошибку с именем некорректного аргумента
- **AND** обработчик не выполняет запрос с нулевым значением вместо переданного аргумента

#### Scenario: Счётчики ограниченной выдачи TRC events

- **GIVEN** фильтру TRC events соответствуют 1500 событий
- **WHEN** вызывается `codebase_trc_events` с `limit=1000`
- **THEN** ответ содержит `filtered_count=1500` и `returned_count=1000`

#### Scenario: Нормализация legacy-алиасов events

- **GIVEN** запущенный MCP-сервер и сохранённая TRC-сессия
- **WHEN** `codebase_trc_events` вызывается с `spid=728`
- **THEN** фильтрация эквивалентна вызову с `spids=[728]`

#### Scenario: Конфликт scalar и array параметров

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается одновременно с `event_name` и `event_names`
- **THEN** возвращена ошибка с предложением использовать `event_names`

#### Scenario: Некорректный элемент массива SPID

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_events` вызывается с `spids=[728, -1]`
- **THEN** возвращена ошибка, содержащая имя аргумента `spids` и индекс некорректного элемента
- **AND** запрос к данным не выполняется

#### Scenario: Массивы и время в procedures через MCP

- **GIVEN** сохранённая TRC-сессия содержит события SPID 728 и 700
- **WHEN** вызывается `codebase_trc_procedures` с `session_id`, `spids=[728, 700]` и `group_by_spid=true`
- **THEN** возвращены агрегаты `(spid, procedure)` для указанных SPID

#### Scenario: Сравнение процедур через MCP

- **GIVEN** сохранённая TRC-сессия, focus SPID 728 и peers 700/179 содержат вызовы общих процедур
- **WHEN** вызывается `codebase_trc_compare_procedures` с `session_id`, `focus_spid=728` и `compare_spids=[700, 179]`
- **THEN** возвращён Top-N процедур по статистике focus с метриками каждого peer, `peer_combined` и ratios
- **AND** ответ содержит warnings о completed-only и elapsed ≠ wall-clock

#### Scenario: Сводка SPID через MCP

- **GIVEN** сохранённая TRC-сессия содержит события нескольких SPID
- **WHEN** вызывается `codebase_trc_spids` с `session_id` и `sort_by=event_count`
- **THEN** возвращена сводка по каждому SPID без выгрузки сырых событий
- **AND** порядок соответствует `sort_by`

#### Scenario: Валидация compare-параметров через MCP

- **GIVEN** запущенный MCP-сервер
- **WHEN** `codebase_trc_compare_procedures` вызывается с `compare_spids=[728]` и `focus_spid=728`
- **THEN** возвращена ошибка: peer-список опустел после удаления focus
- **AND** запрос к данным не выполняется

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

### Requirement: Spec инструменты

Система SHALL предоставлять MCP-инструменты для навигации по спекам: `codebase_query_spec_search` (двухслойный полнотекстовый поиск — см. `query/spec-search`), `codebase_query_spec_by_code` (спеки по имени код-сущности), `codebase_query_spec_deps` (граф зависимостей capability), `codebase_query_spec_usecase` (usecase-слой с involved capabilities), `codebase_query_spec_coverage` (покрытие кода спеками и пробелы), `codebase_query_spec_history` (история capability по changes). Инструменты реализованы поверх сервисного слоя, возвращают чистые доменные данные (без CLI envelope), наследуют общий per-tool таймаут `query_timeout_sec` и существующую пагинацию больших ответов.

#### Scenario: Поиск спеки через MCP

- **GIVEN** запущенный MCP-сервер и проиндексированные спеки финпродуктов
- **WHEN** вызывается `codebase_query_spec_search` с `text = "отключение SMS-уведомлений"`
- **THEN** возвращены точные и семантические совпадения в доменном формате (без envelope) с указанием продукта и границ строк

#### Scenario: Навигация от кода к требованию через MCP

- **GIVEN** запущенный MCP-сервер, проиндексированные спеки и код
- **WHEN** вызывается `codebase_query_spec_by_code` с `name = "API_DepoAccount_MassInsert"`
- **THEN** возвращены capability и требования, ссылающиеся на процедуру, с текстами строк упоминания

#### Scenario: Пагинация большого ответа

- **GIVEN** запрос spec_search, чей результат превышает лимит чанка MCP-ответа
- **WHEN** инструмент формирует ответ
- **THEN** применён существующий механизм пагинации (continuation_id + `codebase_read_more`), без изменений механизма

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

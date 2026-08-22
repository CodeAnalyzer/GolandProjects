# RTI Client Events

## Purpose

Парсинг клиентских событий thick client d5nt из RTI-логов: SQL blocks, recordset open, connection, BPL load, errors, memory. Enrichment из индекса (PAS-методы, DFM-формы, SQL-фрагменты), дерево клиентских событий по PID, единый хронологический timeline серверных и клиентских событий, связывание клиентских событий с серверными вызовами.

## Requirements

### Requirement: Парсинг клиентских событий

Система SHALL парсить клиентские события thick client d5nt из RTI-логов с указанием kind (sql_block, recordset_open, connection, bpl_list, error, memory, generic), timestamp, class/method и enrichment-данными.

#### Scenario: SQL block событие

- **GIVEN** RTI-файл с клиентским SQL block событием (выполнение SQL-запроса из thick client)
- **WHEN** выполняется парсинг
- **THEN** событие сохранено в `rti_client_events` с kind `sql_block`, timestamp и текстом SQL

#### Scenario: Error событие

- **GIVEN** RTI-файл с клиентской ошибкой (exception в thick client)
- **WHEN** выполняется парсинг
- **THEN** событие сохранено с kind `error`, текстом ошибки и class/method

### Requirement: Enrichment клиентских событий

Система SHALL обогащать клиентские события данными из индекса: сопоставление SQL-фрагментов с query_fragments, сопоставление class/method с PAS-методами и DFM-формами.

#### Scenario: Enrichment SQL block

- **GIVEN** клиентский SQL block с текстом `SELECT * FROM tContract WHERE ID = @ID`
- **WHEN** выполняется enrichment
- **THEN** результат содержит ссылку на query_fragment из индекса (если найден)

#### Scenario: Enrichment class/method

- **GIVEN** клиентское событие с class `TMyForm` и method `Button1Click`
- **WHEN** выполняется enrichment
- **THEN** результат содержит ссылку на PAS-метод `Button1Click` класса `TMyForm` (если найден)

### Requirement: Дерево клиентских событий

Система SHALL предоставлять команду `rti client-tree` для построения дерева клиентских событий, сгруппированных по PID, с enrichment-данными из индекса.

#### Scenario: Client tree по PID

- **GIVEN** RTI-файл с клиентскими событиями от PID 1234
- **WHEN** выполняется `codebase rti client-tree file.rti --pid 1234`
- **THEN** возвращено дерево событий для PID 1234 с enrichment-данными

### Requirement: Единый timeline

Система SHALL предоставлять команду `rti timeline` для построения единого хронологического timeline серверных вызовов и клиентских событий, отсортированных по timestamp. CLI поддерживает фильтры `--session`, `--limit`, `--pid`. Фильтрация по времени (`time_from`/`time_to` в RFC3339) доступна через MCP-инструмент `codebase_rti_timeline`.

#### Scenario: Timeline через CLI с фильтром по PID

- **GIVEN** RTI-файл с серверными и клиентскими событиями от PID 1234
- **WHEN** выполняется `codebase rti timeline file.rti --pid 1234`
- **THEN** возвращён timeline событий для PID 1234, с тегами [server] и [client]

#### Scenario: Timeline через MCP с фильтром по времени

- **GIVEN** RTI-файл с серверными и клиентскими событиями
- **WHEN** вызывается MCP-инструмент `codebase_rti_timeline` с `time_from = "2026-01-01T10:00:00+03:00"`
- **THEN** возвращён timeline событий после указанного времени, с тегами [server] и [client]

### Requirement: Связывание клиентских событий с серверными вызовами

Система SHALL связывать клиентские события с серверными вызовами через поле `server_call_id` в `rti_client_events`, когда клиентское событие инициировало серверный вызов.

#### Scenario: Связывание SQL block с серверным вызовом

- **GIVEN** клиентский SQL block, инициировавший серверный вызов процедуры
- **WHEN** выполняется связывание
- **THEN** в `rti_client_events` установлено `server_call_id` ссылающееся на соответствующий `rti_calls` record

### Requirement: Расширенные виды клиентских событий

Система SHALL помимо основных видов (`sql_block`, `recordset_open`, `connection`, `bpl_list`, `error`, `memory`, `generic`) извлекать дополнительные: `trancount` (контроль транзакций), `memory_usage` (детальные метрики памяти), `recordset_open` (с детализацией колонок/строк). Все виды определяются в `parser_client.go`.

#### Scenario: Trancount событие

- **GIVEN** RTI-файл с клиентским событием `trancount` (открытие/закрытие транзакции в thick client)
- **WHEN** выполняется парсинг
- **THEN** событие сохранено с kind `trancount` и указанием счётчика транзакций

#### Scenario: Memory_usage событие

- **GIVEN** RTI-файл с клиентским событием `memory_usage` (детальные метрики потребления памяти)
- **WHEN** выполняется парсинг
- **THEN** событие сохранено с kind `memory_usage` и метриками памяти

### Requirement: Short-форматы для MCP-ответов

Система SHALL предоставлять `Short`-форматы структур для компактных MCP-ответов при `format = "short"`: `RTICallShort`, `RTIClientEventShort`, `RTIClientTreeNodeShort` (`timeline.go`), с преобразованием через `ToShortCall` / `ToShortEvent` / `ToShortClientTreeNode`. Short-форматы исключают тяжёлые поля (BLog-блоки, checkpoints, параметры с детализацией), оставляя только ключевые метаданные для timeline/дерева.

#### Scenario: Timeline с format=short через MCP

- **GIVEN** сохранённая RTI-сессия с большим числом вызовов
- **WHEN** вызывается MCP-инструмент `codebase_rti_timeline` с `format = "short"`
- **THEN** вызовы возвращены как `[]RTICallShort`, события как `[]RTIClientEventShort` (без тяжёлых полей)
- **AND** размер ответа значительно меньше, чем при полном формате

#### Scenario: Client-tree с format=short

- **GIVEN** RTI-сессия с большим клиентским деревом
- **WHEN** вызывается MCP-инструмент `codebase_rti_client_tree` с `format = "short"`
- **THEN** узлы дерева возвращены как `[]RTIClientTreeNodeShort` (без детальных enrichment-полей)

## Related code

- `internal/rti/parser_client.go` — парсинг клиентских событий (thick client d5nt), виды `trancount`/`memory_usage`/`recordset_open` с детализацией
- `internal/rti/enrich_client.go` — `EnrichClientEvents` (enrichment из индекса)
- `internal/rti/link.go` — связывание клиентских событий с серверными вызовами
- `internal/rti/timeline.go` — `FormatUnifiedTimeline`, `FormatUnifiedTimelineEnriched`, `RTICallShort`, `RTIClientEventShort`, `RTIClientTreeNodeShort`, `ToShortCall`, `ToShortEvent`, `ToShortClientTreeNode`
- `internal/rti/model.go` — `RTIClientEvent`
- `cmd/rti.go` — CLI commands `rti client-tree`, `rti timeline`

## Notes

- Клиентские события включают: SQL blocks, recordset open, connection, BPL load, errors, memory, generic, trancount, memory_usage
- Enrichment клиентских событий использует тот же индекс, что и серверных (PAS, DFM, SQL fragments)
- Timeline поддерживает фильтры `time_from`/`time_to` в RFC3339 формате
- Связывание клиент↔сервер выполняется в пост-обработке парсинга
- Execution-слой `internal/rtisvc/runtime.go` (`ExecuteClientTree`, `ExecuteTimeline`) — общая точка входа для CLI (`cmd/rti.go`) и MCP-инструментов `codebase_rti_client_tree`/`codebase_rti_timeline`; устраняет дублирование оркестрации. Транспорт MCP — в `mcp-server/mcp-transport-tools`.
- Short-форматы (`RTICallShort`, `RTIClientEventShort`, `RTIClientTreeNodeShort`) — для компактных MCP-ответов при `format = "short"`; исключают тяжёлые поля (BLog, checkpoints, параметры с детализацией)

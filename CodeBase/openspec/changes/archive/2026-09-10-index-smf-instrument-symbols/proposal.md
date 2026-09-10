## Why

SMF-инструменты (модели Ф.О.) индексируются только в специализированную таблицу `smf_instruments` и не попадают в unified-индекс `symbols`. В результате `query symbol` (и MCP-инструмент `query_symbol`) не находит SMF-инструменты по имени — единственный способ найти их через `query smf-instrument` / `query smf-type`, что нарушает принцип единого индекса и затрудняет семантическую навигацию. Все остальные типы сущностей (SQL procedures/tables, H defines, PAS units/classes/methods, JS functions/constants, DFM forms/components, report forms/params/VB functions, API business objects) создают запись в `symbols` при индексации; SMF — единственное исключение.

## What Changes

- При индексации SMF-файла (`parseSMFFile`) для каждого инструмента создаётся запись в `symbols` с `symbol_type = "smf_instrument"`, `entity_type = "smf"`, `entity_id` = ID инструмента в `smf_instruments`, `line_number` и `signature` = `scenario_type`.
- SMF-инструменты становятся доступны через `query symbol --name <name>` и `query symbol --name <name> --type smf_instrument`.
- Существующие команды `query smf-instrument` и `query smf-type` продолжают работать без изменений (они читают напрямую из `smf_instruments`).
- Добавлен составной индекс `idx_symbols_entity_type_entity_id` на `symbols(entity_type, entity_id)` для подготовки к будущему упрощению запросов, использующих JOIN с `relations.target_id` (например, spec coverage). Сам spec coverage query не изменяется — упрощение отложено до заполнения `symbols` всеми типами сущностей (sql_table, pas_method).
- Переиндексация (`init` / `update`) заполняет `symbols` для SMF-инструментов; для уже проиндексированных проектов требуется переиндексация SMF-файлов.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/scripting-parsing`: требование «Извлечение SMF-инструментов» дополняется созданием записи в unified-индексе `symbols` при индексации SMF-инструмента.
- `query/symbol-search`: требование «Поддерживаемые типы символов» расширяется включением `smf_instrument`; добавляется сценарий поиска SMF-инструмента через `query symbol`.
- `infrastructure/database-schema`: требование «Индексы для производительности» расширяется составным индексом `symbols(entity_type, entity_id)` для JOIN-запросов по `entity_id`.

## Impact

- **Код**: `internal/indexer/indexer.go` (`parseSMFFile`) — добавление создания `model.Symbol` и вызова `BatchInsertSymbols` после `BatchInsertSMFInstruments`; резолв `instrument_id` через существующий `FindLatestSMFInstrumentIDByFile`.
- **Схема БД**: `internal/store/db_schema.go` — добавление `CREATE INDEX IF NOT EXISTS idx_symbols_entity_type_entity_id ON symbols(entity_type, entity_id)` в массив индексов `InitSchemaCtx`.
- **Данные**: таблица `symbols` получает новые строки для SMF-инструментов; новый индекс создаётся идемпотентно при следующем `init`/`update`.
- **API/CLI**: `query symbol` начинает возвращать SMF-инструменты; новые команды не добавляются.
- **MCP**: MCP-инструмент `query_symbol` начинает возвращать SMF-инструменты без изменений в транспорте.
- **Тесты**: `internal/query/query_test.go` и/или indexer-тесты расширяются проверкой наличия SMF-инструмента в `symbols`.

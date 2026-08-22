# MCP Pagination

## Purpose

Автоматическая пагинация больших MCP-ответов: разбиение на чанки, выравнивание по границе UTF-8 руны, хранение в памяти с TTL, инструмент `codebase_read_more` для запроса следующих чанков.

## Requirements

### Requirement: Автоматическое разбиение на чанки

Система SHALL автоматически разбивать MCP-ответы, превышающие лимит (по умолчанию 8000 байт), на чанки с заголовком-подсказкой для LLM-агента.

#### Scenario: Ответ превышает лимит

- **GIVEN** MCP-ответ размером 25000 байт
- **WHEN** ответ сериализован и превышает `pagination_chunk_size`
- **THEN** ответ разбит на 4 чанка (по ~8000 байт)
- **AND** первый чанк возвращён с заголовком `⚠️ PAGINATED RESPONSE: chunk 1/4 | continuation_id="..."`

#### Scenario: Ответ в пределах лимита

- **GIVEN** MCP-ответ размером 3000 байт
- **WHEN** ответ сериализован
- **THEN** ответ возвращён целиком без пагинации

### Requirement: Выравнивание по границе UTF-8

Система SHALL выравнивать чанки по границе UTF-8 руны через `utf8.RuneStart`, не разрезая посередине многобайтовых символов (кириллица и т.п.).

#### Scenario: Разрезание на кириллице

- **GIVEN** MCP-ответ с кириллическим текстом, где граница чанка попадает посередине UTF-8 символа
- **WHEN** выполняется разбиение
- **THEN** граница чанка сдвинута до начала руны
- **AND** ни один символ не разрезан

### Requirement: Инструмент codebase_read_more

Система SHALL предоставлять MCP-инструмент `codebase_read_more` для запроса следующих чанков пагинированного ответа, принимающий параметры `continuation_id` и `chunk` (номер чанка, начиная с 1).

#### Scenario: Запрос следующего чанка

- **GIVEN** пагинированный ответ с continuation_id="abc123" и 4 чанками
- **WHEN** вызывается `codebase_read_more` с `continuation_id = "abc123"` и `chunk = 2`
- **THEN** возвращён чанк 2 с заголовком-подсказкой

#### Scenario: Запрос последнего чанка

- **GIVEN** пагинированный ответ с 4 чанками
- **WHEN** вызывается `codebase_read_more` с `chunk = 4`
- **THEN** возвращён чанк 4 с заголовком `✅ FINAL CHUNK: chunk 4/4`

#### Scenario: Повторный запрос после завершения

- **GIVEN** пагинированный ответ, все чанки уже запрошены
- **WHEN** вызывается `codebase_read_more` с тем же continuation_id
- **THEN** возвращена ошибка — сессия пагинации завершена

### Requirement: TTL и сборка мусора

Система SHALL хранить чанки пагинированных ответов в памяти с TTL (по умолчанию 15 минут) и автоматически удалять истёкшие сессии через сборку мусора. Реализация GC двухуровневая: (1) фоновый цикл `startGCLoop` / `gcTick` / `stopGCLoop` на `time.AfterFunc` с интервалом `TTL / 2` (минимум 1 секунда), запускается из `RunStdio` и останавливается через `defer`; (2) проактивный вызов `gc()` внутри `maybePaginate` и `readChunk` — удаляет просроченные записи при каждой операции, не дожидаясь тика таймера.

#### Scenario: Истечение TTL

- **GIVEN** пагинированный ответ с TTL 15 минут
- **WHEN** проходит 15 минут после создания сессии
- **THEN** сессия пагинации автоматически удалена из памяти фоновым GC

#### Scenario: Фоновый GC запущен при старте MCP-сервера

- **GIVEN** запускается `codebase mcp`
- **WHEN** `RunStdio` вызывает `globalPages.startGCLoop()`
- **THEN** таймер `time.AfterFunc` запущен с интервалом `TTL / 2`
- **AND** при остановке сервера таймер останавливается через `stopGCLoop`

#### Scenario: Проактивная очистка при доступе

- **GIVEN** в store есть просроченные записи
- **WHEN** клиент вызывает `codebase_read_more` или приходит новый пагинированный ответ
- **THEN** `gc()` выполняется синхронно внутри `readChunk`/`maybePaginate` до возвращения результата

### Requirement: Динамическое применение TTL из конфига

Система SHALL применять TTL из секции `[mcp] pagination_ttl` конфигурации при старте MCP-сервера через `SetPaginationTTL(d)`. Значение `<= 0` игнорируется (остаётся прежний TTL). `SetPaginationTTL` меняет пакетную переменную `paginationTTL`, которую используют и фоновый GC (интервал пересчитывается как `TTL / 2`), и `gc()` (cutoff = now − TTL).

#### Scenario: Кастомный TTL из конфига

- **GIVEN** конфигурация с `[mcp] pagination_ttl = "5m"`
- **WHEN** `RunStdio` инициализирует `globalPages` и вызывает `SetPaginationTTL(5 * time.Minute)`
- **THEN** фоновый GC запускается с интервалом 2.5 минуты
- **AND** записи старше 5 минут считаются просроченными

#### Scenario: Некорректный TTL игнорируется

- **GIVEN** конфигурация с `pagination_ttl = "0"` или невалидным значением
- **WHEN** вызывается `SetPaginationTTL(0)`
- **THEN** TTL остаётся прежним (по умолчанию 15 минут), новое значение не применяется

### Requirement: Конфигурация пагинации

Система SHALL поддерживать настройку пагинации через секцию `[mcp]` в `codebase.toml`: `pagination_chunk_size` (размер чанка, по умолчанию 8000) и `pagination_ttl` (TTL, по умолчанию "15m").

#### Scenario: Кастомный размер чанка

- **GIVEN** конфигурация с `[mcp] pagination_chunk_size = 4000`
- **WHEN** MCP-ответ превышает 4000 байт
- **THEN** ответ разбит на чанки по 4000 байт

### Requirement: Sentinel-тип rawMCPText

Система SHALL использовать sentinel-тип `rawMCPText` для инструмента `codebase_read_more`, чтобы сигнализировать `sdkToolPagedResult` пропустить JSON-маршалинг и re-пагинацию.

#### Scenario: rawMCPText не ре-пагинируется

- **GIVEN** чанк пагинированного ответа, возвращённый как `rawMCPText`
- **WHEN** `sdkToolPagedResult` обрабатывает ответ
- **THEN** ответ не подвергается повторному JSON-маршалингу и re-пагинации

## Related code

- `internal/mcp/pagination.go` — `pageStore`, `rawMCPText`, `maybePaginate`, `readChunk`, `splitChunks`, `gc`, `newEntryID`, `SetPaginationTTL`, `startGCLoop`/`gcTick`/`stopGCLoop`
- `internal/mcp/server.go` — `sdkToolPagedResult`, инициализация `globalPages` из конфига, `startGCLoop`/`stopGCLoop` в `RunStdio`
- `internal/mcp/registry.go` — инструмент `codebase_read_more`
- `internal/config/config.go` — `MCPConfig.PaginationChunkSize`, `MCPConfig.PaginationTTL`

## Notes

- Размер чанка 8000 байт ≈ 6600–8000 символов Unicode (безопасно под IDE-лимит ~10000 символов)
- Формат ответа при пагинации: `⚠️ PAGINATED RESPONSE: chunk 1/N | continuation_id="abc"\n👉 Call codebase_read_more(...) for next part.\n\n<данные>`
- Последний чанк помечается `✅ FINAL CHUNK: chunk N/N` и удаляет запись из store (нет смысла хранить завершённую сессию до TTL)
- Конфигурация: `[mcp] pagination_chunk_size = 8000` и `pagination_ttl = "15m"` в `codebase.toml`
- GC двухуровневый: фоновый таймер (`time.AfterFunc` с интервалом `TTL/2`) + проактивный `gc()` в `readChunk`/`maybePaginate`. Это компенсирует сценарии, когда между тиками таймера накапливается много просроченных записей
- `pageStore` — пакетный синглтон `globalPages`, переинициализируется из `RunStdio` по конфигу; `SetPaginationTTL` меняет TTL для уже созданного store

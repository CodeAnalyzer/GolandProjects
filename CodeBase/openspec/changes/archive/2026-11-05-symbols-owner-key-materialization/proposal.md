# Proposal: Материализация owner_name в таблице symbols

## Why

Ключ дедупликации `SearchSymbol`/`SearchInContent` включает `owner_key` (класс для PAS-методов, класс формы для DFM-компонентов), который сегодня вычисляется SQL-выражением `symbolOwnerKeyExpr` с коррелированными подзапросами `pas_methods → pas_classes` и `dfm_components → dfm_forms` для каждой строки CTE `base` до дедупликации. Замер bug-репорта B4 (BUG-query-search-performance-bottlenecks-20261103): `query symbol --name DoAction --type method` → 4 278 строк × SubPlan (28 625 buffer hits), 13,2 с холодный / 259–408 мс прогретый. Отложить вычисление после `DISTINCT ON`/`LIMIT` нельзя — `owner_key` входит в ключ дедупа. Единственный способ убрать per-row подзапросы — материализовать владельца в колонке `symbols.owner_name` на индексации (консенсус bug-репорта: материализация, LATERAL-костыль не делаем). Источник — Modifications/symbols-owner-key-materialization.md, ревизированный по результатам исследования кода 2026-11-04.

## What Changes

- Схема: колонка `symbols.owner_name TEXT NOT NULL DEFAULT ''`, bump `CurrentSchemaVersion` v4 → v5, идемпотентное применение на существующих БД через переход версии схемы.
- Backfill: однократная именованная миграция `symbols_owner_name_backfill_v1` заполняет `owner_name` для существующих строк, семантически эквивалентно прежнему runtime-выражению; два type-scoped set-based UPDATE (method → `pas_methods`/`pas_classes`, component → `dfm_components`/`dfm_forms`), строки прочих типов не переписываются (DEFAULT уже семантически равен). Легаси-методы с нерезолвнутым классом получают пустого владельца: имя класса-ссылки не сохранено в индексе, backfill не восстанавливает его.
- Индексатор: `model.Symbol.OwnerName`; для `method` — имя класса из декларации метода как записано в коде (через `sanitizeUTF8String`, без TRIM), для `component` — `form_class` родительской формы того же файла (через карту `formClassByID`, без дополнительного lookup), остальные типы — DEFAULT `''`.
- Запросы: `SearchSymbol` и `SearchInContent` читают `owner_key` из колонки (`COALESCE(s.owner_name, '')`), константа `symbolOwnerKeyExpr` удаляется; из планов исчезают SubPlan-ы владельца.
- Тест-эталон `legacyReportFormLookup` (query_symbol_types_integration_test.go) — третий потребитель выражения — переходит на инлайн-копию прежнего выражения, сохраняя роль эталона эквивалентности.
- Осознанные семантические отличия (задокументированы в design): методы с нерезолвнутым классом перестают схлопываться в одну группу дедупа (устранение скрытого over-collapse) — для строк, записанных индексатором; ключ владельца для method использует регистр написания в коде метода, а не регистр декларации класса.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/symbol-search`: ключ владельца дедупликации читается из материализованной колонки `symbols.owner_name` без коррелированных подзапросов; уточнена семантика группировки одноимённых методов (включая нерезолвнутые классы).
- `indexing/pas-dfm-parsing`: индексатор заполняет `symbols.owner_name` для символов `method` (имя класса из декларации метода) и `component` (класс родительской формы) при создании символов.
- `infrastructure/database-schema`: колонка `owner_name` в `symbols`, bump маркера версии схемы, однократная set-based backfill-миграция `symbols_owner_name_backfill_v1` по образцу именованных миграций.

## Impact

- Код: `internal/store/db_schema.go` (DDL + миграция + bump версии), `internal/store/db_insert_j.go` (CopyIn), `internal/model/model.go` (поле), `internal/indexer/indexer_sql_pas.go` (method), `internal/indexer/indexer.go` (component, карта formClassByID), `internal/query/query.go` (SearchSymbol/SearchInContent, удаление константы), `internal/query/query_symbol_types_integration_test.go` (эталон).
- Существующие БД: первый `update` после апгрейда бинаря выполняет ALTER + backfill (одна тяжёлая миграция, единицы минут на масштабе FA ~5,8 млн строк, вне критического пути последующих прогонов); read-only команды на непромигрированной БД возвращают «требуется обновление схемы» (существующая readonly-schema-policy).
- CLI/MCP контракт: без изменений — форма `SymbolResult`, JSON envelope, NDJSON не затронуты.
- Инкрементальный update: переиндексация файла пересоздаёт его symbols уже с `owner_name`; reconcile не требуется; pending-резолв классов не меняет записанное значение (владелец фиксируется на момент создания символа).

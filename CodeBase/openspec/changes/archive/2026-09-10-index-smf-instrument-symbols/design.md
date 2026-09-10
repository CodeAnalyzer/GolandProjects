## Context

Все типы сущностей (SQL procedures/tables, H defines, PAS units/classes/methods, JS functions/constants, DFM forms/components, report forms/params/VB functions, API business objects) при индексации создают запись в unified-таблице `symbols` с указанием `entity_type`, `symbol_type`, `entity_id`, `line_number` и `signature`. SMF-инструменты — единственное исключение: `parseSMFFile` сохраняет инструмент только в специализированную таблицу `smf_instruments` через `BatchInsertSMFInstruments` и не вызывает `BatchInsertSymbols`. В результате `SearchSymbol` (читающий только из `symbols`) не возвращает SMF-инструменты.

Резолв `instrument_id` после вставки уже реализован в существующем коде: `FindLatestSMFInstrumentIDByFile` (используется для prequery SQL-фрагментов в том же `parseSMFFile`). Миграция схемы таблиц не требуется — `symbols` принимает произвольные `entity_type`/`symbol_type`.

Отдельная проблема: `symbols` не имеет индекса на `(entity_type, entity_id)`. Запрос `ExecuteSpecCoverage` (`specsvc.go`) использует UNION ALL по 8 отдельным таблицам вместо JOIN с `symbols`, потому что JOIN по `entity_id` без индекса медленный (комментарий в коде: «symbols не используется — нет индекса на entity_id»). Добавление составного индекса подготавливает почву для будущего упрощения этого запроса, но само упрощение выходит за рамки текущего change: `symbols` пока неполон (отсутствуют `sql_table`, `pas_method`, `api_contract` с согласованным `entity_type`).

## Goals / Non-Goals

**Goals:**

- SMF-инструменты появляются в `symbols` при индексации с `symbol_type = "smf_instrument"`, `entity_type = "smf"`.
- `query symbol` (и MCP `query_symbol`) находит SMF-инструменты по имени и фильтрует через `--type smf_instrument`.
- Составной индекс `symbols(entity_type, entity_id)` создаётся идемпотентно при следующем `init`/`update`.
- Минимальное изменение кода: один участок в `parseSMFFile` + одна строка DDL в `db_schema.go`.

**Non-Goals:**

- Не изменяются существующие команды `query smf-instrument` и `query smf-type` (они читают напрямую из `smf_instruments` и возвращают расширенные поля — states/actions/accounts — недоступные в `symbols`).
- Не добавляются новые CLI-команды или MCP-инструменты.
- Не создаётся обратная миграция для уже проиндексированных проектов — заполнение `symbols` происходит при следующей переиндексации (`init`/`update`).
- Не упрощается `ExecuteSpecCoverage` (`specsvc.go`) — запрос остаётся с UNION ALL по 8 таблицам. Упрощение отложено до заполнения `symbols` всеми типами сущностей (`sql_table`, `pas_method`, нормализация `api_contract`). Индекс `(entity_type, entity_id)` подготавливает почву для этого будущего упрощения.

## Decisions

### Decision 1: Создание Symbol в `parseSMFFile` сразу после вставки инструмента

**Решение:** В `parseSMFFile` после `BatchInsertSMFInstruments` резолвим `instrument_id` через `FindLatestSMFInstrumentIDByFile` (уже вызывается ниже для prequery-фрагмента) и создаём `model.Symbol` с полями:
- `FileID = fileID`
- `SymbolName = result.Instrument.InstrumentName`
- `SymbolType = "smf_instrument"`
- `EntityType = "smf"`
- `EntityID = instrumentID`
- `LineNumber = result.Instrument.LineStart`
- `Signature = result.Instrument.ScenarioType`

Затем `BatchInsertSymbols` с batch size из конфига.

**Альтернатива:** Создавать Symbol в общем postprocess-цикле (как `indexer_postprocess_spec.go` для spec-сущностей). Отклонено — SMF-инструмент уже полностью извлечён в `parseSMFFile`, отдельный postprocess-проход добавил бы N+1 запрос без выгоды.

### Decision 2: Переиспользование `FindLatestSMFInstrumentIDByFile` для резолва EntityID

**Решение:** Использовать существующий `FindLatestSMFInstrumentIDByFile(ctx, fileID)` для получения ID только что вставленного инструмента. Этот метод уже применяется в том же `parseSMFFile` для связи prequery SQL-фрагмента с инструментом.

**Альтернатива:** Возвращать ID из `BatchInsertSMFInstruments` через `RETURNING id`. Отклонено — `BatchInsertSMFInstruments` уже реализован и используется; изменение его сигнатуры затронуло бы все вызывающие места. `FindLatestSMFInstrumentIDByFile` даёт тот же результат с минимальным изменением.

### Decision 3: `Signature = ScenarioType` (не `Brief`)

**Решение:** В `signature` хранить `scenario_type` (например, `mass_operation`), а не `brief`. `scenario_type` — короткий стабильный идентификатор типа, удобный для фильтрации и отображения; `brief` — произвольный текст, который может быть пустым.

**Альтернатива:** Хранить `brief` в `signature`. Отклонено — `brief` nullable и не отражает тип сущности, что снижает ценность `signature` для unified-поиска.

### Decision 4: Индекс `symbols(entity_type, entity_id)` — подготовка, не упрощение

**Решение:** Добавить `CREATE INDEX IF NOT EXISTS idx_symbols_entity_type_entity_id ON symbols(entity_type, entity_id)` в `InitSchemaCtx`. Индекс создаётся идемпотентно при следующем `init`/`update` и не требует миграции данных.

**Обоснование:** Текущий `ExecuteSpecCoverage` использует UNION ALL по 8 таблицам, потому что JOIN с `symbols` по `entity_id` без индекса медленный. Индекс устраняет это препятствие. Однако упрощение самого запроса отложено: `symbols` пока неполон (нет `sql_table`, `pas_method`; `api_contract` имеет `entity_type = "xml"` с переменным `symbol_type`). Пока все типы не будут в `symbols` с согласованными `entity_type`, JOIN потребует CASE-маппинга `target_type → entity_type` и не даст полного упрощения.

**Альтернатива:** Не добавлять индекс сейчас, а добавить вместе с упрощением запроса. Отклонено — индекс идемпотентен и безопасен; добавление сейчас разделяет работу на два независимых шага (подготовка → упрощение), каждый из которых можно валидировать отдельно.

## Risks / Trade-offs

- **[Дублирование запроса `FindLatestSMFInstrumentIDByFile`]** → В `parseSMFFile` метод уже вызывается для prequery-фрагмента; для инструмента без prequery это первый вызов. Стоимость одного SELECT по `file_id` (есть индекс `idx_smf_instruments_file_id`) пренебрежимо мала. Можно оптимизировать, вызвав один раз и переиспользовав результат для symbol и prequery, но это усложнит код — отложено.
- **[Заполнение `symbols` только при переиндексации]** → Для уже проиндексированных проектов SMF-инструменты появятся в `symbols` только после `init`/`update` SMF-файлов. Документировать в tasks: пользователь должен переиндексировать.
- **[Нет обратной миграции]** → Если откатить изменение, orphan-записи в `symbols` для SMF останутся до следующей полной переиндексации (cascade-удаление по `file_id` очистит их при `init`). Риск низкий — записи не мешают существующим запросам.

## Decisions (багфиксы парсера)

### Decision 5: Одинарные кавычки в regex-ах SMF-парсера

**Решение:** Обновить regex-ы `instrumentNameRe`, `instrumentBriefRe`, `legacyNameRe`, `legacyBriefRe` до `["']([^"']+)["']` и `instrumentStartRe`/`legacyStartRe` до `(?:"..."|'...'|identifier)`. Реальные SMF-файлы (например `CardTranIn.smf`) используют `Name = '...'` внутри `with(Instrument)` — старый regex искал только `"..."`, имя оставалось пустым.

### Decision 6: UTF-8 приоритет при декодировании SMF

**Решение:** В `ParseBytes` проверять `utf8.Valid(data)` первым — если данные валидный UTF-8, декодировать как UTF-8. Fallback на WIN1251/CP866 только если UTF-8 невалиден. После декодирования применять `NormalizeMojibake` (по аналогии с `indexer_spec.go`). Некоторые SMF-файлы (например `Mass_IFRS9_TOYOTA_Load_Rollback.smf`) объявляют `encoding="windows-1251"` в XML-decl, но реально закодированы в UTF-8 — старый порядок (WIN1251 первый) давал mojibake.

**Альтернатива:** Расширить `garbageCharRe` для обнаружения box-drawing символов (`╨╤`). Отклонено — UTF-8 приоритет надёжнее и соответствует подходу `DetectMarkdownEncoding`.

## Context

См. `proposal.md` — Why. Текущее состояние, определяющее подход:

- `SearchRelations` (`internal/query/query_relations.go:129`) и `buildRelationIDsQuery` (`:179`) пропускают `relationType` в SQL как есть (`r.relation_type = $N`, `:217-221`); `SearchRelationsByEntity` (`:16`, используется `inspect`) — та же картина.
- Индексы `relations` (`internal/store/db_schema.go:944-947`) создаются только с ведущими `source_type`/`target_type`; ведущего `relation_type` нет. Легаси-`DROP INDEX IF EXISTS idx_relations_relation_type` (`:1028`) сохранён и проверяется тестом `schema_integration_test.go:108-115`.
- Прецедент strict-валидации: `normalizeSymbolTypeFilter` (`internal/query/query.go:281`) → `model.ResolveSymbolTypeAlias` → `errs.ErrUnknownSymbolType` → `classifyQueryError` (`cmd/query_execution.go:305`) → `invalid_arguments`.
- Канонического словаря relation/entity-типов в Go нет: описание инструмента `internal/mcp/registry.go:542` перечисляет только 17 relation-типов и 11 entity-типов.
- Живой индекс FA (`relations` = 3 479 404 строки) фактически содержит **21** relation-тип и **17** entity-типов (проверено probe'ами `codebase_query_relations` по `source_type`/`relation_type`): 17 code + `change_modifies`, `depends_on_capability`, `references_code`, `usecase_involves`; entity — 12 code (`+ dfm_form` к реестровым 11) + 5 `spec_*`.

## Goals / Non-Goals

**Goals:**

- Неизвестный `relation_type`/`source_type`/`target_type` → быстрая ошибка валидации со списком валидных значений, без обращения к БД.
- Единый канонический словарь типов графа в `internal/model`, совпадающий с реально записываемыми индексатором значениями.
- Предсказуемая производительность валидных type-only и `source_type + relation_type` запросов через два новых индекса.

**Non-Goals:**

- Валидация `source_name`/`target_name` (это свободный текст, а не перечисление).
- Изменение семантики `ORDER BY r.id DESC` (порядок выдачи не меняется).
- Автодополнение/подсказки типов на уровне CLI/MCP-схемы.
- Рефакторинг `SearchRelationsByEntity` сверх необходимой валидации.

## Decisions

### D1. Канонический словарь в `internal/model`

Новый файл рядом с `symbol_types.go`: словари `RelationTypes` (21) и `RelationEntityTypes` (17) с функциями проверки/списка (по образцу `SymbolTypes`/`SymbolTypeAliasesList`). Источник значений — фактические писатели отношений (`internal/indexer/indexer_relations.go`, `indexer_postprocess_*.go`), а не текст registry.

**Альтернатива** — держать список в `internal/query` или в `registry.go`: отклонено, словарь нужен и запросному слою, и MCP-описанию, и потенциально валидаторам.

### D2. Ошибка, а не пустой результат

Неизвестное значение → новая ошибка `errs.ErrUnknownRelationType` (и её применение к entity-типам) с перечислением валидных значений; в `classifyQueryError` маппится в `invalid_arguments`. Соответствует прецеденту `symbol --type`.

**Альтернатива** — вернуть `count=0` (технически возможно при наличии индекса): отклонено, т.к. маскирует опечатку и противоречит принятому strict-контракту.

### D3. Валидировать и `source_type`/`target_type`

Оба фильтра проверяются по словарю entity-типов (17) до построения SQL. Валидация централизуется в запросном слое и вызывается из `SearchRelations` (публичный путь `query relations` / `codebase_query_relations`). `SearchRelationsByEntity` (внутренняя механика `inspect`) **не** валидирует: `inspect` получает тип из моста `model.RelationTypeForSymbol`, который может выдавать значения вне словаря (`pas_class`, `pas_unit`, `dfm_component`, `api_business_object`, `xml`, `sql`) — строгая валидация там сломала бы `query inspect`.

### D4. Два индекса `relations`

- `idx_relations_relation_type_id` на `(relation_type, id DESC)` — покрывает ветку «только `relation_type`» (в т.ч. мгновенный пустой результат для отсутствующего типа).
- `idx_relations_source_type_relation_id` на `(source_type, relation_type, id DESC)` — покрывает 3-предикатный паттерн `source_type + relation_type + ORDER BY id DESC` (замеры: 1.27M отброшенных строк / ~210 мс warm → 0 отброшенных / единицы мс).

**Альтернатива 1** — один `(relation_type, id DESC)`: отклонено при финальном решении (пользователь выбрал два индекса) — не оптимален для комбинированного фильтра с ведущим `source_type`.
**Альтернатива 2** — смена порядка в запросе на согласованный с существующим `idx_relations_source_type_relation`: отклонено — меняет, *какие* 10 связей возвращаются при `LIMIT`, и не помогает ветке без `source_type`.

### D5. Легаси-DROP и тесты схемы

`DROP INDEX IF EXISTS idx_relations_relation_type` (`db_schema.go:1028`) сохраняется (no-op для нового имени). `schema_integration_test.go:108-115` (список `dropped`) не трогается; добавляются проверки существования двух новых индексов.

### D6. Синхронизация описания инструмента

`registry.go:542` дополняется: relation-типы до 21 (включая spec-связи), entity-типы до 17 (включая `dfm_form` и `spec_*`).

## Risks / Trade-offs

- **BREAKING для вызывающих с неизвестным типом** → митигация: словарь покрывает весь фактический набор (21/17), проверенный на живом индексе; изменение явно помечено.
- **Write-цена двух btrees на `relations` (3.48M строк, массовая вставка)** → митигация: принято осознанно (переоткрытие trade-off `redundant-indexes-cleanup-a41c07`); на больших БД индексы строятся вручную `CREATE INDEX CONCURRENTLY`.
- **Создание индексов внутри транзакции `InitSchema` (не `CONCURRENTLY`) кратко блокирует запись** → митигация: документированная ручная сборка для крупных БД; откат — `DROP INDEX`.
- **Дрейф словаря при добавлении новых relation_type** → митигация: юнит-тест словаря + комментарий в `internal/model` с указанием на писателей.
- **kind-алиасы API-контрактов (`service`/`event`/`callback_event`/`used_service`) не принимаются строгой валидацией** → намеренно: нормализация kind→`api_contract` в name-filter `relations` — предмет отдельного открытого `BUG-relations-kind-type-name-filter-20261011.md` (TC-073); при его исправлении этот словарь дополнится алиасами. Регрессии нет: до изменения `source_type=service` тоже отклонялся (иным сообщением).

## Migration Plan

`InitSchema` идемпотентен: `CREATE INDEX IF NOT EXISTS` создаёт оба индекса на существующих БД; легаси-DROP остаётся no-op. Откат — `DROP INDEX idx_relations_relation_type_id, idx_relations_source_type_relation_id` и снятие валидации.

## Open Questions

(нет — решения D1-D6 приняты)

# dsinfo-integration: профиль развёртывания в анализе

## Why

Срезы `branch-aware-indexing` и `branch-aware-lookup` научили индекс хранить
релизные линии и выбирать копию сущности по приоритету ветки, но у анализа
инцидентов нет способа сообщить, какие линии установлены в конкретном банке.
Профиль dsinfo (снимок `DiasoftSystemInfo.csv`, срез `dsinfo-parser`) уже
хранит отображение «продукт → релизная линия», однако никем не потребляется:
запросы и enrichment RTI/TRC не знают, что Contracts установлен в 11.01.7,
GAAP — в 11.01.4, и сопоставляют трейсы с develop-кодом. Это финальный срез
`Modifications/dsinfo-product-branches-6f2a1c.md`, замыкающий сценарий
«инцидент → конфигурация банка → код нужной линии».

Дополнительно нужно зафиксировать **границы** применения профиля: флаг
`--dsinfo` висит на всей группе `query`, но выбор копии выполняется не везде
(`callers`/`relations`/`inspect` намеренно возвращают все копии, spec-команды
и semantic-слой поиска описаний не имеют продукта-кандидата). Без явных
границ эти поверхности молча игнорировали бы профиль, а реализации грозила
бы неоднозначность.

## What Changes

- **Матчинг профиля на ветки**: вычисляемое на лету сопоставление строк
  манифеста профиля (`dsinfo_products.product_name`/`branch_key`) с
  проиндексированными продуктами и линиями (`ds_products`/`ds_products_branch`)
  со статусами `ok` / `product_not_indexed` / `branch_not_indexed`; снимок
  профиля не изменяется. Матчинг двухуровневый: точное совпадение
  `(product, branch_key)`, затем fallback по числовым токенам хвоста
  (`sup/11-01-7` ↔ `fa-contracts-11-01-7`) в пределах того же продукта.
- **Таблица сопоставления в `parse`/`show`**: вывод matched/missing веток и
  итога «matched N/M; K branches missing».
- **Контекст анализа с приоритетом**: флаг `--dsinfo <id>` на группах
  `query`/`rti`/`trc` (CLI) и параметр `dsinfo_id` (MCP); цепочка
  разрешения: `--dsinfo` > переменная окружения `CODEBASE_DSINFO_ID` >
  активный профиль (`dsinfo use`) > `default_branch`.
- **Порядок осей**: в поверхностях, выбирающих копию, приоритет
  `not_generated` → `profile_match` → `is_default` → больший id;
  `profile_match` действует только для продуктов профиля. Store-lookup'ы
  пост-обработки связей профиль не используют (scope — файл-источник).
- **Семантика fallback**: профиль задаёт приоритет, а не жёсткий фильтр;
  у продукта может быть несколько строк манифеста — проиндексированные
  приоритетнее fallback'а (default-линия → старшая версия); если линий нет,
  выбирается default-ветка, а ответ сообщает `branch_requested` (все линии
  продукта) и `branch_used` (оба — нормализованный `branch_key`).
- **Границы применения**: профиль влияет на entity/API-запросы с выбором
  копии, поиск символов, лексический слой поиска по описаниям и enrichment
  RTI/TRC. `query callers`/`relations`/`inspect`, spec-команды и semantic-слой
  desc-search профиль не учитывают: флаг принимается, результат не зависит от
  профиля, предупреждений не добавляется, идентификатор не проверяется.
- **Enrichment RTI/TRC с профилем**: процедуры из логов резолвятся с
  приоритетом продуктов/веток профиля; результат содержит `product` и
  `branch_used`.
- **Ответы**: `branch_requested` в элементах, выбравших копию, и агрегированный
  `warnings` в конверте ответа (CLI) / результате инструмента (MCP);
  предупреждения вычисляются по фактическим элементам ответа, а не по всему
  профилю.
- **Не меняется**: поведение без профиля (по-прежнему default-ветка), форматы
  RTI/TRC-логов, схема БД, команды `init`/`update`/`review`/`health`/`stats`,
  снимок профиля и идемпотентность `dsinfo parse`.

## Capabilities

### New Capabilities

- `dsinfo-analysis/dsinfo-resolution`: матчинг профиля на индексированные
  ветки (включая fallback по версии линии), таблица сопоставления, цепочка
  разрешения контекста анализа (`--dsinfo`/env/активный профиль/default),
  семантика fallback с предупреждением, границы применения профиля по
  поверхностям, приоритет продуктов профиля в enrichment.

### Modified Capabilities

- `dsinfo-analysis/dsinfo-storage`: требования «Активный профиль» и
  «CLI- и MCP-интерфейсы dsinfo» — активный профиль потребляется командами
  анализа как контекст по умолчанию (в границах `dsinfo-resolution`); команды
  вне анализа (`init`/`update`/`review`/`health`/`stats`) профиль не
  принимают.
- `query/entity-queries`: требования «Детали SQL-процедуры» и «Приоритет
  релизной ветки при выборе сущности» — ось `profile_match` перед
  `is_default`, `branch_requested` в результатах; справочные отсылки к
  будущему изменению заменяются фактическим поведением.
- `query/api-relations-queries`: требование «Приоритет релизной ветки в
  API-запросах» — `profile_match` при выборе копии контракта и `branch_requested`.
- `rti-analysis/rti-parsing`: требование «Enrichment из индекса» — резолв с
  контекстом профиля, поля `product`/`branch_used` в результате.
- `trc-analysis/trc-parsing`: требование «Enrichment из индекса» — то же
  для TRC-событий и агрегаций.
- `mcp-server/mcp-transport-tools`: новое требование — параметр `dsinfo_id`
  у инструментов, учитывающих профиль, и `warnings` в ответе.

## Impact

- **Код**: `internal/store` (матчинг профиля на ветки, fallback по версии),
  `internal/dsinfosvc/` (ResolveProfile, ResolveAnalysisContext), CLI
  `cmd/query.go`/`cmd/rti.go`/`cmd/trc.go` (persistent `--dsinfo`),
  `cmd/query_shared.go` (`warnings` в конверте), `internal/querysvc`
  (options с профилем), `internal/query/*` (`Query` с контекстом профиля,
  ось `profile_match` в двухфазном дедупе, `branch_requested`),
  `internal/rtisvc`/`internal/trcsvc` (проброс `DsinfoID` через Params),
  `internal/rti/enrich.go`, `internal/trc/enrich.go` (`product`/`branch_used`),
  `internal/mcp/registry.go` (параметр `dsinfo_id` и `warnings`), `README.md`.
- **Схема БД**: без новых таблиц; матчинг — запросы JOIN `dsinfo_products`
  × `ds_products` × `ds_products_branch`; fallback — сравнение числовых
  токенов `branch_key` в Go после выборки кандидатов продукта.
- **Совместимость**: без `--dsinfo`/env/активного профиля поведение идентично
  срезу B (default-ветка); JSON расширяется полями `branch_requested` и
  `warnings` (обратно совместимо). Решения обсуждения 2026-10-27: границы
  поверхностей зафиксированы, порядок осей — `not_generated` первым,
  матчинг веток — с fallback по числовым токенам.

# Design: symbol-type-vocabulary

## Context

См. proposal.md — Why. Текущее состояние словарей и мостов:

```
symbols.symbol_type (28 типов)          relations.*_type
─────────────────────────────────       ─────────────────────────
procedure, table  ──── есть мост ────▶  sql_procedure, sql_table      (InspectRelationType)
form, component   ──── есть мост ────▶  dfm_form, dfm_component       (InspectRelationType)
method            ──── НЕТ МОСТА ───▶   pas_method
function          ──── НЕТ МОСТА ───▶   js_function
service/event/callback_event/used_service ── НЕТ МОСТА ──▶ api_contract
smf_instrument, vb_function, report_form ── (=) ── совпадают
pas_unit, pas_class                     (0 исходящих relations — мост не даёт эффекта)
```

Мост `InspectRelationType` (`querysvc/inspect.go:67`) — единственный, покрывает 4 случая, остальные проходят насквозь коротким именем, которого в relations нет. Обратного моста (алиасы для фильтра) нет вовсе; `SearchSymbol` (`query.go:276`) сравнивает `symbol_type = $2` без трансляции.

Точки входа фильтра `type` (4): CLI `query symbol` (`cmd/query_commands.go:24` — вызывает `SearchSymbol` напрямую, минуя querysvc), CLI `query inspect`, MCP `codebase_query_symbol` (`registry.go:402`), MCP `codebase_query_inspect` (через `querysvc.RunInspectQuery`).

## Goals / Non-Goals

Goals:

- Один источник истины для словаря `symbols.symbol_type`, алиасов и моста в relations-словарь.
- Фильтр `type` работает с обоими словарями; неизвестное значение — диагностируемая ошибка.
- Inspect возвращает связи для всех типов, участвующих в графе.

Non-Goals:

- Фильтрация по `entity_type` (`sql`, `js`, `pas`, …) — отдельная возможность, если понадобится.
- Переход индексатора на запись префиксных имён в `symbols` — словарь хранения не меняется.
- Мост для типов без исходящих связей (`pas_unit`, `pas_class`, `dfm_component` как source) — записи переводов безвредны, но результата не дают; не проверяются отдельно.
- `codebase_query_relations` — уже работает в relations-словаре, не меняется.

## Decisions

### 1. Центральная таблица — в `internal/model`

Новый файл (например, `internal/model/symbol_types.go`), чистые данные без зависимостей:

- `SymbolTypes` — канонический набор из 29 значений (список в спеке `query/symbol-search`; включая fallback-тип `xml` — дефолтный kind `classifyPath` для DSArchitect XML вне kind-каталогов, 167 символов в индексе FA);
- `ResolveSymbolTypeAlias(raw) ([]string, bool)` — алиас → набор канонических типов (`api_contract` → 4 kind-типа, остальные алиасы → 1 значение); каноническое значение проходит как есть; регистр и пробелы нормализуются;
- `RelationTypeForSymbol(symbolType, entityType) string` — мост symbols→relations (полная карта, включая kind-типы → `api_contract`).

Альтернативы: `internal/querysvc` (прецедент `InspectRelationType` рядом, но тогда `query.SearchSymbol` не видит таблицу без обратной зависимости) и `internal/query` (виден всем точкам входа, но model чище — таблица про данные, не про запросы). Model виден indexer/query/querysvc/mcp/cmd без циклов.

### 2. Нормализация и валидация — в `SearchSymbol`

`SearchSymbol` — единая точка, через которую идут все 4 входа фильтра. Вход `symbolType` обрабатывается: пусто → нет фильтра (как сейчас); алиас → канонический набор; канонический тип → как есть; иначе — ошибка со списком валидных значений (через `errs`).

Для одного значения фильтр остаётся `symbol_type = $2`; для набора (`api_contract`) — `symbol_type = ANY($2)` с массивом. Это единственное изменение SQL-условия.

Альтернатива — нормализация на каждом входе (registry + cmd, вариант A баг-репорта) — отвергнута: 4 точки, при добавлении входа дрейф повторится; inspect-мост при этом всё равно требовал бы центральной таблицы.

Скоринг `PrioritizeExactSymbolMatches` (+5 за совпадение `item.Type`) при алиасе на входе inspect не сработает, если нормализация только внутри `SearchSymbol`. Поэтому `RunInspectQuery` дополнительно нормализует входной `symbolType` первым каноническим значением перед скорингом (для `api_contract` точный скоринг по kind-типу неоднозначен — достаточно совпадения по имени).

### 3. Strict-валидация вместо pass-through

Неизвестный `type` → ошибка `unknown symbol type '<x>', valid values: …` (CLI — non-zero exit + structured JSON в `--json`, MCP — tool error). Агент получает обратную связь и самокорректируется; контраст с текущим молчаливым `[]`, который агенты трактуют как «символа нет».

Компромисс forward-compat (старый бинаррь + БД, проиндексированная новым бинарём с новым типом) принят: индексация и запросы выполняются бинарём одной версии; при добавлении типа набор в модели пополняется тем же коммитом.

Существующие упоминания алиасов остаются валидными: сценарий `mcp-transport-tools:259` (`type=sql_procedure`) и перекрёстная ссылка `codebase_query_procedure` (`registry.go:449`) после нормализации работают.

### 4. Мост inspect — дополнение `InspectRelationType` из центральной таблицы

`InspectRelationType` переписывается на `RelationTypeForSymbol`: сохраняются текущие 4 перевода, добавляются `method→pas_method`, `function→js_function`, kind-типы (`entity_type = xml`) → `api_contract`. Pass-through сохраняется для совпадающих имён (`smf_instrument`, `vb_function`, `report_form`, `spec_*`).

### 5. Документация — канонический словарь

Описание `codebase_query_symbol` перечисляет канонические типы и declares алиасы принятыми; описание `type` в `codebase_query_inspect` — те же правила; справка CLI `--type` (`cmd/query.go:14`) — канонический список кратко.

## Risks / Trade-offs

- [Strict-валидация ломает скрипты, фильтрующие по значениям entity_type (`sql`, `js`)] → раньше это молча давало `[]` (скрытая ошибка); миграционный путь — ошибка содержит список валидных значений, замена очевидна.
- [`api_contract` → фильтр по 4 типам меняет SQL (ANY-массив)] → поведение чисто расширяющее: раньше алиас давал `[]`, теперь даёт контракты; канонические типы фильтруются как раньше.
- [Центральный набор в model может разойтись с фактическими данными в БД (тип добавлен в парсер, но не в набор)] → задача в tasks: сверка `SELECT DISTINCT symbol_type FROM symbols` с набором при реализации; долгосрочно — место в model рядом с писателями символов.
- [Нормализация в `SearchSymbol` делает query-слой знающим алиасы] → осознанный обмен: единая точка против чистоты слоя; алиасы — часть контракта поиска, а relations-словарь в query-слое уже используется.

## Migration Plan

Изменение кода без миграции данных (словарь хранения не меняется). Деплой: пересборка бинаря; БД и индекс не трогаются. Откат — предыдущий бинарь.

## Open Questions

(нет — три проектных вопроса решены в explore-сессии: таблица в `internal/model`, strict-валидация, один change на оба симптома)

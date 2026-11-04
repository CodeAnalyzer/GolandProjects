# Proposal: optimize-query-search-bottlenecks

## Why

Прогон тест-кейсов на боевом индексе FA (`Bug-reports/BUG-query-search-performance-bottlenecks-20261103.md`, PG 18, symbols 5,8 млн / query_fragments 2,4 млн) зафиксировал системную деградацию поиска: `spec_search` p50 16 с, `sql_fragment` до 36 с с обрывами по `query_timeout_sec = 30`, `symbol --type report_form` до 26 с на холодном кеше. MCP-агент получает таймауты вместо ответов. Корневые причины — SQL-планы (FTS-предикат вне UNION, безлимитные кандидаты trgm/ILIKE), отсутствие обновления статистики после `update` и per-call перезачёты, устранимые без смены схемы БД.

## What Changes

- **B1 — `spec_search`, FTS-предикат внутрь веток UNION**: предикат `search_vector @@ plainto_tsquery(...)` проталкивается в каждую ветку CTE `artifacts`, GIN-индексы `idx_spec_*_fts` начинают использоваться (замер: 2,77 с → 75 мс, ~37×); состав и ранжирование результатов не меняются.
- **B2 — `spec_search`, гейтинг trgm-слоя по форме запроса**: для многословных фраз body-trgm (`%` по body_text/сценариям/usecases) не выполняется — он математически не даёт совпадений similarity короткой фразы с длинным текстом, но стоит до 6 с; tech-name пути (равенство `mention_name`, trgm по `related_code`) сохраняются для любых запросов; семантика слоя `exact` (= лексический слой: tsvector + tech-name trgm) документируется без переименования API.
- **B3 — `symbol --type report_form`, OR → UNION ALL**: трёхполевой fallback (symbol_name / form_name / form_class) переписывается в индексные ветки UNION ALL; план уходит с полного перебора report_form-строк на точечные index scan.
- **B5 — `sql-fragment`, guard-ограничение кандидатов**: CTE `matches` ограничивается настраиваемым потолком (cap+1); при достижении потолка ответ несёт `meta.truncated = true` с подсказкой уточнить шаблон; запросы с числом совпадений ниже потолка возвращают байт-в-байт прежний результат; худший случай перестаёт зависеть от объёма совпадений — таймауты 30 с исчезают.
- **B8 — `desc_search`, кеш вытесняющих контрактов**: множество процедур, вытесняемых контрактами с описанием, вычисляется один раз на поколение LSA-модели и переиспользуется между semantic-вызовами (сегодня — SQL на ~1 с на каждый вызов).
- **ANALYZE после индексации**: успешные прогоны `init`/`update` завершаются `ANALYZE` таблиц кодовой базы (ошибка некритична); read-only команды его не выполняют.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `query/spec-search`: новые требования — использование GIN-индексов лексическим слоем (план без seq scan по spec-таблицам) и гейтинг trgm-поиска по форме запроса с сохранением tech-name путей.
- `query/symbol-search`: новое требование — план трёхполевого поиска report_form строится индексными ветками без полного перебора строк типа.
- `query/entity-queries`: MODIFIED «Поиск SQL/query fragments» — ограничение кандидатов потолком с диагностикой `truncated` в meta.
- `query/description-search`: новое требование — кеш вытесняющих контрактов с инвалидацией по поколению модели.
- `infrastructure/database-schema`: новое требование — ANALYZE таблиц кодовой базы после init/update.
- `infrastructure/configuration`: MODIFIED «Настройки query лимитов» — новый параметр `sql_fragment_match_cap`.

## Impact

- Код: `internal/specsvc/specsvc.go` (searchSpecExact/searchSpecTrgm), `internal/query/query.go` (SearchSymbol/buildReportFormLookupCondition, SearchQueryFragment), `internal/query/desc_query.go` (searchDescriptionsSemantic + кеш), `internal/indexer/runner.go` или точка завершения init/update (ANALYZE), `internal/store` (при необходимости хелпер), `internal/config` + `cmd` (новый параметр, прокинут в CLI/MCP meta), `internal/mcp/registry.go` (описание sql_fragment: флаг truncated).
- Совместимость: CLI-контракт аддитивный — новый опциональный флаг `meta.truncated` в `--json` envelope; переименование слоёв spec_search НЕ выполняется (документационное уточнение); схема БД и данные не меняются (schema version без bump). Единственное изменение внешнего контракта: MCP-ответ `codebase_query_sql_fragment` меняет форму с массива на объект `{items, truncated, hint}` (прецедент `desc_search`; breaking для клиентов, парсящих голый массив, — задокументировано в описании инструмента и закреплено тестом).
- Изменение поведения: (1) trgm-часть лексического слоя не запускается для многословных фраз по body-полям; (2) `sql-fragment` при сверхшироком шаблоне возвращает помеченное усечённое множество вместо таймаута.
- Не входит (отдельные направления): словарь query_texts / versioned lines (`Modifications/query-fragment-dictionary-versioned-lines.md`), материализация owner_key (`Modifications/symbols-owner-key-materialization.md`), B4/B6/B9.

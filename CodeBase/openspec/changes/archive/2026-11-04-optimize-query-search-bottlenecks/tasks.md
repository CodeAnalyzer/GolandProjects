# Tasks: optimize-query-search-bottlenecks

## 1. B1 — FTS-предикат внутрь веток UNION (spec_search exact)

- [x] 1.1 В `searchSpecExact` (`internal/specsvc/specsvc.go`) добавить предикат `search_vector @@ plainto_tsquery('russian', $1)` в WHERE каждой из четырёх веток CTE `artifacts`; внешний предикат в `chosen` и финальном SELECT сохранить. Проверка: integration-тест на тест-БД — состав хитов и ранги совпадают с прежним запросом для пачки фраз.
- [x] 1.2 Снять `EXPLAIN (ANALYZE, BUFFERS)` на FA-индексе до/после: план содержит Bitmap/Index Scan по `idx_spec_*_fts`, Seq Scan по spec-таблицам и материализация полного `artifacts` отсутствуют; зафиксировать замер (было 2,77 с → ожидание ~75 мс).

## 2. B2 — гейтинг body-trgm + документация layer=exact

- [x] 2.1 Разделить `searchSpecTrgm` на tech-часть (related_code, mention_name exact+trgm — выполняется всегда) и body-часть (body_text, конкатенации сценариев/usecases); в `ExecuteSpecSearch` вычислять число word-токенов запроса правилом `specfts.lexicalUnits` (`internal/specfts/tokenizer.go:241`; экспортировать/переиспользовать — буквы/цифры/подчёркивания ≥ 2 рун, минимум одна буква; собственную токенизацию не вводить) и запускать body-часть только при ≤ 2 токенов. Проверка: unit-тест токен-подсчёта (включая `Audit_Insert` = 1 токен, «создание user_profile» = 2); integration-тест — многословная фраза не выполняет body-ветви (по плану/по времени), tech-имя находится любым запросом.
- [x] 2.2 Уточнить описание слоя в help CLI (`--layer`) и в описании MCP-инструмента `codebase_query_spec_search`: exact = лексический слой (tsvector + tech-name trgm). Проверка: текст справки содержит формулировку; значения `exact|semantic|both` не переименованы.

## 3. B3 — report_form OR → UNION ALL (symbol search)

- [x] 3.1 Переписать построение `base` в `SearchSymbol` для `--type report_form`: `UNION ALL` трёх индексных ветвей (symbol_name / form_name / form_class; точные и like-варианты), финальный SELECT — `DISTINCT` по `s.id`. Убрать зависимость от `buildReportFormLookupCondition` (или переиспользовать только как генератор условий ветвей). Проверка: integration-тест — результат трёхполевого поиска идентичен прежнему OR-запросу (включая форму, совпадающую по нескольким полям, и дедуп веток).
- [x] 3.2 Замер на FA-индексе: `query symbol --name FormRepBCHPeriod --type report_form` cold/warm; `EXPLAIN` — точечные index scan без перебора всех report_form-строк. Цель из bug-репорта: < 0,2 с warm, отсутствие 20-секундного холодного пика.

## 4. B5 — guard-потолок sql-fragment + meta.truncated

- [x] 4.1 Добавить параметр `[query] sql_fragment_match_cap` в конфиг (дефолт 50000, 0 — off, < 0 — ошибка загрузки). Проверка: unit-тесты разбора конфига (отсутствует/0/отрицательное).
- [x] 4.2 В `SearchQueryFragment` обернуть выборку `matches` внутренним `LIMIT cap+1` с `count(*) OVER () AS total_matched` снаружи внутреннего LIMIT; сигнатура возвращает `truncated bool`. Проверка: integration-тест — шаблон с числом совпадений ниже потолка даёт байт-в-байт прежний результат (нет truncated); сверхширокий шаблон завершается без таймаута и помечается.
- [x] 4.3 Прокинуть `truncated` + hint в meta CLI `--json` и в MCP-ответ `codebase_query_sql_fragment`; обновить описание инструмента. Проверка: CLI `--json` содержит `meta.truncated`; MCP-вызов возвращает флаг.

## 5. B8 — кеш вытесняющих контрактов (desc_search)

- [x] 5.1 Кешировать displaced-множество по ключу (поколение модели, последний scan_run id) рядом с `descSemanticCache` (mutex/singleflight); на каждый semantic-вызов — дешёвая проверка `SELECT MAX(id) FROM scan_runs` + проверка поколения; перевычисление при изменении любого компонента (update без переобучения модели тоже инвалидирует — F1 ревью). Проверка: integration-тест — два semantic-вызова в одном процессе выполняют выборку вытеснения один раз; update, меняющий контракты без переобучения модели, перевычисляет кеш; смена поколения перевычисляет; состав выдачи идентичен per-call варианту.

## 6. ANALYZE после init/update

- [x] 6.1 Store-хелпер `AnalyzeCodebaseTables(ctx)` (единый `ANALYZE <фиксированный список>`; ошибка → warning) и вызов из runner после успешного `InitCtx`/`UpdateCtx`. Проверка: integration-тест — после update `last_analyze` обновлён, ошибка ANALYZE не меняет статус прогона; query-команда не вызывает.

## 7. Верификация и регресс

- [x] 7.1 Полный прогон `go build ./... && go vet ./... && go test ./...` (+ integration-теги store/query) — зелёные; бамп `buildNumber` в `cmd/root.go:29` c "1663" до "1669" и пересборка `go build -o CodeBase.exe`.
- [x] 7.2 Повторный прогон тест-кейсов `Tests/TEST_CASES.md` на FA-индексе с фиксацией порогов из bug-репорта: `spec_search` < 0,5 с, `sql_fragment` < 1 с, `symbol` < 0,5 с, `symbol report_form` < 0,2 с; замер старта MCP и первых 10 вызовов при включённом warmup (Verification п.3 bug-репорта); отчёт рядом с bug-репортом (по образцу `Tests/RECONCILE_INCREMENTAL_RESULTS_2026-11-01.md`).
- [x] 7.3 Регресс функциональных кейсов `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` — составы результатов не изменились (кроме задокументированного trgm-гейтинга).

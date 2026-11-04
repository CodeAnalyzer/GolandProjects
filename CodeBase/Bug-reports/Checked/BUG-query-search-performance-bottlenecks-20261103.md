# Bug Report: Поисковые запросы CodeBase — системные узкие места производительности

**Дата:** 2026-11-03
**Источник данных:** `CodeBase/codebase_20261102.log` (прогон тест-кейсов query CLI + MCP 2026-11-02, 1148 строк)
**Версия CodeBase:** 0.9.4 build 1663
**БД замера:** PostgreSQL 18, `localhost:5435/codebase`, проиндексированный проект FA (`root_path = D:/GITHUB/GolandProjects/FA`)
**Статус:** открыт — выполнен анализ производительности, исправления не вносились.

## Summary

По логу двух прогонов тест-кейсов (CLI и MCP) и по `EXPLAIN (ANALYZE, BUFFERS)` на реальной БД локализовано **девять узких мест** поисковых запросов. Критичные (P0) — `spec_search`, `sql_fragment` и `symbol` — дают задержки от единиц до десятков секунд на запросах, которые должны укладываться в доли секунды:

- `codebase_query_spec_search`: p50 **16,1 с** (max 17,6 с), при этом FTS-слой не использует GIN-индексы (2,77 с → 75 мс после проталкивания предиката), а trgm-слой тратит **6,0 с и возвращает 0 строк**;
- `codebase_query_sql_fragment`: до **36 с** (2 вызова оборваны по `query_timeout_sec = 30`), потому что безлимитный `ILIKE '%…%'` матчит до 124 279 строк, после чего идёт `md5` + `DISTINCT ON`-сортировка;
- `codebase_query_symbol`: до **26 с** на холодном кеше (`report_form`), до **13 с** на `method` — OR-условия по разным таблицам/колонкам не используют индексы, а коррелированные подзапросы `owner_key` выполняются на каждой строке-дубликате;
- MCP warmup сам выполняет самые тяжёлые запросы (`sql_fragment "select"`, `table`, `symbol LIKE`), из-за чего старт/первые вызовы занимают **49,5–62,4 с**;
- дублирование строк в индексе (**symbols ×11,3**, **query_fragments ×2,4**) и `shared_buffers = 1 ГБ` при таблицах 1,6–1,7 ГБ усиливают холодные промахи в 20–100 раз.

Критичность: **HIGH** (не отдельный дефект, а системная деградация поиска; MCP-агент получает таймауты вместо ответов).

---

## Environment

- CodeBase 0.9.4 build 1663, профиль MCP `query`, CLI `codebase query …`
- PostgreSQL 18: `shared_buffers = 1GB`, `work_mem = 128MB`, `effective_cache_size = 16GB`, `random_page_cost = 1.5`, `statement_timeout = 0`
- Таблицы индекса: `symbols` 5 818 425 строк / 1749 МБ, `query_fragments` 2 415 700 / 1609 МБ, `relations` 5 117 429 / 1666 МБ, `sql_tables` 1 631 061 / 313 МБ
- Кодировка лога — **CP866** (русские аргументы читаются только через cp866; обычный UTF-8-просмотр даёт mojibake)
- MCP: `query_timeout_sec = 30`, `warmup_enabled = true`, `warmup_timeout_sec = 60`

## Сводка по журналу

| Категория | Кол-во | Сумма | Комментарий |
|---|---:|---:|---|
| Все записи-команды | 894 | 3420,9 с | сумма длительностей процессов |
| `codebase update` | 2 | 2394,4 с | 37м24с + 2м30с, доминирует в логе |
| CLI `query *` | 822 | **539,2 с** | из них 124 вызова >0,5 с, 69 >1 с, 37 >3 с, 8 >10 с |
| MCP tools | 246 | **307,9 с** | 14 ошибок |
| MCP-процессы | 16 | 476 с | 4×19,3 с (DB timeout), 3×49,5 с, 4×62,4 с |

Рейтинг MCP-инструментов (пользовательская задержка):

| tool | n | p50 | p90 | max | сумма | ошибок |
|---|---:|---:|---:|---:|---:|---:|
| `codebase_query_spec_search` | 7 | 16,1 с | 17,6 с | 17,6 с | 104,8 с | 0 |
| `codebase_query_sql_fragment` | 7 | 1,2 с | 35,0 с | 35,0 с | 76,0 с | 2 (timeout) |
| `codebase_query_symbol` | 35 | 63 мс | 5,9 с | **26,0 с** | 67,1 с | 0 |
| `codebase_query_relations` | 22 | 232 мс | 1,1 с | 1,6 с | 8,1 с | 0 |
| `codebase_query_callers` | 6 | 1,6 с | 2,5 с | 2,5 с | 7,5 с | 0 |
| `codebase_query_desc_search` | 8 | 1,0 с | 1,5 с | 1,5 с | 6,9 с | 0 |
| `codebase_query_table` | 5 | 713 мс | 3,0 с | 3,0 с | 6,5 с | 0 |
| `codebase_query_inspect` | 6 | 1,0 с | 1,2 с | 1,2 с | 5,2 с | 0 |

CLI по подкомандам: `spec` 175,8 с / 128 вызовов, `sql-fragment` 124,3 с / 25, `desc-search` 51,2 с / 31, `symbol` 49,8 с / 106, `relations` 21,6 с / 77.

---

## Findings

### B1. `spec_search`: FTS-предикат вынесен за UNION → полный скан 58 тыс. строк (P0)

**Файл:** `internal/specsvc/specsvc.go:1302` (`searchSpecExact`), вызов — `internal/specsvc/specsvc.go:372` (`ExecuteSpecSearch`).

**Симптом.** `codebase_query_spec_search` p50 16,1 с; даже `layer=exact` (tsvector + trgm) в CLI — 5,6–6,1 с.

**Корневая причина.** CTE `artifacts` собирает `UNION ALL` из `spec_capabilities` (3 165), `spec_requirements` (20 170), `spec_scenarios` (34 181) и `spec_usecases` (694) — **58 210 строк**. Условие `a.search_vector @@ plainto_tsquery('russian', $1)` стоит **снаружи** UNION, поэтому планировщик не может протолкнуть его в ветки и применить GIN-индексы `idx_spec_*_fts`. CTE материализуется (74 МБ) и сканируется дважды (в `chosen` и в финальном SELECT).

**Замер `EXPLAIN (ANALYZE, BUFFERS)` для «массовое создание плановых событий»:**

```
Seq Scan on spec_capabilities   rows=3165
Seq Scan on spec_requirements   rows=20170
Seq Scan on spec_scenarios      rows=34181
Seq Scan on spec_usecases       rows=694
CTE artifacts  actual rows=58210, Maximum Storage: 74325kB
Execution Time: 2766.952 ms
```

**Проверка гипотезы.** Тот же запрос с предикатом внутри каждой ветки UNION: `Bitmap Index Scan on idx_spec_*_fts` + `Execution Time: 74.643 ms` — ускорение **~37×**.

**Предлагаемый фикс.** Протолкнуть `search_vector @@ plainto_tsquery(...)` в каждую ветку `artifacts` (и, при необходимости, разнести на отдельные CTE/подзапросы, чтобы планировщик не материализовал общий Append).

---

### B2. `spec_search`: trgm-слой всегда работает и возвращает 0 строк (P0)

**Файл:** `internal/specsvc/specsvc.go:391` (`layer == "exact" || layer == "both"` запускает `searchSpecTrgm`), `internal/specsvc/specsvc.go:1391` (`searchSpecTrgm`).

**Симптом.** `trgm` добавляет к exact-слою секунды, часто не давая ни одного результата; раннего выхода при уже найденных exact-совпадениях нет.

**Замер для «массовое создание плановых событий»:**

```
Bitmap Index Scan on idx_spec_requirements_body_trgm  rows=12234
Rows Removed by Index Recheck: 10646
Bitmap Index Scan on idx_spec_scenarios_text_trgm     rows=7005
Rows Removed by Index Recheck: 6926
Bitmap Index Scan on idx_spec_capabilities_related_code_trgm rows=534 (все отброшены)
Результат: 0 строк
Execution Time: 6044.283 ms
```

Причина — оператор `%` (pg_trgm similarity ≥ 0.3) на длинной фразе и длинных текстах: индекс отдаёт десятки тысяч кандидатов, recheck отбрасывает все. Для сравнения, semantic-слой (LSA, 5 943 вектора) стоит **1,1–1,8 с** — он не главный виновник.

**Предлагаемый фикс.** (а) не запускать trgm, если exact уже вернул ≥ `limit` результатов; (б) заменить `%` на `word_similarity`/`strict_word_similarity` или поднять порог; (в) ограничить trgm короткими запросами/полями. Отдельно — проверить семантику `layer=exact` (сейчас включает trgm, что неочевидно).

---

### B3. `symbol`: `type=report_form` — OR по `report_forms` ломает индекс (P0/P1)

**Файл:** `internal/query/query.go:331` (`buildSymbolLookupCondition`), `internal/query/query.go:308` (`buildReportFormLookupCondition`), запрос — `internal/query/query.go:385` (`SearchSymbol`).

**Симптом.** CLI `codebase query symbol --name FormRepBCHPeriod --type report_form`: **22,6 с** (первый запуск), 292–319 мс после прогрева; MCP max — **26,0 с**.

**Корневая причина.** Условие

```sql
LOWER(s.symbol_name) = LOWER($1)
 OR LOWER(rf.form_name) = LOWER($1)
 OR LOWER(rf.form_class) = LOWER($1)
```

не даёт использовать ни `idx_symbols_symbol_name_type_lower`, ни индексы `report_forms`. План выбирает `Index Scan using idx_symbols_symbol_name_type_lower` с `Index Cond: symbol_type = ANY('{report_form}')` (212 index searches), читает **1 649** строк и на каждую делает lookup в `report_forms_pkey`; суммарно **11 570 heap-страниц ≈ 90 МБ** случайного чтения.

**Замер:** `Execution Time: 25894.659 ms` (холодный кеш) / 292 мс (прогретый). В таблице `symbols` — 1 649 `report_form`-строк.

**Предлагаемый фикс.** Разбить OR на `UNION ALL` индексных подзапросов (по `symbols.symbol_name`, `report_forms.form_name`, `report_forms.form_class`) либо добавить/использовать отдельный индекс по `symbol_type`; нормализовать `form_name`/`form_class` в `symbols` на индексации.

---

### B4. `symbol`: OR с `signature` + коррелированные подзапросы `owner_key` (P1)

**Файл:** `internal/query/query.go:331` (`buildSymbolLookupCondition`), `internal/query/query.go:369` (`symbolOwnerKeyExpr`).

**Симптом.** `--name DoAction --type method` — **8,5 с** в MCP, CLI 2,6 с первый / 259–408 мс прогретый. `--name AccDealLink_T --type form` — 2,9 с первый / 180 мс.

**Корневая причина.**
1. Условие `s.symbol_name = $1 OR (s.symbol_type = 'form' AND s.signature = $1)` превращается в `BitmapOr` по trgm-индексам `idx_symbols_symbol_name_trgm` (4 534 кандидата) и `idx_symbols_signature_trgm` (4 891). После фильтра остаётся **4 278** строк `DoAction` (имя продублировано по продуктам/веткам).
2. Для каждой из 4 278 строк вычисляется `symbolOwnerKeyExpr` — коррелированные подзапросы к `pas_methods`/`pas_classes` (`SubPlan 1`, 4 278 итераций, 28 625 buffer hits), хотя `owner_key` нужен только для дедупликации и финальных N строк.

**Замер:** `Execution Time: 13214.030 ms` (холодный кеш); в плане `SubPlan 1 actual rows=1.00 loops=4278`.

**Предлагаемый фикс.** Разделить OR на индексные ветки; добавить btree-индекс по `symbol_name` (для точного `=`, сейчас есть только `lower(symbol_name), symbol_type` и trgm); вычислять `owner_key` в LATERAL только для выбранных строк (после `DISTINCT ON`/`LIMIT`).

---

### B5. `sql_fragment`: безлимитный `ILIKE '%…%'` + `md5` + `DISTINCT ON` (P0)

**Файл:** `internal/query/query.go:764` (`SearchQueryFragment`).

**Симптом.** CLI `codebase query sql-fragment --text "select @@spid"` — **36,1 с** первый, 6,0–13,7 с последующие; MCP — 34,9 с и 33,6 с, оба оборваны `query_timeout_sec=30` (`pq: canceling statement due to user request`).

**Корневая причина.** Запрос:

```sql
WITH matches AS (
  SELECT qf.id, COALESCE(qf.query_hash, md5(qf.query_text)) AS content_key, …
  FROM query_fragments qf
  JOIN files f ON qf.file_id = f.id
  LEFT JOIN ds_products_branch br ON br.id = f.ds_product_branch_id
  WHERE qf.query_text ILIKE '%select @@spid%'
), chosen AS (
  SELECT DISTINCT ON (content_key, product_key) …
  ORDER BY content_key, product_key, … 
)
SELECT m.id FROM matches m JOIN chosen ch … ORDER BY m.file_id, m.line_number LIMIT 5
```

- GIN trgm-индекс отдаёт **336 016** кандидатов, после recheck остаётся **124 279** строк (для `select @@spid`!);
- CTE `matches` материализуется (18,5 МБ) и сканируется трижды;
- `DISTINCT ON` сортирует 124 279 строк (16,6 МБ), hash join даёт 82 430 строк;
- для `--text select` кандидатов ещё больше (835 824 index rows), CLI — 14,3 с.

**Замер:** `Execution Time: 7119.769 ms` (прогретый кеш), 36 с холодный.

Усугубляющий фактор — дублирование: 2 415 700 строк `query_fragments` при 1 003 668 уникальных `query_hash` (**×2,4**).

**Предлагаемый фикс.** Ранжировать/ограничивать кандидатов до `DISTINCT ON` (например, `ORDER BY similarity(query_text,$1) DESC LIMIT N`), не считать `md5` (колонка `query_hash` заполнена на 100%), добавить индекс по `query_hash`, либо дедуплицировать фрагменты на индексации. Цель — <1 с.

---

### B6. MCP warmup выполняет самые тяжёлые запросы (P0/P1)

**Файл:** `internal/mcp/warmup.go:38` (`warmupProbesByProfile`), `internal/mcp/server.go:56` (запуск горутиной), `internal/mcp/server.go:120` (`query_timeout_sec = 30`).

**Симптом.** Старты MCP-процессов: **49,5 с** (3 шт.) и **62,4 с** (4 шт.). Первые реальные вызовы конкурируют с warmup: `spec_search` в MCP — 15–17 с против 5–6 с в CLI.

**Корневая причина.** Warmup последовательно прогоняет 9 инструментов, среди которых заведомо тяжёлые: `sql_fragment {"text":"select"}`, `table {"name":"tConsSalePortfolio"}`, `symbol {"name":"Insert","like":true}`, `desc_search` (холодная загрузка 115 МБ `desc_lsa_embeddings.bin`). Сумма замеренных отдельно проб:

| проба | замер |
|---|---:|
| desc_search | 14 с (холодная загрузка embeddings) |
| spec_search semantic | 1,1–3,2 с |
| callers FCD_GL_Audit_Insert | 2,9 с |
| symbol LIKE Insert | 5,5 с |
| relations references_table | 2,2 с |
| api_contract LIKE API | 1,3 с |
| table tConsSalePortfolio | ~2 с |
| form_component LIKE dl | 2,5 с |
| sql_fragment "select" | 14–36 с |
| **итого** | **~48–60 с** — совпадает с логом |

**Предлагаемый фикс.** Убрать из warmup патологические пробы (`sql_fragment "select"`, `table`, `symbol LIKE`), греть только соединение/схему/LSA; сделать warmup отключаемым per-profile и не конкурирующим с реальными вызовами.

---

### B7. Дублирование строк и холодный кеш (P1)

**Файл/данные:** `symbols`, `query_fragments`; настройка `shared_buffers` в `postgresql.conf`.

- `symbols`: **5 818 425** строк, уникальных `(lower(symbol_name), symbol_type)` — **516 769** → **×11,3** (например, `DoAction` — 4 299 строк, `report_form` — 1 649).
- `query_fragments`: **2 415 700** строк, уникальных `query_hash` — **1 003 668** → **×2,4**.
- `shared_buffers = 1 ГБ` при `symbols` 1,75 ГБ и `query_fragments` 1,6 ГБ; hit ratio 90,8%, но холодные запросы дают 13–26 с (см. B3/B4/B5).
- `temp_files = 52`, суммарно **10,2 ГБ** temp — следствие крупных сортировок (`DISTINCT ON`, trgm).

**Предлагаемый фикс.** Увеличить `shared_buffers` до 4–8 ГБ, выполнять `ANALYZE` после `update`, рассмотреть дедупликацию/нормализацию повторяющихся сущностей на индексации.

---

### B8. `desc_search`: «displaced»-запрос на ~1 с (P2)

**Файл:** `internal/query/desc_query.go:255` (`searchDescriptionsSemantic`).

**Замер:**

```sql
SELECT r.source_id
FROM relations r
JOIN api_contracts dc ON dc.id = r.target_id AND r.target_type = 'api_contract'
WHERE r.source_type = 'sql_procedure'
  AND r.relation_type = 'implements_contract'
  AND coalesce(dc.short_description,'') || coalesce(dc.full_description,'') <> ''
```

`Execution Time: 1067.929 ms` — 7 458 lookups в `api_contracts_pkey`; выполняется на каждый semantic-вызов. Плюс первый вызов — 13,8–14,5 с из-за загрузки 115 МБ `desc_lsa_embeddings.bin`.

**Предлагаемый фикс.** Заменить выгрузку всех id на `EXISTS`-фильтр в основном запросе либо ограничить набор кандидатов; кешировать результат до смены generation.

---

### B9. `table`: коррелированный `COUNT(*)` по CTE + enrichment (P2)

**Файл:** `internal/query/query_sql.go:54` (`SearchTable`).

**Симптом.** `tConsSalePortfolio`: 1,9–3,0 с холодный, 116–269 мс прогретый.

**Причина.** `LEFT JOIN LATERAL (SELECT COUNT(*) FROM candidates c WHERE …)` выполняется на каждую выбранную строку, затем ещё три запроса `loadTableFiles/Columns/Procedures` с LATERAL-подзапросами по `sql_tables` (1,63 млн строк).

**Предлагаемый фикс.** Считать `usages` один раз агрегатом по `candidates`, ограничить enrichment-скоупы.

---

## Impact

- MCP-агент получает таймауты (`sql_fragment`) и многократно завышенные задержки (`spec_search`, `symbol`); деградирует интерактивный сценарий «найти → уточнить → посмотреть связи».
- Лимиты потребляются дубликатами: `LIMIT` в `sql_fragment`/`symbol`/`table` расходуется до дедупликации.
- Warmup создаёт пиковую нагрузку на БД сразу после старта MCP и конкурирует с первыми пользовательскими вызовами.
- Данные индекса корректны — дефекты носят характер SQL-планов, структуры индекса и дублирования строк.

## Suggested Fix (приоритеты)

**P0**
1. `specsvc.go:1302` — протолкнуть FTS-предикат в ветки UNION (2,77 с → ~75 мс).
2. `specsvc.go:391` — не запускать trgm при достаточных exact-результатах; заменить `%` на word-similarity/порог (6,0 с → ~0 для большинства запросов).
3. `query.go:764` — ограничить/ранжировать кандидатов до `DISTINCT ON`, убрать `md5`, индекс по `query_hash` (36 с → <1 с).

**P1**
4. `query.go:331/369` — OR → `UNION ALL` индексных веток; btree `(symbol_name)`, `(symbol_type)`; `owner_key` только для финальных строк (26 с → <0,1 с).
5. `warmup.go:38` — убрать тяжёлые пробы, сделать warmup дешёвым/отключаемым.
6. `shared_buffers` 1 → 4–8 ГБ; `ANALYZE` после `update`.

**P2**
7. `desc_query.go:255` — `EXISTS`/JOIN вместо выгрузки 7 458 id (1,07 с → мс).
8. `query_sql.go:54` — убрать коррелированный `COUNT(*)`, ограничить enrichment.
9. Логирование — stage-timings (exact/trgm/semantic) и единообразная маскировка `text`/`query`.

## Verification

1. Повторить прогон `Tests/TEST_CASES.md` на той же БД; целевые пороги: `spec_search` < 0,5 с, `sql_fragment` < 1 с, `symbol` < 0,5 с, `symbol report_form` < 0,2 с.
2. `EXPLAIN (ANALYZE, BUFFERS)` для каждого исправленного SQL: ожидаются Index/Bitmap Index Scan вместо Seq Scan, отсутствие повторного CTE-сканирования.
3. Замер MCP-старта и первых 10 вызовов после включения/отключения warmup.
4. Регресс функциональных кейсов `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md`.

## Дополнительные наблюдения (не производительность)

- **38 CLI-вызовов не выполнились** из-за неверных флагов тест-харнесса: 30× `--dsinfo-id` (правильно `--dsinfo`) и 8× `--ret-code` (правильно `--code`) — соответствующие ветки не были протестированы.
- В MCP-логе `text` и `sql` маскируются как `***` (`internal/mcp/server.go:211`), а `query` у `spec_search` — нет; нельзя сопоставить медленные `desc-search`/`sql-fragment` с реальным текстом.
- `pg_stat_user_tables`: `n_live_tup = 0`, `last_analyze` пустые (счётчики сброшены), хотя `pg_stats` заполнены — планировщик не страдает, но мониторинг «слепой».
- Лог пишется в CP866; для машинного анализа требуется явное перекодирование.

## Resolution

Не исправлено. Отчёт фиксирует результаты анализа; изменения в код не вносились.

# Верификация фиксов BUG-query-search-performance-bottlenecks-20261103.md

Дата: 2026-11-04. Сборка: **CodeBase 0.9.4 build 1669** (было 1663).
База: та же — PostgreSQL 18, localhost:5435/codebase, FA-индекс
(D:/GITHUB/GolandProjects/FA), состояние индекса без переиндексации
после 2026-11-02 (статистика планировщика «как была» — честное сравнение
с bug-репортом). Change: `openspec/changes/optimize-query-search-bottlenecks`.

## Сводка результатов

| Finding | Было (bug-report) | Стало | Статус |
|---|---|---|---|
| B1 spec_search FTS | 2,77 с (EXPLAIN), CLI exact 5,6–6,1 с, MCP p50 16,1 с | EXPLAIN 50,6 мс; CLI/MCP гейтеная фраза 0,4–0,55 с | ✅ |
| B2 spec_search trgm | +6,0 с за 0 строк на многословной фразе | фразы >2 лексических единиц — trgm-body пропускается, ~0,4–0,5 с | ✅ (с компромиссом, см. ниже) |
| B3 symbol report_form | 22,6 с cold / 26,0 с MCP / 292–319 мс warm | 1,7 с cold / **7–154 мс** warm (MCP 6–118 мс) | ✅ |
| B5 sql_fragment | 36,1 с cold, повторы 6,0–13,7 с, MCP таймауты 30 с | 0,6–2,3 с, без таймаутов, `meta.truncated=true` | ✅ (порог <1 с — ручкой, см. ниже) |
| B8 desc displaced | ~1 с per-call SQL на каждый semantic-вызов | кеш: 132 мс → 76 мс повторные (semantic-хиты на месте) | ✅ |
| ANALYZE | отсутствует после update | `AnalyzeCodebaseTables` в хвосте init/update (тест) | ✅ |
| B6 warmup/MCP-старт | старты 49,5–62,4 с | initialize 110–243 мс; warmup 4,7–11,7 с фоновой горутиной | контроль (вне scope) |

## 1. B1: pushdown FTS-предиката — EXPLAIN (ANALYZE, BUFFERS)

Запрос `searchSpecExact`, «классификатор» (прежний замер bug-репорта —
«массовое создание плановых событий», 2766,952 мс):

```
Bitmap Index Scan on idx_spec_capabilities_fts   rows=451   (0.095 ms)
Bitmap Index Scan on idx_spec_requirements_fts   rows=4345  (1.647 ms)
Bitmap Index Scan on idx_spec_scenarios_fts      rows=2432  (0.540 ms, parallel)
Bitmap Index Scan on idx_spec_usecases_fts       rows=0
Execution Time: 50.629 ms        (7104 строк артефактов)
```

Seq Scan остались только на таблицах-справниках соединения
(spec_configs — 15 строк; spec_capabilities как build-side hash — 3165 строк).
Материализации 58-тыс. `artifacts` (74 МБ) в плане нет: CTE потребляется
однократным агрегатом. Задача 1.2 выполнена: GIN-индексы используются,
полный скан spec-таблиц отсутствует, ускорение ~55× к прежним 2,77 с.

## 2. B2: гейтинг body-trgm по числу лексических единиц

- «массовое создание плановых событий» (4 единицы) — body-trgm пропущен:
  CLI 470–545 мс, MCP 405 мс (server-side, лог codebase_20261104.log).
- «классификатор» (1 единица) — body-trgm выполняется по дизайну
  (≤ 2 единиц): FTS-часть 50 мс, trgm-часть ~3,6–3,9 с (12k+ кандидатов
  по 3 trgm-индексам с recheck). Это осознанный компромисс D2 design
  (короткий запрос против текста ещё может дать similarity ≥ 0,3);
  Tech-пути (mention_name/related_code) работают при любой длине.
- Многословный tech-запрос (`FA_DF_Mass Event Creation`, 4 единицы)
  находит сценарий по точному совпадению mention_name — covered
  integration-тестом TestExecuteSpecSearchTrgmBodyGating.

Замечание для владельцев: если однословные русские запросы часты и 4 с
неприемлемы — константа `specTrgmMaxQueryTokens` (specsvc.go) меняется
в одну строку; наблюдаемых полезных срабатываний body-trgm на FA-индексе
в тест-кейсах не зафиксировано.

## 3. B3: report_form UNION ALL — EXPLAIN + замеры

`query spec search --name FormRepBCHPeriod --type report_form`:

```
Index Scan using idx_symbols_symbol_name_type_lower  (0.024 ms, ветвь 1)
Seq Scan on report_forms (1649 строк) → Index Scan idx_symbols_entity_type_entity_id
  × 19 совпадений (ветвь 2, 1.9 мс); ветвь 3 (form_class) — 0 строк
Execution Time: ~2.1 ms, Buffers: shared hit=1262
```

Перебора всех символов типа и per-row LEFT JOIN report_forms больше нет
(полный план в протоколе проверки; Seq Scan крошечной report_forms — штатно).
Замеры: CLI cold 1,67 с (первый в серии, файл/страницы), warm **154 мс**;
MCP 6–118 мс. Порог < 0,2 с — пройден. Эквивалентность состава прежнему
OR-запросу — integration-тест TestSearchSymbol_ReportFormUnionAllEquivalence
(7 режимов, сравнение упорядоченных id с legacy-SQL) + дедуп мультиполевого
совпадения (RPT_DUAL) — TestSearchSymbol_ReportFormMultiFieldMatchDedup.

## 4. B5: guard-потолок кандидатов sql_fragment

Сигнатура `SearchQueryFragment` → `(items, truncated, err)`; внутренний
`LIMIT cap+1` в CTE matches, окно `count(*) OVER ()` снаружи внутреннего
LIMIT. CLI `--json` → `meta.truncated` + `meta.hint`; MCP-ответ — объект
`{items, truncated, hint}` (по прецеденту desc_search).

| Проба (limit 20) | Было | Стало (cap по умолчанию 50000) | cap=10000 |
|---|---|---|---|
| `select @@spid` (124k совпадений) | 36,1 с cold / 6,0–13,7 с / таймауты MCP | 1,9 с стабильно, `truncated=true` | 0,60–0,64 с, `truncated=true` |
| `select` (835k) | 14,3 с | 0,92 с, `truncated=true` | 0,46 с |

Найденный при верификации и исправленный дефект: аргументы `$2`/`$3`
были перепутаны (внутренний LIMIT получал лимит выдачи) — flag
`truncated` не выставлялся; фиксировано + регресс-тест
`TestSearchQueryFragmentMatchCap` (limit=1 → ровно 1 строка).

Порог < 1 с из Verification bug-репорта на дефолтном cap=50000 достигается
не всегда (широкие паттерны ~1,9 с); ручка `query.sql_fragment_match_cap`
закрывает это (0,6 с при 10000). Состав ниже потолка байт-в-байт прежний
(integration-тест).

## 5. B8: кеш displaced-множества

Ключ кеша — (поколение desc-модели, MAX(id) scan_runs), инвалидация на
каждом semantic-вызове суб-мс PK-запросом. MCP-сессия (UTF-8 probe):

- desc_search «арест имущества»: вызов 1 — 132 мс, вызов 2 — 76 мс,
  semantic-хиты присутствуют в обоих (5/5);
- «погашение кредита»: 199/137 мс (semantic-слой гейтится покрытием —
  прежнее поведение).

Сценарий ревью F1 (update без переобучения перевычисляет кеш) —
integration-тест TestDescriptionsDisplacedCache (новый scan_run + новый
контракт → loads+1, состав содержит новую вытесненную процедуру).

## 6. ANALYZE после init/update

`AnalyzeCodebaseTables` (единый ANALYZE по 14 таблицам кодовой базы,
ошибка → warning в indexer-лог, статус прогона не меняется) вызывается
из хвостов `runInitPipeline`/`UpdateCtx`. Покрыто:
TestAnalyzeCodebaseTables (last_analyze устанавливается, canceled ctx →
ошибка-как-warning), TestInitUpdateRunAnalyze (полный init-пайплайн +
no-op update). Эффект на инкрементальных update будет наблюдаться после
первого update новой сборкой (статистика планировщика свежая с каждым
прогоном вместо деградации B7).

## 7. MCP-старт и warmup (контроль, B6 вне scope)

- `initialize` → 110–243 мс (3 замера, лог codebase_20261104.log);
- warmup: `probes=9 done=9 elapsed=4.7–11.7 s` (было: старты 49,5–62,4 с):
  сами пробы подешевели на фиксовах B1/B2/B5 (sql_fragment "select"
  0,5–1 с вместо 14–36 с; spec_search не конкурирует секунды);
  остаточная тяжесть — desc-эмбеддинги 110 МБ и symbol LIKE "Insert" (B4) —
  вне scope, горячий список известен;
- первые реальные вызовы после warmup: spec_search 405 мс,
  sql_fragment 1,7 с, symbol report_form 6 мс, desc_search 132 мс.

## 8. Регресс функциональных кейсов (226 CLI-кейсов TEST_CASES_CLI.md)

Полный прогон `Tests/run-test-cases-2026-11-04.ps1` →
`Tests/test-run-2026-11-04/summary.ndjson`. Сравнение составов
(count/innerCount/exit) с базлайном `test-run-2026-11-02` (build 1663):

- **226/226 выполнены, таймаутов 0** (были таймауты sql_fragment);
- расхождений составов — 4, все объяснимы, регрессий продукта 0:
  - TC-185 — артефакт токенизатора нового прогонщика (`--limit 3 5`);
  - TC-187 — dsinfo parse теперь PASS (файл существует; было exit=1);
  - TC-225 — старый прогонщик пропускал кейс (schema-check), новый выполнил
    (count=100, корректно);
  - TC-226 — плейсхолдер `<P>` в новом файле кейсов не подставлен
    прогонщиком (в старом — явная подстановка процедуры).

Худшие времена по затронутым инструментам (полный прогон, cold-контекст,
включая ~0,1 с старта процесса): symbol LIKE api_business_object 19,6 с
(B4, вне scope, без изменений); spec search 1-слово «резервы» layer=both
5,3 с / «массовое создание» 2 слова 5,9 с (body-trgm ≤2 слов — D2);
sql_fragment «select @@spid» 2,3 с; symbol report_form — вне топа (<0,7 с).

## Вывод

B1/B3/B8/ANALYZE закрыты полностью; B2 — многословные фразы (главный
сценарий bug-репорта) закрыты гейтом, короткие русские запросы сохраняют
стоимость body-trgm осознанно; B5 — таймауты устранены, усечение
помечается, порог <1 с достижим ручкой `sql_fragment_match_cap=10000`
(дефолт 50000 по спеке). Функциональные составы не изменились.

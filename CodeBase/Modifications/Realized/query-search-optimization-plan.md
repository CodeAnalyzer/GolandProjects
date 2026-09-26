# План оптимизации поисковых запросов CodeBase (P1–P7)

Бэклог по итогам анализа статистики использования `Logs/WORK-LOG` (78 дней, 2673 сессии, 1048 вызовов инструментов: query 562, trc 349, rti 132, review 5) и чтения слоя `internal/query` (5 файлов, ~2700 строк).

Источник данных — лог-файлы с рабочего ПК, формат `key=value` (`profile=… tool=… args=… duration_ms=… status=… error=…`).

> **Оговорка к цифрам.** Лог фиксирует латентность на уровне MCP-тула, а не SQL-плана. Аргументы логируются неполно (`formatToolArgs` пишет только один аргумент, см. A1), поэтому группировать по аргументам нельзя. Значения ниже — это tool-level оценки, план запроса подтверждается отдельно через `EXPLAIN`.

## Статус пунктов

| # | Пункт | Где | Статус |
|---|---|---|---|
| P1 | `query relations` по имени (OR-EXISTS) | `internal/query/query_relations.go` | **Заархивировано** `changes/archive/2026-09-25-optimize-relations-name-search/` |
| P2 | Детализация связей 44 JOIN + fan-out inspect | `query_relations.go:520`, `querysvc/inspect.go` | Warm 6.9ms; 6× амплификация буферов (cold) — опционально |
| P3 | `query sql-fragment --text` (substring + ORDER BY) | `query.go:547` | **Закрыт** — не воспроизводится (см. ниже) |
| P4 | `query callers` (ILIKE + joins) | `query_sql.go:279` | **Закрыт** — warm 77–204ms |
| P5 | `query symbol` / `query content` (OR ILIKE) | `query.go:276`, `query.go:357` | **Закрыт** — warm 76–81ms |
| P6 | `query table` / `table-index` (GROUP BY + оконные догрузки) | `query_sql.go:14` | **Закрыт** — warm 83–94ms |
| P7 | `query procedure`: `sql: no rows` как ошибка | `query_sql.go:479`, `query_sql.go:508` | **Реален** (корректность) — следующий |
| A1 | Неполное логирование аргументов MCP | `mcp/server.go:189` | **Реален** — нужен для метрик |
| A2 | `codebase_stats` медленный / таймаут | `internal/store/db_stats.go` | **Реален: 14.8s** — 40× `COUNT(*)` |

Сводка латентности query-профиля из `WORK-LOG`:

| tool | n | total | avg | max |
|---|---|---|---|---|
| `codebase_query_sql_fragment` | 72 | 26.0s | 361ms | 1412ms |
| `codebase_query_symbol` | 151 | 12.6s | 83ms | 996ms |
| `codebase_query_inspect` | 5 | 9.5s | 1909ms | 9058ms |
| `codebase_query_relations` | 12 | 7.2s | 597ms | 2821ms |
| `codebase_stats` | 1 | 5.2s | 5231ms | — |
| `codebase_query_table_index` | 28 | 3.5s | 124ms | 297ms |
| `codebase_query_table` | 19 | 3.3s | 174ms | 1037ms |
| `codebase_query_procedure` | 87 | 2.9s | 34ms | 209ms |
| `codebase_query_callers` | 30 | 1.8s | 61ms | 350ms |

**Warm-верификация на реальном индексе (2026-09-25), локальная копия:**

| # | Замер (warm) | Вердикт |
|---|---|---|
| P1 | 26 484ms → 1 013ms (26×) | реален, исправлен и заархивирован |
| P2 | детализация **6.9ms** / 6 425 буферов (против 1 010 у точечных lookup'ов) | warm не болит; 6× буферы |
| P3 | SQL 15ms, CLI 67ms | не воспроизводится |
| P4 | 77–204ms | не воспроизводится |
| P5 | 76–81ms | не воспроизводится |
| P6 | 83–94ms | не воспроизводится |
| A2 | **14 814ms** | **реален и воспроизводим** |

> Воспроизводятся только **алгоритмические** дефекты (P1) и `stats` (A2). Latency-пункты P2–P6 на прогретой БД — десятки-сотни ms; секундные значения в `WORK-LOG` — холодный кэш/окружение рабочего ПК. Отличить одно от другого в логах нельзя из-за **A1** (логируется один аргумент) и tool-level duration.

> **Мина `IN (подзапрос)`.** Если детализацию собрать как `... WHERE r.id IN (SELECT …)`, планировщик материализует 44-join по всей `relations` (3.8M строк) до фильтра — **19.4s / ~548k буферов** на 100 строк. Реальный код использует литеральный `id IN ($1..$n)`, поэтому не страдает. Это ограничение стоит зафиксировать: детализацию нельзя собирать через подзапрос.

---

## P1. `query relations` по имени (OR-EXISTS) — реализуется в текущем change

**Симптом.** `codebase_query_relations` до 2.8s на отдельных вызовах. Прошлый майский фикс (`Realized/relations-name-query-optimization-e2660c.md`) довёл name-only сценарии до 326–602ms, но оставил `EXISTS`-логику как fallback, и свежие данные показывают секундные значения на комбинациях, в тот быстрый путь не попавших.

**Причина.** `buildRelationAnyNameExistsCondition` (`internal/query/query_relations.go:185`) строит `OR` из 11 коррелированных `EXISTS` с `ILIKE '%…%'`; `SearchRelations` (`:220`) применяет быстрый двухпроходный путь только при условии «одно имя, другая сторона без имени и типа» (`:221`, `:224`). В общем случае — полное сканирование `relations` с коррелированными подзапросами.

**Предшественник.** `Realized/relations-name-query-optimization-e2660c.md` — добавлены exact-first резолв entity и `VALUES`-join, но только для узкого случая.

**Предлагаемое изменение.** Обобщить двухпроходный алгоритм на все комбинации фильтров: имя → `(entity_type, entity_id)` через индексированные lookup'ы, затем выборка `relations` по составному ключу. Удалить `buildRelationAnyNameExistsCondition` и `buildRelationNameExistsCondition`. Зафиксировать единую exact-first семантику.

**Change.** `openspec/changes/optimize-relations-name-search` (proposal + delta `query/api-relations-queries` + design + tasks). Реализация — на apply-этапе.

**Верификация.** `EXPLAIN` выборки id не содержит `Seq Scan on relations`; замер `codebase_query_relations` до/после; совпадение наборов id.

---

## P2. Детализация связей: 44 JOIN и fan-out inspect

**Симптом.** `codebase_query_inspect` — 5 вызовов, avg 1909ms, max **9058ms** (`name="CON_AfterACCR_MassProcess"`). `codebase_query_relations` — до 2821ms.

**Причина.** `relationSearchBaseQuery` (`internal/query/query_relations.go:520`) — 44 `LEFT JOIN` (по 22 на сторону: 11 таблиц сущностей + `files`). `RunInspectQuery` (`internal/querysvc/inspect.go:21`) для каждого из до 5 символов делает по 2 вызова `SearchRelationsByEntity` (incoming/outgoing), каждый из которых тянет эту детализацию — до 10 тяжёлых запросов.

**Предшественник.** `Realized/query-performance-optimization-e2660c.md` Приоритет 5 (частично: ограничение fan-out), отчёт признал inspect «почти достигнуто» (571ms), но свежие логи показывают секунды.

**Warm-проверка (2026-09-25).** Детализация для 100 исходящих рёбер `CON_AfterACCR_MassProcess` — **6.9ms**, но `Buffers: shared hit=6425`; вариант точечных lookup'ов — 1 010 буферов. Warm-латентность в норме, но запрос читает ~6× больше страниц; на холодном кэше 10 запросов `inspect` × 6425 страниц дают секунды из `WORK-LOG`. `inspect CON_AfterACCR_MassProcess` warm — 217ms (2 сущности: callback_event + procedure; у процедуры incoming=10, outgoing=100).

**Оценка.** Это не warm-latency баг, а **амплификация буферов** в cold-профиле. Приоритет понижен до опционального.

**Предлагаемые варианты.**
- (a) Джойнить только типы, реально присутствующие в выборке (не 22 вслепую).
- (b) Два прохода: `relations` → `(type,id)`, затем имена 11 маленькими `IN()`-lookup'ами.
- (c) Для inspect — batch: incoming/outgoing сразу для группы `(type, entity_id)` за 1–2 запроса.

**Верификация.** `EXPLAIN (ANALYZE, BUFFERS)` детализации — сравнивать **число буферов**, а не только время; замер `inspect` и `query relations --limit`.

---

## P3. `query sql-fragment --text` — закрыт: не воспроизводится

**Статус.** Закрыт (2026-09-25). На текущем коде и индексе проблема не воспроизводится, изменение не требуется.

**Симптом в логах.** `codebase_query_sql_fragment` — 72 вызова, 26s суммарно, avg 361ms, max 1412ms; отдельно `text="tDepartment"` — 1217ms.

**Проверка на реальном индексе** (1.7M строк, heap 761MB, avg `query_text` 351 симв., `pg_trgm` 1.6, `idx_query_fragments_query_text_trgm` на месте):

- `EXPLAIN (ANALYZE, BUFFERS)` запроса `query_text ILIKE '%tDepartment%' ORDER BY file_id, line_number LIMIT 100` → `Bitmap Heap Scan` + `Bitmap Index Scan on idx_query_fragments_query_text_trgm`, **15–17ms**.
- без `ORDER BY` — тот же bitmap, ~15ms.
- `component_name ILIKE '%tDepartment%'` — bitmap по `idx_query_fragments_component_name_trgm`, 8ms.
- CLI end-to-end (прогретый) — 67ms; диапазон query-команд 60–110ms.
- Селективность: `tDepartment` — 1008 из 1.7M (0.06%), `EventType` — 1345, `DepartmentID = 1` — 4.

**Вывод.** `SearchQueryFragment` (`internal/query/query.go:547`) уже использует trigram-индекс; `ORDER BY file_id, line_number` план не ломает. Логи `WORK-LOG` отражают другое окружение/время (крупнее БД на рабочем ПК, холодный кэш, старый бинарник) — точнее не различить, т.к. **A1** (логируется один аргумент) и tool-duration включает не только SQL. Майский отчёт уже помечал пункт достигнутым (295–500ms).

**Остаточный риск.** Для очень широкого паттерна (>несколько % строк) планировщик теоретически может предпочесть `Index Scan using idx_query_fragments_file_line` вместо bitmap; на реальных токенах кода не наблюдается.

---

## P4. `query callers` (ILIKE + joins)

**Симптом.** `codebase_query_callers` — 30 вызовов, 1.8s, avg 61ms, max 350ms.

**Причина.** `FindCallers` (`internal/query/query_sql.go:279`) — `UNION` с 9 `LEFT JOIN`; при `like` условие `sp_target.proc_name ILIKE $1` с ведущим `%` мешает индексу.

**Предлагаемое изменение.** Сначала резолвить id процедуры по индексу (`idx_sql_procedures_proc_name_lower`/trgm), затем выбирать `relations` по `idx_relations_target_type_id`; для точного имени не использовать `ILIKE`.

**Warm-проверка (2026-09-25).** `callers CON_AfterACCR_MassProcess` — 204ms, `callers FCD_Cons_GetRetMessage` — 77ms. Не воспроизводится.

**Верификация (если вернутся секунды).** `EXPLAIN` + замер `query callers` на рабочем ПК.

---

## P5. `query symbol` / `query content` (OR ILIKE)

**Симптом.** `codebase_query_symbol` — 151 вызов (самый частый), 12.6s суммарно, max 996ms.

**Причина.** `SearchSymbol` (`internal/query/query.go:276`) строит `OR` по `symbol_name`/`signature` и добавляет `ORDER BY symbol_name`; `SearchInContent` (`:357`) — `OR` по `symbol_name ILIKE` / `signature ILIKE` без `ORDER BY`.

**Предшественник.** `Realized/query-performance-optimization-e2660c.md` Приоритет 2; отчёт: `--like` не дотянули до <500ms.

**Предлагаемые варианты.** Для точного — `LOWER()=`; для like — `UNION ALL` вместо `OR` (чтобы планировщик мог использовать каждый trgm-индекс отдельно); при необходимости — FTS.

**Warm-проверка (2026-09-25).** `symbol --name tDepartment` — 81ms, `symbol --name Department --like` — 76ms. Не воспроизводится.

**Верификация (если вернутся секунды).** `EXPLAIN` (BitmapOr vs отдельные Index Scan); замер `--like` на рабочем ПК.

---

## P6. `query table` / `table-index` (GROUP BY + оконные догрузки)

**Симптом.** `codebase_query_table` — 19 вызовов, 3.3s, max 1037ms; `codebase_query_table_index` — 28 вызовов, 3.5s.

**Причина.** `SearchTable` (`internal/query/query_sql.go:14`) — `GROUP BY table_name, context ORDER BY usages DESC LIMIT` по частому имени агрегирует всю `sql_tables`, затем 3 догрузки с оконными функциями (`loadTableFiles`, `loadTableColumns`, `loadTableProcedures`).

**Предшественник.** `Realized/query-performance-optimization-e2660c.md` Приоритет 1; отчёт: смешанные результаты для больших таблиц.

**Предлагаемое изменение.** Сузить набор имён подзапросом до агрегации; проверить планы догрузок.

**Warm-проверка (2026-09-25).** `table --name tConsAnalyticsType` — 83ms, `table --name tConsAccountLink` — 94ms, `table-index --name tConsAccountLink` — 86ms. Не воспроизводится.

**Верификация (если вернутся секунды).** Замер `query table --name` на «тяжёлых» таблицах из отчёта на рабочем ПК.

---

## P7. `query procedure`: `sql: no rows` возвращается как ошибка

**Симптом.** 9 ошибок `query failed: sql: no rows in result set` в `WORK-LOG` от `codebase_query_procedure`.

**Причина.** `GetProcedureDetails` (`internal/query/query_sql.go:479`) и `GetProcedureResult` (`:508`) используют `QueryRowContext` и возвращают `sql.ErrNoRows` как ошибку. Спека `query/api-relations-queries` («Поведение при ошибках и пустых результатах API queries») требует пустой результат, а не ошибку.

**Предлагаемое изменение.** Обрабатывать `errors.Is(err, sql.ErrNoRows)` → возвращать «не найдено»/пустой результат с корректным MCP-ответом.

**Верификация.** Тест на отсутствующую процедуру; MCP-ответ `success` без `error`.

---

## Дополнительно (гигиена)

### A1. Неполное логирование аргументов MCP

`formatToolArgs` (`internal/mcp/server.go:189`) пишет **ровно один** аргумент: по приоритетному списку `name/procedure/text/event/table/type`, иначе — первый ключ из `map` (порядок в Go случаен). Из-за этого «одинаковые» `args=limit="10"` дают разброс 44ms…2821ms, а сгруппировать вызовы по реальным аргументам нельзя.

**Изменение.** Логировать все аргументы канонически (JSON), при необходимости маскировать значения. Без этого P1–P7 нельзя сопровождать метриками.

### A2. `codebase_stats` медленный — реален: 14.8s

**Симптом.** `stats` warm = **14 814ms** (2026-09-25); в `WORK-LOG` — 5231ms; MCP-вызов таймаутился. При этом `health` — единицы ms.

**Причина.** `GetStats` (`internal/store/db_stats.go:13`) выполняет ~40 отдельных `SELECT COUNT(*)` подряд по всем таблицам, среди них `relations` (~3.8M), `query_fragments` (1.7M), `files` (144k), плюс `COUNT(*)` по `files` с 13 `FILTER`. Каждый `COUNT(*)` — полный скан.

**Предлагаемые варианты.**
- (a) Оценка через `pg_class.reltuples` / `EXPLAIN` для крупных таблиц, точный `COUNT(*)` для мелких.
- (b) Один запрос `UNION ALL` (убирает round-trip'ы, но не сканы).
- (c) Инкрементальные счётчики, обновляемые индексатором (точно и дёшево; меняет схему/пайплайн).

**Верификация.** Замер `codebase_stats` до/после; расхождение оценки с точным `COUNT(*)` в допустимых пределах.

---

## Порядок работ (предложение)

1. **P1** — заархивировано (`archive/2026-09-25-optimize-relations-name-search/`).
2. **A2** — `stats` 14.8s, воспроизводимо: оценка через `reltuples`/счётчики (`internal/store/db_stats.go`).
3. **P7** — `no rows` → пустой результат (корректность, `query_sql.go:479/508`).
4. **A1** — логировать все аргументы MCP (`mcp/server.go:189`) — разблокирует атрибуцию метрик.
5. **P2** — опционально: 6× амплификация буферов в cold-профиле детализации.
6. **P3, P4, P5, P6** — закрыты: на текущем коде/индексе не воспроизводятся warm.

> **P3–P6** сняты: warm-замеры 15–204ms (см. таблицу верификации). Секунды в `WORK-LOG` объясняются холодным кэшем/окружением рабочего ПК.

## Связанные документы

- `Modifications/Realized/relations-name-query-optimization-e2660c.md` — предшественник P1.
- `Modifications/Realized/query-performance-optimization-e2660c.md` — предшественники P3/P5/P6, частично P2.
- `Modifications/Realized/query-performance-verification-report.md` — замеры прошлого раунда.
- `openspec/changes/archive/2026-09-25-optimize-relations-name-search/` — заархивированный change (P1).

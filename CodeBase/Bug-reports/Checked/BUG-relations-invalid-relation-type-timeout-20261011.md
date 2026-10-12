# Bug Report: relations — неверный relation_type не валидируется и уводит БД-запрос в таймаут

**Дата:** 2026-10-11
**Файл:** `internal/query/query_relations.go` (`SearchRelations`, строки 129–172; `buildRelationIDsQuery`, строки 179–233); `internal/store/db_schema.go` (индексы relations, строки 944–947; удалённый `idx_relations_relation_type` — строки 1028)
**Версия CodeBase:** 0.9.3
**Статус:** исправлено — change [`relations-relation-type-validation-and-index`](../openspec/changes/relations-relation-type-validation-and-index/) (реализован 2026-10-12, `openspec validate --strict` ok; ожидает архивации). Обнаружено при прогоне тест-кейсов `Tests/TEST_CASES.md` (TC-075), зафиксировано в `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` (разбор FAIL, п. 6).

## Summary

Инструмент `codebase_query_relations` при передаче несуществующего `relation_type` (например, `__no_such_relation__`) не отклоняет его на входе, а строит SQL `WHERE r.relation_type = $1 ORDER BY r.id DESC LIMIT $2`. По колонке `relation_type` **нет ведущего индекса** (`idx_relations_relation_type` удалён миграцией, `db_schema.go:1028`), поэтому запрос превращается в полный seq scan + sort большой таблицы `relations` и обрывается по контекстному таймауту: ошибка `pq: canceling statement due to user request` вместо быстрого `count=0` или ошибки валидации.

Критичность: **MEDIUM** (негативный сценарий вместо мгновенного пустого результата/ошибки висит до таймаута; timeout-контекст расходуется, агент получает криптичную ошибку pq).

Тот же дефект плана бьёт и по **валидным** type-only запросам (обратный PK-скан с
отбрасыванием 1.27M строк, ~210 мс warm) — см. раздел «Дополнение 2026-10-11» и
принятое решение (индекс `relations(relation_type, id DESC)`).

---

## Environment

- CodeBase 0.9.3, MCP-инструмент `codebase_query_relations`, CLI `codebase query relations`
- БД с проиндексированным проектом FA (таблица `relations` — миллионы строк)
- Прогон 2026-10-11: таймаут проявился при дефолтном QueryTimeoutSec

---

## Reproduction Steps

1. MCP: `codebase_query_relations` с `{"relation_type":"__no_such_relation__"}` (только relation_type, без имён).
2. CLI: `codebase query relations --relation-type __no_such_relation__ --json`.
3. Сравнение (работает быстро): `{"relation_type":"references_table","limit":10}` → count=10 (TC-065).

## Expected Result

- Либо strict-валидация `relation_type` по словарю известных типов связей (17 значений: `calls_procedure`, `dispatches_to`, `dispatches_to_subscriber`, `selects_from`, `inserts_into`, `updates`, `deletes_from`, `references_table`, `executes_query`, `builds_query`, `implements_contract`, `executes_contract`, `publishes_event`, `subscribes_to_event`, `has_field`, `has_param`, `uses_param`) → ошибка со списком валидных значений (по образцу strict-валидации `type` в `SearchSymbol` из change `symbol-type-vocabulary`);
- либо `count=0` за предсказуемое время.

## Actual Result

Ошибка через таймаут: `query failed: pq: canceling statement due to user request`.

---

## Root Cause Analysis

1. **Нет валидации аргумента.** `SearchRelations` (`query_relations.go:129-172`) проверяет только «хотя бы один фильтр задан» (`errs.ErrNoRelationFilters`); значение `relationType` проходит в SQL как есть (`buildRelationIDsQuery:217-221`: `r.relation_type = $N`). Несуществующее значение не отличается от редкого валидного — оба требуют просмотра таблицы.

2. **Нет индекса по `relation_type`.** Схема (`db_schema.go:944-947`) создаёт только составные индексы с ведущими `source_type`/`target_type`:
   - `idx_relations_source_type_id (source_type, source_id)`
   - `idx_relations_target_type_id (target_type, target_id)`
   - `idx_relations_source_type_relation (source_type, relation_type, source_id)`
   - `idx_relations_target_type_relation (target_type, relation_type, target_id)`

   Отдельный `idx_relations_relation_type` существовал, но **удалён** (`db_schema.go:1028`, `DROP INDEX IF EXISTS`) и в текущем создании отсутствует; в `schema_integration_test.go:109` всё ещё упоминается.

3. **Следствие для планов запросов.** Фильтр «только relation_type» → `WHERE r.relation_type = $1 ORDER BY r.id DESC LIMIT $2`: ни один составной индекс не применим (relation_type — не ведущая колонка) → seq scan всей `relations` + сортировка:
   - **валидный частотный тип** (`references_table`): скан находит достаточно строк, но всё равно читает/сортирует таблицу — быстро только из-за ранних совпадений и кэшей (TC-065 прошёл);
   - **несуществующий тип**: скан обязан прочитать **все** строки, не найдя ни одной → гарантированно максимальное время → отмена по ctx-deadline.

---

## Impact

- **MEDIUM:** негативный сценарий (опечатка в relation_type, эксперименты агента) вместо мгновенного ответа блокирует вызов до таймаута; ошибка pq неинформативна для агента.
- Паттерн распространяется на любые «редкие валидные» значения relation_type без имён сущностей (медленно, но не таймаут) — производительность relation_type-only запросов ниже возможной.
- Контраст с соседними инструментами: `symbol` после `symbol-type-vocabulary` строго валидирует `type` со списком валидных значений — `relations` не валидирует `relation_type`.

---

## Дополнение 2026-10-11: подтверждение на валидном (не негативном) кейсе

Разбор `codebase_20261011.log` (вызов `codebase_query_relations` с
`relation_type=inserts_into, source_type=sql_procedure, target_type=sql_table,
limit=10` → **5373 мс**) показал, что проблема **шире негативного сценария**: тот же
дефект плана воспроизводится на полностью валидном запросе и **не является
cold-cache** (в отличие от большинства «медленных» вызовов лога).

Замеры в DBeaver (`EXPLAIN (ANALYZE, BUFFERS)`, запрос инструмента
`SELECT r.id FROM relations r WHERE source_type=$1 AND target_type=$2 AND
relation_type=$3 ORDER BY r.id DESC LIMIT 10`):

| Прогон | Plan | Execution Time | Buffers |
|---|---|---|---|
| 1 | Index Scan Backward `relations_pkey` | 1782 мс | `hit=26750 read=6` |
| 2 | тот же | 210 мс | `hit=26756` |
| 3 | тот же | **212 мс** | `hit=26756` |

`read≈6` уже на 1-м прогоне → это не диск; warm-стоимость стабильна **~210 мс**
(первый прогон 1.78 с — разогрев плана/буферов). То есть 5.37 с в логе = cold
(≈210 мс warm) — сама warm-стоимость тоже структурно завышена.

Ключевая строка плана:

```
Index Scan Backward using relations_pkey on relations r
  Filter: (source_type='sql_procedure' AND target_type='sql_table' AND relation_type='inserts_into')
  Rows Removed by Filter: 1271674
```

Причина: запрос требует `ORDER BY r.id DESC LIMIT 10`, планировщик идёт **обратным
сканом по PK** и делает heap-fetch на каждую из **1 271 674** просмотренных
несовпадающих строк, пока не наберёт 10 совпадений. Ни один существующий индекс не
поддержан одновременно предикатом и порядком:

- `idx_relations_source_type_relation (source_type, relation_type, source_id)` —
  селективен (предикат даёт **127 947** строк из 3.44M), но упорядочен по
  `source_id`, а не по `id` → для `ORDER BY id DESC` не годится, планировщик его
  **не берёт**;
- принудительный `Parallel Seq Scan` (шаг 4 диагностики) — **596 мс**, читает всю
  таблицу (127947 совпадений + 1.12M отброшенных).

Селективность (шаг 2): `source_type+relation_type` = `full_pred` =
`only_relation_type` = **127 947** — `target_type` не отсекает ничего, весь
`inserts_into` это `sql_procedure → sql_table`. Шаг 5 (`только relation_type`)
тоже уходит в обратный PK-скан (211 мс); для несуществующего типа он обязан
прочитать **всю** таблицу → отсюда исходный 30-секундный таймаут.

**Следствие:** индекс `relations(relation_type, id DESC)` (Fix #2) чинит **оба**
случая одним механизмом — и невалидный (O(log n) → 0 строк мгновенно), и валидный
(скан `inserts_into` сразу в порядке `id DESC`, первые 10 записей совпадают →
миллисекунды, `Rows Removed by Filter = 0`).

### Конфликт с `Modifications/Realized/redundant-indexes-cleanup-a41c07.md`

Именно `idx_relations_relation_type` был **удалён** в том предложении со
следующей аргументацией: «запросов с фильтром только по `relation_type` в коде
нет… ad-hoc сценарий через `codebase_query_relations` на практике не
используется (0 сканов); деградация до seq scan для этого редкого пути —
осознанный trade-off ради удешевления массовой вставки в `relations`».

Сегодняшние замеры показывают, что аргумент опирался на неполное наблюдение:
инструмент `relations` **штатно** порождает такие запросы (`source_type +
relation_type + ORDER BY id DESC`), а `idx_scan=0` тогда означал лишь, что путь
не прогонялся, и индекс не совпадал с порядком выдачи. Trade-off **пересмотрен и
закрыт**: индекс `(relation_type, id DESC)` возвращён под новым именем
`idx_relations_relation_type_id` (см. «Решение»); вариант с правкой запроса
отклонён ниже.

### Альтернатива (отклонена): смена порядка в запросе

В `buildRelationIDsQuery` (`internal/query/query_relations.go:179-233`) для
type-only ветки вместо `ORDER BY r.id DESC` можно упорядочить по колонке,
согласованной с существующим индексом (например `r.source_id DESC` при заданном
`source_type`) — тогда используется `idx_relations_source_type_relation` без sort,
write-цена не растёт. **Отклонено:** меняет, *какие именно* 10 связей
возвращаются при `LIMIT` (порядок `source_id` вместо `id`, т.е. семантика «последние
связи» ломается), и не помогает ветке «только `relation_type`» без `source_type`.
Победил вариант A (индекс), сохраняющий семантику запроса без правок Go-кода.

---

## Решение (принято): индекс `relations(relation_type, id DESC)`

Единый механизм закрывает **оба** случая — невалидный тип (быстрый O(log n) → 0
строк вместо seq-скана/таймаута) и валидный type-only запрос (скан `relation_type`
сразу в порядке `id DESC`, первые 10 записей совпадают → миллисекунды,
`Rows Removed by Filter = 0`). Правок в `buildRelationIDsQuery` **не требуется** —
семантика `ORDER BY r.id DESC` сохраняется.

### DDL

```sql
CREATE INDEX IF NOT EXISTS idx_relations_relation_type_id
    ON relations (relation_type, id DESC);
```

Имя выбрано новым (`..._id`, не `idx_relations_relation_type`): совпадает с
проверенным в DBeaver и не конфликтует с легаси-`DROP INDEX IF EXISTS
idx_relations_relation_type` (`db_schema.go:1028`), который можно оставить как
идемпотентную чистку старых БД.

### Изменения в коде

- `internal/store/db_schema.go:944-947` — добавить `CREATE INDEX IF NOT EXISTS
  idx_relations_relation_type_id ON relations(relation_type, id DESC)` в блок
  индексов `relations` (рядом с четырьмя существующими составными).
- `internal/store/db_schema.go:1028` — легаси-`DROP INDEX IF EXISTS
  idx_relations_relation_type` оставить (no-op для нового имени, дочищает старые
  БД); при желании убрать и синхронизировать `schema_integration_test.go:108-115`.
- `internal/store/schema_integration_test.go:108-115` — изменений **не требует**:
  список `dropped` проверяет отсутствие старого имени, которое по-прежнему не
  создаётся. Если легаси-DROP убирается — удалить `idx_relations_relation_type` из
  списка.
- `internal/query/query_relations.go` — **без изменений**.

### Trade-off (write-цена)

`relations` — крупная таблица (~3.44M строк, массовая вставка при индексации),
поэтому в `redundant-indexes-cleanup-a41c07.md` от индекса отказались. Принято:
**вернуть**, т.к. `codebase_query_relations` — штатный путь, а деградация
(1.27M отброшенных строк / 30-секундный таймаут) неприемлема; +1 btree на вставку
приемлем ради предсказуемости чтения.

### Риски

- Создание индекса на большой существующей БД строится в одной транзакции
  `InitSchema` (не `CONCURRENTLY` — тот не работает в транзакции) и кратко
  блокирует запись в `relations`; на больших БД индекс можно построить вне
  приложения вручную `CREATE INDEX CONCURRENTLY`.
- Откат: `DROP INDEX idx_relations_relation_type_id`.

---

## Suggested Fix

1. **Strict-валидация `relation_type`** (основной фикс): словарь известных типов связей (17 значений, уже перечислены в описании инструмента `registry.go` и спеке `query/api-relations-queries`) в центральном словаре (`internal/model`, рядом с таблицей symbol-типов из `symbol-type-vocabulary`); неизвестное значение → ошибка со списком валидных значений. Закрывает TC-075 полностью.
2. **Индекс для relation_type-запросов** (принято, см. «Решение» выше): добавить `CREATE INDEX IF NOT EXISTS idx_relations_relation_type_id ON relations(relation_type, id DESC)` в `db_schema.go:944-947`. Убирает не только чтение всей таблицы для невалидного типа, но и обратный PK-скан с отбрасыванием 1.27M строк для **валидного** type-only запроса.
3. Легаси-`DROP INDEX IF EXISTS idx_relations_relation_type` (`db_schema.go:1028`) оставить (no-op для нового имени); `schema_integration_test.go:108-115` изменений не требует, если DROP сохранён.
4. Дельта-спека `query/api-relations-queries`: требование валидации `relation_type` (сценарий «неизвестный тип связи → ошибка со списком валидных значений»).

---

## Verification

1. `codebase_query_relations` `{"relation_type":"__no_such_relation__"}` → мгновенная ошибка со списком валидных типов (не pq-timeout).
2. `{"relation_type":"references_table","limit":10}` → count=10 без регресса (TC-065).
3. Все 24 кейса `relations` из `Tests/TEST_CASES.md` (TC-062–077, TC-172–177) — PASS; TC-075 переформулируется на ожидание ошибки валидации.
4. `go build ./... && go test ./internal/query/... ./internal/store/...` (+ юнит-тест словаря relation-типов).
5. При добавлении индекса: `EXPLAIN (ANALYZE, BUFFERS)` валидного type-only запроса
   (`source_type='sql_procedure' AND target_type='sql_table' AND
   relation_type='inserts_into' ORDER BY id DESC LIMIT 10`) → `Index Scan using
   idx_relations_relation_type_id`, `Rows Removed by Filter = 0`,
   Execution Time единицы мс (было ~210 мс warm, 1.27M отброшенных строк);
   невалидный `relation_type` → 0 строк мгновенно (было — таймаут).

---

## Реализация (2026-10-12)

Change: [`relations-relation-type-validation-and-index`](../openspec/changes/relations-relation-type-validation-and-index/) (2 capabilities: `query/api-relations-queries`, `infrastructure/database-schema`).

- **Строгая валидация фильтров графа связей:** словари `RelationTypes` и `RelationEntityTypes` в `internal/model/relation_types.go`, `errs.ErrUnknownRelationType`; проверка `relationType`/`sourceType`/`targetType` в `SearchRelations` **до** обращения к БД. `SearchRelationsByEntity` (механика `inspect`) намеренно не валидируется — принимает типы вне словаря (`pas_class`, `pas_unit`, `dfm_component`, …).
- **Фактический словарь шире заявленного в баге:** не 17, а **21** тип связи (добавлены spec-связи `change_modifies`, `depends_on_capability`, `references_code`, `usecase_involves`) и **17** entity-типов (`dfm_form` + 5 `spec_*`). Словарь проверен на живом индексе FA.
- **Маппинг ошибки** в `invalid_arguments` (`classifyQueryError`, `cmd/query_execution.go`).
- **Два индекса** (`internal/store/db_schema.go`): `idx_relations_relation_type_id (relation_type, id DESC)` и `idx_relations_source_type_relation_id (source_type, relation_type, id DESC)` (второй — под 3-предикатный паттерн `source_type + relation_type + ORDER BY id DESC`). Легаси-DROP `idx_relations_relation_type` оставлен как no-op.
- Обновлены описание `codebase_query_relations` (`registry.go`, 21/17), `README.md` (раздел «Поиск relations» + changelog), TC-075.
- **Проверка:** `EXPLAIN (ANALYZE, BUFFERS)` — type-only → `Index Only Scan using idx_relations_relation_type_id` (0.226 мс), 3-предикатный → `Index Scan using idx_relations_source_type_relation_id` (0.182 мс), `Rows Removed by Filter = 0`. E2E: `--relation-type __no_such_relation__` / `--source-type __no_such_type__` → мгновенный `invalid_arguments` со списком; TC-062–072/074/175–177 — PASS.
- **Вне объёма:** TC-073 (kind-алиасы `service`/`event`/`callback_event`/`used_service`) — предмет отдельного открытого `BUG-relations-kind-type-name-filter-20261011.md`; регрессии нет (до изменения тип тоже отклонялся, иным сообщением).

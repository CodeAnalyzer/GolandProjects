# Bug Report: methods-by-table возвращает дубликаты строк — fan-out JOIN без DISTINCT

**Дата:** 2026-10-11
**Файл:** `internal/query/query_sql.go` (`FindMethodsByTable`, строки 377–419); MCP `codebase_query_methods`; CLI `query methods`
**Версия CodeBase:** 0.9.3
**Статус:** исправлено (2026-10-12). Обнаружено при прогоне тест-кейсов `Tests/TEST_CASES.md` (TC-056), зафиксировано в `Tests/CODEBASE_QUERY_TEST_RESULTS_2026-10-11.md` (разбор FAIL, п. 3).
**Исправлено в change:** `fix-methods-by-table-duplicates` (архив: `openspec/changes/archive/2026-10-12-fix-methods-by-table-duplicates`).

## Summary

Инструмент `codebase_query_methods` (поиск PAS-методов, обращающихся к SQL-таблице через query-фрагменты) возвращает **задвоенные строки на один и тот же метод**. SQL запроса соединяет цепочку «метод → query-фрагменты → таблица» без `DISTINCT`/`GROUP BY`/дедупликации — каждый query-фрагмент метода, содержащий таблицу, умножает строку результата.

Критичность: **LOW–MEDIUM** (некорректный `count`, дубли в выдаче MCP-агенту и CLI; данные индекса корректны — дефект только в SQL инструмента).

---

## Environment

- CodeBase 0.9.3, MCP-инструмент `codebase_query_methods`, CLI `codebase query methods`
- БД с проиндексированным проектом FA (прогон 2026-10-11)

---

## Reproduction Steps

1. MCP: `codebase_query_methods` с `{"table": "tAim"}`.
2. CLI: `codebase query methods --table tAim --json`.
3. Прямая проверка данных: в `pas_methods` метод `GetFilterQueryText` класса `TE_ObjClsRelation_Grid_Aim` (unit `E_ObjClsRelation_Grid_Aim`, файл `fa-contracts/Consumer/CLIENT/Consumer/E_ObjClsRelation_Grid_Aim.pas:168`) — **одна** запись (id 4390594); аналогично `TE_ObjClsRelation_Grid_LoanAim` (id 4398526) — одна запись.

## Expected Result

`count=2`: два уникальных метода `GetFilterQueryText` (`E_ObjClsRelation_Grid_Aim`, `E_ObjClsRelation_Grid_LoanAim`), по одной строке на метод.

(Тест-кейс TC-056 в `Tests/TEST_CASES.md` на момент составления ожидал `count=3` — это отдельный дрейф ожидания: фактически методов с `table_usage=tAim` в индексе два.)

## Actual Result

`count=4` — каждый метод возвращен **дважды** (id 4390594 ×2, id 4398526 ×2), строки полностью идентичны:

```json
{"id":4390594,"unit_id":44692,"method_name":"GetFilterQueryText","class_name":"TE_ObjClsRelation_Grid_Aim","unit_name":"E_ObjClsRelation_Grid_Aim","file":".../E_ObjClsRelation_Grid_Aim.pas","line_number":168,"table_usage":"tAim"}
{"id":4390594,... (повтор)}
{"id":4398526,"method_name":"GetFilterQueryText","class_name":"TE_ObjClsRelation_Grid_LoanAim",...}
{"id":4398526,... (повтор)}
```

---

## Root Cause Analysis

SQL `FindMethodsByTable` (`query_sql.go:378-401`):

```sql
FROM pas_methods pm
LEFT JOIN pas_classes pc ON pm.class_id = pc.id
JOIN pas_units pu ON pm.unit_id = pu.id
JOIN files f ON pu.file_id = f.id
JOIN relations r_query ON r_query.source_type = 'pas_method' AND r_query.source_id = pm.id
JOIN query_fragments qf ON r_query.target_type = 'query_fragment' AND r_query.target_id = qf.id
JOIN relations r_table ON r_table.source_type = 'query_fragment' AND r_table.source_id = qf.id
JOIN sql_tables st ON r_table.target_type = 'sql_table' AND r_table.target_id = st.id
WHERE LOWER(st.table_name) = LOWER($1)
  AND r_query.relation_type IN ('executes_query', 'builds_query')
  AND r_table.relation_type = 'references_table'
```

1. **Fan-out по цепочке JOIN'ов.** Один PAS-метод строит **несколько** query-фрагментов (у `GetFilterQueryText` — несколько связей `builds_query`; ср. выдачу `codebase_query_relations` TC-069: у одноимённого метода `TE_UserRightSelect_Product` два фрагмента 7268676/7268677). Каждый фрагмент, содержащий `from tAim`, даёт свою связь `references_table`. Декартово произведение по промежуточным связям → 2 строки на метод.

2. **Возможный второй множитель:** пересечение `executes_query` ∩ `builds_query` на один и тот же фрагмент — фильтр `relation_type IN (...)` пропускает обе связи, каждая даёт строку.

3. **Нет подавления дубликатов:** в SELECT нет `DISTINCT`/`GROUP BY`, в Go-коде (`query_sql.go:409-416`) — простое накопление строк без дедупа по `pm.id`.

Прямая проверка исходников подтверждает: методов два, дублей в коде/индексе нет — все выбираемые колонки функционально зависят от `pm.id` (кроме `st.table_name`, зафиксированного в `WHERE`), т.е. дубли — чисто SQL-артефакт.

---

## Impact

- **Некорректный `count`** (2 → 4) в CLI `--json` envelope и MCP-ответе; агенты получают завышенное число методов и дубли в `items`.
- Лимит `LIMIT $2` расходуется на дубликатах: при `limit=N` уникальных методов может вернуться меньше N.
- Затронут только `codebase_query_methods` / `query methods`; данные индекса и остальные инструменты корректны.

---

## Suggested Fix

Минимальный (SQL):

```sql
SELECT DISTINCT
    pm.id, pm.unit_id, pm.method_name, pc.class_name,
    pu.unit_name, f.rel_path, pm.line_number, st.table_name
FROM ... (без изменений)
```

Альтернативы: `GROUP BY pm.id` (PostgreSQL допускает при функциональной зависимости от PK) либо дедупликация по `pm.id` в Go при сканировании. `DISTINCT` безопасен: все колонки строки идентичны для одного метода.

Дополнительно (опционально, за пределами бага): актуализировать ожидание TC-056 в `Tests/TEST_CASES.md` (`count=3` → `count=2` после фикса; примечание об исходном дрейфе 3→2 уже зафиксировано в отчёте прогона).

---

## Verification

1. `codebase query methods --table tAim --json` → `count=2`, без дублей.
2. `go build ./... && go test ./internal/query/...` (+ юнит-тест на дедуп: метод с двумя фрагментами, ссылающимися на одну таблицу → одна строка).
3. Регресс позитивных кейсов: TC-057 (`pConsDocAmount` → 0), TC-058 (`tConsSalePortfolio` → 0) не изменятся; TC-056 станет PASS с ожиданием `count=2`.

---

## Resolution

Исправлено в OpenSpec-change **`fix-methods-by-table-duplicates`**
(архив: `openspec/changes/archive/2026-10-12-fix-methods-by-table-duplicates`).

**Изменение кода:** `internal/query/query_sql.go` — в `FindMethodsByTable` первый
`SELECT` заменён на `SELECT DISTINCT` (минимальный фикс, идиоматичный для этого
файла). Дедупликация в SQL, а не в Go: `LIMIT $2` вычисляется до передачи строк в
Go, поэтому Go-дедуп не устранил бы «съедание» лимита дубликатами.

**Отклонённые альтернативы:** `GROUP BY pm.id` — падает (PostgreSQL выводит
функциональную зависимость только для колонок таблицы, чей PK сгруппирован;
`pc.class_name`/`pu.unit_name`/`f.rel_path`/`st.table_name` — чужие таблицы);
`EXISTS`-подзапрос — убирает `st.table_name` из select-list и меняет источник
`TableUsage`, выгода не оправдана.

**Тест:** `internal/query/query_sql_methods_integration_test.go` (`//go:build integration`):
`TestFindMethodsByTable_DeduplicatesMethodsWithMultipleFragments` (метод с двумя
фрагментами + дубль `executes_query` → одна строка; второй метод с одним
фрагментом → одна строка; итого `len=2`) и
`TestFindMethodsByTable_UnknownTableReturnsEmpty`. Без фикса тест падает
(`row count = 4`), с фиксом — проходит.

**Проверки (2026-10-12):**
- `go build ./...`, `go test ./...` — зелёные; `go vet ./internal/query/...` — чисто.
- `go test -tags integration ./internal/query/...` — зелёный.
- `query methods --table tAim --json` против FA → `count=2` (id 4390594
  `E_ObjClsRelation_Grid_Aim` + id 4398526 `E_ObjClsRelation_Grid_LoanAim`), без дублей.
- TC-057 (`pConsDocAmount` → 0), TC-058 (`tConsSalePortfolio` → 0) — без регресса.
- Ожидание TC-056 в `Tests/TEST_CASES.md` актуализировано: `count=3` → `count=2`.
- Спека `query/entity-queries` («Поиск PAS-методов») дополнена гарантией
  уникальности строк и сценарием дедупликации.

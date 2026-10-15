# Bug Report: `tableFullScan` — ложное срабатывание на derived table: `object=select` вместо имени таблицы

**Дата:** 2026-10-15
**Правило:** `tableFullScan`
**Файлы кода:** `internal/review/review_lookup.go` (`parseTableWithAlias`, ~:881; `findStatementStart`, ~:1380), `internal/review/review_rules.go` (`analyzeStatementForFullScan`, ~:2913)
**Файл для воспроизведения:** `FA/fa-contracts/Consumer/SERVER/Accrual/CreditTurnoverLink_FastProcess.sql`
**Версия CodeBase:** 0.9.3 (build 1598)
**Статус:** Решено (2026-10-15). Исправлено в change [`tablefullscan-derived-table`](../openspec/changes/archive/2026-10-15-tablefullscan-derived-table/) — рекурсивный разбор derived tables со своей областью фильтрации, guard SQL-ключевых слов в `parseTableWithAlias`, INSERT…SELECT ловится со строки `insert`; попутно устранён дефект «комментарий внутри WHERE съедает хвост оператора» (`removeComments` + построчная чистка в `analyzeStatementForFullScan`). Проверено юнит-тестами и контрольным прогоном на 470 файлах `fa-contracts/Consumer/SERVER/Accrual` (дифф = только исчезающие `object=select`).
**Обнаружено:** вручную — `codebase review` на процедуре `CreditTurnoverLink_FastProcess.sql` (CP-866) вернул finding `line=857 object=select`.

## Summary

```
- [1] tableFullScan line=857 object=select
  Таблица select не имеет условия фильтрации (полное сканирование)
```

«Таблица» `select` не существует: это **derived table** (подзапрос в FROM) из INSERT…SELECT (строки 855–896). Обе реальные таблицы внутри подзапросов фильтруются по `SPID = @@spid` — нарушения нет, срабатывание ложное.

```sql
insert into pCons_CTL_SubcontoID M_WITH_ROWLOCK (SPID, LoanID, PropVal)   -- 855
select @@spid, t.LoanID, t.ConsDebtSubcontoID                             -- 857 ← line=finding
  from (select ct.LoanID, ct.ConsDebtSubcontoID,
               sum(convert(numeric(19, 4), abs(ct.Amount))) as Need
          from pCons_CTL_Turnover ct M_NOLOCK_INDEX(XIE0pCons_CTL_Turnover)
         where ct.SPID      = @@spid                                      -- фильтр ЕСТЬ
           and ct.Direction = 1
           and ct.Amount    < 0
         group by ct.LoanID, ct.ConsDebtSubcontoID) t                     -- ← «таблица select»
  left join (select l.ContractCreditID as LoanID, l.ConsDebtSubcontoID,
                    sum(convert(numeric(19, 4), abs(l.LinkAmount))) as Got
               from pCons_CTL_Link l M_NOLOCK_INDEX(XIE1pCons_CTL_Link)
              where l.SPID     = @@spid                                   -- фильтр ЕСТЬ
                and l.Flag & 1 = 1
              group by l.ContractCreditID, l.ConsDebtSubcontoID) g        -- ← «таблица select»
    on g.LoanID             = t.LoanID
   and g.ConsDebtSubcontoID = t.ConsDebtSubcontoID
 where isnull(g.Got, 0) <> t.Need
 union ...
```

---

## Environment

- CodeBase 0.9.3 build 1598, CLI `codebase review <file>`.
- Любой SQL с паттерном `from (select ...) alias` в первом top-level FROM (union-ветки не обязательны).

---

## Reproduction Steps

1. `codebase review D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\Accrual\CreditTurnoverLink_FastProcess.sql`
2. Наблюдать finding `tableFullScan line=857 object=select`.

Минимальный репро (без макросов):

```sql
create procedure MinRepro as
begin
  insert into pMinRepro_Target (SPID, K)
  select @@spid, t.K
    from (select k.K, sum(k.A) as S
            from pMinRepro_Src k
           where k.SPID = @@spid
           group by k.K) t
end
```

## Expected Result

Finding отсутствует: `pMinRepro_Src` / `pCons_CTL_Turnover` / `pCons_CTL_Link` отфильтрованы по `SPID` внутри подзапросов.

## Actual Result

Один finding `Severity=deploy-stopper`, `line=857`, `object=select`, сообщение «Таблица select не имеет условия фильтрации (полное сканирование)».

---

## Root Cause Analysis

Два сопряжённых дефекта парсера правила:

### Дефект 1 (основной): `parseTableWithAlias` не понимает derived tables

`extractTablesFromFromClause` корректно находит top-level FROM (`findTopLevelFromClauseBounds`, скобки/строки учитываются) и границу до top-level `WHERE`. Далее `parseTablesInFromClause` (review_lookup.go:842):

1. ` left join ` → `,`; разбиение по запятым с учётом скобок → части `(select ...) t` и `(select ...) g on ...`;
2. каждая часть уходит в `parseTableWithAlias`, где (review_lookup.go:914):

```go
result.TableName = strings.Trim(tokens[0], "()")
```

`tokens[0]` для derived table — это `(select`; `Trim` срезает скобку → **`TableName = "select"`**. Подзапрос как сущность не распознаётся: его внутренние таблицы не извлекаются, а мусорный «алиас» = следующий токен (`ct.LoanID,` — с запятой).

Далее в `isTableFiltered` (review_helpers.go:873) алиас `ct.loanid,` не совпадает ни с одним ref из WHERE/ON (`t`, `g`); ветка «одна таблица + неквалифицированные условия» не работает (`len(tables)==2`) → таблица «не отфильтрована» → finding с `Object = "select"`.

Finding один, т.к. цикл по таблицам возвращает первое неотфильтрованное — обе derived tables дали бы одинаковый `select`.

### Дефект 2 (побочный): `findStatementStart` не знает `insert`

`findStatementStart` (review_lookup.go:1380) распознаёт только `select|delete|update|merge`. INSERT…SELECT со строки 855 ловится со своего `select` на 857 → `line=857` вместо 855, и целевая таблица INSERT (`pCons_CTL_SubcontoID`) правилом не анализируется вовсе.

### Трассировка

```
findStatementStart (нет insert)            → stmt начинается с select, line=857
findTopLevelFromClauseBounds               → from … where (top-level)
parseTablesInFromClause                    → ["(select …) t", "(select …) g on …"]
parseTableWithAlias                        → Trim(tokens[0],"()"): "(select" → "select"
isTableFiltered                            → alias "ct.loanid," ∉ {t,g} → false
analyzeStatementForFullScan                → Finding{Object:"select", Line:857}
```

---

## Impact

- **MEDIUM:** ложные deploy-stopper'ы на распространённом паттерне `from (select …) alias` (CTE-стиль до CTE, группировки/агрегации в FROM). Объект в finding'е бесполезен для навигации (`select`).
- Для INSERT…SELECT line указывает на `select`, а не на начало оператора; целевая таблица INSERT не проверяется на отсутствие WHERE (для INSERT это допустимо, но поведение недокументировано).

---

## Suggested Fix

1. **Derived tables** — в `parseTablesInFromClause`/`parseTableWithAlias` распознавать часть, начинающуюся с `(` и содержащую подзапрос: брать алиас после парной закрывающей скобки; вариант (минимальный) — пропускать derived tables целиком (внутренние WHERE у них свои); вариант (полный) — рекурсивно анализировать внутренний FROM со своим WHERE (таблицы внутри подзапроса всё равно обязаны быть отфильтрованы). Как минимум — guard: не выпускать `TableName` из SQL-ключевых слов (`select` и т.п.).
2. **INSERT** — добавить `insert` в `findStatementStart` для tableFullScan: анализировать только SELECT-часть (цель INSERT не сканируется, только пишется) — line тогда будет указывать на начало оператора.

Тесты: `internal/review/review_rules_test.go` — кейс derived table с фильтром внутри (нет finding), derived table без фильтра внутри (finding с именем внутренней таблицы — при выборе полного варианта), INSERT…SELECT (line = строка insert, object ≠ select).

---

## Verification

- До фикса: `codebase review FA/fa-contracts/Consumer/SERVER/Accrual/CreditTurnoverLink_FastProcess.sql` → 1 ложный finding `object=select line=857` (воспроизведено 2026-10-15).
- После фикса: тот же вызов → 0 findings; минимальный репро выше → 0 findings.

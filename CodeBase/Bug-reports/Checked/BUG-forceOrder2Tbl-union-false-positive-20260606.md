# Bug: forceOrder2Tbl — ложное срабатывание для UNION-запросов с макросом M_FORCEORDER

**Правило:** `forceOrder2Tbl`  
**Severity:** 1  
**Статус:** Исправлено  
**Дата обнаружения:** 2026-06-06  
**Дата исправления:** 2026-06-06  
**Файл:** `fa-contracts/Consumer/SERVER/EffectiveRate/Cons_PaymentSelect.sql`, строки 1842, 1876

## Описание проблемы

Правило `forceOrder2Tbl` сообщает, что запросы с 2 таблицами требуют макроса `M_FORCEORDER`, хотя макрос присутствует в запросе. Проблема возникает в UNION-запросах, где макрос `M_FORCEORDER` находится после UNION (в конце второго SELECT), а правило проверяет только первый SELECT.

## Пример запроса, на котором сломалась проверка

```sql
insert pConsumerState M_WITH_ROWLOCK
    (
    SPID      ,
    ContractID,
    StateOrder
    )
select @@spid           ,
       p.ContractID     ,
       max(s.StateOrder)
  from pCreditInfo             p M_NOLOCK_INDEX(XPKpCreditInfo)
 inner join tConsChngStateSync s M_NOLOCK_INDEX(XPKtConsChngStateSync)
         on s.ContractID  = p.ContractID
        and s.FinOperID   = p.InstrumentID
        and s.InDateTime <= p.DateCalc
 where p.SPID       = @@spid
   and p.IsLine     = 0  /* не линия */
   and p.ContractID > 0
 group by p.ContractID
 union
select @@spid            ,
       s2.ContractID     ,
       max(s2.StateOrder)
  from pCreditInfo2            p2 M_NOLOCK_INDEX(XPKpCreditInfo2)
 inner join tContract          c2 M_NOLOCK_INDEX(XPKtContract)
         on c2.ContractID = p2.ContractID
 inner join tConsChngStateSync s2 M_NOLOCK_INDEX(XPKtConsChngStateSync)
         on s2.ContractID  = c2.ContractID
        and s2.FinOperID   = c2.InstrumentID
        and s2.InDateTime <= p2.DateCalc
 where p2.SPID       = @@spid
   and p2.ContractID > 0
 group by s2.ContractID
M_FORCEORDER
```

## Анализ

1. **UNION не разрывает оператор** в функции `hasStatementEnded` (internal/review/review_parser.go:321-362). Весь UNION-запрос считается одним оператором.

2. **`extractTablesFromFromClause`** (internal/review/review_lookup.go:552-570) использует `findTopLevelFromClauseBounds`, который останавливается на UNION (строка 443):
   ```go
   keywordMatchAt(lower, i, "union")
   ```
   Поэтому извлекается только первый FROM clause (до UNION).

3. **Таблицы:** Из первого SELECT извлекаются только `pCreditInfo, tConsChngStateSync` (2 таблицы). Таблицы из второго SELECT (`pCreditInfo2, tContract, tConsChngStateSync`) игнорируются.

4. **Макрос `M_FORCEORDER`** находится после UNION (в конце второго SELECT), поэтому не попадает в `trimmedText`, который анализируется правилом.

5. **Проверка макроса** (internal/review/review_rules.go:733-743) ищет его в `trimmedText`, но его там нет:
   ```go
   lower := strings.ToLower(trimmedText)  // Только первый SELECT, без M_FORCEORDER
   if strings.Contains(lower, macroLower) {  // Не находит!
       hasForceOrderMacro = true
   }
   ```

6. Результат: правило видит 2 таблицы без макроса → формирует finding.

## Ожидаемое поведение

Правило `forceOrder2Tbl` должно:
- Либо разрывать UNION-запрос на отдельные операторы и проверять каждый SELECT отдельно
- Либо проверять наличие макроса `M_FORCEORDER*` во всем UNION-запросе, а не только в первом SELECT
- Либо извлекать таблицы из всех SELECT в UNION, а не только из первого

## Предложение по исправлению

Вариант 1: Добавить `union` в список ключевых слов, разрывающих оператор в `hasStatementEnded` (internal/review/review_parser.go:324).

Вариант 2: В `analyzeStatementForForceOrder2Tbl` (internal/review/review_rules.go:719) извлекать все FROM clause из UNION-запроса и проверять каждый SELECT отдельно, либо проверять макрос во всем тексте оператора, а не только в `trimmedText`.

Вариант 3: В `extractTablesFromFromClause` извлекать таблицы из всех SELECT в UNION, а не только из первого FROM clause.

## Решение

Реализован комбинированный подход:

1. **`analyzeStatementForForceOrder2Tbl`** (internal/review/review_rules.go:734) — проверка макроса `M_FORCEORDER*` во всем тексте оператора (`fullText`), а не только в `trimmedText`.

2. **`hasStatementEnded`** (internal/review/review_parser.go:350-353) — добавлена проверка: если в текущем операторе есть `union`, не разрывать его при новом SELECT (UNION-запросы рассматриваются как один составной оператор).

**Изменения:**
- `internal/review/review_rules.go:734` — заменена проверка с `trimmedText` на `fullText`
- `internal/review/review_parser.go:350-353` — добавлен guard для UNION: не разрывать оператор если есть UNION
- `internal/review/runner_test.go` — добавлены тесты:
  - `TestAnalyzeStatementForForceOrder2Tbl_UnionWithMacroAfterUnion` — UNION с макросом после UNION (не должен выдавать finding)
  - `TestAnalyzeStatementForForceOrder2Tbl_UnionWithoutMacro` — UNION без макроса (должен выдавать finding)

**Проверка:**
- `go test ./internal/review/...` — пройден
- `go build ./...` — успешно
- Проверка на реальном файле `Cons_PaymentSelect.sql` — findings исчезли

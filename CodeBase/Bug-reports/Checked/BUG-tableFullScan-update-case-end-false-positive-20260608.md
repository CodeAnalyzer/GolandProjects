# Bug Report: false positive `tableFullScan` для `UPDATE ... FROM ... WHERE` с `CASE ... END` в `SET`

## Summary

Команда:

```powershell
.\codebase review D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\Consumer\Cons_AutoRepayment.sql --rules tableFullScan
```

возвращает finding:

- `tableFullScan line=1103 object=pconsacclistid`
- Сообщение: `Таблица pconsacclistid не имеет условия фильтрации (полное сканирование)`

При этом сам SQL-оператор содержит корректный `FROM` и `WHERE` фильтр:

```sql
update pConsAccListID
   set Rest = case
                when @AccCount = 1 and @Qty > @QtyPrepayment then Rest - @QtyPrepayment
                when @AccCount = 1 and @Qty <= @QtyPrepayment then 0
                when @AccCount > 1 then 0
                else 0
              end
  from pConsAccListID M_UPDLOCK_INDEX(XPKpConsAccListID)
 where SPID      = @@SPID
   and AccountID = @CurrAccountID
```

---

## Environment

- Repo: `D:\GITHUB\GolandProjects\CodeBase`
- Tool: `codebase review`
- Version: `CodeBase 0.7.5 build 869`
- Reviewed file: `D:/GITHUB/GolandProjects/FA/fa-contracts/Consumer/SERVER/Consumer/Cons_AutoRepayment.sql`
- Rule: `tableFullScan`

---

## Reproduction Steps

1. Выполнить:
   ```powershell
   .\codebase review D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\Consumer\Cons_AutoRepayment.sql --rules tableFullScan
   ```
2. Проверить вывод review.
3. Убедиться, что есть finding по строке `1103` для `pconsacclistid`.
4. Открыть SQL и проверить, что в этом `UPDATE` присутствуют `FROM` и `WHERE` с фильтрами по `SPID` и `AccountID`.

---

## Expected Result

Правило `tableFullScan` не должно срабатывать для данного `UPDATE`, так как фильтрация присутствует (`WHERE SPID = @@SPID AND AccountID = @CurrAccountID`).

---

## Actual Result

Правило ошибочно сообщает о полном сканировании таблицы (`tableFullScan`).

---

## Root Cause Analysis

Предполагаемая причина — некорректное определение границы SQL-оператора в парсере review:

- В `internal/review/review_rules.go` (`checkTableFullScan`) оператор собирается построчно до `hasStatementEnded(...)`.
- В `internal/review/review_parser.go` (`hasStatementEnded`) ключевое слово `end` считается завершением SQL-оператора.
- В рассматриваемом `UPDATE` слово `END` относится к `CASE` внутри `SET`, а не к концу всего `UPDATE`.
- Из-за преждевременного завершения анализируется обрезанный фрагмент без `FROM`/`WHERE`, что и даёт ложный `tableFullScan`.

---

## Impact

- Ложные `deploy-stopper` findings по корректным `UPDATE`.
- Снижение доверия к `review --rules tableFullScan`.
- Риск ручного подавления/игнорирования полезных предупреждений.

---

## Suggested Fix

1. Скорректировать `hasStatementEnded(...)` для `tableFullScan`-сценариев: не считать `END` концом SQL-оператора, если это `END` блока `CASE` внутри незавершённого DML.
2. Добавить regression-тест в `internal/review/runner_test.go`:
   - `UPDATE ... SET ... CASE ... END ... FROM ... WHERE ...`
   - ожидается отсутствие finding `tableFullScan`.
3. Прогнать `go test ./internal/review/...` после фикса.

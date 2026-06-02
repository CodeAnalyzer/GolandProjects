# Bug: tableHintIsRight — ложное срабатывание для M_UPDLOCK_INDEX при UPDATE

**Правило:** `tableHintIsRight`  
**Severity:** 1  
**Статус:** Ложное срабатывание  
**Дата обнаружения:** 2026-06-02  
**Файл:** `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetListLimit.sql`, строки 416–421

## Описание проблемы

Правило `tableHintIsRight` сообщает, что таблица `pAPI_Acc_GetListLimit_Out` имеет неправильный хинт `M_UPDLOCK_INDEX` для операции `UPDATE`. Однако использование `M_UPDLOCK_INDEX` здесь **корректно**: апдейт выполняется именно по таблице `pAPI_Acc_GetListLimit_Out`, и хинт `M_UPDLOCK_INDEX` с указанием её индекса является правильным способом блокировки строк при обновлении через `FROM`.

## Пример запроса, на котором сломалась проверка

```sql
        update pAPI_Acc_GetListLimit_Out
           set Limit       = @Rest,     
               PlanLimit   = @RestPlan, 
               LimitBs     = @RestBs,
               PlanLimitBs = @RestPlanBs
          from pAPI_Acc_GetListLimit_Out p M_UPDLOCK_INDEX(XPKpAPI_Acc_GetListLimit_Out)
         where p.SPID        = @@SPID
           and p.AccountID   = @AccountID
```

## Анализ

- Конструкция `UPDATE t SET ... FROM t p M_UPDLOCK_INDEX(...)` является стандартным паттерном обновления p-таблицы с явным хинтом блокировки
- Цель таблицы в `UPDATE` и таблица в `FROM` — одна и та же (`pAPI_Acc_GetListLimit_Out`), что является корректным синтаксисом Sybase/MSSQL для UPDATE с JOIN/фильтром
- `M_UPDLOCK_INDEX` применяется к таблице-цели обновления, что семантически правильно
- Правило, по всей видимости, ошибочно интерпретирует `UPDATE ... FROM` как ситуацию, где в `FROM` находится вспомогательная таблица, к которой не должен применяться `UPDLOCK`

## Ожидаемое поведение

Правило `tableHintIsRight` не должно выдавать предупреждение, если `M_UPDLOCK_INDEX` применён к таблице, которая является **целью обновления** в конструкции `UPDATE t SET ... FROM t ... WHERE ...`.

## Предложение по исправлению

В логике правила `tableHintIsRight` добавить проверку: если имя таблицы в `FROM` совпадает с именем таблицы в `UPDATE`, то хинт `M_UPDLOCK_INDEX` считается корректным (это обновление собственной таблицы с блокировкой строк).

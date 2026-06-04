# Bug: tableHintIsRight — ложное несрабатывание для UPDATE с целевой таблицей в JOIN

**Правило:** `tableHintIsRight`  
**Severity:** 1  
**Статус:** Ложное несрабатывание  
**Дата обнаружения:** 2026-06-03  
**Файл:** `fa-contracts-ext-novikom-persform/API_Credit_Ext/Server/LoanExt/API_LnExt_MassSaveSubjectInfo.sql`, строки 879–883

## Описание проблемы

Правило `tableHintIsRight` не обнаруживает неправильный хинт `M_NOLOCK_INDEX` для целевой таблицы `tCNE_PFInstProfileStatus` в операции `UPDATE`, когда эта таблица также появляется в `FROM clause` с алиасом. Должен использоваться хинт `M_UPDLOCK_INDEX` для предотвращения deadlock при обновлении постоянной таблицы, но правило не выдает предупреждение.

## Пример запроса, на котором произошло ложное несрабатывание

```sql
  -- Кладем изменения в постоянную таблицу
  update tCNE_PFInstProfileStatus
     set StatusCode                    = p.StatusCode
    from pCNE_PFInstProfileStatus      p M_NOLOCK_INDEX(XIE0pCNE_PFInstProfileStatus)
   inner join tCNE_PFInstProfileStatus t M_NOLOCK_INDEX(XPKtCNE_PFInstProfileStatus)
           on t.PFInstProfileStatusID  = p.PFInstProfileStatusID
    where p.SPID                        = @@spid
  M_FORCEORDER
```

## Анализ

- Целевая таблица UPDATE: `tCNE_PFInstProfileStatus` (первая строка)
- Таблица `tCNE_PFInstProfileStatus` появляется в FROM clause с алиасом `t` и хинтом `M_NOLOCK_INDEX(XPKtCNE_PFInstProfileStatus)`
- Для операции UPDATE постоянной таблицы должен использоваться хинт `M_UPDLOCK_INDEX`, а не `M_NOLOCK_INDEX`
- Правило `checkTableHintIsRight` извлекает таблицы только из FROM clause через `extractTablesFromFromClause`
- Правило определяет целевую таблицу через `extractUpdateTargetTable` из первой строки UPDATE
- Логика сравнения `sameTableReference(table.TableName, targetTable)` может не корректно связать таблицу из JOIN с целевой таблицей UPDATE
- В результате правило не проверяет хинт для таблицы `t` как для целевой таблицы обновления

## Ожидаемое поведение

Правило `tableHintIsRight` должно выдавать предупреждение, если в конструкции `UPDATE t SET ... FROM ... JOIN t alias M_NOLOCK_INDEX(...)` таблица с алиасом совпадает с целевой таблицей UPDATE и имеет неправильный хинт (например, `M_NOLOCK_INDEX` вместо `M_UPDLOCK_INDEX`).

## Предложение по исправлению

В логике правила `checkTableHintIsRight` (функция `analyzeStatementForHintType`):

1. Улучшить логику сравнения `sameTableReference` для корректного сопоставления целевой таблицы UPDATE с таблицами из FROM clause, даже если они имеют разные алиасы
2. Добавить явную проверку: если имя таблицы в FROM (с учетом алиаса) совпадает с именем целевой таблицы UPDATE, то для этой таблицы применять список допустимых хинтов для целевой таблицы (`updateHints`), а не для вспомогательной (`readHints`)
3. Учесть сценарий, когда целевая таблица появляется в FROM clause с алиасом и неправильным хинтом

# Bug: ansiInJoin — ложное срабатывание при корректном ANSI JOIN

**Правило:** `ansiInJoin`  
**Severity:** 1  
**Статус:** Ложное срабатывание  
**Дата обнаружения:** 2026-06-02  
**Файл:** `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetListLimit.sql`, строка 56

## Описание проблемы

Правило `ansiInJoin` сообщает об использовании не-ANSI синтаксиса соединения таблиц (запятая в FROM), однако в данном случае код использует корректный ANSI-синтаксис с явным `inner join`. Запятых в секции `FROM` нет.

## Пример запроса, на котором сломалась проверка

```sql
    insert pContractInfo M_WITH_ROWLOCK
           (
           SPID           ,
           ObjectID       ,
           Date           
           )
    select @@Spid,
           a.ContractID,
           Max(a.OnDate)
      from tConsAccountLink          a M_NOLOCK_INDEX(XIE4tConsAccountLink)   
     inner join tConsRuleAccSync     r M_NOLOCK_INDEX(XPKtConsRuleAccSync)
             on r.RuleID             = a.RuleID
            and r.PropVal           in (RESDEP_C_STTLOVR, RESDEP_C_STTL)
            and r.RelType            = 1
            and r.LinkType           = 0
     inner join tConsInstrumentSync  i M_NOLOCK_INDEX(XPKtConsInstrumentSync)
             on i.InstrumentID       = r.ObjectID
            and i.InterfaceObjectID in (DEALTYPE_OVERDRAFT_JUR, DEALTYPE_OVERDRAFT)
     inner join tContract            c M_NOLOCK_INDEX(XPKtContract)
             on c.ContractID         = a.ContractID
            and c.InstrumentID       = i.InstrumentID
            and c.DateFrom          <= @Date
            and (c.IsActive          = IS_ACTIVE 
                 or (c.IsActive      = ISNT_ACTIVE and c.DateTo >= @Date))
     where a.ResourceID              = @AccountID
       and a.OnDate                 <= @Date
       and (a.DateLast               = '19000101' 
            or a.DateLast           >= @Date)
    group by a.ContractID
    M_FORCEORDER
```

## Анализ

- Все соединения выполнены через `inner join ... on` — ANSI-синтаксис соблюдён
- Запятых в `FROM` нет
- Возможная причина срабатывания: парсер правила ошибочно реагирует на наличие макросов `M_NOLOCK_INDEX(...)` рядом с именем таблицы или на многострочный `insert ... select ... from` с блоком `insert` выше

## Ожидаемое поведение

Правило `ansiInJoin` не должно срабатывать, если все соединения таблиц выполнены через `INNER JOIN ... ON`.

## Предложение по исправлению

Проверить, не реагирует ли парсер `ansiInJoin` на конструкцию `INSERT ... SELECT ... FROM` как на источник не-ANSI синтаксиса, либо на наличие скобок в хинтах `M_NOLOCK_INDEX(...)` в `FROM`-части.

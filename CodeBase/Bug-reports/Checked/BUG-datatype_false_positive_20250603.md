# Ложное срабатывание правила datatype

**Дата:** 2025-06-03  
**Файл:** C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\ConsReservePortfolio\Cons_RP_MassElem_Prefill.sql  
**Правило:** datatype  
**Версия CodeBase:** 0.7.4 build 817

## Описание

Правило `datatype` выдает ложное срабатывание на явное преобразование типа данных с помощью функции `convert()`. Инспекция интерпретирует это как потерю точности, хотя это намеренное и корректное приведение типа.

## Найденное ложное срабатывание

### Строка 139
```sql
convert(smalldatetime,cc.CreditDateFrom)
```

В контексте INSERT запроса:
```sql
insert pConsRPBuffer M_WITH_ROWLOCK
       (
       SPID              ,
       ContractID        ,
       Number            ,
       InstitutionID     ,
       InstitutionName   ,
       PropDealPart      ,
       FundID            ,
       InstrumentID      ,
       ValueDate         ,  -- поле типа smalldatetime
       MainContractID    ,
       type,
       BranchID,
       BranchExtID,
       Flag
       )
select @@spid,
       c.ContractID,
       substring(c.Number + isnull(ce.FullNumber,''),1,60),
       c.InstitutionID,
       '',
       0,
       c.FundID,
       c.InstrumentID,
       convert(smalldatetime,cc.CreditDateFrom),  -- явное преобразование
       c.ContractID,
       i.InterfaceObjectID,
       c.BranchID,
       c.BranchExtID,
       cc.Flag & CONSUMER_CTRCR_NO_PORTFOLIO
  from pConsTblCursor3     p    M_NOLOCK_INDEX(XPKpConsTblCursor3)
 ...
```

## Почему это ложное срабатывание

1. **Явное преобразование:** Использование функции `convert(smalldatetime, ...)` является явным и намеренным приведением типа данных
2. **Корректное использование:** Преобразование из `DSDATETIME` (или другого типа) в `smalldatetime` (DSOPERDAY) выполняется намеренно для соответствия типу поля в таблице назначения
3. **Не является ошибкой:** Это стандартная практика в SQL для приведения типов при вставке данных
4. **Потеря точности ожидаема:** Если поле в таблице имеет тип `smalldatetime`, то преобразование в этот тип является корректным и ожидаемым

## Рекомендация

Изменить правило `datatype` для:
1. Игнорирования явных преобразований типов с помощью функций `convert()` и `cast()`
2. Срабатывания только на неявных преобразованиях, которые могут привести к неожиданной потере точности
3. Учета контекста назначения (тип поля в таблице назначения)

Правило должно предупреждать о потенциальных проблемах только в случаях, когда:
- Преобразование выполняется неявно (без явного вызова convert/cast)
- Тип назначения не соответствует ожидаемому типу поля
- Потеря точности является неожиданной для разработчика

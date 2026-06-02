# Bug: indexWrong — ложное срабатывание при соединении по PK с дополнительным фильтром

**Правило:** `indexWrong`  
**Severity:** 1  
**Статус:** Ложное срабатывание  
**Дата обнаружения:** 2026-06-02  
**Файл:** `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetListLimit.sql`

## Описание проблемы

Правило `indexWrong` предлагает заменить PK-индекс на неключевой индекс в ситуации, когда:
- Таблица соединяется по полю первичного ключа (`JOIN ... ON t.PK = ...`)
- Дополнительно присутствует фильтр по неключевому полю (`AND t.NonKeyField = ...`)

В данном случае PK-индекс является **более селективным** вариантом для выполнения соединения, а предложенный индекс по неключевым полям (`ObjectID, PropVal`) не содержит поля соединения и не подходит для JOIN.

## Пример 1: tConsRuleAccSync, строка 80

```sql
     inner join tConsRuleAccSync     r M_NOLOCK_INDEX(XPKtConsRuleAccSync)
             on r.RuleID             = a.RuleID          -- соединение по PK (RuleID)
            and r.PropVal           in (RESDEP_C_STTLOVR, RESDEP_C_STTL)  -- доп. фильтр
            and r.RelType            = 1
            and r.LinkType           = 0
```

**Указан:** `XPKtConsRuleAccSync` — уникальный индекс по `RuleID` (PK)  
**Предложен:** `XIE1tConsRuleAccSync` — индекс по `(ObjectID, PropVal)`

**Почему предложение некорректно:**
- Соединение выполняется по `r.RuleID = a.RuleID` — именно PK-индекс обеспечивает максимальную селективность для этого соединения
- Индекс `XIE1tConsRuleAccSync (ObjectID, PropVal)` не содержит поле `RuleID` и непригоден для данного JOIN
- Использование `XIE1tConsRuleAccSync` привело бы к деградации производительности

## Пример 2: tContract, строка 185

```sql
     inner join tContract            c M_NOLOCK_INDEX(XPKtContract)
             on c.ContractID         = p.ObjectID         -- соединение по PK (ContractID)
            and c.DateFrom           = isNull((select Max(c1.DateFrom) 
                                                 from tContract c1 M_NOLOCK_INDEX(XPKtContract)
                                                where c1.ContractID = p.ObjectID), '19000101')
```

**Указан:** `XPKtContract` — уникальный индекс по `ContractID` (PK)  
**Предложены:** `XIE0tContract (InstrumentID, BranchID, BranchExtID, DateFrom)`, `XIE1tContract (InstrumentID, InstitutionID)`, `XIE2tContract (InstrumentID, IsActive, BranchID, BranchExtID)`, `XIE3tContract (InstrumentID, ContractGroupID)`

**Почему предложение некорректно:**
- Соединение выполняется по `c.ContractID = p.ObjectID` — PK-индекс является оптимальным
- Ни один из предложенных индексов не содержит поле `ContractID` в качестве первого поля
- Дополнительный фильтр по `DateFrom` через подзапрос не меняет факт того, что главный критерий соединения — `ContractID` (PK)

## Анализ

Правило `indexWrong`, по всей видимости, видит наличие дополнительных условий (AND-фильтры) в `ON`-секции и пытается подобрать индекс, покрывающий все поля условия. Однако оно не учитывает, что:

1. Соединение по PK всегда выгодно — это точечный lookup по уникальному значению
2. Дополнительные фильтры в `ON` применяются **после** поиска по PK, а не вместо него
3. Неключевой составной индекс без поля соединения не может использоваться для JOIN

## Ожидаемое поведение

Правило `indexWrong` не должно предлагать замену PK-индекса на составной неключевой индекс, если соединение выполняется по полю первичного ключа таблицы.

## Предложение по исправлению

В логике правила `indexWrong` добавить проверку: если в хинте указан PK-индекс (`is_unique = true`) и соединение в `ON` выполняется по полю, входящему в этот индекс, — не предлагать замену на индексы, не содержащие поле соединения.

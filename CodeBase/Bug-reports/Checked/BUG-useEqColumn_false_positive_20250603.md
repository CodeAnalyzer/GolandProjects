# Ложное срабатывание правила useEqColumn

**Дата:** 2025-06-03  
**Файл:** C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\ConsReservePortfolio\Cons_RP_MassElem_Prefill.sql  
**Правило:** useEqColumn  
**Версия CodeBase:** 0.7.4 build 817

## Описание

Правило `useEqColumn` выдает ложные срабатывания на конструкции `where 1=1`, которые являются стандартным шаблоном для условий WHERE в SQL. Конструкция `1=1` используется для упрощения динамического добавления условий в запросах и не является сравнением столбца с самим собой.

## Найденные ложные срабатывания

### Случай 1 - Строка 278
```sql
select @MaxID = max(p.AlienID)
  from #Cons_Identity_Prefill p M_NOLOCK_INDEX(XIE0Cons_Identity_Prefill)
 where 1=1
M_ISOLAT
```

### Случай 2 - Строка 308
```sql
select @MaxID = max(p.AlienID)
  from #Cons_Identity_Prefill p M_NOLOCK_INDEX(XIE0Cons_Identity_Prefill)
 where 1=1
M_ISOLAT
```

### Случай 3 - Строка 370
```sql
select @MaxID = max(p.AlienID)
  from #Cons_Identity_Prefill p M_NOLOCK_INDEX(XIE0Cons_Identity_Prefill)
 where 1=1
M_ISOLAT
```

## Почему это ложное срабатывание

Конструкция `where 1=1` - это стандартный паттерн в SQL, который:
1. Не сравнивает столбец с самим собой
2. Используется как заглушка для динамического формирования условий WHERE
3. Всегда возвращает TRUE и не влияет на логику запроса
4. Позволяет легко добавлять дополнительные условия через `AND` без проверки на первое условие

## Рекомендация

Изменить правило `useEqColumn` для игнорирования конструкции `where 1=1` и `where 1=1` с последующими условиями через `AND`. Правило должно срабатывать только на реальных сравнениях столбца с самим собой, например:
- `where column = column`
- `where table1.column = table1.column`

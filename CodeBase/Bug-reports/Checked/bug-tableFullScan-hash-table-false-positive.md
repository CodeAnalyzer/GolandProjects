# Bug: tableFullScan — ложное срабатывание для #-таблиц (сессионных временных таблиц)

## Правило
`tableFullScan` (Severity 1)

## Файл с воспроизведением
`fa-contracts/API_Credit/Server/ContractOver/API_ContractOver_CancelAction.sql`, строка 158

## Описание проблемы

Правило `tableFullScan` срабатывает на обращениях к `#`-таблицам (сессионным временным таблицам
SQL Server) без условия фильтрации. Однако `#`-таблицы изолированы по сессии автоматически —
каждая сессия видит только свои строки, поэтому фильтр по `SPID` для них **не нужен и не применяется**.

Правило корректно для `p`-таблиц (общих временных таблиц Diasoft), которые разделяются между
сессиями и требуют обязательного фильтра `WHERE SPID = @@SPID`. Для `#`-таблиц это требование
неприменимо.

## Воспроизведение

Файл: `fa-contracts/API_Credit/Server/ContractOver/API_ContractOver_CancelAction.sql`

Таблица создаётся как сессионная `#`-таблица:

```sql
-- строки 53-57
create table #ProtocolList 
           (
             ProtocolID DSIDENTIFIER,
             StateOrder DSINT_KEY          
           ) 
```

Обращения к таблице без фильтра по `SPID` — корректны, так как `#`-таблица изолирована по сессии:

```sql
-- строка 158 — вызывает срабатывание tableFullScan
select @StateOrder = max(p.StateOrder)
  from #ProtocolList  p 
M_ISOLAT

-- строка 166 — аналогичное срабатывание
select @ProtocolID  = p.ProtocolID
  from #ProtocolList  p 
 where p.StateOrder = @StateOrder 
M_ISOLAT

-- строка 238 — аналогичное срабатывание
select @StateOrder = max(p.StateOrder)
  from #ProtocolList  p 
 where p.StateOrder < @StateOrder
M_ISOLAT
```

Выдача CodeBase:

```json
{
  "rule": "tableFullScan",
  "severity": 1,
  "message": "Таблица #protocollist не имеет условия фильтрации (полное сканирование)",
  "file": "C:/NT/FA#/7.2GIT/fa-contracts/API_Credit/Server/ContractOver/API_ContractOver_CancelAction.sql",
  "line": 158,
  "object": "#protocollist"
}
```

## Причина

Правило `tableFullScan` проверяет наличие условия фильтрации (в частности `SPID = @@SPID`)
для всех временных таблиц без исключения. При этом не учитывается тип таблицы:

- **`p`-таблицы** (например, `pAPI_SM_Protocol`) — общие для всех сессий, требуют `WHERE SPID = @@SPID`
- **`#`-таблицы** (например, `#ProtocolList`) — сессионные, изолированы автоматически, фильтр не нужен

## Решение

В логике правила `tableFullScan` добавить проверку имени таблицы:
если имя начинается с `#` — пропускать проверку на наличие условия фильтрации.

```go
// Псевдокод исправления в review_rules.go
if strings.HasPrefix(strings.ToLower(tableName), "#") {
    continue // #-таблицы изолированы по сессии, фильтр не нужен
}
```

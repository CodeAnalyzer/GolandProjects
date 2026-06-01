# BUG: tableFullScan — ложное срабатывание при неквалифицированном WHERE-условии

## Описание

Правило `tableFullScan` ошибочно срабатывает для таблицы, у которой есть условие фильтрации в `WHERE`, но условие написано **без алиаса** (неквалифицированное: `where SPID = @@spid` вместо `where p.SPID = @@spid`).

## Воспроизведение

Файл: `fa-contracts-ext-novikom-persform/API_Credit_Ext/Server/LoanExt/API_LoanExt_FindListReqByParam.sql`

Фрагмент кода (строка 69):
```sql
select @Check = 1
  from pAPI_LoanExt_SubjInList M_NOLOCK_INDEX(XPKpAPI_LoanExt_SubjInList)
 where SPID = @@spid
M_ISOLAT
```

Результат ревью:
```
- [1] tableFullScan line=69 object=papi_loanext_subjinlist
  Таблица papi_loanext_subjinlist не имеет условия фильтрации (полное сканирование)
```

Фильтрация по `SPID = @@spid` присутствует и использует первичный ключ таблицы. Срабатывание ложное.

## Корневая причина

Функция `extractColumnRefsFromWhere` (файл `internal/review/review_rules.go`, строка ~3331) извлекает из `WHERE`-части только **квалифицированные** ссылки вида `alias.column`:

```go
re := regexp.MustCompile(`(?i)(\w+)\.(\w+)`)
matches := re.FindAllStringSubmatch(wherePart, -1)
for _, m := range matches {
    result = append(result, strings.ToLower(m[1]))  // только alias
}
```

Неквалифицированные условия (`SPID = @@spid`, `StatusCode = @StatusCode`) в результат не попадают.

Далее функция `isTableFiltered` (строка ~3395) проверяет, есть ли алиас или имя таблицы в списке `whereRefs`. Так как неквалифицированные условия не добавляются в `whereRefs`, таблица без алиаса (используемая в запросе с одной таблицей) считается нефильтрованной.

При этом для случая одной таблицы в `collectColumnsFromConditionBranch` (строка ~418) вызывается `extractUnqualifiedConditionColumns` — но это только в правиле `indexWrong`, а не в `tableFullScan`.

## Ожидаемое поведение

Если `SELECT` содержит одну таблицу и в `WHERE` есть хотя бы одно условие (квалифицированное или нет), таблица должна считаться отфильтрованной.

## Предлагаемое исправление

В функции `extractColumnRefsFromWhere` дополнительно обрабатывать неквалифицированные условия при наличии единственной таблицы. Либо в `analyzeStatementForFullScan` при наличии ровно одной таблицы проверять наличие любого `WHERE`-условия:

```go
// Если одна таблица и WHERE-часть не пустая — считать отфильтрованной
if len(tables) == 1 && strings.Contains(lower, " where ") {
    whereContent := extractWherePartForIndexWrong(lower)
    if strings.TrimSpace(whereContent) != "" {
        return nil
    }
}
```

Также стоит рассмотреть случай `M_ISOLAT` — макроса, который раскрывается в `SET ISOLATION LEVEL ...` и не является частью условия фильтрации, но может мешать парсингу конца `WHERE`-блока.

## Severity

Severity 1 (deploy stopper) — ложное срабатывание блокирует деплой без реальной причины.

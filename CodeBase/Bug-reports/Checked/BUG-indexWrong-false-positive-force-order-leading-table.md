# Bug: indexWrong — ложное срабатывание при M_FORCEORDER на ведущей таблице (WHERE vs ON)

**Правило:** `indexWrong`  
**Severity:** 1 (Deploy Stopper)  
**Статус:** Ложное срабатывание (false positive)  
**Дата обнаружения:** 2026-06-24  
**Файл:** `fa-contracts/API_Credit/Server/Callback/API_CON_Acc_GetListLimit.sql`

## Описание проблемы

Правило `indexWrong` выдаёт ложное срабатывание на строке 80 для таблицы `tConsAccountLink`, указывая, что индекс `XIE4tConsAccountLink` хуже подходящего `XIE1tConsAccountLink`. Однако в запросе присутствует макрос `M_FORCEORDER`, который жёстко фиксирует порядок соединений: `tConsAccountLink` — ведущая (первая) таблица. Для ведущей таблицы при `FORCE ORDER` index seek возможен только по полям из `WHERE`, а не из `JOIN ON`. Правило игнорирует это различие и объединяет колонки из `WHERE` и `ON` в общий набор, что приводит к завышению оценки индекса `XIE1tConsAccountLink`.

## Воспроизведение

```powershell
codebase review C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\Server\Callback\API_CON_Acc_GetListLimit.sql
```

Результат:
```
- [1] indexWrong line=80 object=tconsaccountlink.xie4tconsaccountlink
  Для таблицы tconsaccountlink указан индекс xie4tconsaccountlink, но по условиям лучше подходит XIE1tConsAccountLink
```

Запрос (строки 88-117):

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

### Индексы таблицы `tConsAccountLink`

| Индекс | Поля |
|--------|------|
| `XPKtConsAccountLink` | `ConsAccountLinkID` |
| `XIE1tConsAccountLink` | `ContractID, RuleID, ResourceID` |
| `XIE4tConsAccountLink` | `ResourceID, RuleID` |

### Условия на таблицу `a` (alias `tConsAccountLink`)

| Условие | Колонка | Тип | Где |
|---------|---------|-----|-----|
| `a.ResourceID = @AccountID` | `ResourceID` | equality | WHERE |
| `a.OnDate <= @Date` | `OnDate` | range | WHERE |
| `a.DateLast = '19000101' OR a.DateLast >= @Date` | `DateLast` | range/OR | WHERE |
| `r.RuleID = a.RuleID` | `RuleID` | equality | JOIN ON |
| `c.ContractID = a.ContractID` | `ContractID` | equality | JOIN ON |

### Почему `XIE4tConsAccountLink` выбран правильно

При `M_FORCEORDER` таблица `a` — ведущая (первая в FROM). Для ведущей таблицы index seek возможен только по полям из `WHERE`:

- **`XIE4tConsAccountLink`** (`ResourceID, RuleID`): leading column `ResourceID` — в WHERE (equality) → **index seek** ✓
- **`XIE1tConsAccountLink`** (`ContractID, RuleID, ResourceID`): leading column `ContractID` — только в JOIN ON, **отсутствует в WHERE** → index seek невозможен, потребуется **full scan** ✗

## Причина

### Механизм ложного срабатывания

Функция `extractConditionColumnsForIndexWrong` (`internal/review/review_helpers.go:384-401`) собирает колонки из **обоих** источников — `WHERE` и `JOIN ON` — в единый набор для каждой таблицы:

```go
// review_helpers.go:384-401
func extractConditionColumnsForIndexWrong(fullText string, tables []tableFromClause) map[string]map[string]struct{} {
    result := make(map[string]map[string]map[string]struct{})
    // ...

    // Колонки из WHERE
    wherePart := extractWherePartForIndexWrong(fullText)
    if strings.TrimSpace(wherePart) != "" {
        mergeTableColumns(result, collectColumnsFromConditionExpression(wherePart, tables))
    }

    // Колонки из ON — добавляются в тот же набор!
    onParts := extractOnPartsForIndexWrong(fullText)
    for _, onPart := range onParts {
        mergeTableColumns(result, collectColumnsFromConditionExpression(onPart, tables))
    }

    return result
}
```

Для таблицы `a` (alias `tConsAccountLink`) результат:

```
conditionColumns["a"] = {ResourceID, OnDate, DateLast, RuleID, ContractID}
```

Затем `calculateIndexPrefixMatch` (`review_helpers.go:256-274`) считает длину префиксного совпадения полей индекса с этим набором:

```go
// review_helpers.go:256-274
func calculateIndexPrefixMatch(indexFields []string, columns map[string]struct{}) int {
    matched := 0
    for _, field := range indexFields {
        normalized := normalizeIdentifier(field)
        if normalized == "" {
            break
        }
        if _, exists := columns[normalized]; !exists {
            break
        }
        matched++
    }
    return matched
}
```

Расчёт оценок:

| Индекс | Поля | Совпадение | Score |
|--------|------|------------|-------|
| `XIE1tConsAccountLink` | `ContractID, RuleID, ResourceID` | ContractID ✓ (ON), RuleID ✓ (ON), ResourceID ✓ (WHERE) | **3** |
| `XIE4tConsAccountLink` | `ResourceID, RuleID` | ResourceID ✓ (WHERE), RuleID ✓ (ON) | **2** |
| `XPKtConsAccountLink` | `ConsAccountLinkID` | ConsAccountLinkID ✗ | 0 |

`XIE1` получает score=3 > `XIE4` score=2 → правило сообщает, что `XIE1` лучше.

### Корневая причина

`analyzeStatementForIndexWrong` (`review_rules.go:296-394`) **не проверяет наличие `M_FORCEORDER`** и не различает `WHERE` и `ON` условия при оценке индекса:

1. **Нет определения ведущей таблицы** — функция не определяет, какая таблица является ведущей (первой в FROM clause) при `FORCE ORDER`
2. **Нет раздельного учёта WHERE vs ON** — колонки из `WHERE` и `ON` объединяются в единый набор, хотя для ведущей таблицы при `FORCE ORDER` только `WHERE`-колонки могут быть использованы для index seek
3. **Нет проверки `M_FORCEORDER`** — правило вообще не анализирует наличие макросов `M_FORCEORDER` / `M_FORCEORDER_NOSPOOL` в запросе

Существующая функция `shouldKeepChosenIndexForPKJoin` (`review_helpers.go:354-375`) защищает только `XPK`-индексы, у которых поля совпадают с JOIN-колонками. Она не помогает, когда выбранный индекс — не `XPK` (как `XIE4` в данном случае).

## Ожидаемое поведение

1. При наличии `M_FORCEORDER` / `M_FORCEORDER_NOSPOOL` для ведущей таблицы (первой в FROM clause) оценка индекса должна учитывать только колонки из `WHERE`, а не из `JOIN ON`
2. `XIE4tConsAccountLink` (score=1 по WHERE: `ResourceID`) должен считаться лучше, чем `XIE1tConsAccountLink` (score=0 по WHERE: `ContractID` отсутствует в WHERE)
3. Замечание `indexWrong` для данного запроса не должно генерироваться

## Предложение по исправлению

### Вариант 1: Раздельный учёт WHERE и ON для ведущей таблицы при FORCE ORDER (предпочтительный)

В функции `analyzeStatementForIndexWrong` (`internal/review/review_rules.go:296`) добавить проверку на `M_FORCEORDER` и раздельный расчёт для ведущей таблицы:

```go
// 1. Проверить наличие M_FORCEORDER / M_FORCEORDER_NOSPOOL в запросе
hasForceOrder := containsForceOrderMacro(trimmedText)

// 2. Определить ведущую таблицу (первую в FROM clause)
leadingTableKey := ""
if hasForceOrder && len(tables) > 0 {
    leadingTableKey = tableConditionKey(tables[0])
}

// 3. Для ведущей таблицы при FORCE ORDER использовать только WHERE-колонки
whereColumns := extractWhereOnlyColumnsForIndexWrong(trimmedText, tables)

// В цикле по кандидатам:
for _, candidate := range candidates {
    var score int
    if hasForceOrder && tableConditionKey(table) == leadingTableKey {
        // Ведущая таблица: только WHERE-колонки
        score = calculateIndexPrefixMatch(candidate.Fields, whereColumns[tableConditionKey(table)])
    } else {
        // Остальные таблицы: WHERE + ON (как сейчас)
        score = calculateIndexPrefixMatch(candidate.Fields, conditionColumns[tableConditionKey(table)])
    }
    // ... остальная логика без изменений
}
```

Потребуется добавить функцию `extractWhereOnlyColumnsForIndexWrong`, которая собирает колонки только из `WHERE`, без `ON` (по аналогии с `extractConditionColumnsForIndexWrong`, но без блока `onParts`).

### Вариант 2: Подавлять правило indexWrong при наличии M_FORCEORDER (минимальный)

Если реализация раздельного учёта WHERE/ON сложна, можно просто подавлять `indexWrong` для ведущей таблицы при наличии `M_FORCEORDER`:

```go
if hasForceOrder && tableConditionKey(table) == leadingTableKey {
    continue // пропустить проверку indexWrong для ведущей таблицы
}
```

**Недостаток варианта 2:** подавляет все замечания для ведущей таблицы, даже если выбранный индекс действительно неверный (например, leading column вообще не в WHERE).

### Вариант 3: Понижение severity для ведущей таблицы при FORCE ORDER

Вместо полного подавления — понижать severity с 1 (Deploy Stopper) до 3 (Info) для ведущей таблицы при `M_FORCEORDER`, чтобы замечание не блокировало деплой, но оставалось видно.

## Затронутые файлы

- `internal/review/review_rules.go` — функция `analyzeStatementForIndexWrong`, строки 296-394 (основное изменение)
- `internal/review/review_helpers.go` — функция `extractConditionColumnsForIndexWrong`, строки 384-401 (добавление `extractWhereOnlyColumnsForIndexWrong`)
- `internal/review/review_helpers.go` — функция `calculateIndexPrefixMatch`, строки 256-274 (без изменений, используется как есть)

## Влияние

Без исправления правило `indexWrong` генерирует ложные срабатывания (severity 1, deploy stopper) на запросах с `M_FORCEORDER`, где ведущая таблица использует индекс с leading column из `WHERE`, а более длинный индекс имеет leading column только из `JOIN ON`. Это может блокировать деплой файлов с корректно выбранными индексами.

Широкое распространение: `M_FORCEORDER` — стандартный макрос в Diasoft 5NT, применяется во всех запросах с `INNER JOIN` 2+ таблиц. Любой запрос, где ведущая таблица имеет индекс с WHERE-колонкой не на первом месте в более длинном альтернативном индексе, потенциально затронут.

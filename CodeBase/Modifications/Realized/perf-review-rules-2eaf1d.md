# Оптимизация медленных правил review

Ускорение `checkDiffTypesComparison`, `checkDateIntoString`, `checkEmptyStringDate` и сопутствующих правил путём вынесения regex в package-level и кэширования дорогих операций.

## Файлы

- `internal/review/review_rules.go` — основные изменения

---

## Шаг 1. Вынести regex в package-level (review_rules.go:14–27)

Добавить в существующий `var (...)` блок новые константные regex, которые сейчас создаются при каждом вызове функции или **внутри цикла по фрагментам**.

| Переменная | Где сейчас | Проблема |
|---|---|---|
| `valuesRe` | `checkDateIntoString` line 4043 | per function call |
| `valuesRe` | `checkEmptyStringDate` line 4396 | per function call |
| `convertRe`, `castRe` | `checkEmptyStringDate` lines 4440–4441 | per function call |
| `convertRe` | `checkFloatToStringConvert` line 5906 | **ВНУТРИ цикла по фрагментам** |
| `castRe` | `checkFloatToStringConvert` line 5955 | **ВНУТРИ цикла по фрагментам** |
| `rowcountRe` | `checkSelectAfterSetRowcount` line 6024 | **ВНУТРИ цикла по фрагментам** |
| 8 cursor regex | `checkUseOnlyDeclaredCursors` lines 4600–4616 | per function call |
| `assignRe` | `checkUsageVarInSameSelect` line 4838 | per function call |
| `eqRe` | line 2604 | per function call |

Итого добавить ~12 преком. regex в блок `var (...)`, убрать объявления из функций.

---

## Шаг 2. Кэш aliasMap в checkDiffTypesComparison

Сейчас `parseAliasMap(extractFromClause(queryText))` вызывается для **каждого фрагмента** — в большой процедуре это сотни одинаковых вызовов.

```go
// До цикла по фрагментам:
aliasCache := make(map[string]map[string]string)

// Внутри цикла (замена строки 5770):
fromClause := extractFromClause(queryText)
aliasMap, ok := aliasCache[fromClause]
if !ok {
    aliasMap = parseAliasMap(fromClause)
    aliasCache[fromClause] = aliasMap
}
```

---

## Шаг 3. Fast-path для @var = @var в checkDiffTypesComparison

Если оба операнда — переменные (начинаются с `@`), DB-запрос не нужен. Добавить ранний выход перед `resolveArgType`:

```go
for _, cmp := range comparisons {
    if isLiteralArg(cmp.left) || isLiteralArg(cmp.right) {
        continue
    }
    // Fast-path: оба операнда — переменные, без DB
    if strings.HasPrefix(cmp.left, "@") && strings.HasPrefix(cmp.right, "@") {
        t1 := variableTypes[normalizeVariableName(cmp.left)]
        t2 := variableTypes[normalizeVariableName(cmp.right)]
        if t1 == "" || t2 == "" || areEquivalentTypes(t1, t2) {
            continue
        }
        // ... добавить finding
        continue
    }
    // обычный путь через resolveArgType
    type1 := r.resolveArgType(cmp.left, variableTypes, aliasMap)
    type2 := r.resolveArgType(cmp.right, variableTypes, aliasMap)
    ...
}
```

---

## Шаг 4. Pre-warm colTypeCache перед циклом по фрагментам

Собрать все уникальные таблицы из aliasMap всех фрагментов, загрузить все их колонки **одним запросом** до начала анализа. Это конвертирует N промахов `cachedFindColumnDefinitionType` в 1 батч-запрос на таблицу.

```go
// Собрать все таблицы из всех FROM-частей
tables := collectAllTablesFromFragments(parsed.Fragments)
r.prewarmColTypeCacheForTables(tables)
```

Требует добавить метод `prewarmColTypeCacheForTables` в `runner.go` или `review_lookup.go`.

> **Примечание**: этот шаг более сложный и его можно выполнить отдельно.

---

## Ожидаемый эффект

| Шаг | Правило | Ожидаемое ускорение |
|---|---|---|
| 1 (regex) | dateIntoString, emptyStringDate, floatToStringConvert | 40–70% |
| 2 (aliasMap cache) | diffTypesComparison | 30–50% |
| 3 (fast-path @var) | diffTypesComparison | 10–20% |
| 4 (prewarm cache) | diffTypesComparison, datatype | 20–40% холодный кэш |

---

## Порядок выполнения

1. [ ] Шаг 1: вынести все regex в package-level var-блок
2. [ ] Шаг 2: кэш aliasMap в checkDiffTypesComparison
3. [ ] Шаг 3: fast-path @var = @var
4. [ ] Шаг 4: prewarm colTypeCache (опционально, отдельной задачей)
5. [ ] Сборка и замер времени на CON_X5_ContractRanking.sql

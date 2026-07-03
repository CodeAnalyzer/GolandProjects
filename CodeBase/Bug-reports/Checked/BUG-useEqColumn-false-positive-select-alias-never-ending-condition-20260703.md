# Bug Report: Ложное срабатывание useEqColumn на алиасах SELECT из-за неразрывного буфера условия

**Дата:** 2026-07-03
**Статус:** Open
**Приоритет:** High
**Компонент:** SQL Review — правило `useEqColumn`

## Описание проблемы

Правило `useEqColumn` ложно срабатывает на алиасах `SELECT` в конструкции `INSERT ... SELECT`. Корневая причина — две ошибки, работающие совместно:

1. **`hasConditionEnded` не распознаёт границы операторов** — не обнаруживает `go`, `GO`, макросы `$_END`, `__END_PROCEDURE__`, `M_FORCEORDER`, `M_KEEPPLAN`, новые операторы (`update`, `insert`, `delete`) как признаки завершения условия
2. **`analyzeConditionForEqColumn` не различает алиасы SELECT и сравнения WHERE** — регулярное выражение `reEqOperands` матчает `identifier = identifier` в любом контексте, включая `select TownName = TownName` (присваивание алиаса в T-SQL)

## Файл с ошибкой

**Файл:** `C:\NT\FA#\7.2GIT\fa-contracts\API_LoanBureau\Server\Facade\FCD_BCH_Legal_FindListByID.sql`

## Ложное срабатывание

| Строка | Правило | Объект | Сообщение |
|--------|---------|--------|-----------|
| 70 | `useEqColumn` | `townname` | Нельзя сравнивать столбец с самим собой |

## Проблемный код

Строка 70 — это начало условия JOIN `on i.InstitutionID = fl.LegalID`:

```sql
-- Строки 68-74
  from pAPI_Legal_FindList  fl  M_NOLOCK_INDEX(XPKpAPI_Legal_FindList)
 inner join tInstitution    i   M_NOLOCK_INDEX(XPKtInstitution)
         on i.InstitutionID  = fl.LegalID          -- ← строка 70: "on" распознан как начало условия
  left join tInstAttr       ia  M_NOLOCK_INDEX(XPKtInstAttr)
         on ia.InstitutionID = i.InstitutionID
 where fl.SPID = @@spid
M_FORCEORDER
```

Далее в файле (строки 139-182) — блок `INSERT ... SELECT` с алиасами:

```sql
-- Строки 161-173
select SPID        = SPID
      ,ClientID    = LegalID
      ,AddressID   = AddressID
      ,RegionName  = RegionName
      ,RegionKind  = RegionKind
      ,AreaName    = AreaName
      ,AreaKind    = AreaKind
      ,CityName    = CityName
      ,CityKind    = CityKind
      ,TownName    = TownName       -- ← строка 170: алиас = колонка (то же имя)
      ,TownKind    = TownKind
      ,StreetName  = StreetName
      ,StreetKind  = StreetKind
```

В T-SQL `select TownName = TownName` — это **присваивание алиаса выходной колонке**, а не сравнение.

## Причина ложного срабатывания

### Механизм срабатывания

**Файлы исходного кода:**
- `Source/internal/review/review_rules.go` — функции `checkUseEqColumn` и `analyzeConditionForEqColumn`
- `Source/internal/review/review_parser.go` — функции `findConditionStart` и `hasConditionEnded`

### Шаг 1: `findConditionStart` находит "on" на строке 70

```go
// review_parser.go:1168-1177
func findConditionStart(lower string) int {
    kws := []string{"where", "on", "having"}
    for _, kw := range kws {
        idx := findKeywordPosition(lower, kw)
        if idx >= 0 {
            return idx
        }
    }
    return -1
}
```

Строка 70 содержит `on i.InstitutionID = fl.LegalID`. Слово "on" найдено как самостоятельное слово. Глубина скобок перед "on" равна 0 — условие принимается:

```go
// review_rules.go:2762-2767
if depthBefore == 0 {
    inCondition = true
    conditionStartLine = lineNum   // = 70
    conditionBuffer = []string{line}
    parenDepth = countParensRespectingStrings(line)
}
```

### Шаг 2: `hasConditionEnded` никогда не возвращает true

```go
// review_parser.go:1180-1188
func hasConditionEnded(lower string) bool {
    kws := []string{"group by", "order by", "union", "except", "intersect", ";"}
    for _, kw := range kws {
        if strings.Contains(lower, kw) {
            return true
        }
    }
    return false
}
```

В файле нет ни одного из этих ключевых слов. Файл использует:
- `go` / `GO` как разделитель батчей (не распознано)
- `M_FORCEORDER` / `M_KEEPPLAN` как макросы завершения запроса (не распознано)
- `$_END` / `__END_PROCEDURE__` как макросы завершения блока (не распознано)
- Новые операторы `update`, `insert`, `delete` (не распознано)

**Буфер условия растёт со строки 70 до конца файла (строка 193), накапливая 124 строки смешанного SQL.**

Переменная `parenDepth` отслеживается, но **никогда не используется** для проверки завершения — мёртвый код.

### Шаг 3: `analyzeConditionForEqColumn` обрабатывает алиасы SELECT как сравнения

```go
// review_rules.go:2800-2857
func analyzeConditionForEqColumn(lines []string, startLine int, file *indexedFile) []Finding {
    fullText := strings.Join(lines, " ")   // 124 строки в одну строку
    // ...
    matches := reEqOperands.FindAllStringSubmatch(fullText, -1)
    // reEqOperands = (?i)(@?\w+(?:\.\w+)?)\s*=\s*(@?\w+(?:\.\w+)?)
    // Матчит ЛЮБОЕ identifier = identifier, независимо от SQL-контекста
```

Регулярное выражение находит `TownName = TownName` (строка 170 в файле) в объединённом тексте. Проверки не помогают:

- **`@`-префикс:** `TownName` — не переменная, проверка `strings.HasPrefix(m[1], "@")` не срабатывает
- **Битовые операторы:** `&` есть на строке 65, но она **до** строки 70 и не попадает в буфер
- **Числовые литералы:** `TownName` — не число

`normalizeIdentifier("TownName")` = `"townname"` = `normalizeIdentifier("TownName")` → `left == right` → finding создано с `Line: startLine` (70) и `Object: "townname"`.

### Шаг 4: Все находки сообщают line=70

```go
// review_rules.go:2844-2852
findings = append(findings, Finding{
    ...
    Line:   startLine,  // = 70 — начало блока условия, а не строка совпадения
    Object: left,       // = "townname"
})
```

`startLine` — это `conditionStartLine = 70` (строка с "on"), а не строка 170, где фактически находится `TownName = TownName`.

## Почему это ложное срабатывание

| Выражение | Контекст | Значение |
|-----------|----------|----------|
| `TownName = TownName` | `select` list | Алиас выходной колонки = значение колонки |
| `TownName = TownName` | `where` clause | Сравнение колонки с самой собой (бессмысленно) |

Инструмент не различает эти два контекста, т.к. `analyzeConditionForEqColumn` работает с объединённым текстом без определения SQL-секции.

## Другие ложные находки в том же буфере

Из тех же 124 строк буфера также ложно срабатывают (все с `line=70`):

- `SPID = SPID` (строка 161)
- `AddressID = AddressID` (строка 163)
- `RegionName = RegionName` (строка 164)
- `RegionKind = RegionKind` (строка 165)
- `AreaName = AreaName` (строка 166)
- `AreaKind = AreaKind` (строка 167)
- `CityName = CityName` (строка 168)
- `CityKind = CityKind` (строка 169)
- `TownKind = TownKind` (строка 171)
- `StreetName = StreetName` (строка 172)
- `StreetKind = StreetKind` (строка 173)
- `RegionCode = RegionCode` (строка 176)

Map `seen` дедуплицирует по имени колонки, поэтому каждое уникальное имя генерирует только одну находку.

## Рекомендации по исправлению

### Вариант 1: Расширить `hasConditionEnded` (минимальное исправление)

Добавить распознавание границ операторов и макросов:

```go
func hasConditionEnded(lower string) bool {
    // Существующие ключевые слова
    kws := []string{"group by", "order by", "union", "except", "intersect", ";"}
    for _, kw := range kws {
        if strings.Contains(lower, kw) {
            return true
        }
    }
    // Новые операторы (начало нового SQL-оператора = конец условия)
    stmtKws := []string{"insert ", "update ", "delete ", "select "}
    for _, kw := range stmtKws {
        if strings.HasPrefix(strings.TrimSpace(lower), kw) {
            return true
        }
    }
    // Макросы завершения запроса
    macros := []string{"m_forceorder", "m_keepplan", "m_forceorder_nospool", "m_isolat"}
    for _, m := range macros {
        if strings.Contains(lower, m) {
            return true
        }
    }
    // Разделители батчей
    if strings.TrimSpace(lower) == "go" {
        return true
    }
    return false
}
```

### Вариант 2: Использовать `parenDepth` для определения границ (рекомендуется)

Переменная `parenDepth` уже отслеживается, но не используется. Можно завершать условие при возврате к глубине 0:

```go
// В checkUseEqColumn, после обновления parenDepth:
if parenDepth <= 0 && hasConditionEnded(lower) {
    // ... анализировать буфер
}
// Также: завершать при глубине 0 после WHERE/ON,
// если следующая строка не содержит AND/OR
```

### Вариант 3: Определять контекст SELECT vs WHERE (полное исправление)

В `analyzeConditionForEqColumn` определять, находится ли `=` в секции `SELECT` или `WHERE`:

```go
func analyzeConditionForEqColumn(lines []string, startLine int, file *indexedFile) []Finding {
    // ...
    for _, m := range matches {
        // Определять позицию match в lines и проверять контекст
        if isInSelectClause(lines, matchPos) {
            continue  // Алиас в SELECT — не сравнение
        }
        // ... остальная проверка
    }
}
```

### Комбинированный подход (рекомендуется)

Варианты 1 + 3 вместе:
- Вариант 1 предотвращает переполнение буфера (границы операторов)
- Вариант 3 обрабатывает случаи, когда SELECT и WHERE в одном операторе

## Воспроизведение

**Шаги:**
1. Выполнить: `codebase review C:\NT\FA#\7.2GIT\fa-contracts\API_LoanBureau\Server\Facade\FCD_BCH_Legal_FindListByID.sql`
2. Проверить находки правила `useEqColumn`

**Ожидаемый результат:** Нет замечаний `useEqColumn` (алиасы `SELECT` — не сравнения)
**Фактический результат:** Замечание `useEqColumn line=70 object=townname` — "Нельзя сравнивать столбец с самим собой"

## Заключение

Правило `useEqColumn` имеет две системные проблемы:
1. **Неразрывный буфер условия** — `hasConditionEnded` не распознаёт макросы Diasoft (`M_FORCEORDER`, `M_KEEPPLAN`, `$_END`, `__END_PROCEDURE__`), разделители `go`/`GO` и начала новых операторов
2. **Отсутствие контекста SQL-секции** — `reEqOperands` матчит `=` в любой позиции, не различая `SELECT alias = column` и `WHERE column = column`

**Рекомендуемый приоритет:** High (массовые ложные срабатывания на файлах с INSERT...SELECT)
**Сложность исправления:** Medium
**Затронутое правило:** `useEqColumn`

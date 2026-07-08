# Bug: `datatype` — не проверяются параметры API-контракта при `select @Param = ...` в процедурах с `API_CREATE_PROC`

**Правило:** `datatype`  
**Severity:** 3 (фильтр "делай красиво")  
**Статус:** Open  
**Дата обнаружения:** 2026-07-08  
**Файл:** `fa-contracts/API_Credit/Server/ContractOver/API_COver_CreateAgreement.sql`

## Описание проблемы

Правило `datatype` / подправило `checkDatatypeSelectAssign` не обнаруживает потерю точности при присваивании значения в переменную `@AgreementDate` (тип `DSOPERDAY` по API-контракту), когда источник — выражение типа `DSDATETIME`:

```sql
select @AgreementDate  = case 
                           when @Tmp = 0 then dateadd(dd, convert(numeric, Val), '19000101')
                           else convert(datetime, Val, 103)
                         end
  from pAPI_User_FindSetting M_NOLOCK_INDEX(XPKpAPI_User_FindSetting)
 where SPID   = @@spid
   and UserID = @UserID
   and Type   = SET_CUR_OPER_DAY
M_KEEPPLAN
M_ISOLAT
```

При этом аналогичный finding для `@CreditDateFrom` (строка 264) обнаруживается корректно, потому что эта переменная объявлена в блоке `declare`.

## Типы данных в конфликте

| Переменная | Источник типа | Тип переменной | Тип выражения-источника | Проблема |
|------------|---------------|----------------|-------------------------|----------|
| `@AgreementDate` | API-контракт `API_COver_CreateAgreement` | `DSOPERDAY` | `DSDATETIME` (dateadd/convert datetime) | DSDATETIME → DSOPERDAY |
| `@CreditDateFrom` | `declare` блок (строка 216) | `DSOPERDAY` | `DSDATETIME` (cc.CreditDateFrom) | DSDATETIME → DSOPERDAY ✓ обнаружен |

## Шаги воспроизведения

```powershell
PS C:\NT\FA#\7.2GIT> codebase review C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\Server\ContractOver\API_COver_CreateAgreement.sql
```

**Фактический результат:**
- Finding для `@CreditDateFrom` (line=264): `Потеря точности типов данных: DSDATETIME -> DSOPERDAY`
- Нет finding для `@AgreementDate` (line=250)

**Ожидаемый результат:**
- Дополнительно finding `datatype` line=250 object=@AgreementDate: `Потеря точности типов данных: DSDATETIME -> DSOPERDAY`

## Технический анализ (корневая причина)

### 1) `collectVariableTypes` не знает о параметрах API-контракта

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_helpers.go` (строка 1997)

Функция `collectVariableTypes` собирает типы переменных из двух источников:

```go
func collectVariableTypes(parsed *sqlparser.ParseResult, content string) map[string]string {
    result := make(map[string]string)
    // Источник 1: параметры процедуры из parsed.Procedures[*].Params
    for _, proc := range parsed.Procedures {
        for _, p := range proc.Params {
            result[normalizeVariableName(p.Name)] = p.Type
        }
    }
    // Источник 2: переменные из блоков declare
    for _, block := range extractDeclareBlocks(content) {
        for _, m := range reDeclareVar.FindAllStringSubmatch(block, -1) {
            result[normalizeVariableName(m[1])] = m[2]
        }
    }
    return result
}
```

Для процедур с `API_CREATE_PROC` **ни один из этих источников не содержит параметры API-контракта**.

### 2) SQL-парсер не извлекает параметры из `API_CREATE_PROC`

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go`

При обработке `API_CREATE_PROC(API_COver_CreateAgreement)` (строка 930-932):

```go
if matches := p.procCreateRe.FindStringSubmatch(trimmed); matches != nil {
    flushStatement(lineNum - 1)
    continue  // ← просто пропускает, не создаёт процедуру с параметрами
}
```

Далее, `__BEGIN_PROCEDURE__(API_COver_CreateAgreement)` (строка 938-952):

```go
if matches := p.procBeginRe.FindStringSubmatch(trimmed); matches != nil {
    ...
    inProcSignature = !strings.Contains(strings.ToUpper(trimmed), "__BEGIN_PROCEDURE__")
    // ↑ для __BEGIN_PROCEDURE__ inProcSignature = false → параметры НЕ парсятся
    currentProc = &model.SQLProcedure{
        ProcName: procName,
        Params:   make([]model.SQLParam, 0), // ← пустой список параметров
    }
}
```

### 3) `checkDatatypeSelectAssign` пропускает неизвестные переменные

Файл: `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go` (строка 194-198):

```go
targetType, exists := variableTypes[targetVariable]
if !exists || targetType == "" {
    continue  // ← @AgreementDate не найден в variableTypes → проверка пропущена
}
```

### Цепочка причин

```
API_CREATE_PROC(name) → парсер не создаёт proc с params
    → __BEGIN_PROCEDURE__(name) → inProcSignature = false → params не парсятся
        → parsed.Procedures[0].Params = [] (пусто)
            → collectVariableTypes не находит @AgreementDate
                → checkDatatypeSelectAssign пропускает проверку
```

## Масштаб проблемы

Все процедуры, реализованные через макрос `API_CREATE_PROC` (это ВСЕ API-процедуры в каталогах `API_*/Server/`), имеют параметры, определённые в API-контракте (XML), а не в теле SQL-файла. Для таких процедур:

- Переменные-параметры API **не попадают** в `variableTypes`
- Потеря точности при `select @ApiParam = expression` **не обнаруживается**
- Потеря точности при `exec other_proc @Param = @ApiParam` **не обнаруживается** (в `checkDatatypeExecParams` используется тот же `collectVariableTypes`)

## Предложение по исправлению

### Вариант 1: Дополнить `collectVariableTypes` fallback-запросом к БД (рекомендуется)

В `checkDatatypeSelectAssign` (и аналогично в `checkDatatypeExecParams`) после вызова `collectVariableTypes`:

1. Определить имя текущей процедуры (из `parsed.Procedures[0].ProcName` или из `API_CREATE_PROC(...)` в содержимом файла)
2. Вызвать `r.lookupProcedureParams(procName)` — эта функция уже существует и поддерживает fallback в `lookupAPIContractParams`
3. Добавить полученные параметры в `variableTypes`

```go
// После: variableTypes := collectVariableTypes(parsed, string(content))
// Добавить:
if r.db != nil {
    procName := extractCurrentProcName(parsed, string(content))
    if procName != "" {
        if apiParams, err := r.lookupProcedureParams(procName); err == nil {
            for _, p := range apiParams {
                name := normalizeVariableName(p.Name)
                if _, exists := variableTypes[name]; !exists {
                    variableTypes[name] = p.Type
                }
            }
        }
    }
}
```

### Вариант 2: Расширить парсер для обработки `API_CREATE_PROC`

В `sql_parser.go` при обнаружении `API_CREATE_PROC(name)` — обращаться к БД индекса за параметрами контракта и заполнять `currentProc.Params`. Более инвазивный вариант, требует передачи DB-handle в парсер.

## Связанные файлы

- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_rules.go` — `checkDatatypeSelectAssign`, `checkDatatypeExecParams`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_helpers.go` — `collectVariableTypes`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\review\review_lookup.go` — `lookupProcedureParams`, `lookupAPIContractParams`
- `C:\NT\FA#\7.2GIT\Tools\CodeBase\Source\internal\parser\sql\sql_parser.go` — обработка `API_CREATE_PROC`
- `C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\Server\ContractOver\API_COver_CreateAgreement.sql` — файл-пример
- `C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\DSArchitectData\BObject\ContractOver\Service\API_COver_CreateAgreement.xml` — API-контракт с параметрами

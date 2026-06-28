# datatype: проверка потери точности при вызове процедур (EXEC)

Доработать существующее правило `datatype` для детектирования сужения точности и несовместимых типов при передаче аргументов в параметры вызываемой процедуры (EXEC/EXECUTE).

## Контекст

Текущее правило `datatype` проверяет 5 кейсов: ColumnDefinitions, INSERT...SELECT, UPDATE...SET, SELECT @var=expr, FETCH INTO. Вызовы процедур (EXEC) не проверяются. Вся инфраструктура уже есть: `lookupProcedureParams`, `parseExecArguments`, `resolveArgType`, `collectExecCallLines`, `collectVariableTypes`, `areEquivalentTypes`, `normalizeDataType`.

## План

### Шаг 1: Функция `checkDatatypeExecParams` (review_rules.go)

Новый метод `checkDatatypeExecParams(parsed, file)`:

1. Получить `variableTypes` через `collectVariableTypes(parsed, content)`
2. Для каждого вызова из `dedupeProcedureCalls(parsed.Calls)`:
   - `lookupProcedureParams(call.Name)` — получить `[]model.SQLParam` (Name, Type, Direction)
   - Если `sql.ErrNoRows` — пропустить (это ловит `execNotExistsProc`)
   - `collectExecCallLines(r.exec.lines, call.Line)` → `parseExecArguments(callText, call.Name)`
   - Сопоставить аргументы с параметрами:
     - **Именованные**: `arg.Name` → найти в `paramMap[normalizeIdentifier(p.Name)]`
     - **Позиционные**: по индексу `i` → `params[i]`
   - Для каждого сопоставленного аргумента:
     - `paramType = normalizeDataType(param.Type)`
     - `argType = r.resolveArgType(arg.Value, variableTypes, nil)` (aliasMap=nil, т.к. EXEC не в SELECT-контексте)
     - Если `argType == ""` или `paramType == ""` — пропустить
     - Проверить `areEquivalentTypes(argType, paramType)`:
       - Если false → finding `datatype` с сообщением о несовместимости/сужении
       - Использовать существующую логику `hasPrecisionLoss(srcType, dstType)` для детектирования сужения vs несовместимости
   - Пропустить `arg.Value == "null"` (NULL совместим с любым типом)
   - Пропустить output-параметры (IsOutput=true) — для OUT параметров тип передаётся FROM процедуры, не INTO неё

### Шаг 2: Регистрация в `checkDatatype` (review_rules.go)

Добавить вызов `checkDatatypeExecParams` в `checkDatatype` (после `fetchIntoFindings`):
```go
execParamsFindings, err := r.checkDatatypeExecParams(parsed, file)
```

### Шаг 3: Хелпер `hasPrecisionLoss` (review_helpers.go)

Если ещё не существует — добавить функцию для классификации:
- **Сужение точности**: `datetime→date/smalldatetime`, `numeric(15,2)→numeric(10,0)`, `varchar(250)→varchar(20)`, `DSBIGMONEY→DSMONEY`
- **Несовместимые типы**: `varchar→int`, `float→varchar`, `datetime→int` и т.д.
- Использует `typeGroup()` и `normalizeDataType()` уже существующие

Сообщение finding:
- Сужение: `"Потеря точности при передаче в параметр @Name: %s → %s"`
- Несовместимость: `"Несовместимые типы при передаче в параметр @Name: %s → %s"`

### Шаг 4: Тесты (review_rules_test.go)

- `TestCheckDatatypeExecParams_NoPrecisionLoss` — `exec proc @Date = @OperDay` где оба DSOPERDAY
- `TestCheckDatatypeExecParams_DatetimeToDate` — `exec proc @Date = @FullDate` где FullDate=datetime, param=date → finding
- `TestCheckDatatypeExecParams_NumericNarrowing` — `exec proc @Amount = @BigAmount` где BigAmount=numeric(15,2), param=numeric(10,0) → finding
- `TestCheckDatatypeExecParams_VarcharNarrowing` — `exec proc @Name = @LongName` где LongName=varchar(250), param=varchar(20) → finding
- `TestCheckDatatypeExecParams_IncompatibleTypes` — `exec proc @Count = @Name` где Name=varchar, param=int → finding
- `TestCheckDatatypeExecParams_NullArg_NoFinding` — `exec proc @Date = null` → no finding
- `TestCheckDatatypeExecParams_OutputParam_NoFinding` — `exec proc @Result = @Var output` → no finding
- `TestCheckDatatypeExecParams_PositionalArgs` — позиционные аргументы с сужением
- `TestCheckDatatypeExecParams_ProcNotFound_NoFinding` — процедура не в БД → no finding

### Шаг 5: Сборка и проверка

- `go build ./...`
- `go test ./internal/review/... -run TestCheckDatatype -v -count=1`
- `codebase review <файл> --rules datatype` на реальном файле

## Файлы

- `internal/review/review_rules.go` — новый метод `checkDatatypeExecParams`, регистрация в `checkDatatype`
- `internal/review/review_helpers.go` — хелпер `hasPrecisionLoss` (классификация сужения/несовместимости)
- `internal/review/review_rules_test.go` — новые тесты

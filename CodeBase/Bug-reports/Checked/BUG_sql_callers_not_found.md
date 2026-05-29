# Bug Report: SQL Procedure Callers Not Found via Query Callers

## Версия CodeBase
- **Версия**: 0.6.7 build 636
- **Дата обнаружения**: 05.05.2026

## Описание проблемы
`query callers --procedure FCD_CCred_FindListIDByParam` возвращает пустой результат, хотя в файле `API_CCred_FindListIDByParam.sql` есть вызов этой процедуры.

## Пример воспроизведения
```powershell
& ".\Tools\CodeBase\CodeBase.exe" query callers --procedure FCD_CCred_FindListIDByParam
```

**Ожидаемый результат:** Найден caller `API_CCred_FindListIDByParam`

**Фактический результат:** Пустой результат `[]`

## Файл с примером
`C:\NT\FA#\7.2GIT\fa-contracts\API_Credit\Server\ContractCredit\API_CCred_FindListIDByParam.sql`

Содержимое (строки 91-98):
```sql
__BEGIN_PROCEDURE__(API_CCred_FindListIDByParam)
  M_BUSINESSLOG_BEGIN

  declare @FinOperID  DSIDENTIFIER

  M_DELETE_PTABLE(pAPI_ContractCredit_ID)

  exec @RetVal = FCD_CCred_FindListIDByParam
                   @ParticipantID      = @ParticipantID,
                   @ParticipantType    = @ParticipantType,
                   ...
```

## Проверка существования процедур
```powershell
& ".\Tools\CodeBase\CodeBase.exe" query procedure --name FCD_CCred_FindListIDByParam
# Результат: ID:8458, файл: fa-contracts/API_Credit/Server/Facade/FCD_CCred_FindListIDByParam.sql

& ".\Tools\CodeBase\CodeBase.exe" query procedure --name API_CCred_FindListIDByParam
# Результат: ID:8114, файл: fa-contracts/API_Credit/Server/ContractCredit/API_CCred_FindListIDByParam.sql
```

Обе процедуры существуют в индексе.

## Корневая причина

### 1. SQL Parser (`Source/internal/parser/sql/sql_parser.go` строки 1492-1506)
```go
// Exec procedure calls
if matches := p.execRe.FindStringSubmatch(trimmed); matches != nil {
    callerName := ""
    if currentProc != nil {
        callerName = currentProc.ProcName
    }
    calleeName := strings.TrimSpace(strings.Trim(matches[1], "[]"))
    if calleeName == "" {
        continue
    }
    result.Calls = append(result.Calls, &model.SQLProcedureCall{
        CallerName: callerName,
        CalleeName: calleeName,
        LineNumber: lineNum,
    })
}
```

Парсер корректно распознает exec вызовы и сохраняет CallerName и CalleeName.

### 2. Индексатор SQL Relations (`Source/internal/indexer/indexer_relations.go` строки 224-254)
```go
func (idx *Indexer) buildSQLProcedureRelations(fileID int64, procedures []*model.SQLProcedure, tables []*model.SQLTable, calls []*model.SQLProcedureCall) ([]*model.Relation, error) {
    procedureIDs, err := idx.db.FindSQLProcedureIDsByFile(fileID)
    if err != nil {
        return nil, err
    }
    ...
    for _, call := range calls {
        if call == nil {
            continue
        }
        sourceID := procedureIDs[strings.ToLower(strings.TrimSpace(call.CallerName))]
        if sourceID == 0 {
            continue  // <--- ПРОБЛЕМА: если CallerName не найден в этом файле, relation не создается
        }
        targetID, err := idx.db.FindLatestSQLProcedureIDByName(call.CalleeName)
        if err != nil {
            if err == dbsql.ErrNoRows {
                continue
            }
            return nil, err
        }
        ...
    }
}
```

### 3. Проблема
Функция `buildSQLProcedureRelations` ищет `sourceID` только среди процедур **в том же файле** (`FindSQLProcedureIDsByFile(fileID)`).

В примере:
- Вызов происходит в файле `API_CCred_FindListIDByParam.sql`
- Вызывающая процедура `API_CCred_FindListIDByParam` находится в этом же файле (ID:8114)
- Вызываемая процедура `FCD_CCred_FindListIDByParam` находится в другом файле `FCD_CCred_FindListIDByParam.sql` (ID:8458)

Код должен работать корректно в этом случае, так как:
1. `CallerName` должен быть установлен парсером в "API_CCred_FindListIDByParam"
2. `procedureIDs` должен содержать этот ID для этого файла

**Возможные причины:**
1. Парсер не распознает процедуру с макросом `__BEGIN_PROCEDURE__` как текущую процедуру (`currentProc` остается nil)
2. `CallerName` остается пустым, поэтому `sourceID` == 0 и relation не создается
3. Или проблема в другом месте логики

## Предлагаемое решение
Нужно добавить отладочное логирование в `buildSQLProcedureRelations` для проверки:
1. Какие CallerName и CalleeName приходят из парсера
2. Какие procedureIDs загружаются для файла
3. Почему sourceID == 0 для этого вызова

Также можно улучшить логику: если CallerName пустой, искать процедуру по номеру строки (как это делается в `buildT01GeneratedSubscriberRelations`).

## Влияние
- **Серьезность**: Medium
- **Влияние**: Пользователи не могут найти callers для процедур, если relation не создается
- **Обходной путь**: Использовать grep для поиска вызовов в файлах

## Связанные файлы
- `Source/internal/parser/sql/sql_parser.go` - парсер SQL процедур и exec вызовов
- `Source/internal/indexer/indexer_relations.go` - создание relations для calls_procedure
- `Source/internal/indexer/indexer_sql_pas.go` - вызов buildSQLProcedureRelations

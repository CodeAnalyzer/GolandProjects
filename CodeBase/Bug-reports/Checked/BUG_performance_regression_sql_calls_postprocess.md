# Bug Report: Performance Regression - SQL Procedure Call Relations Post-Processing

## Версия CodeBase
- **Версия**: 0.6.7 build 636+
- **Дата обнаружения**: 05.05.2026
- **Связанный баг**: BUG_sql_callers_not_found.md

## Описание проблемы
После исправления BUG_sql_callers_not_found.md время выполнения `codebase init` существенно увеличилось.

## Метрики производительности

### До исправления
```
started_at=2026-05-05 00:02:05
command="codebase init"
duration=51m19.015s
duration_ms=3079014
status=success
error=""
```

### После исправления
```
started_at=2026-05-05 21:19:14
command="codebase init"
duration=1h12m32.304s
duration_ms=4352303
status=success
error=""
```

### Изменение
- **Увеличение времени**: +21 минута 13 секунд (+40%)
- **Размер кодовой базы**: 109377 файлов, 101219 проиндексировано
- **Параллелизм**: 12 воркеров

## Корневая причина

Исправление BUG_sql_callers_not_found.md перенесло создание relations для SQL procedure calls из параллельной обработки файлов в последовательную пост-обработку.

### Было (до исправления)
```go
// internal/indexer/indexer_relations.go (удалено)
func (idx *Indexer) buildSQLProcedureRelations(fileID int64, procedures []*model.SQLProcedure, tables []*model.SQLTable, calls []*model.SQLProcedureCall) ([]*model.Relation, error) {
    // Relations создавались сразу во время обработки каждого файла
    // Выполнялось параллельно воркерами
}
```

### Стало (после исправления)
```go
// internal/indexer/indexer_sql_pas.go (строка 342)
idx.addPendingSQLCalls(fileID, path, proceduresBatch, procedureIDs, result.Calls)

// internal/indexer/indexer.go (строки 177-190)
func (idx *Indexer) addPendingSQLCalls(fileID int64, filePath string, procedures []*model.SQLProcedure, procedureIDs map[string]int64, calls []*model.SQLProcedureCall) {
    // Клонирование всех процедур, procedureIDs и вызовов в память
    idx.pendingSQLCalls = append(idx.pendingSQLCalls, &PendingSQLCallFile{
        FileID:       fileID,
        FilePath:     filePath,
        Procedures:   append([]*model.SQLProcedure(nil), procedures...),
        ProcedureIDs: cloneInt64Map(procedureIDs),
        Calls:        append([]*model.SQLProcedureCall(nil), calls...),
    })
}

// internal/indexer/runner.go (строка 103)
workersWG.Wait()
idx.postProcessSQLProcedureCallRelations(collector)  // Последовательная пост-обработка
```

## Факторы замедления

1. **Дополнительное копирование памяти**
   - Для каждого SQL файла клонируются процедуры, procedureIDs и вызовы
   - При 101219 проиндексированных файлах это существенный overhead

2. **Последовательная пост-обработка**
   - Relations для SQL calls создаются последовательно одним потоком
   - Выполняется после завершения всех параллельных воркеров (`workersWG.Wait()`)
   - Ранее создавались параллельно во время обработки файлов

3. **Увеличение пикового потребления памяти**
   - Все pending данные хранятся в памяти до завершения индексации
   - `snapshotPendingSQLCalls()` копирует весь массив перед обработкой

## Влияние
- **Серьезность**: Medium
- **Влияние**: Увеличение времени индексации на ~40%
- **Обходной путь**: Нет

## Связанные файлы
- `internal/indexer/indexer_sql_pas.go` - вызов addPendingSQLCalls
- `internal/indexer/indexer.go` - PendingSQLCallFile, addPendingSQLCalls, snapshotPendingSQLCalls
- `internal/indexer/indexer_postprocess_sql_calls.go` - postProcessSQLProcedureCallRelations
- `internal/indexer/indexer_relations.go` - buildSQLProcedureCallRelationsWithResolvers
- `internal/indexer/runner.go` - вызов postProcessSQLProcedureCallRelations

## Возможные оптимизации

1. **Параллельная пост-обработка**
   - Разделить pendingSQLCalls на чанки и обрабатывать параллельно
   - Использовать worker pool для buildSQLProcedureCallRelationsWithResolvers

2. **Уменьшение копирования**
   - Хранить только минимально необходимые данные в PendingSQLCallFile
   - Избегать клонирования procedureIDs если возможно

3. **Batch-обработка**
   - Обрабатывать чанки pending данных по мере накопления
   - Не ждать завершения всех файлов

## Статус
- **Тип**: Регресс производительности
- **Причина**: Известно (исправление BUG_sql_callers_not_found.md)
- **Приоритет оптимизации**: Medium

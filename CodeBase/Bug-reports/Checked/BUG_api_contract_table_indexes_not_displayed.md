# BUG: API Table Indexes Not Displayed in `codebase init`

## Description

При выполнении `codebase init` в выводе показывается `Table indexes: 0` в разделе "API XML Entities", хотя через `codebase stats --json` видно, что в базе данных есть 7901 индекс таблиц API (`"api_table_indexes": 7901`).

## Example

**Вывод `codebase init`:**
```
API XML Entities:
  Contracts:      11728
  Params:         58001
  Tables:         29817
  Table fields:   220828
  Table indexes:  0
```

**Вывод `codebase stats --json`:**
```json
{
  "entities": {
    "api_table_indexes": 7901
  }
}
```

## Root Cause

В функции `mergeScanStats` в `internal/indexer/indexer.go` отсутствует строка для мержинга поля `APITableIndexes`. При обработке файлов в worker pool локальная статистика (`localStats`) обновляется правильно (включая `APITableIndexes` при парсинге XML бизнес-объектов), но при мерже в collector это поле не учитывается.

**Детали:**

1. В `internal/indexer/indexer.go` (строка 890) при парсинге XML бизнес-объектов статистика обновляется правильно:
   ```go
   stats.APITableIndexes += len(result.BusinessTableIndexes)
   ```

2. В `internal/indexer/indexer.go` (строки 119-121) локальная статистика мержится в collector:
   ```go
   collector.Add(func(stats *model.ScanStats) {
       mergeScanStats(stats, localStats)
   })
   ```

3. В функции `mergeScanStats` (строки 61-90) мержатся все поля статистики, **но нет строки для `APITableIndexes`**:
   ```go
   func mergeScanStats(dst *model.ScanStats, src *model.ScanStats) {
       dst.FilesScanned += src.FilesScanned
       dst.FilesIndexed += src.FilesIndexed
       // ... другие поля ...
       dst.APITables += src.APITables
       dst.APITableFields += src.APITableFields
       // ОТСУТСТВУЕТ: dst.APITableIndexes += src.APITableIndexes
       dst.Procedures += src.Procedures
       // ... другие поля ...
   }
   ```

## Files Fixed

**`internal/indexer/indexer.go`** (строка 80):
```go
dst.APITableFields += src.APITableFields
dst.APITableIndexes += src.APITableIndexes  // Добавлено
dst.Procedures += src.Procedures
```

## Impact

- Статистика `codebase init` показывала неверное количество индексов таблиц API (0 вместо реального значения)
- После исправления статистика будет показывать корректное количество индексов бизнес-объектов

## Status

**FIXED** - Добавлено мержинг `APITableIndexes` в функцию `mergeScanStats`.

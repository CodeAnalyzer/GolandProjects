# Bug Report: API Table Indexes не индексируются в unified symbols index

**ID:** 001  
**Дата:** 06.05.2026  
**Версия CodeBase:** 0.6.7+  
**Статус:** Open  
**Приоритет:** Medium

## Описание

API table indexes (индексы API таблиц из DSArchitect XML) не добавляются в unified symbols index (таблица `symbols`), что делает невозможным их поиск через команду `query symbol --type api_table_index`.

## Ожидаемое поведение

API table indexes должны индексироваться в таблице `symbols` с типом `api_table_index`, аналогично тому как:
- SQL индексы индексируются с типом `index`
- API таблицы индексируются с типом `api_table`
- API бизнес-объекты индексируются с типом `api_business_object`

Это позволит использовать `query symbol --type api_table_index` для поиска API table indexes через unified symbols index.

## Фактическое поведение

API table indexes хранятся только в отдельной таблице `api_business_object_table_indexes` и не добавляются в таблицу `symbols`. Команда `query symbol --type api_table_index` возвращает пустой результат.

Для поиска API table indexes необходимо использовать специализированную команду `query api-table-index`.

## Шаги воспроизведения

1. Выполнить команду:
   ```
   codebase query symbol --name XIE0pAPI_CCred_Agreement --type api_table_index
   ```
2. Результат: `count: 0, items: []`

3. Выполнить команду:
   ```
   codebase query api-table-index --name XIE0pAPI_CCred_Agreement
   ```
4. Результат: индекс найден (2 записи в CreditBankProduct и ContractCredit)

## Анализ кода

**Место индексации API table indexes:**
- Файл: `internal/indexer/indexer.go`
- Строка: ~1030
- Функция: `BatchInsertAPIBusinessObjectTableIndexes`
- Таблица: `api_business_object_table_indexes`

**Место индексации SQL индексов (для сравнения):**
- Файл: `internal/indexer/indexer_sql_pas.go`
- Строка: ~228
- Код:
  ```go
  indexSymbolsBatch = append(indexSymbolsBatch, &model.Symbol{
      FileID:     fileID,
      SymbolName: indexDefinition.IndexName,
      SymbolType: "index",
      EntityType: "sql",
      EntityID:   indexID,
      LineNumber: indexDefinition.LineNumber,
      Signature:  signature,
  })
  ```

**Место индексации API таблиц (для сравнения):**
- Файл: `internal/parser/dsxml/parser.go`
- Строка: ~225
- Код:
  ```go
  res.Symbols = append(res.Symbols, &model.Symbol{
      SymbolName: strings.TrimSpace(table.ParamName),
      SymbolType: "api_table",
      EntityType: "xml",
      LineNumber: 1,
      Signature: strings.TrimSpace(table.ParamName)
  })
  ```

**Проблема:** В коде индексации API table indexes отсутствует добавление в `res.Symbols` аналогично SQL индексам и API таблицам.

## Влияние

- **Функциональное:** Невозможно использовать `query symbol --type api_table_index` для поиска API table indexes
- **Тестирование:** Тестовый план содержит ошибочные тесты, ожидающие работу этой команды
- **Пользовательский опыт:** Непоследовательность в API - некоторые типы индексов доступны через unified symbols, другие - только через специализированные команды

## Предлагаемое решение

Добавить индексацию API table indexes в таблицу `symbols` с типом `api_table_index` в файле `internal/indexer/indexer.go` после строки 1030:

```go
// После BatchInsertAPIBusinessObjectTableIndexes
businessTableIndexIDs, err := idx.db.FindAPIBusinessObjectTableIndexIDsByFile(fileID)
if err != nil {
    return err
}

// Добавить индексы в symbols
for _, item := range result.BusinessTableIndexes {
    key := store.BuildAPIBusinessObjectTableIndexLookupKey(item.BusinessObject, item.ParentTableName, item.IndexName)
    if indexID, exists := businessTableIndexIDs[key]; exists {
        indexSymbolsBatch = append(indexSymbolsBatch, &model.Symbol{
            FileID:     fileID,
            SymbolName: item.IndexName,
            SymbolType: "api_table_index",
            EntityType: "api",
            EntityID:   indexID,
            LineNumber: item.LineNumber,
            Signature:  item.IndexName,
        })
    }
}
```

Также добавить функцию `BuildAPIBusinessObjectTableIndexLookupKey` в `internal/store/api_store.go` для генерации ключа поиска.

## Альтернативное решение

Если добавление в unified symbols index нежелательно, обновить документацию и тестовый план, указав что:
- API table indexes доступны только через `query api-table-index`
- Тип `api_table_index` не поддерживается в `query symbol`

## Зависимости

- Требуется обновление тестового плана `codebase-testing-plan.md`
- Возможно обновление документации CodeBase

## Приложения

- Тест-план: `Tests/codebase-testing-plan.md`
- Отчет о тестировании: `Tests/test-report.md`
- XML файл с индексом: `fa-contracts/API_Credit/DSArchitectData/BObject/ContractCredit/Table/pAPI_CCred_Agreement.xml` (строка 100-114)

## История изменений

- 06.05.2026 - Создан баг-репорт

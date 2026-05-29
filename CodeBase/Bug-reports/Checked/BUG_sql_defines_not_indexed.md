# Bug Report: SQL #define Macros Not Indexed

## Версия CodeBase
- **Версия**: 0.6.7 build 636
- **Дата обнаружения**: 05.05.2026

## Описание проблемы
`#define` макросы из SQL файлов не индексируются и не находятся через команду `query symbol --name <NAME> --type define`.

## Пример воспроизведения
```powershell
& ".\Tools\CodeBase\CodeBase.exe" query symbol --name M_MAIN_RISK_GROUPID_BRIEF --type define
```

**Ожидаемый результат:** Найден макрос `M_MAIN_RISK_GROUPID_BRIEF` из файла `fa-contracts/Consumer/SERVER/Consumer/API_Cons_Disp_QtyTags.sql`

**Фактический результат:** Пустой результат `[]`

## Файл с примером
`C:\NT\FA#\7.2GIT\fa-contracts\Consumer\SERVER\Consumer\API_Cons_Disp_QtyTags.sql`

Содержимое (строка 8):
```sql
#define M_MAIN_RISK_GROUPID_BRIEF 'ГрРискаОсн'
```

## Корневая причина

### 1. SQL Parser (`Source/internal/parser/sql/sql_parser.go`)
- Парсер корректно находит `#define` в SQL файлах (строки 808-813)
- Сохраняет в `result.Defines[matches[1]] = matches[2]`
- Но это только парсинг, без сохранения в БД

```go
// Проверяем #define
if matches := p.defineRe.FindStringSubmatch(trimmed); matches != nil {
    flushStatement(lineNum)
    result.Defines[matches[1]] = matches[2]
    continue
}
```

### 2. SQL Indexer (`Source/internal/indexer/indexer_sql_pas.go`)
- Функция `parseSQLLikeFile` (строки 22-346) обрабатывает SQL файлы
- **НЕ использует** `result.Defines` из SQL файлов
- Обрабатывает только: procedures, tables, columns, column_definitions, index_definitions, fragments, relations

### 3. H Indexer (`Source/internal/indexer/indexer.go`)
- Обрабатывает `result.Defines` только для H-файлов (строки 172-199)
- Создает записи в таблице `h_files_defines` и символы с типом `define`

## Вывод
`#define` макросы индексируются только из H-файлов, но не из SQL файлов. Макросы из SQL файлов игнорируются при индексации.

## Предлагаемое решение
Добавить обработку `result.Defines` из SQL файлов в функции `parseSQLLikeFile` в `indexer_sql_pas.go`, аналогично обработке в H-файлах:

1. Создать batch для SQL defines
2. Вставить в таблицу `h_files_defines` (или создать отдельную таблицу для SQL defines)
3. Создать символы с типом `define` в unified symbols index

## Влияние
- **Серьезность**: Medium
- **Влияние**: Пользователи не могут искать макросы из SQL файлов через CodeBase
- **Обходной путь**: Искать макросы вручную в файлах или использовать H-файлы

## Связанные файлы
- `Source/internal/parser/sql/sql_parser.go` - парсер SQL файлов
- `Source/internal/indexer/indexer_sql_pas.go` - индексатор SQL файлов
- `Source/internal/indexer/indexer.go` - индексатор H файлов (пример обработки defines)
- `Source/internal/store/db.go` - схема БД (таблица `h_files_defines`)

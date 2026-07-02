# Доработка правила indexExistsInDB: проверка индексов для макросов M_DELETE_PTABLE*

Расширить правило `indexExistsInDB` для обнаружения вызовов макросов семейства `M_DELETE_PTABLE*` и проверки существования подразумеваемых/явных индексов в БД.

## Контекст

Макрос `M_DELETE_PTABLE(table)` определён в `MACROS.H` и раскрывается в `M_DELETE_PTABLE_INDEX(table, XPKtable)`. Препроцессор CodeBase (`replaceMacros`) раскрывает только `#define` из самого SQL-файла, поэтому вызов `M_DELETE_PTABLE(pTable)` остаётся нераскрытым. Текущее правило `checkIndexExistsInDB` ищет хинты `M_*_INDEX(name)` в FROM-клаузе — и не видит эти макросы.

## Макросы для проверки

| Макрос | Аргумент-таблица | Индекс | Источник |
|--------|-----------------|--------|----------|
| `M_DELETE_PTABLE(table)` | arg 1 | `XPK` + table | авто |
| `M_DELETE_PTABLE_INMEM(table)` | arg 1 | `XPK` + table | авто |
| `M_DELETE_PTABLE_PARALLEL(table, spid, col, batch, parallel)` | arg 1 | `XPK` + table | авто |
| `M_DELETE_PTABLE_INDEX(table, index)` | arg 1 | arg 2 | явный |
| `M_DELETE_PTABLE_SPID_INDEX(table, index, spid)` | arg 1 | arg 2 | явный |
| `M_DELETE_PTABLE_SPID_UNIQUE(table, index, spid, unique)` | arg 1 | arg 2 | явный |

## План реализации

### Шаг 1: Добавить функцию `extractDeletePtableCalls`

В `internal/review/review_rules.go` (или `review_lookup.go`):

- Регулярка для поиска `M_DELETE_PTABLE*` вызовов в тексте (case-insensitive, с учётом границ слова)
- Парсинг аргументов макроса (splitTopLevelCSV по запятым внутри скобок)
- Возвращает `[]deletePtableCall { tableName, indexName, macroName, line }`
- Для авто-вариантов: `indexName = "XPK" + tableName`
- Для явных: `indexName = arg2`
- Пропускать `M_DELETE_PTABLE_RUN` (это не вызов удаления, а макрос управления параллельностью)

### Шаг 2: Интегрировать в `checkIndexExistsInDB`

В функции `checkIndexExistsInDB` (review_rules.go:1279):

- После основного цикла по строкам (или параллельно с ним) — добавить сканирование обработанного контента на наличие `M_DELETE_PTABLE*` вызовов
- Для каждого найденного вызова: `lookupIndexExists(tableName, indexName)`
- Если индекс не найден → Finding с сообщением: `Для таблицы <table> не найден индекс <index>, используемый макросом <macro>`
- Line: маппинг через `mapExpandedLineToOriginal` (использовать SourceMap из macroResult)
- Object: `<table>.<index>`

### Шаг 3: Unit-тесты

В `internal/review/review_rules_test.go`:

1. **TestExtractDeletePtableCalls_BasicMacro** — `M_DELETE_PTABLE(pMyTable)` → tableName=pMyTable, indexName=XPKpMyTable
2. **TestExtractDeletePtableCalls_Inmem** — `M_DELETE_PTABLE_INMEM(pMyTable)` → тот же результат
3. **TestExtractDeletePtableCalls_Parallel** — `M_DELETE_PTABLE_PARALLEL(pMyTable, @@spid, ID, 1000, 4)` → tableName=pMyTable, indexName=XPKpMyTable
4. **TestExtractDeletePtableCalls_ExplicitIndex** — `M_DELETE_PTABLE_INDEX(pMyTable, XPKpMyOtherIndex)` → tableName=pMyTable, indexName=XPKpMyOtherIndex
5. **TestExtractDeletePtableCalls_SpidIndex** — `M_DELETE_PTABLE_SPID_INDEX(pMyTable, XPKpMyTable, @@spid)` → tableName=pMyTable, indexName=XPKpMyTable
6. **TestExtractDeletePtableCalls_SpidUnique** — `M_DELETE_PTABLE_SPID_UNIQUE(pMyTable, XPKpMyTable, @@spid, ID)` → tableName=pMyTable, indexName=XPKpMyTable
7. **TestExtractDeletePtableCalls_MultipleCalls** — несколько вызовов в одном тексте
8. **TestExtractDeletePtableCalls_SkipsRun** — `M_DELETE_PTABLE_RUN(4)` не должен детектиться
9. **TestExtractDeletePtableCalls_CaseInsensitive** — `m_delete_ptable(pMyTable)` (нижний регистр)

### Шаг 4: Проверка (пользователь делает сам)

- `go build ./...`
- `go test ./internal/review/... -count=1`
- Тест на реальном файле из репозитория FA

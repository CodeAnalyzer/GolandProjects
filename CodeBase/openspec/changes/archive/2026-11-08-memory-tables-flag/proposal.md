## Why

В Diasoft 5NT часть таблиц (как API-таблиц `pAPI_*`, так и обычных `p*`-таблиц,
например `pRuleDoc_QtyStack`) переводится в память через installation-extension
с файлом `ModuleTableInMemory.lst`. CodeBase не видит этот факт: «таблица в памяти»
нигде в индексе не отражена, хотя признак важен для навигации, анализа зависимостей
(процедуры, зависящие от in-memory таблиц, переливаются при смене расположения)
и будущих review-проверок (memory-optimized таблицы в MSSQL имеют ограничения T-SQL).
Файл декларативно версируется в git рядом с исходниками (checkout
`fa-dbmaintenance-ext-tbl-in-memory` репозитория `fa-dbmaintenance-ext`, ветка
`prj/TblInMemory`; в develop-копии файла нет), но не индексируется: расширение
`.lst` не входит в include-паттерны, отсутствует в белом списке поддерживаемых
расширений walker'а и формат не разбирается.

## What Changes

- `.lst`-файлы попадают в обход индексации: `*.lst` добавляется в дефолтные
  `include_patterns` (конфиг-фолбэк и `codebase.toml`).
- Новый парсер формата: файл с каноническим именем `ModuleTableInMemory.lst`
  разбирается (секции `[<Продукт>]` + строки `<Таблица>;<Модуль>`); прочие `.lst`
  (например `ld_AfterApiReg.lst`) индексируются как файлы без сущностей — по
  прецеденту XML вне `/DSArchitectData/`.
- Новая таблица БД `memory_tables` (схема v7): нормализованный продукт из секции,
  модуль, имя таблицы, привязка к файлу с каскадным удалением.
- Признак «объявлена как таблица в памяти» обогащает результаты
  `query table` / `codebase_query_table` и `query api-table` /
  `codebase_query_api_table`: join чисто по имени таблицы (физическая таблица
  одна на БД — флаг верен для вхождений в любом продукте).
- Хранение выбрано доступным для будущих review-правил (атрибут в собственной
  таблице, читаемой статическим анализом), сами правила — вне scope.

## Capabilities

### New Capabilities

- `indexing/lst-parsing`: разбор `ModuleTableInMemory.lst` — диспетчеризация по
  каноническому имени файла, нормализация продукта из секции, lenient-разбор
  строк `Таблица;Модуль`, семантика признака «объявлена как таблица в памяти».

### Modified Capabilities

- `indexing/file-walking`: дефолтные `include_patterns` пополняются `*.lst`.
- `infrastructure/database-schema`: новая таблица сущностей `memory_tables`
  (file_id CASCADE, product_name, module_name, table_name, line_number),
  bump маркера версии схемы.
- `query/entity-queries`: результат `query table` обогащается признаком
  in-memory (name-based, без product-scope).
- `query/api-relations-queries`: результат `query api-table` обогащается тем же
  признаком.

## Impact

- **Код**: `internal/fswalk` (карта расширений: `lst` → language; белый список
  `GetSupportedExtensions` — без него паттерн `*.lst` молча отбрасывается
  `walkerPatterns`), `internal/config` (`*.lst` в оба дефолтных списка:
  `NormalizeConfig` и `CreateDefault`; `codebase.docker.toml`, `README`),
  `codebase.toml`, новый `internal/parser/lst` + диспетчеризация в
  `internal/indexer` (lenient-диагностика без отката транзакции файла,
  `mergeScanStats` + печать счётчика LST в `cmd/init.go`/`cmd/update.go`),
  `internal/store` (`db_schema.go` v7, batch-insert `memory_tables`,
  `db_reset.go` — `memory_tables` в `codebaseResetTables`, сторож-тест схемы),
  `internal/query` (`SearchTable`, `SearchAPITable` — LATERAL-обогащение без
  размножения строк), README (перечень форматов, пример include_patterns).
- **Схема БД**: bump `CurrentSchemaVersion` → v7; первый `update` на существующих БД
  применит DDL `memory_tables` (таблица маленькая, окно DDL — миллисекунды).
- **Конфиги деплоев**: `include_patterns` в `codebase.toml` на рабочих установках
  правятся вручную (без `*.lst` файл просто не попадёт в обход).
- **MCP**: поля добавляются в существующие ответы `codebase_query_table` /
  `codebase_query_api_table` — обратно совместимо, новые инструменты не вводятся.
- **Review**: атрибут становится читаемым для будущих правил; поведенческих
  изменений review в этом change нет.

## Why

Индексатор теряет реальные имена таблиц в SQL-источниках существования:
`create table dbo.tReplTables` индексируется как создатель таблицы «dbo»
(формы `dbo.<t>` и `MIDDLE..<t>`; `CREATE UNLOGGED TABLE` не распознаётся
вовсе), `ALTER TABLE … ADD CONSTRAINT` попадает в `sql_column_definitions`
как колонка «CONSTRAINT», а реестр таблиц продукта
`__REG_TABLE_NAME__(<имя>)` в `DB/ProductTable.sql` не индексируется.
Первопричина уже зафиксирована в design `table-existence-evidence`
(Measurements) и уточнена change-review'ем: 116 вхождений
`create table dbo.<имя>`/`MIDDLE..<имя>` в 92 файлах FA (10 имён),
2 кривые записи `alter_add`, 21 таблица реестра без единого
покрытого свидетельства. Итог — ложные severity-1 findings
`tableExistsInMetadata`: `tReplTables` (22 finding в `eis_5nt.sql`),
`tKey` (623 finding / 147 файлов в fa-payments) и шум по остальным именам.

## What Changes

- **Парсер SQL, создание таблиц**: `CREATE TABLE` распознаётся с
  schema/db-префиксом имени (`dbo.tSeed`, `MIDDLE..dcSubcontoConnect`) и
  модификаторами (`UNLOGGED`, `TEMP`, `TEMPORARY`, `GLOBAL TEMPORARY`);
  именем таблицы считается последний сегмент dotted-имени; в
  `sql_tables`/`sql_column_definitions` попадает реальное имя, а не
  префикс.
- **Парсер SQL, ссылки**: то же правило последнего сегмента применяется к
  ссылочной экстракции после DML-ключевых слов (`FROM`/`JOIN`/`INTO`/
  `UPDATE`, `INSERT INTO`, `DELETE`, `SELECT … INTO`): `from dbo.tConfigParam`
  даёт ссылку на `tConfigParam`, а не «dbo»; а также к имени таблицы в
  `ALTER TABLE`, `CREATE INDEX … ON`, UPDATE-SET-аккумуляторе и имени
  процедуры в `CREATE PROCEDURE` с префиксом.
- **Парсер SQL, schema patches**: `ALTER TABLE … ADD <constraint>` (семья
  `CONSTRAINT` / `PRIMARY KEY` / `FOREIGN KEY` / `UNIQUE` / `CHECK` /
  `DEFAULT`) не записывается в `sql_column_definitions` как колонка.
- **Реестр таблиц продукта**: новая таблица индекса `reg_table_name`;
  парсер извлекает `__REG_TABLE_NAME__(<имя>)` из `DB/ProductTable.sql`;
  при переиндексации файла записи обновляются (каскад файлов + вставка).
- **Миграции (SQL-only, bump схемы v8→v9)**: backfill создателей таблиц
  из `query_fragments` (покрытие 116/116 вхождений create-with-prefix,
  включая `UNLOGGED`); backfill `reg_table_name` из `query_fragments`
  (32 файла ProductTable.sql, в т.ч. имена в квадратных скобках); очистка
  мусорных записей «dbo»/«MIDDLE» (`sql_tables` 658, `sql_columns` 2097,
  `sql_column_definitions` 273, `sql_index_definitions` 12,
  `sql_procedures` 1, `symbols` 659, `relations` 950; легитимные
  6 define `MIDDLE` и 1 column_definition `Middle` не затрагиваются)
  и кривых `alter_add`-CONSTRAINT (2 записи).
- **Review**: реестр — третье свидетельство существования таблицы в
  `BatchLookupTableExistenceEvidence` (свидетельство, не создатель: гейт
  `indexExistsInDB` не открывается); сообщение `tableExistsInMetadata`
  перечисляет реестр в списке проверенных источников. Побочно закрывается
  FN `viewUsageInServerCode` для квалифицированных обращений
  (`from dbo.tfund` → распознаётся `tfund`).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `indexing/sql-parsing`: требование «Извлечение SQL-таблиц и колонок» —
  префиксы/модификаторы при создании и нормализация ссылок по последнему
  сегменту; требование «Schema patches» — пропуск constraint-семьи;
  новое требование «Извлечение реестра таблиц продукта»
  (`__REG_TABLE_NAME__` → `reg_table_name`).
- `review/deploy-stoppers`: требование «Существование таблицы в
  метаданных» — реестр таблиц продукта в списке свидетельств и в тексте
  сообщения finding.
- `review/review-scope`: требование «Область видимости EXT-продуктов» —
  реестр подчиняется той же областной привязке, что и свидетельства
  патчей.
- `infrastructure/database-schema`: требование «Таблицы сущностей» —
  `reg_table_name` в схеме; новое требование об именованных
  backfill-миграциях источников существования таблиц.

## Impact

- `internal/parser/sql/sql_parser.go` — `createTableRe`
  (префиксы+модификаторы), обработчик `alterTableAddRe` (пропуск
  constraint-семьи), нормализация dotted-имён в ссылочных регэкспах,
  `regTableNameRe`; `internal/model/model.go` — новая сущность реестра.
- `internal/store/db_schema.go` — DDL `reg_table_name` + индексы
  (`LOWER(table_name)`, `file_id`), bump `CurrentSchemaVersion` → v9,
  три именованные SQL-миграции backfill/очистки в `InitSchemaCtx`.
- `internal/store/db_insert_sql.go` — `BatchInsertRegTables`;
  `internal/indexer/indexer.go` — вставка reg-строк в `processFileCtx`.
- `internal/store/db_lookup_sql.go` — ветвь `reg_table_name` в
  `BatchLookupTableExistenceEvidence` (обе формы: глобальная и
  с `areaFilter`); `internal/review/review_rules.go` — текст сообщения.
- Тесты: `internal/parser/sql` (префиксы, модификаторы, constraint-семья,
  реестр, ссылки), `internal/store` (lookup, миграции — интеграционные),
  `internal/review` (сценарии реестра + области).
- Замеры: fa-contracts (протокол Measurements design
  `add-table-exists-in-metadata`) и fa-payments-корпус 275 файлов — до/после
  на одном снимке индекса; CLI/MCP-контракт не меняется, новых флагов и
  правил нет.

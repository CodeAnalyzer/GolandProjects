## Why

Правило `tableExistsInMetadata` подтверждает существование таблицы только
создающим источником (`CREATE TABLE`, `SELECT ... INTO`, объявление DSArchitect).
Для базовых таблиц, чей DDL поставляется вне исходников FA, источника в индексе
нет, и правило даёт ложный severity-1 finding; по той же причине
`indexExistsInDB` ложится на PK-индекс такой таблицы (`XPK…` объявлен в
отсутствующем DDL). Пример: `ParticipantSB_Delete.sql` — 2 finding'а на
`tRequisitParticipantSB`, хотя патчи `patch7_2_271` (`M_ADD_FIELD`) и
`patch7_2_273` (`M_CRT_INDEX`) уже проиндексированы и свидетельствуют о
существовании таблицы. Это класс, а не частный случай: в
`fa-payments/Payments/SERVER/DB/ProductTable.sql` зарегистрировано 161 таблица,
у 29 из них нет `CREATE TABLE` нигде в FA.

## What Changes

- `tableExistsInMetadata`: помимо создающих источников, существование таблицы
  подтверждают свидетельства изменения её структуры в разрешённой области —
  добавление колонки (`ALTER TABLE … ADD`, `M_ADD_FIELD`) и создание индекса
  (`CREATE INDEX`, `M_CRT_INDEX`) на таблице. Обращение к таблице, у которой
  есть такое свидетельство, finding не даёт; сообщение правила перечисляет
  расширенный набор источников.
- `indexExistsInDB`: для таблицы без создающего DDL в индексе проверка индексов
  неверифицируема — finding не формируется (fail-open). Опечатка в имени таблицы
  по-прежнему ловится правилом `tableExistsInMetadata` (одна причина — один
  finding).
- Повторный замер шума `tableExistsInMetadata` по протоколу design
  `add-table-exists-in-metadata` (fa-contracts): свежий baseline на сборке до
  правки и прогон после на том же снимке индекса; исторический контроль
  100 имён / 239 файла (build 1684) — только справка. Плюс проверка кейса
  `ParticipantSB_Delete.sql` до/после.
- Без миграции данных и реиндексации: используются уже проиндексированные
  `sql_column_definitions` и `sql_index_definitions`. Единственное изменение
  схемы — дополнительный индекс `LOWER(table_name)` на `sql_index_definitions`
  (bump `CurrentSchemaVersion`), чтобы ветвь свидетельств не давала seq scan.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: требование «Существование таблицы в метаданных» —
  расширенный набор источников подтверждения существования и их областная
  привязка; требование «Использование индексов, отсутствующих в БД» — проверка
  не выполняется для таблиц без создающего DDL.
- `review/review-scope`: требование «Область видимости EXT-продуктов» —
  уточнение, что свидетельство изменения структуры патчем подчиняется той же
  области `develop ∪ prj`, что и создающие источники.

## Impact

- `internal/store/db_lookup_sql.go` — lookup свидетельств существования
  (`sql_column_definitions.definition_kind IN ('alter_add','macro_add_field')`,
  `sql_index_definitions.table_name`) с тем же продуктово-веточным фильтром,
  что у `BatchLookupTableCreators`; `internal/store/db_schema.go` — индекс
  `idx_sql_index_definitions_table_name_lower`, bump `CurrentSchemaVersion`.
- `internal/review/review_rules.go` — `checkTableExistsInMetadata` (учёт
  свидетельств, текст сообщения), `checkIndexExistsInDB` /
  `checkDeletePtableMacros` (пропуск таблиц без создающего DDL).
- `internal/review/review_lookup.go`, `runner.go` — prewarm-кэш свидетельств и
  условие prewarm'а создателей при включённом `indexExistsInDB`.
- Тесты: `internal/store` (lookup), `internal/review`
  (`table_exists_metadata_test.go`, coverage-тесты `indexExistsInDB`).
- CLI/MCP-контракт не меняется; новых флагов и правил нет.

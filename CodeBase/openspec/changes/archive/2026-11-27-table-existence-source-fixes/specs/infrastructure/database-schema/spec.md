## MODIFIED Requirements

### Requirement: Таблицы сущностей

Система SHALL создавать таблицы для всех типов сущностей: `files`, `scan_runs`, `ds_products`, `sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`, `reg_table_name` (регистрация таблицы в реестре продукта `__REG_TABLE_NAME__`: file_id с ON DELETE CASCADE, table_name, line_number), `pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`, `report_forms`, `report_fields`, `report_params`, `vb_functions`, `h_files_defines`, `api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`, `symbols`, `relations`, `query_fragments`, `include_directives`, `memory_tables` (признак «объявлена как таблица в памяти» из `ModuleTableInMemory.lst`: file_id с ON DELETE CASCADE, нормализованный product_name, module_name, table_name, line_number), а также таблицы спек: `spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`.

#### Scenario: Создание таблицы процедур

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** таблица `sql_procedures` создана с колонками: id, file_id, procedure_name, body, start_line, end_line, ...

#### Scenario: Каталог продуктов и JS-константы

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `ds_products` (каталог продуктов Diasoft для review `foreign*`) и `js_constants` (константы из JS, попадающие в `symbols` как `constant`)

#### Scenario: Создание таблиц спек

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** созданы таблицы `spec_configs`, `spec_capabilities` (parent_id, title, purpose, notes, related_code, метрики покрытия), `spec_requirements`, `spec_scenarios` (given/when/then), `spec_usecases` (+ source_dir, usecase_kind, page_id), `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, staging `spec_code_mentions`, а также `spec_vocab` и `spec_embeddings` полнотекстового слоя

#### Scenario: Таблица признака таблиц в памяти

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создана таблица `memory_tables` с колонками id, file_id (FK на files с ON DELETE CASCADE), product_name, module_name, table_name, line_number
- **AND** удаление файла `ModuleTableInMemory.lst` из индекса каскадно удаляет его записи признака

#### Scenario: Таблица реестра таблиц продукта

- **GIVEN** пустая БД
- **WHEN** выполняется `InitSchema`
- **THEN** создана таблица `reg_table_name` с колонками id, file_id (FK на files с ON DELETE CASCADE), table_name, line_number
- **AND** созданы индексы `LOWER(table_name)` и `file_id`
- **AND** удаление файла `ProductTable.sql` из индекса каскадно удаляет его регистрации

## ADDED Requirements

### Requirement: Backfill-миграции источников существования таблиц

Введение таблицы `reg_table_name` и исправление извлечения имён таблиц
сопровождается bump маркера версии схемы (readonly-schema-policy).
Заполнение новых и исправление унаследованных данных на существующих БД
SHALL выполняться однократными именованными SQL-миграциями с маркерами в
таблице учёта миграций, без реиндексации исходников:

- `reg_table_name_backfill_v1` — вставка регистраций из `query_fragments`
  (регулярное извлечение `__REG_TABLE_NAME__(<имя>)` из `query_text`
  фрагментов, содержащих маркер, в том числе имён в квадратных скобках;
  полные `--`-строки вырезаются до извлечения, как их пропускает парсер;
  line_number — стартовая строка фрагмента);
- `sql_table_creators_backfill_v1` — вставка создателей таблиц
  (контекст `create` в `sql_tables` и соответствующие строки `symbols`) из
  `query_fragments` для операторов `CREATE [модификаторы] TABLE` с
  schema/db-префиксом имени или модификатором (`dbo.<t>`, `MIDDLE..<t>`,
  `UNLOGGED`): именем считается последний непустой сегмент dotted-имени;
- `sql_tables_prefix_garbage_cleanup_v1` — удаление унаследованного мусора
  префиксной экстракции: строки `sql_tables`, `sql_column_definitions` и
  `sql_columns` с `table_name` `dbo`/`MIDDLE`, строки
  `sql_index_definitions` с `table_name` `dbo`/`MIDDLE`, строка
  `sql_procedures` с `proc_name = 'dbo'` вместе с её входящими рёбрами
  `calls_procedure`, строки `symbols` типов `table`/`procedure` с именем
  `dbo`/`MIDDLE`, рёбра `relations`,
  указывающие на удаляемые строки `sql_tables`, и записи
  `sql_column_definitions` с `definition_kind = 'alter_add'`, чьё определение
  начинается с ключевого слова constraint-семьи (`CONSTRAINT`, `PRIMARY`,
  `FOREIGN`, `UNIQUE`, `CHECK`, `DEFAULT`, `COLUMN`).

После применения маркеров повторные прогоны инициализации схемы SHALL NOT
повторять вставки и удаления. Свежая БД получает маркеры без легаси-данных.
Новые записи во всех прогонах SHALL поставляться индексатором при вставке
(переиндексация файла обновляет его строки обычным каскадом).

#### Scenario: Существующая БД получает реестр и создателей

- **GIVEN** БД предыдущей версии схемы с проиндексированным FA (фрагменты `create table dbo.tReplTables` и `__REG_TABLE_NAME__(tKey)` в `query_fragments`)
- **WHEN** выполняется первый `codebase update` после апгрейда
- **THEN** в `sql_tables` есть создатель `tReplTables` с context=`create`, в `symbols` — соответствующий символ
- **AND** в `reg_table_name` есть регистрация `tKey` с file_id файла `ProductTable.sql`
- **AND** применение миграций зафиксировано маркерами в `schema_migrations`

#### Scenario: Мусор префиксной экстракции удалён

- **GIVEN** БД предыдущей версии схемы с мусорными строками: `sql_tables`, `sql_column_definitions` и `sql_columns` с `table_name` `dbo`/`MIDDLE`, `sql_index_definitions` с `table_name = 'MIDDLE'`, `sql_procedures` с `proc_name = 'dbo'` и его входящим ребром `calls_procedure`, `symbols` типов `table`/`procedure` с именем `dbo`/`MIDDLE`, `relations` на эти строки `sql_tables`
- **WHEN** выполняется миграция очистки
- **THEN** мусорных строк нет ни в одной из перечисленных таблиц, символов `dbo`/`MIDDLE` типов `table`/`procedure` нет, рёбер на удалённые строки нет
- **AND** легитимные сущности с именем `MIDDLE` других типов (например, define) остаются

#### Scenario: Закомментированная регистрация не вставляется

- **GIVEN** БД с фрагментом, содержащим строку `-- __REG_TABLE_NAME__(tOld)` (полный `--`-комментарий)
- **WHEN** применяется миграция `reg_table_name_backfill_v1`
- **THEN** регистрация `tOld` не вставляется

#### Scenario: Кривые alter_add-CONSTRAINT удалены

- **GIVEN** БД предыдущей версии схемы с записями `sql_column_definitions` вида `definition_kind = 'alter_add'`, `column_name = 'CONSTRAINT'`
- **WHEN** выполняется миграция очистки
- **THEN** такие записи отсутствуют

#### Scenario: Повторные прогоны — no-op

- **GIVEN** БД с применёнными маркерами backfill-миграций
- **WHEN** выполняется повторная инициализация схемы (в том числе после bump версии по другим причинам)
- **THEN** вставки и удаления миграций не выполняются: количество строк `reg_table_name`, создателей и мусора не меняется

#### Scenario: Свежая БД — маркеры без данных

- **GIVEN** пустая БД
- **WHEN** выполняется `codebase init`
- **THEN** таблица `reg_table_name` создана, маркеры backfill-миграций зафиксированы
- **AND** строки не вставлялись (легаси-данных нет)

## MODIFIED Requirements

### Requirement: Сброс таблиц кодовой базы (ResetCodebaseTables)

Store SHALL предоставлять метод `ResetCodebaseTables(ctx)`, очищающий одним statement `TRUNCATE TABLE ... CASCADE` все таблицы, заполняемые парсерами кодовой базы: файлы и символы (`files`, `symbols`, `ds_products`, `stats_snapshot`); SQL (`sql_procedures`, `sql_tables`, `sql_columns`, `sql_column_definitions`, `sql_index_definitions`, `sql_index_definition_fields`, `reg_table_name`); PAS/H/JS/SMF/DFM (`pas_units`, `pas_classes`, `pas_methods`, `pas_fields`, `h_files_defines`, `js_functions`, `js_constants`, `smf_instruments`, `dfm_forms`, `dfm_components`); отчёты (`report_forms`, `report_fields`, `report_params`, `vb_functions`); фрагменты/include (`query_fragments`, `include_directives`); API XML (`api_business_objects`, `api_contracts`, `api_contract_params`, `api_contract_tables`, `api_contract_table_fields`, `api_business_object_params`, `api_business_object_tables`, `api_business_object_table_fields`, `api_business_object_table_indexes`, `api_business_object_table_index_fields`, `api_contract_return_values`, `api_contract_contexts`, `api_macro_invocations`); связи и retcode (`relations`, `ds_return_codes`); признак таблиц в памяти (`memory_tables`); LSA корпуса описаний (`desc_vocab`, `desc_embeddings`); спеки (`spec_configs`, `spec_capabilities`, `spec_requirements`, `spec_scenarios`, `spec_usecases`, `spec_usecase_steps`, `spec_changes`, `spec_change_delta`, `spec_code_mentions`, `spec_vocab`, `spec_embeddings`). Метод SHALL сохранять таблицы анализаторов и служебные: `rti_sessions`, `rti_calls`, `rti_params`, `rti_checkpoints`, `rti_blog_blocks`, `rti_blog_tables`, `rti_client_events`, `trc_sessions`, `trc_events`, `schema_migrations`, `scan_runs`. Полный список усекаемых таблиц фиксируется тестом; если у сохраняемых таблиц появятся FK на усекаемые, тест SHALL это обнаружить. Метод идемпотентен и безопасен для пустых таблиц.

#### Scenario: После сброса таблицы кодовой базы пусты, анализаторы не тронуты

- **GIVEN** БД с заполненным индексом и сохранёнными RTI/TRC-сессиями
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** все таблицы кодовой базы из списка усечения пусты
- **AND** строки `rti_sessions`, `rti_calls`, `rti_client_events`, `trc_sessions`, `trc_events` остались на месте
- **AND** `schema_migrations` не изменилась

#### Scenario: История scan_runs сохраняется

- **GIVEN** БД с несколькими завершёнными прогонами в `scan_runs` и связанными записями в `files`
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** записи `files` удалены
- **AND** строки истории `scan_runs` остались на месте

#### Scenario: Идемпотентность на пустой БД

- **GIVEN** БД после `InitSchema` без данных
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** метод завершается без ошибок
- **AND** повторный вызов также завершается без ошибок

#### Scenario: Записи признака таблиц в памяти усекаются вместе с индексом

- **GIVEN** БД с заполненными `memory_tables`
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** `memory_tables` пуста — таблица входит в список усечения, а не в сохраняемые

#### Scenario: Записи реестра таблиц усекаются вместе с индексом

- **GIVEN** БД с заполненным `reg_table_name` (backfill или индексация)
- **WHEN** выполняется `ResetCodebaseTables`
- **THEN** `reg_table_name` пуста — таблица входит в список усечения, а не в сохраняемые

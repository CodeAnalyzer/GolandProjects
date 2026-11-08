## MODIFIED Requirements

### Requirement: Поиск таблиц и схем

Система SHALL предоставлять команды `query table` (поиск таблиц по имени), `query table-schema` (определения колонок из CREATE TABLE и schema patches) и `query table-index` (индексы обычных SQL-таблиц). Результат `query table` SHALL включать признак in-memory («объявлена как таблица в памяти», семантика — `indexing/lst-parsing`): признак сопоставляется с таблицей чисто по имени без привязки к продукту вхождения (физическая таблица одна на БД — декларация действительна для вхождений в любом продукте), и при установленном признаке результат SHALL сопровождать его источником — продуктом и модулем из декларации `ModuleTableInMemory.lst`. Отсутствие декларации SHALL отображаться как неустановленный признак.

#### Scenario: Поиск таблицы с схемой

- **GIVEN** проиндексированный проект с таблицей `tContract`
- **WHEN** выполняется `query table-schema --name tContract`
- **THEN** возвращены все определения колонок из `CREATE TABLE` и schema patches с указанием origin (`create_table`, `alter_add`, `macro_add_field`)

#### Scenario: Поиск индексов таблицы

- **GIVEN** проиндексированный проект с индексами на таблице `tContract`
- **WHEN** выполняется `query table-index --name tContract`
- **THEN** возвращены все индексы таблицы с полями

#### Scenario: Признак у обычной p-таблицы

- **GIVEN** `ModuleTableInMemory.lst` с секцией `[FA-GAAP]` и строкой `pRuleDoc_QtyStack;GAAP`; таблица `pRuleDoc_QtyStack` встречается в SQL-коде продукта `fa-gaap`
- **WHEN** выполняется `query table --name pRuleDoc_QtyStack`
- **THEN** результат несёт установленный признак in-memory с источником: продукт `fa-gaap`, модуль `GAAP`

#### Scenario: Признак у вхождения в чужом продукте

- **GIVEN** таблица `pAPI_AttrValue` объявлена в `[FA-Administrator]`, но встречается в SQL-коде продукта `fa-cards`
- **WHEN** выполняется `query table --name pAPI_AttrValue`
- **THEN** строка результата для `fa-cards` несёт установленный признак in-memory: сопоставление по имени, без product-scope

#### Scenario: Таблица без декларации

- **GIVEN** таблица `tContract` не упомянута ни в одном `ModuleTableInMemory.lst`
- **WHEN** выполняется `query table --name tContract`
- **THEN** признак in-memory в результате не установлен

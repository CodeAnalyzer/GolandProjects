## MODIFIED Requirements

### Requirement: Поиск API-таблиц, параметров и индексов

Система SHALL предоставлять команды `query api-table`, `query api-param`, `query api-table-index` для поиска standalone таблиц, параметров BObject и индексов standalone API-таблиц. Результат `query api-table` SHALL включать признак in-memory («объявлена как таблица в памяти», семантика — `indexing/lst-parsing`): сопоставление чисто по имени таблицы без product-scope, при установленном признаке результат сопровождается продуктом и модулем из декларации `ModuleTableInMemory.lst`.

#### Scenario: Поиск API-таблицы

- **GIVEN** проиндексированный проект с API-таблицей `pAPI_CredS_InsertCCred`
- **WHEN** выполняется `query api-table --name pAPI_CredS_InsertCCred`
- **THEN** возвращена таблица с полями

#### Scenario: Поиск индекса API-таблицы

- **GIVEN** проиндексированный проект с индексом `XIE0pAPI_CCred_Agreement`
- **WHEN** выполняется `query api-table-index --name XIE0pAPI_CCred_Agreement`
- **THEN** возвращен индекс с полями и указанием таблицы

#### Scenario: Признак in-memory у API-таблицы

- **GIVEN** `ModuleTableInMemory.lst` с секцией `[FA-Administrator]` и строкой `pAPI_AttrValue;API_Administrator`; в индексе есть standalone API-таблица `pAPI_AttrValue`
- **WHEN** выполняется `query api-table --name pAPI_AttrValue`
- **THEN** результат несёт установленный признак in-memory с источником: продукт `fa-administrator`, модуль `API_Administrator`

#### Scenario: API-таблица без декларации

- **GIVEN** standalone API-таблица `pAPI_CredS_InsertCCred` не упомянута в `ModuleTableInMemory.lst`
- **WHEN** выполняется `query api-table --name pAPI_CredS_InsertCCred`
- **THEN** признак in-memory в результате не установлен

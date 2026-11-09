## ADDED Requirements

### Requirement: Изменение полей уникального индекса таблицы в памяти

Система SHALL обнаруживать `UPDATE`, целевая таблица которого объявлена «таблицей в памяти» (есть запись в признаке in-memory по имени таблицы, без привязки к продукту), и в секции `SET` которого изменяется колонка, входящая в состав уникального индекса этой таблицы, и формировать finding `memoryUniqueIndexUpdate` severity 1. Уникальными считаются только индексы, отмеченные признаком уникальности в индексе: уникальный индекс обычной p-таблицы либо уникальный индекс API-таблицы `pAPI_*` (признак уникален для обоих физических видов: 2 и 3). Индекс без признака уникальности — в том числе первичный ключ — уникальным не считается: имя индекса конвенцией уникальности не является. Сопоставление целевой таблицы с признаком in-memory выполняется по имени таблицы.

#### Scenario: UPDATE колонки уникального индекса in-memory p-таблицы

- **GIVEN** SQL-файл с `UPDATE pRuleDoc_QtyStack SET Brief = @Brief WHERE ...`, таблица `pRuleDoc_QtyStack` объявлена в памяти, колонка `Brief` входит в её уникальный индекс `XPKpRuleDoc_QtyStack` (объявлен `M_CRT_INDEX('UNIQUE', ...)`)
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: UPDATE колонки уникального индекса pAPI-таблицы

- **GIVEN** SQL-файл с `UPDATE pAPI_LstTpl SET TemplateID = @TemplateID ...`, таблица `pAPI_LstTpl` объявлена в памяти, колонка `TemplateID` входит в её индекс `XPKpAPI_LstTpl` с признаком уникальности вида 2 или 3
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: UPDATE неуникальной колонки in-memory таблицы

- **GIVEN** SQL-файл с `UPDATE pRuleDoc_QtyStack SET Qty = @Qty ...`, колонка `Qty` не входит ни в один уникальный индекс таблицы
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: UPDATE уникальной колонки обычной таблицы

- **GIVEN** SQL-файл с `UPDATE tContract SET ContractID = @ID ...`, колонка `ContractID` входит в уникальный индекс `XPKtContract`, но таблица `tContract` НЕ объявлена в памяти
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: Индекс без признака уникальности

- **GIVEN** SQL-файл с `UPDATE pObjectForRight SET ObjectID = @ID ...`, таблица `pObjectForRight` объявлена в памяти, колонка `ObjectID` входит только в индекс `XPKpObjectForRight`, объявленный без признака уникальности (`M_CRT_INDEX('', ...)`)
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: UPDATE через алиас целевой таблицы

- **GIVEN** SQL-файл с `UPDATE x SET Brief = @Brief FROM pRuleDoc_QtyStack x WHERE ...`, таблица `pRuleDoc_QtyStack` объявлена в памяти, колонка `Brief` входит в её уникальный индекс
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1

#### Scenario: Декларация in-memory в другом продукте

- **GIVEN** SQL-файл любого продукта с `UPDATE pAPI_LstTpl SET TemplateID = @TemplateID ...`, признак in-memory для `pAPI_LstTpl` объявлен декларацией extension-репозитория установки (файл `ModuleTableInMemory.lst` вне продуктов кодовой базы)
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `memoryUniqueIndexUpdate`, severity 1 (сопоставление по имени таблицы, без product-scope)

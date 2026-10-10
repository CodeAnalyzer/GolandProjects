## MODIFIED Requirements

### Requirement: Inspect сущности с graph context

Система SHALL предоставлять команду `query inspect` для глубокого анализа сущности: поиск символа, всех входящих и исходящих связей в графе, и соседних символов. Связи SHALL разрешаться для сущностей всех типов, участвующих в графе relations, включая PAS-методы, JS-функции и API-контракты.

#### Scenario: Inspect процедуры

- **GIVEN** проиндексированный проект с процедурой `Cons_Check_Restr_API`
- **WHEN** выполняется `query inspect --name Cons_Check_Restr_API`
- **THEN** возвращена сущность с метаданными, всеми входящими и исходящими relations и соседними символами

#### Scenario: Inspect PAS-метода

- **GIVEN** проиндексированный проект с PAS-методом `DeleteLinkToPolicy`, у которого есть связь `builds_query` с query fragment
- **WHEN** выполняется `query inspect --name DeleteLinkToPolicy`
- **THEN** возвращена сущность с метаданными метода
- **AND** исходящие связи включают `builds_query`-связи метода

#### Scenario: Inspect API-контракта

- **GIVEN** проиндексированный проект с контрактом `API_MyContract`, который реализуется SQL-процедурой (связь `implements_contract`)
- **WHEN** выполняется `query inspect --name API_MyContract`
- **THEN** возвращена сущность контракта с метаданными
- **AND** входящие связи включают `implements_contract` от реализующей процедуры

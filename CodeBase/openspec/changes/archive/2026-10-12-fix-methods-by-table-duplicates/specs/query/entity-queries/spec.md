## MODIFIED Requirements

### Requirement: Поиск PAS-методов

Система SHALL предоставлять команду `query method` для поиска PAS-методов по имени и `query methods --table` для поиска методов, работающих с указанной SQL-таблицей через query fragments. Результат `query methods --table` SHALL содержать не более одной строки на уникальный PAS-метод: количество query-фрагментов метода, ссылающихся на искомую таблицу, MUST NOT увеличивать число строк или значение `count`.

#### Scenario: Методы по имени

- **GIVEN** проиндексированный проект с методом `Execute`
- **WHEN** выполняется `query method --name Execute`
- **THEN** возвращены все методы с именем `Execute` с указанием unit, class, файла, строки, signature и visibility

#### Scenario: Методы, работающие с таблицей

- **GIVEN** проиндексированный проект с PAS-методами, содержащими SQL-запросы к `pAPI_Accrual_Object`
- **WHEN** выполняется `query methods --table pAPI_Accrual_Object`
- **THEN** возвращены PAS-методы, связанные с таблицей через query fragments

#### Scenario: Метод с несколькими фрагментами возвращается один раз

- **GIVEN** проиндексированный проект, где PAS-метод строит два и более query-фрагмента, каждый из которых ссылается на таблицу `tAim`
- **WHEN** выполняется `query methods --table tAim --json`
- **THEN** этот метод присутствует в `items` ровно один раз
- **AND** `count` равен числу уникальных методов, а не числу пар «метод–фрагмент»

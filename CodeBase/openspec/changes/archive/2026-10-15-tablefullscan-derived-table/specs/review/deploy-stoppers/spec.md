## MODIFIED Requirements

### Requirement: Full scan условия

Система SHALL обнаруживать условия в WHERE/JOIN, приводящие к full scan (отсутствие SARGable предикатов), и формировать finding `tableFullScan`. При анализе FROM-части система MUST корректно обрабатывать derived tables (подзапросы в скобках с алиасом): таблицы внутри подзапроса анализируются по WHERE/ON этого подзапроса, а finding MUST ссылаться на реальную таблицу — объект finding'а не может быть SQL-ключевым словом. Для INSERT…SELECT finding MUST быть привязан к строке начала оператора (`insert`), анализу подлежит только SELECT-часть.

#### Scenario: Non-SARGable условие

- **GIVEN** SQL-файл с `WHERE SUBSTRING(Name, 1, 3) = 'abc'`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableFullScan`, severity 1

#### Scenario: Derived table с фильтром внутри — нет finding

- **GIVEN** SQL-файл с `select ... from (select ... from pSrc where k.SPID = @@spid group by k.K) t left join (select ... from pLnk where l.SPID = @@spid) g on ...`
- **WHEN** выполняется review
- **THEN** finding `tableFullScan` не формируется
- **AND** объект любого finding'а не равен `select`

#### Scenario: Derived table без фильтра внутри — finding по внутренней таблице

- **GIVEN** SQL-файл с `select ... from (select ... from pSrc) t` без условий фильтрации ни снаружи, ни внутри подзапроса
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tableFullScan`, severity 1
- **AND** объект finding'а — имя таблицы внутри подзапроса (`psrc`)

#### Scenario: INSERT…SELECT привязан к началу оператора

- **GIVEN** SQL-файл с `insert into pTarget (SPID, K)` на строке N и `select ...` на строке N+1, SELECT-часть без FROM-таблиц
- **WHEN** выполняется review
- **THEN** finding `tableFullScan` не формируется (цель INSERT не подлежит требованию фильтрации)
- **AND** при наличии нарушения в SELECT-части line finding'а указывает на строку начала оператора INSERT

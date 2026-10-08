## MODIFIED Requirements

### Requirement: SQL-процедуры в H-файлах

Система SHALL повторно разбирать H-файлы SQL-парсером для извлечения процедур, определённых через `DCL_PROC_BEGIN`. Найденные процедуры сохраняются в `sql_procedures` и `symbols` (тип `procedure`). Процедуры, попавшие внутрь `#define`-макросов (например, `#define DCL_PROC_BEGIN(NAME) ...`), отфильтровываются и не индексируются. SQL-парсер SHALL запускаться только для H-файлов, содержащих маркеры процедур (`DCL_PROC_BEGIN(` или `__BEGIN_PROCEDURE__(`); H-файлы без маркеров не парсятся как SQL. Из результатов SQL-разбора H-файла система SHALL строить исходящие связи процедур и сохранять все сопутствующие сущности: таблицы и колонки (в `sql_tables`/`sql_columns` с привязкой к файлу), `query_fragments` с parent-binding к содержащим их процедурам, исходящие `calls_procedure` (вызовы `exec` из тел процедур) и табличные relations (`selects_from`/`inserts_into`/`updates`/`deletes_from`) по образцу пайплайна SQL-файлов. `Defines` из результата SQL-разбора H-файла SHALL игнорироваться (их извлекает H-парсер — иначе дубли в `h_files_defines`).

#### Scenario: Процедура DCL_PROC_BEGIN в H-файле

- **GIVEN** H-файл с `DCL_PROC_BEGIN(MyProc) ... DCL_PROC_END` вне макросов
- **WHEN** выполняется индексация файла
- **THEN** процедура `MyProc` сохранена в `sql_procedures` с указанием файла и строк
- **AND** доступна через `query symbol --name MyProc --type procedure`

#### Scenario: Процедура внутри #define — отфильтрована

- **GIVEN** H-файл с `#define DCL_PROC_BEGIN(NAME) ...` содержащим определение процедуры
- **WHEN** выполняется индексация файла
- **THEN** процедура внутри `#define` не сохраняется в `sql_procedures`
- **AND** `isLineInsideMacroDefinition` возвращает true для строк внутри макроса

#### Scenario: Исходящие связи процедуры в H-файле

- **GIVEN** H-файл с процедурой `DCL_PROC_BEGIN(MyProc)`, тело которой содержит `exec OtherProc` и `insert into pSomeTable`
- **WHEN** выполняется индексация файла и завершается пост-обработка связей
- **THEN** существует связь `calls_procedure` от `MyProc` к `OtherProc`
- **AND** `query callers --procedure OtherProc` находит `MyProc` как источник вызова
- **AND** существует связь `inserts_into` от `MyProc` к таблице `pSomeTable`

#### Scenario: Query fragments из тела процедуры в H-файле

- **GIVEN** H-файл с процедурой, тело которой содержит SQL-запрос `select * from pTable`
- **WHEN** выполняется индексация файла
- **THEN** SQL-фрагмент сохранён в `query_fragments` с привязкой (parent) к процедуре и указанием файла и строки

#### Scenario: Pre-check маркеров — файл без процедур не парсится как SQL

- **GIVEN** H-файл без вхождений `DCL_PROC_BEGIN(` и `__BEGIN_PROCEDURE__(`
- **WHEN** выполняется индексация файла
- **THEN** SQL-парсер для этого файла не запускается
- **AND** H-определения (defines) и include-директивы индексируются как обычно

#### Scenario: Маркер процедуры внутри #define не даёт сущностей

- **GIVEN** H-файл, где маркер `DCL_PROC_BEGIN(` встречается только внутри тела `#define`-макроса
- **WHEN** выполняется индексация файла
- **THEN** SQL-разбор не порождает процедур и query-фрагментов из тела макроса

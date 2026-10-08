# H/Report Parsing

## Purpose

Парсинг H-файлов (.h) с константами и макросами, TPR-файлов (.tpr) с отчётными формами, полями и параметрами, и RPT-файлов (.rpt) с отчётными формами, параметрами и VBScript-функциями.

## Requirements

### Requirement: Извлечение H-defines

Система SHALL извлекать из H-файлов определения (`#define`, константы, макросы) с их именем и значением для unified symbols index.

#### Scenario: Define константа

- **GIVEN** H-файл с `#define MAX_ROWS 1000`
- **WHEN** выполняется индексация файла
- **THEN** define `MAX_ROWS` сохранён в `h_files_defines` со значением `1000`
- **AND** доступен через `query symbol --name MAX_ROWS --type define`

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

### Requirement: Извлечение TPR report forms

Система SHALL извлекать из TPR-файлов отчётные формы (report forms) с их именем, типом и связанными полями (report fields) и параметрами (report params).

#### Scenario: TPR отчётная форма с полями

- **GIVEN** TPR-файл с отчётной формой `CreditReport`, содержащей поля `Sum`, `Rate` и параметр `InstitutionID`
- **WHEN** выполняется индексация файла
- **THEN** форма `CreditReport` сохранена в `report_forms`
- **AND** поля `Sum`, `Rate` сохранены в `report_fields` со связью к форме
- **AND** параметр `InstitutionID` сохранён в `report_params` со связью к форме

### Requirement: Извлечение RPT report forms и VB functions

Система SHALL извлекать из RPT-файлов отчётные формы (report forms), параметры (report params) и VBScript-функции (VB functions) с их именем, файлом и строкой.

#### Scenario: RPT форма с VB-функцией

- **GIVEN** RPT-файл с отчётной формой `Portfolio` и VBScript-функцией `CalculateTotal`
- **WHEN** выполняется индексация файла
- **THEN** форма `Portfolio` сохранена в `report_forms`
- **AND** функция `CalculateTotal` сохранена в `vb_functions` с указанием файла и строки
- **AND** функция доступна через `query vb-function --name CalculateTotal`

### Requirement: Include-директивы

Система SHALL извлекать include-директивы из TPR-файлов и разрешать их (указывать целевой файл) для построения графа зависимостей. RPT-файлы не содержат include-директив. Include-директивы также извлекаются из SQL-блоков внутри TPR (`@Name@ = SQL { ... }`, `tpr_parser.go`) и SQL-фрагментов `script.strings = (` в RPT (`rpt_parser.go`), что позволяет учитывать include-зависимости внутри встроенных SQL-секций отчётов.

#### Scenario: Include-директива

- **GIVEN** TPR-файл с include-директивой, ссылающейся на `CommonReport.tpr`
- **WHEN** выполняется индексация файла
- **THEN** include-директива сохранена в `include_directives` с указанием источника и цели

#### Scenario: Include-директива внутри SQL-блока TPR

- **GIVEN** TPR-файл с SQL-блоком `@MyName@ = SQL { #include "common.sql" }`
- **WHEN** выполняется индексация и `tpr_parser` обрабатывает SQL-блок
- **THEN** include-директива внутри SQL-блока извлечена и сохранена в `include_directives`

### Requirement: SQL-блоки в отчётах

Система SHALL извлекать SQL-блоки из TPR/RPT-файлов и сохранять их в `query_fragments` с связью к родительской отчётной форме. В RPT SQL-фрагменты извлекаются также из `script.strings = (` секций (`rpt_parser.go`).

#### Scenario: SQL-запрос в отчёте

- **GIVEN** TPR-файл с отчётной формой, содержащей SQL-запрос `SELECT * FROM tContract`
- **WHEN** выполняется индексация файла
- **THEN** SQL-фрагмент сохранён в `query_fragments` с указанием родительской отчётной формы

#### Scenario: SQL из script.strings в RPT

- **GIVEN** RPT-файл с секцией `script.strings = ( 'SELECT * FROM tAccount' )`
- **WHEN** выполняется индексация через `rpt_parser`
- **THEN** SQL-фрагмент `SELECT * FROM tAccount` сохранён в `query_fragments`

## Related code

- `internal/parser/h/` — H-парсер: defines, constants, macros, multi-line continuation `#define` (учёт `\` в конце строки)
- `internal/parser/tpr/` — TPR-парсер: report forms, fields, params, SQL blocks, include directives (в т.ч. внутри `@Name@ = SQL { ... }`)
- `internal/parser/rpt/` — RPT-парсер: report forms, params, VB functions, embedded SQL, `script.strings = (` SQL extraction
- `internal/indexer/indexer.go` — `parseHFile`, `parseTPRFile`, `parseRPTFile`, `isLineInsideMacroDefinition` (фильтрация процедур внутри `#define`-макросов с учётом продолжения строки через `\`)
- `internal/store/db_insert_h.go` — batch insert для H defines
- `internal/store/db_insert_reports.go` — batch insert для report entities

## Notes

- H-файлы читаются в кодировке CP866
- TPR и RPT файлы читаются с авто-детекцией кодировки (CP866/CP1251/UTF8)
- Report fields доступны только для TPR-файлов; RPT-файлы содержат params и VB functions
- Multi-line continuation `#define` в H-парсере: строки, оканчивающиеся на `\`, склеиваются со следующей; `isLineInsideMacroDefinition` учитывает продолжение при фильтрации процедур внутри макросов
- Include-директивы извлекаются не только на уровне TPR/RPT-файла, но и внутри SQL-блоков TPR (`@Name@ = SQL { ... }`) и секций `script.strings = (` в RPT

# Entity Queries

## Purpose

Запросы к индексу по типам сущностей: SQL tables/procedures, callers, query fragments, table-schema, table-index, PAS methods, DFM forms/components, report forms/fields/params, VB functions, JS functions, SMF instruments, коды возврата.

## Requirements

### Requirement: Поиск таблиц и схем

Система SHALL предоставлять команды `query table` (поиск таблиц по имени), `query table-schema` (определения колонок из CREATE TABLE и schema patches) и `query table-index` (индексы обычных SQL-таблиц).

#### Scenario: Поиск таблицы с схемой

- **GIVEN** проиндексированный проект с таблицей `tContract`
- **WHEN** выполняется `query table-schema --name tContract`
- **THEN** возвращены все определения колонок из `CREATE TABLE` и schema patches с указанием origin (`create_table`, `alter_add`, `macro_add_field`)

#### Scenario: Поиск индексов таблицы

- **GIVEN** проиндексированный проект с индексами на таблице `tContract`
- **WHEN** выполняется `query table-index --name tContract`
- **THEN** возвращены все индексы таблицы с полями

### Requirement: Детали SQL-процедуры

Система SHALL предоставлять команду `query procedure` для получения деталей хранимой процедуры по точному имени: путь к файлу, диапазон строк, список параметров с типами и направлением.

#### Scenario: Детали существующей процедуры

- **GIVEN** проиндексированный проект с процедурой `API_RuleDoc_MassCreateDocument`
- **WHEN** выполняется `query procedure --name API_RuleDoc_MassCreateDocument`
- **THEN** возвращены имя, файл, `line_start`/`line_end` и параметры процедуры

### Requirement: Поиск вызывающих процедуру

Система SHALL предоставлять команду `query callers` для поиска вызывающих указанную SQL-процедуру: прямые callers (`calls_procedure`, `dispatches_to_subscriber`) из sql_procedure, pas_method, js_function, report_form, vb_function, query_fragment и косвенные callers до 2 hop через цепочки `calls_procedure`/`dispatches_to`/`dispatches_to_subscriber`.

#### Scenario: Прямые и generated callers

- **GIVEN** проиндексированный проект, где `CallerProc` вызывает `TargetProc`, а `.t01` содержит `dispatches_to_subscriber` на `TargetProc`
- **WHEN** выполняется `query callers --procedure TargetProc`
- **THEN** возвращены и `CallerProc`, и generated subscriber call

### Requirement: Поиск SQL/query fragments

Система SHALL предоставлять команду `query sql-fragment` для полнотекстового поиска по `query_fragments.query_text` (GIN/`pg_trgm`).

#### Scenario: Поиск фрагмента по тексту

- **GIVEN** проиндексированная процедура, содержащая `SELECT * FROM tContract WHERE ID = @ID`
- **WHEN** выполняется `query sql-fragment --text "from tContract"`
- **THEN** фрагмент найден, результат содержит родительскую сущность, файл и строку

### Requirement: Поиск кодов возврата

Система SHALL предоставлять команду `query retcode` для поиска в `ds_return_codes`: по числовому коду (`--code`, точное совпадение) или по фрагменту сообщения (`--message`, case-insensitive ILIKE). Хотя бы один из флагов MUST быть указан.

#### Scenario: Поиск по коду

- **GIVEN** справочник содержит код `1001` с сообщением
- **WHEN** выполняется `query retcode --code 1001`
- **THEN** возвращена запись с `ret_code`, сообщением, `proc_name` и `module_id`

#### Scenario: Поиск по тексту сообщения

- **GIVEN** справочник содержит несколько кодов с текстом «не найден»
- **WHEN** выполняется `query retcode --message "не найден"`
- **THEN** возвращены все совпадающие коды

### Requirement: Поиск PAS-методов

Система SHALL предоставлять команду `query method` для поиска PAS-методов по имени и `query methods --table` для поиска методов, работающих с указанной SQL-таблицей через query fragments.

#### Scenario: Методы по имени

- **GIVEN** проиндексированный проект с методом `Execute`
- **WHEN** выполняется `query method --name Execute`
- **THEN** возвращены все методы с именем `Execute` с указанием unit, class, файла, строки, signature и visibility

#### Scenario: Методы, работающие с таблицей

- **GIVEN** проиндексированный проект с PAS-методами, содержащими SQL-запросы к `pAPI_Accrual_Object`
- **WHEN** выполняется `query methods --table pAPI_Accrual_Object`
- **THEN** возвращены PAS-методы, связанные с таблицей через query fragments

### Requirement: Поиск DFM forms и components

Система SHALL предоставлять команды `query form` (поиск по form_name, form_class, caption) и `query form-component` (поиск по component_name, component_type, caption, form_name, form_class).

#### Scenario: Поиск формы по caption

- **GIVEN** проиндексированный проект с формой, имеющей caption "Цель кредита"
- **WHEN** выполняется `query form --name "Цель кредита"`
- **THEN** возвращена форма с указанием form_name, form_class и файла

#### Scenario: Поиск компонента по имени

- **GIVEN** проиндексированный проект с компонентом `dlName`
- **WHEN** выполняется `query form-component --name dlName`
- **THEN** возвращен компонент с указанием component_type, caption, form_name

### Requirement: Поиск report entities

Система SHALL предоставлять команды `query report-form`, `query report-field`, `query report-param`, `query vb-function` для поиска отчётных форм, полей отчётов, параметров отчётов и VBScript-функций.

#### Scenario: Поиск отчётной формы

- **GIVEN** проиндексированный проект с отчётной формой `CreditReport`
- **WHEN** выполняется `query report-form --name Credit`
- **THEN** возвращена форма `CreditReport` с указанием файла и типа

#### Scenario: Поиск VB-функции

- **GIVEN** проиндексированный проект с VBScript-функцией `CalculateTotal`
- **WHEN** выполняется `query vb-function --name Calculate`
- **THEN** возвращена функция `CalculateTotal` с указанием файла и строки

### Requirement: Поиск JS functions и SMF instruments

Система SHALL предоставлять команды `query js-function`, `query smf-instrument`, `query smf-type` для поиска JS-функций, SMF-инструментов и фильтрации SMF по типу сценария.

#### Scenario: Поиск SMF по типу

- **GIVEN** проиндексированный проект с SMF-инструментами разных типов
- **WHEN** выполняется `query smf-type --type mass_operation`
- **THEN** возвращены все инструменты с типом `mass_operation`

### Requirement: Единый контракт поиска по имени

Система SHALL применять единый принцип для всех name-based lookup команд: без `--like` — точный поиск, с `--like` — поиск по подстроке.

#### Scenario: Точный поиск

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query form --name AimCnt` (без `--like`)
- **THEN** возвращена только форма с точным именем `AimCnt`

#### Scenario: Поиск по подстроке

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query form --name Aim --like`
- **THEN** возвращены все формы, имя которых содержит подстроку `Aim`

### Requirement: Поведение при пустых результатах entity queries

Система SHALL возвращать пустой массив `[]` (не `null`) при отсутствии результатов для всех entity query команд.

#### Scenario: Таблица не найдена

- **GIVEN** проиндексированный проект без таблицы `tNonExistent`
- **WHEN** выполняется `query table --name tNonExistent --json`
- **THEN** возвращён JSON с `"count": 0` и `"items": []`

#### Scenario: Методы для таблицы не найдены

- **GIVEN** проиндексированный проект и таблица `tEmpty` без связанных PAS-методов
- **WHEN** выполняется `query methods --table tEmpty`
- **THEN** возвращён пустой результат

## Related code

- `internal/query/query_sql.go` — `SearchTable`, `SearchTableSchema`, `SearchSQLTableIndex`, `GetProcedureResult`, `FindCallers`, `FindPASMethodsByName`, `FindMethodsByTable`
- `internal/query/query.go` — `SearchQueryFragment`, `SearchDFMForm`, `SearchDFMComponent`, `SearchReportForm`, `SearchReportField`, `SearchReportParam`, `SearchVBFunction`, `SearchJSFunction`, `SearchSMFInstrument`, `SearchSMFByType`, `LookupRetCode`, `LookupRetCodeByMessage`
- `cmd/query_commands.go` — CLI commands для entity queries
- `cmd/query_execution.go` — выполнение query и форматирование вывода

## Notes

- Все команды поддерживают `--json`, `--summary`, `--ndjson`, `--limit`
- `query table` по умолчанию — точный поиск; `--like` — поиск по подстроке
- `query procedure` ищет по точному имени; для нечёткого поиска используется `query symbol --type procedure --like`
- `query callers` включает generated subscriber calls из `.t01` (`dispatches_to_subscriber`)
- `query retcode` требует `--code` или `--message`
- Пустые результаты возвращаются как пустые массивы `[]`, а не `null`
- Execution-слой `internal/querysvc` (`runtime.go`) — общая точка входа для CLI (`cmd/query_execution.go`) и MCP (`internal/mcp/registry.go`); устраняет дублирование оркестрации. Поведение команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.
- Вспомогательная механика (не отдельные команды): `GetProcedureDetails` (`query_sql.go`) — альтернативная версия `GetProcedureResult` с другой структурой результата; `loadTableFiles`/`loadTableColumns`/`loadTableProcedures` — внутреннее обогащение `SearchTable`.
- Batch-lookup слой в `internal/store/db_lookup_sql.go` (12 функций: `FindLatestSQLProcedureIDByName`/`FindLatestSQLProcedureIDsByNames`/`FindSQLProcedureIDsByFile`/`FindLatestSQLTableIDByName` и т.п.) используется indexing и review для дедупликации при индексации, а не query-командами; специфицирован опосредованно в `database-schema` и `relations-postprocessing`.

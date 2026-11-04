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

Система SHALL предоставлять команду `query procedure` для получения деталей хранимой процедуры по точному имени: путь к файлу, диапазон строк, список параметров с типами и направлением. Результат SHALL быть представлен как массив из нуля или одного элемента. Если процедура с указанным именем не найдена, система SHALL вернуть пустой результат (`count: 0`, пустой массив) без ошибки. При наличии нескольких копий процедуры (мультиветочная индексация) система SHALL выбирать копию детерминированно по убыванию приоритета: не сгенерированная копия (`not_generated`), совпадение с линией профиля dsinfo для продукта копии (`profile_match`, только при заданном профиле), копия default-ветки (`is_default`), больший id; результат SHALL содержать фактическую ветку (`branch_used`, нормализованный `branch_key`) и `branch_requested` — все линии манифеста продукта через запятую (пусто, если профиль не задан или продукт вне профиля). Если `branch_used` не входит в `branch_requested`, ответ SHALL содержать предупреждение о возможном различии кода, вычисленное по фактическим элементам ответа (правила — `dsinfo-analysis/dsinfo-resolution`).

#### Scenario: Детали существующей процедуры

- **GIVEN** проиндексированный проект с процедурой `API_RuleDoc_MassCreateDocument`
- **WHEN** выполняется `query procedure --name API_RuleDoc_MassCreateDocument`
- **THEN** возвращены имя, файл, `line_start`/`line_end`, параметры процедуры и `branch_used`

#### Scenario: Процедура не найдена

- **GIVEN** проиндексированный проект без процедуры `API_NonExistent`
- **WHEN** выполняется `query procedure --name API_NonExistent --json`
- **THEN** возвращён успешный ответ с `"count": 0` и `"items": []`
- **AND** запрос не завершается ошибкой `sql: no rows in result set`

#### Scenario: Две ветки одного продукта

- **GIVEN** процедура `MyProc` в develop и в `fa-contracts-11-01-7` с разными телами
- **WHEN** выполняется `query procedure --name MyProc` без указания ветки
- **THEN** возвращена копия из default-ветки (develop)
- **AND** `branch_used` сообщает выбранную ветку

#### Scenario: Копия из линии профиля

- **GIVEN** процедура `MyProc` в develop и в `fa-contracts-11-01-7` с разными телами; профиль задаёт линию `fa-contracts-11-01-7`
- **WHEN** выполняется `query procedure --name MyProc --dsinfo <id>`
- **THEN** возвращена копия из `fa-contracts-11-01-7`
- **AND** `branch_requested` = `branch_used` = `fa-contracts-11-01-7`, предупреждений нет

### Requirement: Поиск вызывающих процедуру

Система SHALL предоставлять команду `query callers` для поиска вызывающих указанную SQL-процедуру: прямые callers (`calls_procedure`, `dispatches_to_subscriber`) из sql_procedure, pas_method, js_function, report_form, vb_function, query_fragment и косвенные callers до 2 hop через цепочки `calls_procedure`/`dispatches_to`/`dispatches_to_subscriber`.

#### Scenario: Прямые и generated callers

- **GIVEN** проиндексированный проект, где `CallerProc` вызывает `TargetProc`, а `.t01` содержит `dispatches_to_subscriber` на `TargetProc`
- **WHEN** выполняется `query callers --procedure TargetProc`
- **THEN** возвращены и `CallerProc`, и generated subscriber call

### Requirement: Поиск SQL/query fragments

Система SHALL предоставлять команду `query sql-fragment` для полнотекстового поиска по `query_fragments.query_text` (GIN/`pg_trgm`). MCP-инструмент `codebase_query_sql_fragment` SHALL возвращать объект `{items, truncated, hint}` (изменение формы относительно массива — по прецеденту `desc_search`), где `items` — те же результаты, что и в CLI.

Множество кандидатов поиска SHALL ограничиваться потолком `sql_fragment_match_cap`
(секция `[query]`, по умолчанию 50000; `0` отключает ограничение). Если число
совпадений превысило потолок, ответ SHALL содержать диагностику в meta:
флаг `truncated = true` и подсказку уточнить шаблон; результат в этом случае —
подмножество совпадений (первый cap по порядку обхода индекса) с сохранением
дедупликации по содержимому и продуктам. Если совпадений не больше потолка,
результат MUST быть идентичен поиску без ограничения (байт-в-байт по составу и
порядку). Ограничение MUST делать худший случай ограниченным по времени: запрос
по сверхширокому шаблону завершается в пределах клиентского таймаута, а не
обрывается по `query_timeout_sec`.

#### Scenario: Поиск фрагмента по тексту

- **GIVEN** проиндексированная процедура, содержащая `SELECT * FROM tContract WHERE ID = @ID`
- **WHEN** выполняется `query sql-fragment --text "from tContract"`
- **THEN** фрагмент найден, результат содержит родительскую сущность, файл и строку

#### Scenario: Селективный шаблон — результат без изменений

- **GIVEN** проиндексированная БД и шаблон, дающий меньше совпадений, чем потолок
- **WHEN** выполняется `query sql-fragment --text <шаблон>` до и после введения потолка
- **THEN** возвращаются идентичные множества фрагментов в том же порядке, meta не содержит `truncated`

#### Scenario: Сверхширокий шаблон помечается усечённым

- **GIVEN** мультиветочный индекс масштаба FA, где шаблон `select` матчит больше `sql_fragment_match_cap` фрагментов
- **WHEN** выполняется `query sql-fragment --text select --json`
- **THEN** команда завершается успешно в пределах клиентского таймаута
- **AND** meta ответа содержит `truncated = true` и подсказку уточнить шаблон
- **AND** MCP-ответ `codebase_query_sql_fragment` по тому же шаблону — объект `{items, truncated: true, hint}`

#### Scenario: Отключение потолка конфигурацией

- **GIVEN** конфигурация с `[query] sql_fragment_match_cap = 0`
- **WHEN** выполняется поиск по сверхширокому шаблону
- **THEN** ограничение не применяется, поведение соответствует полному обходу совпадений

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

Система SHALL предоставлять команды `query report-form`, `query report-field`, `query report-param`, `query vb-function` для поиска отчётных форм, полей отчётов, параметров отчётов и VBScript-функций. Поиск `query report-form` (и MCP `codebase_query_report_form`) SHALL выполняться по имени отчёта (`report_name`), имени DFM-формы (`form_name`) и классу DFM-формы (`form_class`).

#### Scenario: Поиск отчётной формы

- **GIVEN** проиндексированный проект с отчётной формой `CreditReport`
- **WHEN** выполняется `query report-form --name Credit`
- **THEN** возвращена форма `CreditReport` с указанием файла и типа

#### Scenario: Поиск отчётной формы по имени DFM-формы

- **GIVEN** проиндексированный проект с отчётной формой `CreditReport` и DFM-формой `FormCreditReport`
- **WHEN** выполняется `query report-form --name FormCreditReport`
- **THEN** возвращена форма `CreditReport` с указанием файла и типа

#### Scenario: Поиск отчётной формы по классу DFM-формы

- **GIVEN** проиндексированный проект с отчётной формой `CreditReport` и классом DFM-формы `TFormCreditReport`
- **WHEN** выполняется MCP-запрос `codebase_query_report_form` с `{"name":"TFormCreditReport"}`
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

### Requirement: Приоритет релизной ветки при выборе сущности

При нескольких копиях сущности одного имени (SQL-процедуры, таблицы,
колонки, PAS-классы, DFM-формы, фрагменты и т. п. — все entity queries,
выполняющие выбор копии) система SHALL выбирать копию детерминированно по
убыванию приоритета: `not_generated`, `profile_match` (совпадение продукта и
линии с контекстом профиля dsinfo; только при заданном профиле),
default-ветка (`is_default`), больший id. `profile_match` SHALL вычисляться
по паре (продукт, ветка), поэтому профиль выбирает линию только для своих
продуктов; продукты вне профиля выбираются по правилам default. Оси
совпадения ветки и продукта в пост-обработке связей
(`relations-postprocessing`) от профиля не зависят: scope задаётся
файлом-источником. Результаты entity queries SHALL содержать фактическую
ветку (`branch_used`, нормализованный `branch_key`) и `branch_requested`
(все линии манифеста продукта через запятую) для продуктов профиля;
при одно-веточном индексе поле равно единственной ветке
или отсутствует без изменения остальных полей.

Списковые entity queries (таблицы, методы, формы, компоненты, отчёты,
JS/SMF) SHALL возвращать не более одной строки на сущность одного
продукта: копии, отличающиеся только веткой одного продукта, схлопываются
по тому же приоритету (естественный ключ сущности — имя; для PAS-методов —
класс и имя), сущности разных продуктов остаются отдельными строками.
Дедупликация SHALL быть двухфазной — сначала выбирается ветка для ключа,
затем возвращаются все строки выбранной ветки, поэтому одноимённые
сущности внутри одной ветки не схлопываются. Для поиска по содержимому
без естественного ключа (query fragments) побайтово идентичные копии из
разных веток SHALL схлопываться в одну строку.

Профиль SHALL влиять только на поверхности, перечисленные в
`dsinfo-analysis/dsinfo-resolution` («Границы применения профиля по
поверхностям»): `query callers`, `query relations` и `query inspect` выбор
копии не выполняют и профиль не учитывают.

#### Scenario: Таблица в двух ветках

- **GIVEN** таблица `TA_CONTRACTS`, проиндексированная в develop и в `fa-contracts-11-01-7`
- **WHEN** выполняется поиск таблицы по имени без указания ветки
- **THEN** выбрана копия из default-ветки, `branch_used` сообщает develop

#### Scenario: Одинаковое имя в двух продуктах

- **GIVEN** процедура `UTIL_DateFmt` в продуктах `fa-contracts` (develop) и `fa-payments` (develop)
- **WHEN** выполняется поиск по имени без указания профиля
- **THEN** выбор детерминирован (приоритет продукта не определён запросом — решает старший id) и одинаков при повторных вызовах

#### Scenario: Повторный вызов даёт ту же копию

- **GIVEN** мультиветочный индекс и поиск `query procedure --name MyProc`
- **WHEN** команда выполняется дважды подряд
- **THEN** оба ответа содержат одну и ту же копию и одинаковый `branch_used`

#### Scenario: Список таблиц без дублей веток

- **GIVEN** таблица `TA_CONTRACTS`, проиндексированная в develop и в `fa-contracts-11-01-7`, и одноимённая таблица в продукте fa-payments
- **WHEN** выполняется `query tables --like TA_CONTRACTS%`
- **THEN** список содержит одну строку `TA_CONTRACTS` продукта fa-contracts с `branch_used` = develop
- **AND** таблица `TA_CONTRACTS` продукта fa-payments остаётся отдельной строкой

#### Scenario: Профиль выбирает линию продукта

- **GIVEN** таблица `TA_CONTRACTS` в develop и в `fa-contracts-11-01-7` и профиль с линией `fa-contracts-11-01-7`
- **WHEN** выполняется `query table --name TA_CONTRACTS --dsinfo <id>`
- **THEN** выбрана копия из `fa-contracts-11-01-7`
- **AND** `branch_requested` = `branch_used` = `fa-contracts-11-01-7`

#### Scenario: Линия профиля не проиндексирована

- **GIVEN** таблица `TA_CONTRACTS` есть только в develop, а профиль задаёт непроиндексированную линию продукта `fa-contracts`
- **WHEN** выполняется `query table --name TA_CONTRACTS --dsinfo <id>`
- **THEN** выбрана develop-копия, `branch_used` = develop
- **AND** ответ содержит предупреждение о возможном различии кода

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

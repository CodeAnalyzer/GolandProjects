# CodeBase

Локальный индексатор исходников Diasoft 5NT для семантической навигации по кодовой базе.

## Возможности

- **Индексация файлов**: SQL, H, PAS, INC, JS, SMF, DFM, TPR, RPT, XML; опционально `.t01`
- **Извлечение сущностей**:
  - SQL: процедуры, таблицы, поля, определения колонок, определения индексов, отдельные SQL statements/query fragments, schema patches (ALTER TABLE ... ADD, M_ADD_FIELD, CREATE INDEX, M_CRT_INDEX)
  - H-файлы: константы, макросы, определения
  - PAS: юниты, классы, методы, поля, SQL-фрагменты, прямые ссылки на DFM forms/components
  - JS: функции, константы, вызовы процедур, SQL-запросы
  - SMF: модели Ф.О. (состояния, действия, счета), встроенный JavaScript
  - DFM: формы, компоненты, `Caption`, встроенные запросы
  - TPR: report forms (отчётные формы), report fields (поля отчёта), report params (параметры отчёта), SQL blocks (SQL-блоки), include directives (директивы include)
  - RPT: report forms (отчётные формы), report params (параметры формы), VB functions (VBScript-функции), embedded SQL (встроенный SQL)
  - DSArchitect XML и `.t01` поддерживаются парсерами и индексатором. `.t01` **не входит в дефолтные** `include_patterns` и для индексации требует явного добавления в конфиг; `*.xml` может уже присутствовать в вашем `codebase.toml`, но если его нет, добавьте явно
  - DSArchitect XML: `service` (сервисные контракты), `event` (событийные контракты), `used_service` (используемые сервисы), `callback_event` (callback-события), `api_table` (табличные структуры), `api_table_index` (индексы standalone API-таблиц), `api_param` (параметры BObject)
  - API macros (макросы API) из SQL: `API_CREATE_PROC`, `API_INIT_EVENT`, `API_EXEC`
  - `.t01`: препроцессированный SQL (процедуры, таблицы, поля, SQL statements/query fragments, вызовы процедур) и generated subscriber calls/dispatch-вызовы из раскрытых `API_INIT_EVENT`
- **Граф связей**:
  - SQL procedure -> SQL procedure / SQL table
  - parent entity -> query fragment
  - query fragment -> SQL procedure / SQL table / report param
  - report form -> report field / report param
  - VB function -> query fragment
  - SQL procedure -> API contract (`implements_contract`)
  - SQL procedure -> event contract (`publishes_event`)
  - SQL procedure -> API contract (`executes_contract`)
  - SQL procedure -> SQL procedure (`dispatches_to_subscriber`) для generated subscriber calls из `.t01`
- **Поиск и запросы**:
  - Поиск сущностей по имени через unified symbols index
  - Поиск использований таблиц с точным совпадением по умолчанию и опциональным нечётким режимом
  - Поиск вызовов процедур
  - Поиск PAS-методов по имени и методов, работающих с таблицей
  - Поиск SQL/query fragments по тексту SQL
  - Поиск schema таблиц (определений колонок из CREATE TABLE и schema patches)
  - Поиск индексов обычных SQL-таблиц (CREATE INDEX, M_CRT_INDEX)
  - Поиск DFM forms (форм) по имени, классу и `Caption`
  - Поиск DFM form components (компонентов формы) по имени, типу и `Caption`
  - Поиск JS-функций
  - Поиск SMF инструментов (Ф.О.)
  - Поиск report forms (отчётных форм)
  - Поиск report fields (полей отчёта)
  - Поиск report params (параметров отчёта)
  - Поиск VBScript functions (VBScript-функций)
  - Поиск API contracts (контрактов API), API tables (таблиц API), API table indexes (индексов API-таблиц), API params (параметров API), implementations (реализаций), publishers (публикаторов событий), consumers (потребителей контрактов)
  - Unified `query symbol` для SQL procedures/tables/indexes/column definitions, H defines, PAS units/classes/methods, JS functions/constants, DFM forms/components, report forms/params/VB functions, API business objects и XML/API symbols
- **Review (проверка SQL перед деплоем)**: статический анализ SQL-файлов с детекцией deploy stoppers (использование внешних таблиц/процедур, небезопасные конструкции IF/EXISTS, отсутствие required hints, и т.д.)
- **RTI-анализатор** (`codebase rti`): парсинг и анализ RTI-трейс логов Diasoft 5NT; извлечение вызовов процедур, параметров, контрольных точек, кодов ошибок, бизнес-лог блоков (`M_BUSINESSLOG_BLOCK_BEGIN/END`), checkpoint-временных меток, дампов таблиц (`M_LOG_TABLE`/`M_LOG_TABLE_LISTID`); сохранение в БД для повторного анализа
- **Кодировки**: CP866/WIN1251/UTF8 с эвристическим выбором для legacy-форматов, включая TPR и препроцессированные `.t01`

## Требования

- Go 1.21+
- PostgreSQL 14+ (порт 5435 по умолчанию)

## Установка

```bash
cd CodeBase
go mod tidy
go build -o codebase.exe
```

## Настройка

1. Создайте или отредактируйте `codebase.toml` в каталоге проекта:

```toml
# Путь к проекту Diasoft 5NT
root_path = "D:/GITHUB/FA/fa-contracts/Consumer"

# Настройки PostgreSQL
[database]
host = "localhost"
port = 5435
database = "codebase"
user = "postgres"
password = ""
sslmode = "disable"
```

Пример секции индексатора:

```toml
[indexer]
parallel = 12
batch_size = 500
include_patterns = ["*.sql", "*.h", "*.pas", "*.inc", "*.js", "*.smf", "*.dfm", "*.tpr", "*.rpt", "*.xml"]
exclude_patterns = ["*/.*", "*~", "*.bak", "*.old"]

[logging]
command_enabled = true
```

**Примечание:** При первом запуске `codebase init` база данных и схема создаются автоматически.

Если вы хотите индексировать препроцессированные `.t01`, добавьте `*.t01` в `indexer.include_patterns`.

Если в вашем конфиге ещё нет поддержки DSArchitect XML, также добавьте `*.xml` в `indexer.include_patterns`.

`.sql` остаётся первичным дистрибутивным источником, а `.t01` рассматривается как опциональный временный артефакт препроцессора: он может отсутствовать и может лежать в отдельном рабочем каталоге препроцессора.

Если флаг `--config` не передан, CLI ищет `codebase.toml` **рядом с executable (исполняемым файлом)**. Файл в текущем рабочем каталоге автоматически не подхватывается, если это не каталог самого executable.

## Использование

### Инициализация индекса

Полное сканирование проекта:

```bash
codebase init <путь_к_проекту>
codebase init --path <путь_к_проекту>
```

Или с использованием пути из конфига:

```bash
codebase init
```

Опции:
- `-p, --path` - корневой путь для сканирования
- `-j, --parallel` - количество параллельных workers (по умолчанию 4)
- `--yes` - пропуск подтверждений

### Обновление индекса

Инкрементальное обновление по изменённым файлам:

```bash
codebase update
```

Опции:
- `--modified` - сканировать только изменённые файлы (по умолчанию true)
- `-j, --parallel` - количество параллельных workers

### Запросы к индексу

#### Поиск сущности по имени

```bash
codebase query symbol --name MassAccrual_Start
codebase query symbol --name MassAccrual --like
codebase query symbol --name MassAccrual --type procedure --json
codebase query symbol --name TMyForm --type class
codebase query symbol --name Execute --type method --like
codebase query symbol --name SomeColumn --type column_definition
codebase query symbol --name XIE0SomeIndex --type index
codebase query symbol --name SomeComponent --type component
codebase query symbol --name SOME_CONST --type constant
codebase query symbol --name SomeBObject --type api_business_object
codebase query symbol --name API --summary
codebase query symbol --name API --ndjson
```

`query symbol` по умолчанию выполняет **точный поиск** по `name`.

Для поиска по подстроке используйте флаг `--like`.

В `symbols` индексируются основные name-based сущности: SQL procedures/tables/indexes/column definitions, H defines, PAS units/classes/methods, JS functions/constants, DFM forms/components, report forms/params/VB functions, API business objects и XML/API symbols. Сущности начинают появляться в `query symbol` после переиндексации соответствующих файлов.

#### Поиск информации о таблице

```bash
codebase query table --name tDocument
codebase query table --name tDocument --like
codebase query table --name tContract --json
codebase query table-schema --name tContract --json
```

`query table` по умолчанию выполняет **точный поиск** по имени таблицы.

Для старого режима поиска по подстроке используйте флаг `--like`.

`query table-schema` показывает определения колонок таблицы из `CREATE TABLE` и schema patches (`ALTER TABLE ... ADD`, `M_ADD_FIELD`).

Для name-based lookup команд ниже (`symbol`, `method`, `form`, `form-component`, `report-form`, `report-field`, `report-param`, `js-function`, `vb-function`, `smf-instrument`, `api-contract`, `api-table`, `api-param`, `api-table-index`) действует тот же принцип:

- без `--like` — точный поиск,
- с `--like` — поиск по подстроке.

#### Поиск вызовов процедуры

```bash
codebase query callers --procedure API_RuleDoc_MassCreateDocument
codebase query callers --procedure FCD_Cons_tConfigParam --limit 100 --json
```

`query callers` показывает как обычные `calls_procedure`, так и generated subscriber calls (`dispatches_to_subscriber`) из препроцессированных `.t01`, если такие файлы были проиндексированы.

#### Поиск PAS-методов по имени

```bash
codebase query method --name Execute
codebase query method --name Exec --like
codebase query method --name Execute --json
```

`query method` ищет PAS-методы напрямую в `pas_methods` и возвращает unit, class, файл, строку, signature и visibility.

#### Поиск методов, работающих с таблицей

```bash
codebase query methods --table pAPI_Accrual_Object
```

`query methods --table` использует graph relations и показывает PAS-методы, связанные с указанной SQL-таблицей через query fragments.

#### Поиск SQL/query fragments по тексту

```bash
codebase query sql-fragment --text "from tContract"
codebase query sql-fragment --text "exec API_" --json
```

Поиск выполняется по:

- `query_fragments.query_text`

Для ускорения используется `GIN` индекс с `pg_trgm` по `query_fragments.query_text`.

#### Поиск DFM forms

```bash
codebase query form --name AimCnt
codebase query form --name Aim --like
codebase query form --name "Цель кредита" --json
```

Поиск выполняется по:

- `form_name`
- `form_class`
- `caption`

#### Поиск DFM form components

```bash
codebase query form-component --name dlName
codebase query form-component --name Name --like
codebase query form-component --name "Наименование" --json
```

Поиск выполняется по:

- `component_name`
- `component_type`
- `caption`
- `form_name`
- `form_class`

#### Поиск report forms

```bash
codebase query report-form --name Credit
codebase query report-form --name Cred --like
codebase query report-form --name Portfolio --json
```

#### Поиск report fields

```bash
codebase query report-field --name Sum
codebase query report-field --name Su --like
```

#### Поиск report params

```bash
codebase query report-param --name InstitutionID
codebase query report-param --name Date --like
codebase query report-param --name Date --json
```

#### Поиск VBScript functions

```bash
codebase query vb-function --name Create
codebase query vb-function --name Creat --like
codebase query vb-function --name Execute --json
```

#### Поиск DSArchitect API contracts

```bash
codebase query api-contract --name API_CCred_BindClassifier
codebase query api-contract --name API_CCred --like
codebase query api-contract --name OnAfterLoan_MassInsert --json
```

#### Поиск DSArchitect API tables

```bash
codebase query api-table --name pAPI_CredS_InsertCCred
codebase query api-table --name pAPI_CredS --like
codebase query api-table --name pAPI_ContractCredit_ID --json
```

#### Поиск индексов standalone DSArchitect API tables

```bash
codebase query api-table-index --name XIE0pAPI_CCred_Agreement
codebase query api-table-index --name pAPI_CCred_Agreement --json
```

#### Поиск индексов обычных SQL-таблиц

```bash
codebase query table-index --name IX_Contract

codebase query table-index --name tContract --json
```

Поиск выполняется по:

- `index_name`
- `table_name`

#### Поиск DSArchitect API params

```bash
codebase query api-param --name BranchID
codebase query api-param --name Branch --like
codebase query api-param --name AccountID --json
```

#### Поиск JS functions (JS-функций)

```bash
codebase query js-function --name OnClick
codebase query js-function --name Click --like
codebase query js-function --name Execute --json
```

#### Поиск SMF instruments (SMF-инструментов)

```bash
codebase query smf-instrument --name CreditMassOperation
codebase query smf-instrument --name Credit --like
codebase query smf-instrument --name Accrual --json
codebase query smf-instrument --name ТР_ГПККНач
codebase query smf-instrument --name TS_CardCreditMassAcrual --json
```

Поиск выполняется по:

- `instrument_name` (внутреннее имя инструмента)
- `brief` (краткое название)
- имени файла SMF-сценария

#### Поиск SMF по типу сценария

```bash
codebase query smf-type --type instrument_model
codebase query smf-type --type mass_operation --json
```

#### Поиск relations (связей)

```bash
codebase query relations --source-type procedure --source-name API_ --limit 100
codebase query relations --target-type table --target-name tContract --relation-type uses_table --json
```

#### Поиск SQL implementations (реализаций SQL) API contracts

```bash
codebase query api-impl --name API_CCred_BindClassifier
codebase query api-impl --name CON_RuleDoc_MassGetLimitAcc --json
```

#### Поиск publishers (публикаторов) событий API

```bash
codebase query api-publishers --event OnAfterLoan_MassInsert
codebase query api-publishers --event OnCCred_FindListIDByParam --json
```

#### Поиск consumers (потребителей) API contracts

```bash
codebase query api-consumers --name API_Account_FindIDByNumber
codebase query api-consumers --name API_CCred_BindClassifier --json
```

#### Inspect сущности с graph context (контекстом графа)

```bash
codebase query inspect --name Cons_Check_Restr_API
codebase query inspect --name Cons_Check_Restr_API --json
codebase query inspect --name MassAccrual_Start --type procedure --json
```

Опции для всех запросов:
- `--json` - вывод в формате JSON
- `--summary` - summary output (сводный вывод) по результатам запроса
- `--ndjson` - NDJSON output (построчный JSON) для pipeline/automation
- `--limit` - максимум результатов (по умолчанию 100)

#### CLI contract для machine-readable modes (машино-читаемых режимов)

Короткий список режимов, на которые можно безопасно опираться в `Skills` и automation (автоматизации):

- `codebase query <subcommand> --json`
  - JSON envelope (JSON-конверт) с полями `success`, `format_version`, `command`, `count`, `items`, `meta`
  - при использовании `--summary` дополнительно возвращается поле `summary`
- `codebase query <subcommand> --summary`
  - summary-only JSON object (только JSON-объект со сводкой)
- `codebase query <subcommand> --ndjson`
  - NDJSON stream (поток NDJSON): один result item (элемент результата) на строку
- `codebase stats --json`
  - JSON envelope со статистикой индекса
- `codebase health --json`
  - JSON envelope со status (статусом) и массивом `checks`

Гарантии для machine-readable modes:

- banner/output noise (лишний баннер и служебный вывод) подавляются
- ошибки возвращаются в structured JSON format (структурированном JSON-формате)
- пустые результаты стабильно возвращаются как пустые массивы `[]`, а не `null`
- `format_version` зафиксирован как `1.0`

### Статистика

Вывод статистики по индексу:

```bash
codebase stats
codebase stats --json
```

### Health checks (проверки готовности)

Проверка readiness (готовности) CLI, БД и индекса:

```bash
codebase health
codebase health --json
```

Команда проверяет:

- `config`
- `database`
- `schema`
- `index readiness` (готовность индекса по completed scan run)

Пример вывода:

```
Health status: ok

- config: ok
- database: ok
- schema: ok
- index: ok
```

### RTI-анализатор

Анализ RTI-трейс логов (`.rti`) Diasoft 5NT:

```bash
# Парсинг файла и сохранение в БД
codebase rti parse path/to/file.rti

# Сводка по логу
codebase rti summary path/to/file.rti
codebase rti summary --session 42
codebase rti summary --json

# Дерево вызовов
codebase rti tree path/to/file.rti
codebase rti tree --session 42 --proc MyProc --max-depth 3

# Ошибочные вызовы (ret_val ≠ 0)
codebase rti errors path/to/file.rti
codebase rti errors --session 42 --json

# Медленные вызовы
codebase rti slow path/to/file.rti
codebase rti slow --session 42 --threshold 500

# Детали конкретной процедуры (параметры, checkpoints, enrichment)
codebase rti details path/to/file.rti --proc MyProc
codebase rti details --session 42 --proc MyProc --json

# Бизнес-лог процедуры: блоки, checkpoints с timestamp, дампы таблиц
codebase rti blog path/to/file.rti --proc FCD_CORE_MassAccrual_Start
codebase rti blog --session 42 --proc FCD_CORE_MassAccrual_Start --json

# Список сохранённых сессий
codebase rti list
codebase rti list --limit 10

# Управление сессиями
codebase rti delete --session 42
codebase rti prune --keep-last 5
```

Подкоманды:
- **`parse`** — распарсить `.rti` файл и сохранить результат в БД; выводит сводку + session ID
- **`summary`** — общая сводка: total_calls, errors_count, max_nest_level, top_slow
- **`tree`** — дерево вызовов с временами и кодами возврата
- **`errors`** — вызовы с ненулевым ret_val + расшифровка кода из `ds_return_codes`
- **`slow`** — вызовы медленнее порога (по умолчанию 100 мс)
- **`details`** — параметры, checkpoints, enrichment (путь к файлу и строки из индекса) для конкретной процедуры
- **`blog`** — бизнес-лог процедуры: блоки с Enter/Exit и elapsed_ms, контрольные точки с timestamp, дампы таблиц с заголовками и строками
- **`list`** — список сохранённых сессий
- **`delete`** — удалить сессию (CASCADE удаляет все связанные записи)
- **`prune`** — удалить старые сессии, оставить последние N

Общие флаги для большинства подкоманд:
- `--session` — загрузить данные из сохранённой сессии вместо файла
- `--proc` — имя процедуры (для `details`, `blog`, `tree`)
- `--json` — вывод в JSON

### Review (проверка SQL перед деплоем)

Статический анализ SQL-файлов для выявления проблем перед деплоем:

```bash
codebase review <путь_к_файлу.sql>
codebase review <путь_к_файлу.sql> --json
codebase review <путь_к_файлу.sql> --rules foreignTablesUsing,execNotExistsProc
```

**Deploy stoppers** (severity=1, критические ошибки, блокирующие деплой):

- `execNotExistsProc` — `EXEC` несуществующей процедуры (проверка по индексу)
- `procDuplicate` — дублирование имени процедуры в одном файле
- `procParamDefValue` — параметр процедуры с DEFAULT значением
- `procElseCase` — `ELSE` в `CASE` без обработки ошибок
- `useSelectAll` — использование `SELECT *`
- `truncTbl` — использование `TRUNCATE TABLE`
- `ansiInJoin` — ANSI-89 join syntax (comma joins)
- `insertRowLock` — `INSERT` без `ROWLOCK`
- `useEqColumn` — сравнение колонок разных типов
- `tableFullScan` — условия, приводящие к full scan
- `tableHintExists` — отсутствие required table hints
- `tableHintIsRight` — проверка корректности table hints
- `indexExistsInDB` — использование индекса, отсутствующего в БД
- `indexWrong` — неправильное использование индекса (проверка SPID для p-таблиц)
- `updateOnlyVar` — `UPDATE` только переменных без обновления полей таблицы
- `pTableSpid` — отсутствие условия по `SPID` для p-таблицы
- `forceOrder2Tbl` — запрос с 2+ таблицами без `M_FORCEORDER`
- `saveTran` — использование `SAVE TRAN/TRANSACTION`
- `useDrop` — использование `DROP`/`DROP_CREATE` в теле процедуры
- `mathOperations` — математические операции в `BETWEEN` и сравнениях (риск overflow)
- `existsWithAndInIf` — `IF` с запросом к таблицам и `AND` (нет гарантии short-circuit)

**PostgreReq** (severity=2, требования PostgreSQL):

- `nullComparison` — сравнение с `NULL` через `=`/`<>` вместо `IS NULL`/`IS NOT NULL`
- `shouldBeCP866` — строковые литералы должны быть в кодировке CP866
- `tooManyJoins` — превышение лимита JOIN (более 12)
- `maxProcParam` — превышение лимита параметров процедуры (более 90)
- `modifyOutProc` — модификация выходных параметров процедуры
- `emptyReturn` — пустой `RETURN` без значения
- `rawTransactionControl` — прямое управление транзакциями (`BEGIN TRAN`/`COMMIT`/`ROLLBACK`)
- `deferredUpdate` — отложенное обновление (UPDATE после INSERT в той же таблице)
- `inSubQuery` — использование `IN` с подзапросом (рекомендуется `EXISTS`)
- `varcharSize` — `VARCHAR` без указания размера
- `columnInsert` — `INSERT` без явного указания колонок
- `postgreLabelGotoLevel` — использование `GOTO` и меток
- `dateIntoString` — приведение даты к строке
- `emptyStringDate` — сравнение даты с пустой строкой
- `varUseAfterCursor` — использование переменной после закрытия курсора
- `excessProcParams` — избыточные параметры процедуры
- `duplicateOutputVariable` — дублирование выходной переменной
- `useOnlyDeclaredCursors` — использование курсоров без объявления
- `cursorFetchArguments` — проверка аргументов `FETCH` курсора
- `usageVarInSameSelect` — использование переменной в том же `SELECT`
- `varAssignInUpdate` — присвоение переменной в `UPDATE`
- `statementsWithJoinsRequireAliases` — запросы с JOIN требуют алиасы таблиц
- `useFuncInIndCol` — использование функции в индексированной колонке
- `isNullSameTypes` — `ISNULL` с аргументами разных типов
- `diffTypesComparison` — сравнение выражений разных типов в `WHERE`/`ON`/`IF`/`CASE WHEN`
- `floatToStringConvert` — приведение `FLOAT`/`DSFLOAT` к строке через `CONVERT`/`CAST`
- `selectAfterSetRowcount` — `SELECT` в переменные и `INSERT...SELECT` после `SET ROWCOUNT N` без `ORDER BY`
- `aliasWhenUsingUnion` — колонки `ORDER BY` при `UNION` должны быть в алиасах первого `SELECT`

**Fine code** (severity=3, рекомендации по качеству кода):

- `foreignTablesUsing` — использование таблиц из других продуктов
- `foreignPTablesUsing` — использование p-таблиц из других продуктов
- `foreignProcedureUsing` — вызов процедур из других продуктов
- `datatype` — потенциальная потеря точности при assignment/conversion

Опции:
- `--rules` — список проверяемых правил (по умолчанию все deploy stoppers)
- `--min-severity` — минимальный уровень серьезности (1=deploy stopper, 3=fine code)
- `--json` — вывод в формате JSON

Пример JSON вывода:
```json
{
  "success": true,
  "format_version": "1.0",
  "command": "review",
  "count": 2,
  "items": [
    {
      "rule": "foreignTablesUsing",
      "severity": 1,
      "message": "Использование таблицы из другого продукта: OtherProduct.tTable",
      "file": "D:/.../Procedure.sql",
      "line": 42,
      "object": "MyProcedure"
    }
  ]
}
```

### MCP режим (stdio JSON-RPC)

Запуск MCP сервера:

```bash
codebase mcp
```

Особенности режима:

- используется явный запуск `codebase mcp` (авто-детект по TTY не используется)
- `stdout` зарезервирован под JSON-RPC транспорт (без баннера и лишнего текстового вывода)
- инструменты MCP реализованы поверх внутреннего сервисного слоя, без вызова Cobra-команд

Ключевые MCP tools:

- `codebase.health`
- `codebase.stats`
- `codebase.query.*` для всех query-подкоманд CLI

**RTI tools:**

| Tool | Описание | Обязательные параметры |
|------|----------|------------------------|
| `codebase_rti_parse` | Парсинг `.rti` файла и сохранение в БД | `file_path` |
| `codebase_rti_list` | Список сохранённых сессий | — |
| `codebase_rti_summary` | Сводка сессии | `session_id` или `file_path` |
| `codebase_rti_tree` | Дерево вызовов | `session_id` или `file_path` |
| `codebase_rti_errors` | Вызовы с ошибками (ret_val ≠ 0) | `session_id` или `file_path` |
| `codebase_rti_slow` | Медленные вызовы | `session_id` или `file_path` |
| `codebase_rti_details` | Детали процедуры с enrichment из индекса | `session_id` или `file_path`, `procedure` |
| `codebase_rti_blog` | Бизнес-лог процедуры: блоки/checkpoints/таблицы | `session_id` или `file_path`, `procedure` |
| `codebase_rti_delete` | Удалить сессию | `session_id` |
| `codebase_rti_prune` | Удалить старые сессии, оставить N | `keep_last` |

Пример вывода `codebase_rti_blog`:
```json
{
  "procedure": "FCD_CORE_MassAccrual_Start",
  "count": 1,
  "calls": [{
    "enter_line": 42,
    "elapsed_ms": 1430,
    "blog_blocks": [
      { "block_name": "Основной блок", "enter_time": "...", "exit_time": "...", "elapsed_ms": 143 }
    ],
    "checkpoints": [
      { "label": "FCD_CORE_MassAccrual_Start_Begin_1", "timestamp": "16:59:03.500", "elapsed_ms": 263 }
    ],
    "blog_tables": [
      { "table_name": "tAccrualData", "columns": ["ID:int", "Amount:numeric"], "row_count": 42, "rows": ["..."] }
    ]
  }]
}
```

Контракт формата:

- **MCP**: tools возвращают чистые доменные данные (например `{ "count": N, "items": [...] }`)
- **CLI**: сохраняет текущий JSON envelope (`success`, `format_version`, `command`, `meta`, ...)

## Архитектура

```
CodeBase/
├── cmd/                           # CLI команды и wiring
│   ├── root.go                    # Корневая команда и bootstrap CLI
│   ├── init.go                    # Полная инициализация индекса
│   ├── update.go                  # Инкрементальное обновление индекса
│   ├── query.go                   # Регистрация query-флагов и подкоманд
│   ├── query_shared.go            # Общие типы/флаги query CLI
│   ├── query_commands.go          # Query-команды поиска по индексу
│   ├── query_execution.go         # Выполнение query и форматирование вывода
│   ├── query_api.go               # API query-команды
│   ├── review.go                  # Review команда (проверка SQL перед деплоем)
│   ├── rti.go                     # RTI-анализатор: parse/summary/tree/errors/slow/details/blog/list/delete/prune
│   ├── stats.go                   # Команда stats
│   ├── health.go                  # Команда health
│   └── mcp.go                     # Команда запуска MCP сервера
├── internal/
│   ├── config/                    # Конфигурация
│   ├── encoding/                  # Кодировки CP866/WIN1251
│   ├── fswalk/                    # Обход файловой системы
│   ├── indexer/
│   │   ├── indexer.go             # Базовый тип Indexer и общие file processors
│   │   ├── runner.go              # Init/Update pipeline, worker pool, progress
│   │   ├── indexer_sql_pas.go     # Индексация SQL/PAS и SQL-like pipeline для препроцессированных `.t01`
│   │   ├── indexer_relations.go   # Построение relations и query-fragment helpers
│   │   └── indexer_postprocess_pas.go # Постобработка PAS классов/методов/полей
│   ├── model/                     # Модели данных
│   ├── parser/
│   │   ├── sql/                   # SQL-парсер
│   │   ├── h/                     # H-файлов парсер
│   │   ├── dfm/                   # DFM-парсер
│   │   ├── pas/                   # PAS-парсер
│   │   ├── js/                    # JS-парсер
│   │   ├── smf/                   # SMF-парсер
│   │   ├── tpr/                   # TPR-парсер
│   │   ├── rpt/                   # RPT-парсер
│   │   ├── dsxml/                 # DSArchitect XML-парсер
│   │   └── apimacro/              # API macro parser для исходных SQL
│   ├── query/
│   │   ├── query.go               # Базовые query-типы и прочие read-model сценарии
│   │   ├── query_sql.go           # SQL/table/procedure query-сценарии
│   │   ├── query_relations.go     # Запросы relation graph
│   │   └── api_query.go           # Query API-контрактов и DSArchitect сущностей
│   ├── querysvc/                  # Внутренний runtime и compose-логика query (CLI + MCP)
│   ├── systemsvc/                 # Внутренний runtime для health/stats (CLI + MCP)
│   ├── review/                    # Review rules engine и SQL checker
│   │   ├── types.go               # Типы Finding, RuleID, Severity
│   │   ├── review_rules.go        # Реализация проверок (deploy stoppers)
│   │   ├── review_helpers.go      # Вспомогательные функции
│   │   ├── runner.go              # Review runner и execution pipeline
│   │   └── runner_test.go         # Тесты для review rules
│   ├── rti/                       # RTI-анализатор
│   │   ├── model.go               # RTICall, RTIParam, RTICheckpoint, RTIBLogBlock, RTIBLogTable, RTISummary
│   │   ├── parser.go              # ParseFile, parseContent (regex state machine, CP866/UTF8)
│   │   ├── parser_test.go         # Тесты парсера
│   │   ├── tree.go                # BuildTree, FormatTree, RTITreeNode
│   │   ├── symbols.go             # moduleIDMap (94 записи), ModuleNameByID
│   │   ├── enrich.go              # ProcedureLookup, EnrichCalls (enrichment из индекса)
│   │   └── store.go               # SaveSession, LoadCalls, LoadBLogBlocks, LoadBLogTables, ListSessions, ...
│   ├── mcp/                       # MCP stdio JSON-RPC transport, tools registry, handlers
│   └── store/
│       ├── db.go                  # Основной persistence layer и batch insert helpers
│       └── api_store.go           # Persistence для API/DSArchitect сущностей
├── main.go                        # Точка входа приложения
└── codebase.toml                  # Конфигурация
```

## Схема БД

Основные таблицы:

- `scan_runs` - метадеанные запусков сканирования
- `files` - индекс файлов
- `sql_procedures` - SQL-процедуры
- `sql_tables` - таблицы в SQL
- `sql_columns` - поля таблиц
- `sql_column_definitions` - определения колонок таблиц из `CREATE TABLE` и schema patches (`ALTER TABLE ... ADD`, `M_ADD_FIELD`)
- `sql_index_definitions` - определения индексов обычных SQL-таблиц из `CREATE INDEX` и `M_CRT_INDEX`
- `sql_index_definition_fields` - поля индексов обычных SQL-таблиц
- `pas_units` - Pascal юниты
- `pas_classes` - Pascal классы с прямой ссылкой `dfm_form_id` на DFM форму
- `pas_methods` - Pascal методы
- `pas_fields` - Pascal поля с прямой ссылкой `dfm_component_id` на DFM компонент
- `js_functions` - JavaScript функции
- `smf_instruments` - модели Ф.О. из SMF
- `dfm_forms` - DFM формы с `caption`
- `dfm_components` - DFM компоненты формы с `caption` и `parent_name`
- `report_forms` - отчётные формы TPR/RPT
- `report_fields` - поля отчётов TPR
- `report_params` - параметры отчётов TPR/RPT
- `vb_functions` - VBScript-функции из RPT
- `h_files_defines` - Определения из H-файлов
- `api_business_objects` - бизнес-объекты DSArchitect
- `api_contracts` - контракты DSArchitect API
- `api_contract_params` - scalar-параметры контрактов
- `api_contract_tables` - табличные параметры контрактов
- `api_contract_table_fields` - поля табличных параметров контрактов
- `api_business_object_params` - standalone params (отдельные параметры) BObject
- `api_business_object_tables` - standalone tables (отдельные таблицы) BObject
- `api_business_object_table_fields` - поля standalone tables BObject
- `api_business_object_table_indexes` - индексы standalone tables BObject
- `api_business_object_table_index_fields` - поля индексов standalone tables BObject
- `api_contract_return_values` - return values (возвращаемые значения) контрактов
- `api_contract_contexts` - contexts (контексты) контрактов
- `api_macro_invocations` - извлечённые API macros (макросы API) из исходных `.sql`
- `rti_sessions` — сессии парсинга RTI-логов
- `rti_calls` — вызовы процедур из RTI-лога (Enter/Exit, elapsed_ms, ret_val, nest_level)
- `rti_params` — параметры вызовов
- `rti_checkpoints` — контрольные точки (`M_BUSINESSLOG_CHECKPOINT`) с timestamp
- `rti_blog_blocks` — бизнес-лог блоки (`M_BUSINESSLOG_BLOCK_BEGIN/END`) с Enter/Exit временами и elapsed_ms
- `rti_blog_tables` — дампы таблиц из бизнес-лога (`M_LOG_TABLE`/`M_LOG_TABLE_LISTID`) с заголовками и строками
- `ds_return_codes` — справочник кодов возврата процедур
- `relations` - Связи между сущностями
- `query_fragments` - SQL-фрагменты в коде, включая отдельные SQL statements из `.sql` и препроцессированных `.t01` procedures/scripts, пригодные для текстового поиска
- `include_directives` - include-директивы и их разрешение
- `symbols` - Унифицированный индекс для поиска

## Актуальные детали индексации

### SQL schema patches

- Поддерживаются schema patches для обычных SQL-таблиц из Consumer patch-файлов:
  - `ALTER TABLE <table> ADD <column_def>` - добавление колонок
  - `M_ADD_FIELD('<table>','<column_def>')` - макрос добавления колонок
  - `CREATE [UNIQUE] INDEX <index> ON <table>(<fields>)` - создание индексов
  - `M_CRT_INDEX('<type>','<index>','<table>','<fields>')` - макрос создания индексов
- Определения колонок из `CREATE TABLE` и schema patches индексируются в `sql_column_definitions` с полем `definition_kind` для различения origin (create_table, alter_add, macro_add_field)
- Определения индексов индексируются в `sql_index_definitions` с полями индексов в отдельной таблице `sql_index_definition_fields`
- Поиск schema таблиц: `query table-schema --name <table>` - показывает все определения колонок с указанием origin
- Поиск индексов SQL-таблиц: `query table-index --name <index_or_table>` - поиск по имени индекса или таблицы

### DSArchitect XML

- Поддерживаются XML-схемы `Object`, `Table`, `Param` для DSArchitect API/BObject сущностей.
- Для standalone `Table` XML индексируются не только поля таблицы, но и секция `Indexses` с индексами и составом полей индекса.
- XML с root element (корневым элементом) `<message>` считаются неподдерживаемым форматом и пропускаются без ошибки.
- Для `api_business_objects.module_name` значение извлекается только из path (пути), как имя каталога перед `DSArchitectData`.
- Для `api_contracts.owner_module` используется то же path-based rule (правило на основе пути).
- Для XML с declared encoding (заявленной кодировкой) `windows-1251` поддерживается корректное decoding (декодирование) через `CharsetReader`.

### Препроцессированные `.t01`

- `.t01` индексируются как SQL-like layer: из них извлекаются процедуры, вызовы процедур, query fragments и table usage.
- Для `.t01` не выполняется API macro extraction: предполагается, что макросы уже раскрыты препроцессором.
- Поверх SQL parsing для `.t01` дополнительно извлекаются generated subscriber calls по паттернам `exec GetAPIProcessID ...` и `exec @RetVal = <proc> ... @ProcessID = @GlobalProcessID`.
- Такие вызовы сохраняются в graph как relation `dispatches_to_subscriber`.
- Индексация `.t01` остаётся опциональной: отсутствие `.t01` не мешает базовой индексации проекта по исходным `.sql`.

### Логирование

#### Command logs (логи команд CLI)

- Все команды CLI логируются в файл `codebase_YYYYMMDD.log` (один файл на день).
- Лог содержит информацию о каждой выполненной команде: `started_at`, `command`, `duration`, `status`, `error`.
- Логирование включено по умолчанию.
- Для отключения установите `logging.command_enabled = false` в `codebase.toml`.

#### Error logs (логи ошибок индексатора)

- Ошибки индексатора пишутся в отдельный log file (лог-файл) на каждый запуск.
- Формат имени: `indexer_errors_YYYYMMDD_HHMMSS.log`.
- В каждой записи указывается путь файла, на котором произошла ошибка.
- Это предотвращает смешивание ошибок от разных запусков `init`/`update`.

## Планы развития

### Текущее состояние
- [x] Каркас CLI и БД
- [x] Индекс файлов и метаданных
- [x] SQL-парсер (процедуры, таблицы, поля)
- [x] SQL schema symbols (определения колонок и индексов в `query symbol`)
- [x] H-парсер (константы, макросы)
- [x] PAS-парсер (юниты, классы, методы, поля, SQL-фрагменты)
- [x] PAS symbols (юниты, классы и методы в `query symbol`)
- [x] JS-парсер (функции, константы, SQL-запросы)
- [x] JS constants storage и symbols (`js_constants`, `query symbol --type constant`)
- [x] DFM-парсер (формы, компоненты, `Caption`, запросы)
- [x] DFM component symbols (`query symbol --type component`)
- [x] SMF-парсер (модели Ф.О., встроенный JavaScript)
- [x] TPR-парсер
- [x] RPT-парсер
- [x] Индексация relations (связей) между сущностями
- [x] Базовый unified symbols index
- [x] Query mode (режим query) для DFM/report-сущностей
- [x] Unified JSON output (унифицированный JSON-вывод) для query/stats
- [x] Query relations / inspect / summary / ndjson
- [x] Полнотекстовый поиск по SQL/query fragments (`query sql-fragment`)
- [x] Health command (команда health) с readiness checks (проверками готовности)
- [x] DSArchitect XML indexing (индексация DSArchitect XML)
- [x] API business object symbols (`query symbol --type api_business_object`)
- [x] API macro extraction из исходных SQL и SQL-like indexing/preprocessed dispatch extraction для `.t01`
- [x] Query mode (режим query) для `api-contract` / `api-table` / `api-param`
- [x] Query mode (режим query) для `api-impl` / `api-publishers` / `api-consumers`
- [x] SQL schema patches (ALTER TABLE ... ADD, M_ADD_FIELD, CREATE INDEX, M_CRT_INDEX) для обычных SQL-таблиц
- [x] Query mode (режим query) для `table-schema` и `table-index` обычных SQL-таблиц
- [x] RTI-анализатор: парсинг трейс-логов, сохранение в БД, CLI и MCP tools
- [x] RTI бизнес-лог: блоки `M_BUSINESSLOG_BLOCK_BEGIN/END`, checkpoint timestamps, дампы `M_LOG_TABLE`

## Лицензия

Внутренний инструмент Diasoft.

# Покрытие индексируемых сущностей командами `codebase query`

Документ сопоставляет сущности, которые индексируются CodeBase, с доступными командами `codebase query`, и отдельно отмечает сущности без прямого CLI-поиска.

## Универсальные query-команды

- **`query symbol --name ... [--type ...] [--like]`**
  - Работает только по сущностям, которые индексатор кладёт в `symbols`.
  - Сейчас туда явно попадают не все сущности.

- **`query inspect --name ... [--type ...]`**
  - Стартует от `symbols`, затем подтягивает входящие/исходящие `relations`.
  - Если сущность не попала в `symbols`, напрямую через `inspect` её не найти.

- **`query relations ...`**
  - Ищет связи в `relations`.
  - Может показать сущность косвенно, если она участвует в relation.

## SQL / `.sql`, `.t01`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| SQL-процедуры / scripts | `query procedure --name ...`, `query symbol --type procedure --name ...`, `query callers --procedure ...`, `query relations`, `query inspect` | Есть прямой доступ. |
| Параметры SQL-процедур | `query procedure --name ...` | Как часть детального результата процедуры. Отдельного `query procedure-param` нет. |
| Вызовы SQL-процедур | `query callers --procedure ...`, `query relations --relation-type calls_procedure` | Сами вызовы хранятся как `relations`, не как отдельная searchable entity. |
| SQL-таблицы / usage | `query table --name ...`, `query symbol --type table --name ...`, `query methods --table ...`, `query relations` | Есть прямой доступ. |
| SQL-колонки usage | **[нет прямого query]** | Индексируются в `sql_columns`, но отдельной команды поиска колонок нет. |
| Определения колонок таблиц | `query table-schema --name <table>` | Доступны через схему таблицы, но не как `query column`. |
| Определения SQL-индексов | `query table-index --name <index_or_table>` | Есть прямой доступ. |
| Поля SQL-индексов | `query table-index --name ...` | Доступны в составе результата индекса. Отдельной команды нет. |
| SQL/query fragments | `query sql-fragment --text ...`, `query relations`, `query callers` частично | Есть прямой полнотекстовый поиск по тексту SQL-фрагмента. |
| Include-директивы SQL | **[нет прямого query]** | Индексируются, но CLI-команды поиска include нет. |
| Defines из SQL parser result | **[нет прямого query]** | В `ParseResult` есть `Defines`, но прямой query-команды нет. |

## API macros из SQL

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| `API_CREATE_PROC` invocations | `query api-impl --name ...`, `query relations` | Обычно проявляется через связи API contract -> SQL implementation. |
| `API_INIT_EVENT` invocations | `query api-publishers --event ...`, `query relations` | Доступны через publishers/relations. |
| `API_EXEC` invocations | `query api-consumers --name ...`, `query relations` | Доступны через consumers/relations. |
| Сырые `APIMacroInvocation` записи | **[нет прямого query]** | Нет команды типа `query api-macro`. |
| Generated subscriber/dispatch calls из `.t01` | `query callers --procedure ...`, `query relations --relation-type dispatches_to_subscriber` | Доступны как relation, не как отдельная сущность. |

## H / `.h`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| Defines | `query symbol --type define --name ...` | Индексируются в `symbols`. |
| Constants из `#define` | `query symbol --type define --name ...` | Тип в symbols всё равно `define`; более точного query по const нет. |
| Macros из `#define MACRO(...)` | `query symbol --type define --name ...` | Также представлены как define. |
| Empty defines/macros | `query symbol --type define --name ...` | Если сохранены как `HDefine`. |
| Include-директивы H | **[нет прямого query]** | Индексируются как include directives, но команды поиска include нет. |

## PAS / `.pas`, `.inc`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| Pascal units | `query symbol --type unit --name ...` | Индексируются в `pas_units` и `symbols`. |
| Pascal classes | `query symbol --type class --name ...` | Индексируются в `pas_classes` и `symbols`. |
| Pascal methods | `query method --name ...`, `query symbol --type method --name ...`, `query methods --table ...`, `query relations`, `query inspect` | Есть прямой поиск по имени и специализированный поиск методов, работающих с таблицей. |
| Pascal fields | **[нет прямого query]** | Индексируются, связываются с DFM component, но команды поиска нет. |
| SQL-фрагменты внутри Pascal | `query sql-fragment --text ...`, `query relations` | Через `query_fragments`. |
| Вызовы из PAS | `query relations`, частично `query callers` если сохранены как `calls_procedure` | Нет отдельного `query pas-call`. |
| SQL tables из PAS embedded SQL | `query table --name ...`, `query methods --table ...`, `query relations` | Табличные usage доступны через table/methods. |
| DFM queries, найденные из PAS/DFM контекста | `query sql-fragment --text ...` если сохранены как fragments | Отдельной команды нет. |

## JS / `.js`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| JS-функции | `query js-function --name ...`, `query symbol --type function --name ...`, `query inspect` | Есть прямой доступ. |
| Script objects | **[нет прямого query]** | Парсер извлекает, но отдельной таблицы/команды поиска по ним не видно. |
| `ExecProc` / `JSProcedureCall` | `query callers --procedure ...`, `query relations --source-type js_function --relation-type calls_procedure` | После фикса доступны как relation. |
| `ExecQuery` / `JSQueryCall` | `query sql-fragment --text ...`, `query relations` | Сохраняются как `QueryFragment`. |
| JS-константы | **[нет прямого query]** | Парсер извлекает, но индексатор в видимом коде не сохраняет их в symbols/таблицу. |

## SMF / `.smf`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| SMF instrument | `query smf-instrument --name ...`, `query smf-type --type ...` | Есть прямой доступ. |
| JS-функции внутри SMF | `query js-function --name ...`, `query symbol --type function --name ...` | Сохраняются как JS functions с файлом `.smf`. |
| `ExecProc` внутри SMF JS | `query callers --procedure ...`, `query relations --source-type js_function --relation-type calls_procedure` | После фикса. |
| `ExecQuery` внутри SMF JS | `query sql-fragment --text ...`, `query relations` | Если сохранён как query fragment. |
| Prequery SQL | `query sql-fragment --text ...`, `query relations` | Контекст `smf_prequery`. |
| Includes SMF | **[нет прямого query]** | Сохраняются, но команды нет. |
| Description SMF | `query smf-instrument --name ... --like` частично | Поиск заявлен по имени/brief/file; description может быть не покрыт. |

## DFM / `.dfm`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| DFM forms | `query form --name ...`, `query symbol --type form --name ...`, `query inspect` | Есть прямой доступ. |
| DFM components | `query form-component --name ...` | Есть прямой доступ. В `symbols` компоненты явно не добавляются. |
| Captions форм | `query form --name ...`, `query form --name ... --like` | Поиск идёт по имени/классу/caption. |
| Captions компонентов | `query form-component --name ...`, `--like` | Поиск по имени/типу/caption/form. |
| DFM queries | `query sql-fragment --text ...`, `query relations` | Если сохранены как query fragments. |
| SQL tables из DFM queries | `query table --name ...`, `query relations` | Через извлечённые `SQLTable`. |

## TPR / `.tpr`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| Report form | `query report-form --name ...`, `query symbol --type report_form --name ...`, `query inspect` | Есть прямой доступ. |
| Report fields | `query report-field --name ...` | Есть прямой доступ. |
| Report params | `query report-param --name ...`, `query symbol --type report_param --name ...` | Есть прямой доступ. |
| Include-директивы TPR | **[нет прямого query]** | Индексируются, но поиска include нет. |
| SQL/query fragments | `query sql-fragment --text ...`, `query relations` | Есть прямой текстовый поиск. |
| Tables из SQL fragments | `query table --name ...`, `query relations` | Через SQL table usage. |

## RPT / `.rpt`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| Report form | `query report-form --name ...`, `query symbol --type report_form --name ...` | Есть прямой доступ. |
| Report params | `query report-param --name ...`, `query symbol --type report_param --name ...` | Есть прямой доступ. |
| VBScript functions | `query vb-function --name ...`, `query symbol --type vb_function --name ...`, `query relations` | Есть прямой доступ. |
| SQL/query fragments | `query sql-fragment --text ...`, `query relations` | Есть прямой текстовый поиск. |
| Tables из SQL fragments | `query table --name ...`, `query relations` | Через SQL table usage. |

## DSArchitect XML / `.xml`

| Сущность | Доступ через `codebase query` | Комментарий |
|---|---|---|
| API business objects | **[нет прямого query]** | Индексируются, но команды `api-business-object` нет. |
| API contracts | `query api-contract --name ...`, `query api-impl`, `query api-publishers`, `query api-consumers`, `query relations` | Есть прямой доступ. |
| Event contracts | `query api-contract --name ...`, `query api-publishers --event ...`, `query relations` | Как разновидность API contract. |
| Callback events | `query api-contract --name ...`, `query relations --relation-type subscribes_to_event` | Прямо как contract; спец-команды callback нет. |
| Used services / used contracts | `query api-consumers --name ...`, `query relations` | Как связи. |
| API contract scalar params | `query api-param --name ...` | Есть прямой доступ. |
| API contract tables | `query api-table --name ...` | Есть прямой доступ. |
| API contract table fields | `query api-table --name ...` | Обычно доступны в составе результата таблицы/контракта; отдельной команды field нет. |
| API business object params | `query api-param --name ...` | Команда ищет API params, включая standalone BObject params, если метод реализован так. |
| API business object tables | `query api-table --name ...` | Доступны как API tables. |
| API business object table fields | `query api-table --name ...` | В составе результата. Отдельной команды нет. |
| API business object table indexes | `query api-table-index --name ...` | Есть прямой доступ. |
| API business object table index fields | `query api-table-index --name ...` | В составе результата. Отдельной команды нет. |
| API contract return values | **[нет прямого query]** | Индексируются, но команды поиска return values нет. |
| API contract contexts | **[нет прямого query]** | Индексируются, но команды поиска contexts нет. |

## Relations / граф

| Сущность/связь | Доступ через `codebase query` | Комментарий |
|---|---|---|
| Все relation-записи | `query relations ...` | Общая команда. |
| Кто вызывает SQL-процедуру | `query callers --procedure ...` | Фильтр по target procedure. |
| Методы, работающие с таблицей | `query methods --table ...` | Специализированный read-model. |
| API implementations | `query api-impl --name ...` | Для `implements_contract`. |
| API publishers | `query api-publishers --event ...` | Для событий. |
| API consumers | `query api-consumers --name ...` | Для `executes_contract`/consumer-связей. |
| Graph context сущности | `query inspect --name ...` | Только если сущность есть в `symbols`. |

## Сущности без прямого `codebase query`

- **SQL columns usage**
  - Есть `sql_columns`.
  - Нет `query column`.

- **Include directives**
  - Есть `include_directives`.
  - Нет `query include`.

- **PAS fields**
  - Есть `pas_fields`.
  - Нет `query pas-field`.

- **JS script objects**
  - Парсер извлекает, но прямой query отсутствует.

- **Raw `JSProcedureCall` / `JSQueryCall`**
  - Не ищутся как отдельные сущности.
  - Доступны только через `relations` / `sql-fragment` / `callers`.

- **Raw `SQLProcedureCall`**
  - Не ищется как отдельная сущность.
  - Доступен через `relations` / `callers`.

- **API macro invocations как сырые записи**
  - Есть `api_macro_invocations`.
  - Нет `query api-macro`.

- **API business objects**
  - Есть `api_business_objects`.
  - Нет `query api-business-object`.

- **API contract return values**
  - Есть таблица.
  - Нет query-команды.

- **API contract contexts**
  - Есть таблица.
  - Нет query-команды.

- **Contract/API table fields как отдельная сущность**
  - Индексируются.
  - Обычно доступны только внутри `api-table`/`api-contract`, но не через отдельный поиск поля.

- **Index fields**
  - Индексируются.
  - Доступны через `table-index` / `api-table-index`, но не отдельным поиском по field.

## Короткий вывод

- **Хорошо покрыты query-командами:** процедуры, таблицы, table schema, table indexes, DFM forms/components, SQL fragments, JS functions, SMF instruments, reports, report params/fields, VB functions, API contracts/tables/params/indexes, relations.
- **Индексируются, но плохо достижимы напрямую:** колонки, includes, PAS units/methods/fields, JS constants/script objects, raw calls, raw API macros, API business objects, return values, contexts.
- **Частично достижимы только через граф:** вызовы процедур, API consumer/publisher/implementation связи, generated subscriber calls.

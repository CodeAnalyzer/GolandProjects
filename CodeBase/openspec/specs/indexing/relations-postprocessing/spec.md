# Relations Graph and Postprocessing

## Purpose

Построение графа связей между сущностями (procedures, tables, methods, forms, API contracts) и постобработка: связывание PAS↔DFM, извлечение вызовов процедур, разрешение retcode-констант, построение callback subscriber relations.

## Requirements

### Requirement: Граф связей между сущностями

Система SHALL строить направленный граф связей (relations) между всеми типами сущностей: SQL procedure → SQL procedure, SQL procedure → SQL table, query fragment → SQL table/procedure, report form → report field/param, VB function → query fragment, SQL procedure → API contract.

#### Scenario: Procedure вызывает table

- **GIVEN** процедура `ProcA`, содержащая `SELECT * FROM tContract`
- **WHEN** выполняется индексация и построение графа
- **THEN** в графе создана relation `selects_from` от `ProcA` к `tContract`

#### Scenario: Procedure реализует API contract

- **GIVEN** процедура `API_MyProc`, реализующая API-контракт `API_MyProc` (через `implements_contract`)
- **WHEN** выполняется индексация
- **THEN** в графе создана relation `implements_contract` от процедуры к контракту

#### Scenario: Procedure публикует event

- **GIVEN** процедура, вызывающая `API_INIT_EVENT` для события `OnAfterInsert`
- **WHEN** выполняется индексация
- **THEN** в графе создана relation `publishes_event` от процедуры к event-контракту

### Requirement: Типы relations

Система SHALL поддерживать следующие типы relations: `calls_procedure`, `selects_from`, `inserts_into`, `updates`, `deletes_from`, `references_table`, `executes_query`, `builds_query`, `implements_contract`, `executes_contract`, `publishes_event`, `subscribes_to_event`, `dispatches_to`, `dispatches_to_subscriber`, `has_field`, `has_param`, `uses_param`.

#### Scenario: Все типы relations

- **GIVEN** проиндексированный проект с различными типами связей
- **WHEN** выполняется `query relations --source-type procedure --source-name MyProc`
- **THEN** возвращены все relations исходящие от `MyProc` с указанием типа, цели и файла

### Requirement: Постобработка PAS↔DFM связей

Система SHALL в постобработке связывать PAS-классы с DFM-формами и PAS-поля с DFM-компонентами по совпадению имён, после завершения индексации всех файлов.

#### Scenario: Связывание после индексации

- **GIVEN** проиндексированные PAS и DFM файлы
- **WHEN** выполняется постобработка
- **THEN** PAS-классы связаны с DFM-формами через `dfm_form_id`
- **AND** PAS-поля связаны с DFM-компонентами через `dfm_component_id`

### Requirement: Постобработка вызовов процедур

Система SHALL в постобработке извлекать и сохранять вызовы процедур из query fragments и JS-кода, создавая relations `calls_procedure`. Имена целевых процедур собираются из pending-сущностей и резолвятся в ID одним batch-запросом (`FindLatestSQLProcedureIDsByNames`), что устраняет N+1 запросов. Построение relations параллелится чанками по `parallel` воркерам (`buildSQLProcedureCallRelationsParallel`) с дедупликацией результата по ключу `relationDedupKey` (source_type|source_id|target_type|target_id|relation_type|line_number).

#### Scenario: Вызов из query fragment

- **GIVEN** query fragment, содержащий `exec MyProc @Param = 1`
- **WHEN** выполняется постобработка
- **THEN** создана relation `calls_procedure` от родительской сущности к `MyProc`

#### Scenario: Batch-резолв имён процедур

- **GIVEN** pending-список из 200 уникальных имён вызываемых процедур
- **WHEN** выполняется `postProcessSQLProcedureCallRelations`
- **THEN** все 200 имён резолвятся в ID одним batch-запросом `FindLatestSQLProcedureIDsByNames`
- **AND** отсутствуют построчные `SELECT ... WHERE name = ?` запросы (нет N+1)

### Requirement: Разрешение retcode-констант

Система SHALL в постобработке заменять константы `LOC_RETCODE_*` в описаниях кодов возврата на их фактические значения из `ds_return_codes`.

#### Scenario: Замена константы

- **GIVEN** процедура с `RETURN LOC_RETCODE_SUCCESS`
- **WHEN** выполняется постобработка `ResolveRetCodeConstants`
- **THEN** константа `LOC_RETCODE_SUCCESS` заменена на числовое значение из `ds_return_codes`

### Requirement: Постобработка callback subscribers

Система SHALL в постобработке (глобально, после завершения Init/Update) перестраивать все `subscribes_to_event` relations, чтобы гарантировать полноту callback-подписок.

#### Scenario: Восстановление callback подписок

- **GIVEN** проиндексированный проект с API event-контрактами и callback-процедурами
- **WHEN** выполняется глобальная постобработка
- **THEN** все `subscribes_to_event` relations перестроены и полны

### Requirement: Report structure relations (`has_field`, `has_param`)

Система SHALL после индексации RPT-файла строить relations между report form и его fields/params: `has_field` (report_form → report_field) и `has_param` (report_form → report_param). Резолв ID полей/параметров выполняется батчами через `FindReportFieldIDsByForm` / `FindReportParamIDsByForm` (по составному ключу `name|line_number`), что устраняет N+1 запросов. Дубликаты отсеиваются по составному ключу `source|target|type|line`.

#### Scenario: Form со ссылкой на field

- **GIVEN** проиндексированный report form `MyForm` с полями `AccountID` (line 10) и `Amount` (line 20)
- **WHEN** выполняется `buildReportStructureRelations`
- **THEN** созданы relations `has_field` от `MyForm` к `AccountID` и `Amount`
- **AND** ID полей резолвятся одним batch-запросом `FindReportFieldIDsByForm`

#### Scenario: Form с параметрами

- **GIVEN** report form `MyForm` с параметрами `DateFrom` и `DateTo`
- **WHEN** выполняется `buildReportStructureRelations`
- **THEN** созданы relations `has_param` от `MyForm` к `DateFrom` и `DateTo`

### Requirement: Report param usage relations (`uses_param`)

Система SHALL для каждого query fragment внутри RPT-файла строить relations `uses_param` от `query_fragment` к `report_param`, если текст fragment-а ссылается на параметр (паттерны `%Param`, `:Param`, `@Param`, `[Param]`, голое имя). ID fragments и params резолвятся батчами (`FindQueryFragmentIDsByFileAndHash`, `FindReportParamIDsByForm`); дубликаты отсеиваются.

#### Scenario: Fragment ссылается на параметр

- **GIVEN** query fragment с текстом `WHERE date >= :DateFrom` внутри report form `MyForm`
- **WHEN** выполняется `buildReportParamUsageRelations`
- **THEN** создана relation `uses_param` от fragment-а к параметру `DateFrom`

#### Scenario: Fragment без ссылок на параметры

- **GIVEN** query fragment `SELECT 1` без ссылок на параметры формы
- **WHEN** выполняется `buildReportParamUsageRelations`
- **THEN** relations `uses_param` не создаются

### Requirement: VB function → query fragment relations (`executes_query`)

Система SHALL для каждой VB function в RPT-файле строить relations `executes_query` от `vb_function` к `query_fragment`, если тело функции содержит имя компонента fragment-а (case-insensitive). ID functions и fragments резолвятся батчами (`FindVBFunctionIDsByForm`, `FindQueryFragmentIDsByFileAndHash`); дубликаты отсеиваются.

#### Scenario: VB function вызывает fragment

- **GIVEN** VB function `cmdOK_Click`, в теле которой упоминается компонент `grData`, и query fragment с `ComponentName = "grData"`
- **WHEN** выполняется `buildVBFunctionQueryRelations`
- **THEN** создана relation `executes_query` от `cmdOK_Click` к fragment-у

#### Scenario: VB function без fragment-ов

- **GIVEN** VB function `Form_Load`, не ссылающаяся ни на один компонент fragment-а
- **WHEN** выполняется `buildVBFunctionQueryRelations`
- **THEN** relations `executes_query` не создаются

### Requirement: Вложенный параллелизм фрагментных relations

Система SHALL запускать четыре глобальных резолва fragment-ных relations параллельно внутри одного из пяти верхних постпроцессоров: (1) fragment refs (PAS/JS → query fragment), (2) JS call refs, (3) T01 subscriber refs, (4) API macro refs — через `postProcessAllFragmentRelations` с `sync.WaitGroup` из 4 горутин. Каждый из них использует собственный batch-резолв имён контрактов/процедур (`FindLatestAPIContractIDsByNamesAndKinds`, `FindLatestSQLProcedureIDsByNames`, `FindLatestEventContractIDsByNames`), что устраняет N+1 запросов.

#### Scenario: Параллельный резолв 4 видов fragment relations

- **GIVEN** проиндексированный проект с fragment refs, JS calls, T01 subscribers и API macro refs
- **WHEN** выполняется `postProcessAllFragmentRelations`
- **THEN** все 4 резолва выполняются параллельно (4 горутины)
- **AND** каждый использует собственный batch-резолв имён в ID
- **AND** дубликаты отношений отсеиваются

### Requirement: Извлечение depends_on_capability из паттерна "поддомену/поддоменам"

Система SHALL извлекать relations `depends_on_capability` из текста spec-файла при обнаружении паттерна "поддомену" или "поддоменам" с последующим списком slug'ов в backtick-обёртке. Двоеточие после слова опционально. Каждому резолвнутому slug'у создаётся relation с `confidence: "notes"`.

#### Scenario: Поддомены без двоеточия

- **GIVEN** spec-файл capability `consumer-cession` с текстом в Notes: "Подробные спецификации по каждому поддомену: `portfolio-management`, `nominal-calculation`, `purchase`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `consumer-cession` к `consumer-cession/portfolio-management`, `consumer-cession/nominal-calculation`, `consumer-cession/purchase` с `confidence: "notes"`

#### Scenario: Поддомены с двоеточием

- **GIVEN** spec-файл capability `my-domain` с текстом: "Поддоменам: `child-a`, `child-b`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `my-domain` к `my-domain/child-a` и `my-domain/child-b`

### Requirement: Опциональное двоеточие в маркере "Связан с доменами"

Система SHALL распознавать маркер "Связан с доменами" как с двоеточием, так и без него. Slug'и извлекаются из backtick-обёрток и резолвятся через `resolveSlug` с sibling-префиксом.

#### Scenario: Связан с доменами без двоеточия

- **GIVEN** spec-файл capability `consumer-cession` с текстом в Notes: "Связан с доменами `consumer-credit`, `api-credit`, `client-ui-consumer`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `consumer-cession` к `consumer-credit`, `api-credit`, `client-ui-consumer` с `confidence: "notes"`

#### Scenario: Связан с доменами с двоеточием

- **GIVEN** spec-файл capability `my-domain` с текстом: "Связан с доменами: `dep-a`, `dep-b`"
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** созданы relations `depends_on_capability` от `my-domain` к `dep-a` и `dep-b`

### Requirement: Раскрытие иерархии capabilities в depends_on_capability

Система SHALL после извлечения прямых зависимостей раскрывать неявные связи по иерархии `parent_id` в обоих направлениях. При упоминании родительского домена создаются неявные зависимости на всех его детей. При упоминании дочернего домена создаётся неявная зависимость на его родителя. Неявные связи получают `confidence: "hierarchy"` и дедуплицируются с прямыми. Если source и target имеют общего родителя (siblings), child→parent расширение не создаётся, чтобы избежать циклов внутри одного домена.

#### Scenario: Упоминание родителя → зависимости на детей

- **GIVEN** capability `accrual-core` (parent) имеет детей `accrual-core/base-algorithms` и `accrual-core/accrual-engine`
- **AND** spec-файл capability `my-feature` содержит markdown-ссылку на `accrual-core`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана прямая relation `depends_on_capability` от `my-feature` к `accrual-core` с `confidence: "explicit"`
- **AND** созданы неявные relations `depends_on_capability` от `my-feature` к `accrual-core/base-algorithms` и `accrual-core/accrual-engine` с `confidence: "hierarchy"`

#### Scenario: Упоминание ребёнка → зависимость на родителя

- **GIVEN** capability `accrual-core` (parent) имеет ребёнка `accrual-core/base-algorithms`
- **AND** spec-файл capability `my-feature` содержит markdown-ссылку на `accrual-core/base-algorithms`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** создана прямая relation `depends_on_capability` от `my-feature` к `accrual-core/base-algorithms` с `confidence: "explicit"`
- **AND** создана неявная relation `depends_on_capability` от `my-feature` к `accrual-core` с `confidence: "hierarchy"`

#### Scenario: Дедупликация прямых и иерархических связей

- **GIVEN** capability `accrual-core` имеет ребёнка `accrual-core/base-algorithms`
- **AND** spec-файл capability `my-feature` явно упоминает оба slug'а: `accrual-core` и `accrual-core/base-algorithms`
- **WHEN** выполняется постобработка `postProcessSpecDependencies`
- **THEN** для каждого target_id существует ровно одна relation (прямая имеет приоритет над иерархической)

## Related code

- `internal/indexer/indexer_relations.go` — `buildReportStructureRelations`, `buildReportParamUsageRelations`, `buildVBFunctionQueryRelations`, `buildQueryFragmentRelations`, `extractReportParamRefs`, query-fragment helpers
- `internal/indexer/indexer_postprocess_pas.go` — постобработка PAS↔DFM (`FindLatestPASClassIDsByNames`, `FindLatestDFMFormIDsByClassNames`, `FindPASFieldDFMLinkCandidates`)
- `internal/indexer/indexer_postprocess_sql_calls.go` — `postProcessSQLProcedureCallRelations`, `buildSQLProcedureCallRelationsParallel`, `relationDedupKey`
- `internal/indexer/indexer_postprocess_retcode.go` — `ResolveRetCodeConstants`
- `internal/indexer/indexer_postprocess_callbacks.go` — перестроение callback subscribers
- `internal/indexer/indexer_postprocess_fragments.go` — `postProcessAllFragmentRelations` (4 параллельных резолва), `postProcessFragmentRelations`, `postProcessJSCallRelations`, `postProcessT01SubscriberRelations`, `postProcessAPIMacroRelations`, `buildAPIMacroRefRelations`
- `internal/indexer/runner.go` — `runPostProcessingParallel` (5 верхних постпроцессоров, `DeleteSubscribesToEventRelations` перед параллельным запуском)
- `internal/store/db_lookup_sql.go` — `FindLatestSQLProcedureIDsByNames`, `FindSQLProcedureIDsByFile` (batch-резолв)
- `internal/store/db_lookup_reports.go` — `FindReportFieldIDsByForm`, `FindReportParamIDsByForm` (batch-резолв с составным ключом name|line)
- `internal/store/db_lookup_j.go` — `FindJSFunctionIDRangesByFile`, `FindJSFunctionIDsByFile`, `FindLatestSMFInstrumentIDByFile`
- `internal/store/db_lookup_pas.go` — `FindLatestPASClassIDsByNames`, `FindPASFieldDFMLinkCandidates` (batch-резолв)
- `internal/store/api_store.go` — `FindLatestAPIContractIDsByNamesAndKinds`, `FindLatestEventContractIDsByNames` (batch-резолв)
- `internal/indexer/indexer_postprocess_spec_deps.go` — `postProcessSpecDependencies`, `extractCapabilityDeps`, `expandHierarchyDeps` (depends_on_capability parsing, hierarchy expansion)
- `internal/store/db_lookup_spec_deps.go` — `LoadAllSpecCapabilitiesForDeps` (batch-загрузка capabilities с parent_id)
- `internal/store/db_schema.go` — таблица `relations`

## Notes

- Callback subscriber relations перестраиваются глобально в постобработке, а не per-file, для предотвращения потерь при параллельной индексации
- `subscribes_to_event` relations удаляются (`DeleteSubscribesToEventRelations`) ДО запуска 5 параллельных постпроцессоров, чтобы не конкурировать с параллельным `COPY INTO relations`
- Retcode-константы извлекаются из H-файлов и сохраняются в `ds_return_codes` при индексации
- Граф связей используется для impact analysis и query inspect
- Все постпроцессоры используют batch-резолв имён/ключей в ID (а не построчные запросы) — устраняет N+1 при больших проектах (доработка по оптимизации индексации)
- Архитектура постобработки двухуровневая: 5 верхних постпроцессоров параллельны; один из них (`postProcessAllFragmentRelations`) внутри запускает ещё 4 параллельных резолва. Итого до 8 одновременных горутин постобработки
- `depends_on_capability` relations строятся в постобработке `postProcessSpecDependencies` из текстов spec-файлов (Purpose, Notes, Related code) по паттернам: markdown-ссылки, "Связан с доменами", "поддомену/поддоменам", inline "(см. `slug`)", cci-суффикс, `ExtractSpecReferences`
- Иерархическое расширение `expandHierarchyDeps` добавляет неявные связи parent→children и child→parent с `confidence: "hierarchy"`; siblings внутри одного домена не создают child→parent циклов

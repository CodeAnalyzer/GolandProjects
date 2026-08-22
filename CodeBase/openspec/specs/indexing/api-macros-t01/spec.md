# API Macros and Preprocessed .t01

## Purpose

Извлечение API-макросов (API_CREATE_PROC, API_INIT_EVENT, API_EXEC) из исходных SQL-файлов и индексация препроцессированных .t01 файлов как SQL-like layer с извлечением generated subscriber calls.

## Requirements

### Requirement: Извлечение API-макросов из SQL

Система SHALL извлекать API-макросы (`API_CREATE_PROC`, `API_INIT_EVENT`, `API_EXEC`) из исходных SQL-файлов и сохранять их в `api_macro_invocations` с указанием файла, строки и раскрытого текста.

#### Scenario: API_CREATE_PROC макрос

- **GIVEN** SQL-файл с `API_CREATE_PROC(MyProc, @Param1 int, @Param2 varchar(50))`
- **WHEN** выполняется индексация файла
- **THEN** макрос сохранён в `api_macro_invocations` с типом `API_CREATE_PROC`
- **AND** параметры процедуры извлечены и сохранены

#### Scenario: API_INIT_EVENT макрос

- **GIVEN** SQL-файл с `API_INIT_EVENT('OnAfterInsert', 'MyProc')`
- **WHEN** выполняется индексация файла
- **THEN** макрос сохранён в `api_macro_invocations` с типом `API_INIT_EVENT`

### Requirement: Индексация .t01 как SQL-like layer

Система SHALL индексировать препроцессированные `.t01` файлы как SQL-like layer: извлекать процедуры, вызовы процедур, query fragments и table usage, используя тот же SQL-парсер.

#### Scenario: Процедура в .t01

- **GIVEN** `.t01` файл с `CREATE PROCEDURE MyProc AS BEGIN ... END`
- **WHEN** выполняется индексация файла
- **THEN** процедура `MyProc` сохранена в `sql_procedures`
- **AND** query fragments извлечены и сохранены

### Requirement: Generated subscriber calls из .t01

Система SHALL извлекать generated subscriber calls из препроцессированных `.t01` по паттернам `exec GetAPIProcessID ...` и `exec @RetVal = <proc> ... @ProcessID = @GlobalProcessID`, сохраняя их в графе как relation `dispatches_to_subscriber`. Для T01 также строится relation `dispatches_to` (от процедуры-источника к процедуре-диспетчеру, `apimacro/parser.go`) — отдельный тип от `dispatches_to_subscriber`.

#### Scenario: Dispatch-вызов

- **GIVEN** `.t01` файл с раскрытым `API_INIT_EVENT`, содержащим `exec @RetVal = SubscriberProc ... @ProcessID = @GlobalProcessID`
- **WHEN** выполняется индексация и постобработка
- **THEN** в графе связей создана relation `dispatches_to_subscriber` от процедуры-источника к `SubscriberProc`

#### Scenario: T01 dispatches_to

- **GIVEN** `.t01` файл с паттерном dispatch от `SourceProc` к `DispatcherProc`
- **WHEN** выполняется индексация и `buildT01GeneratedSubscriberRelations`
- **THEN** в графе создана relation `dispatches_to` от `SourceProc` к `DispatcherProc`

### Requirement: Relations из API-макросов (implements_contract, publishes_event, executes_contract)

Система SHALL при индексации API-макросов (`indexAPIMacros`, `indexer_sql_pas.go`) строить следующие relations: `implements_contract` (от `API_CREATE_PROC` к service/callback_event-контракту), `publishes_event` (от `API_INIT_EVENT` к event-контракту), `executes_contract` (от `API_EXEC` к used_service/service-контракту). Глобальный резолв ссылок выполняется в постобработке `PostProcessAPIMacroRelations` (см. `relations-postprocessing`) батчами через `FindLatestAPIContractIDsByNamesAndKinds` и `FindLatestSQLProcedureIDsByNames`.

#### Scenario: API_CREATE_PROC → implements_contract

- **GIVEN** SQL-файл с `API_CREATE_PROC(MyProc, ...)` и существующим service-контрактом `MyProc`
- **WHEN** выполняется индексация и `postProcessAPIMacroRelations`
- **THEN** в графе создана relation `implements_contract` от `MyProc` (sql_procedure) к контракту `MyProc` (api_contract, kind=service)

#### Scenario: API_INIT_EVENT → publishes_event

- **GIVEN** SQL-файл с `API_INIT_EVENT('OnAfterInsert', 'MyProc')`
- **WHEN** выполняется индексация и постобработка
- **THEN** в графе создана relation `publishes_event` от `MyProc` к event-контракту `OnAfterInsert`

#### Scenario: API_EXEC → executes_contract

- **GIVEN** SQL-файл с `API_EXEC('SomeService')` внутри `CallerProc`
- **WHEN** выполняется индексация и постобработка
- **THEN** в графе создана relation `executes_contract` от `CallerProc` к service-контракту `SomeService`

### Requirement: Пропуск API macro extraction для .t01

Система SHALL NOT выполнять API macro extraction для `.t01` файлов, так как макросы уже раскрыты препроцессором.

#### Scenario: .t01 без macro extraction

- **GIVEN** `.t01` файл с раскрытыми макросами
- **WHEN** выполняется индексация файла
- **THEN** процедуры и query fragments извлечены
- **AND** `api_macro_invocations` не пополняется

### Requirement: Опциональность .t01

Система SHALL рассматривать `.t01` как опциональный артефакт: отсутствие `.t01` файлов не мешает базовой индексации проекта по исходным `.sql`.

#### Scenario: Проект без .t01

- **GIVEN** проект без `.t01` файлов
- **WHEN** выполняется `codebase init`
- **THEN** базовая индексация по `.sql` выполняется без ошибок
- **AND** `dispatches_to_subscriber` relations не создаются

## Related code

- `internal/parser/apimacro/parser.go` — парсер API-макросов
- `internal/indexer/indexer_sql_pas.go` — `parseT01File`, `parseSQLLikeFile`, SQL-like pipeline для .t01
- `internal/indexer/indexer_relations.go` — извлечение dispatches_to_subscriber
- `internal/store/api_store.go` — `BatchInsertAPIMacroInvocations`

## Notes

- `.t01` не входит в дефолтные `include_patterns` — требует явного добавления
- `.sql` остаётся первичным дистрибутивным источником, `.t01` — опциональный временный артефакт
- Generated subscriber calls создают relation `dispatches_to_subscriber` (не `calls_procedure`)

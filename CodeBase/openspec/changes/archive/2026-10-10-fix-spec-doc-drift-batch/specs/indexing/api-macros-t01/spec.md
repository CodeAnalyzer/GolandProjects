## MODIFIED Requirements

### Requirement: Relations из API-макросов (implements_contract, publishes_event, executes_contract)

Система SHALL при индексации API-макросов (`indexAPIMacros` — определена в `internal/indexer/indexer.go`, вызывается из пайплайна `indexer_sql_pas.go`) строить следующие relations: `implements_contract` (от `API_CREATE_PROC` к service/callback_event-контракту), `publishes_event` (от `API_INIT_EVENT` к event-контракту), `executes_contract` (от `API_EXEC` к used_service/service-контракту). Глобальный резолв ссылок выполняется в постобработке `PostProcessAPIMacroRelations` (см. `relations-postprocessing`) батчами через `FindLatestAPIContractIDsByNamesAndKinds` и `FindLatestSQLProcedureIDsByNames`.

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

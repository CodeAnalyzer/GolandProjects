# API and Relations Queries

## Purpose

Запросы к API-контрактам DSArchitect (api-contract, api-table, api-param, api-table-index, api-impl, api-publishers, api-consumers), запросы к графу связей (relations) и inspect сущностей с graph context.

## Requirements

### Requirement: Поиск API-контрактов

Система SHALL предоставлять команду `query api-contract` для поиска контрактов DSArchitect API по имени (service, event, callback_event, used_service) с указанием kind, файла и параметров.

#### Scenario: Поиск service-контракта

- **GIVEN** проиндексированный проект с service-контрактом `API_CCred_BindClassifier`
- **WHEN** выполняется `query api-contract --name API_CCred_BindClassifier`
- **THEN** возвращен контракт с kind `service`, файлом и параметрами

#### Scenario: Поиск по подстроке

- **GIVEN** проиндексированный проект с контрактами `API_CCred_BindClassifier`, `API_CCred_FindList`
- **WHEN** выполняется `query api-contract --name API_CCred --like`
- **THEN** возвращены оба контракта

### Requirement: Поиск API-таблиц, параметров и индексов

Система SHALL предоставлять команды `query api-table`, `query api-param`, `query api-table-index` для поиска standalone таблиц, параметров BObject и индексов standalone API-таблиц.

#### Scenario: Поиск API-таблицы

- **GIVEN** проиндексированный проект с API-таблицей `pAPI_CredS_InsertCCred`
- **WHEN** выполняется `query api-table --name pAPI_CredS_InsertCCred`
- **THEN** возвращена таблица с полями

#### Scenario: Поиск индекса API-таблицы

- **GIVEN** проиндексированный проект с индексом `XIE0pAPI_CCred_Agreement`
- **WHEN** выполняется `query api-table-index --name XIE0pAPI_CCred_Agreement`
- **THEN** возвращен индекс с полями и указанием таблицы

### Requirement: Поиск реализаций API-контрактов

Система SHALL предоставлять команду `query api-impl` для поиска SQL-процедур, реализующих указанный API-контракт (relation `implements_contract`).

#### Scenario: Поиск реализации

- **GIVEN** проиндексированный проект, где процедура `CON_RuleDoc_MassGetLimitAcc` реализует контракт `API_CCred_BindClassifier`
- **WHEN** выполняется `query api-impl --name API_CCred_BindClassifier`
- **THEN** возвращена процедура `CON_RuleDoc_MassGetLimitAcc` с указанием файла и строк

### Requirement: Поиск публикаторов событий

Система SHALL предоставлять команду `query api-publishers` для поиска SQL-процедур, публикующих указанное event-событие (relation `publishes_event`), и callback-контрактов, подписанных на него.

#### Scenario: Поиск публикаторов события

- **GIVEN** проиндексированный проект с событием `OnAfterLoan_MassInsert`
- **WHEN** выполняется `query api-publishers --event OnAfterLoan_MassInsert`
- **THEN** возвращены процедуры, публикующие событие, и callback-контракты, подписанные на него

### Requirement: Поиск потребителей API-контрактов

Система SHALL предоставлять команду `query api-consumers` для поиска SQL-процедур, вызывающих указанный API-контракт (relation `executes_contract`), включая прямые и косвенные потребителей.

#### Scenario: Поиск потребителей

- **GIVEN** проиндексированный проект, где процедура `MyProc` вызывает `API_Account_FindIDByNumber`
- **WHEN** выполняется `query api-consumers --name API_Account_FindIDByNumber`
- **THEN** возвращена процедура `MyProc` как потребитель контракта

### Requirement: Запрос к графу связей

Система SHALL предоставлять команду `query relations` для поиска связей в графе с фильтрацией по source_type, source_name, target_type, target_name и relation_type.

#### Scenario: Фильтрация по source

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --source-type procedure --source-name API_ --limit 100`
- **THEN** возвращены до 100 relations, исходящих от процедур с именем, начинающимся на `API_`

#### Scenario: Фильтрация по target и relation_type

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query relations --target-type table --target-name tContract --relation-type selects_from`
- **THEN** возвращены все relations `selects_from` к таблице `tContract`

### Requirement: Inspect сущности с graph context

Система SHALL предоставлять команду `query inspect` для глубокого анализа сущности: поиск символа, всех входящих и исходящих связей в графе, и соседних символов.

#### Scenario: Inspect процедуры

- **GIVEN** проиндексированный проект с процедурой `Cons_Check_Restr_API`
- **WHEN** выполняется `query inspect --name Cons_Check_Restr_API`
- **THEN** возвращена сущность с метаданными, всеми входящими и исходящими relations и соседними символами

### Requirement: Поведение при ошибках и пустых результатах API queries

Система SHALL возвращать пустой массив при отсутствии API-контрактов и ограничивать результаты через `--limit`.

#### Scenario: API-контракт не найден

- **GIVEN** проиндексированный проект без контракта `API_NonExistent`
- **WHEN** выполняется `query api-contract --name API_NonExistent --json`
- **THEN** возвращён JSON с `"count": 0` и `"items": []`

#### Scenario: Relations не найдены

- **GIVEN** проиндексированный проект и процедура `MyProc` без связей
- **WHEN** выполняется `query relations --source-name MyProc`
- **THEN** возвращён пустой результат

#### Scenario: Inspect несуществующей сущности

- **GIVEN** проиндексированный проект без сущности `NonExistent`
- **WHEN** выполняется `query inspect --name NonExistent`
- **THEN** возвращён пустой результат (сущность не найдена)

## Related code

- `internal/query/api_query.go` — `SearchAPIContract`, `SearchAPITable`, `SearchAPIParam`, `SearchAPITableIndex`, `SearchAPIImplementations`, `SearchAPIPublishers`, `SearchAPIConsumers`
- `internal/query/query_relations.go` — `SearchRelations`, `SearchRelationsByEntity`
- `internal/querysvc/runtime.go` — execution-слой для всех API/relations queries (CLI + MCP)
- `internal/querysvc/inspect.go` — `RunInspectQuery`, inspect logic с graph context
- `cmd/query_api.go` — CLI commands для API queries
- `cmd/query_commands.go` — CLI command `query relations`, `query inspect`

## Notes

- API queries поддерживают `--like` для поиска по подстроке
- `query api-consumers` находит как прямых, так и косвенных потребителей
- Inspect объединяет данные из symbols и relations для полного контекста сущности
- Inspect ограничивает количество символов до 5 (или `--limit`, если меньше) и приоритизирует точные совпадения имён
- Execution-слой `internal/querysvc` (`runtime.go` для всех query-команд, `inspect.go` для `query inspect`) — общая точка входа для CLI (`cmd/query_execution.go`) и MCP (`internal/mcp/registry.go`); устраняет дублирование оркестрации. Поведение команд специфицировано здесь; транспорт MCP — в `mcp-server/mcp-transport-tools`.
- Вспомогательная механика inspect (не отдельные команды): `SearchRelationsByEntity` (`query_relations.go`) — поиск relations по ID (используется в `inspect`), в отличие от документированного поиска по имени в `query relations`; `LimitInspectSymbols`, `InspectRelationType`, `PrioritizeExactSymbolMatches`, `CollectInspectNeighbors` (`querysvc/inspect.go`) — внутренние шаги inspect.
- Вспомогательные `loadAPIContractParams`/`loadAPIContractTables` и `searchAPIRelatedProcedures` (`api_query.go`) — внутреннее обогащение `SearchAPIContract` и `SearchAPIConsumers`.

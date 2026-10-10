## MODIFIED Requirements

### Requirement: Поддерживаемые типы символов

Система SHALL индексировать в `symbols` следующие типы сущностей: `procedure`, `table`, `index`, `column_definition`, `define`, `unit`, `class`, `method`, `function`, `constant`, `form`, `component`, `report_form`, `report_param`, `vb_function`, `smf_instrument`, `api_business_object`, `api_table`, `api_table_index`, `api_param`, `spec_capability`, `spec_change`, `spec_requirement`, `spec_usecase`, а также kind-типы API-контрактов: `service`, `event`, `callback_event`, `used_service` (вид контракта из DSArchitect XML записывается как тип символа, `entity_type = "xml"`), и fallback-тип `xml` для DSArchitect XML вне kind-каталогов (Service/Event/Table/Param/UsedService/CallbackEvent).

#### Scenario: Поиск компонента формы

- **GIVEN** проиндексированный проект с компонентом `dlName`
- **WHEN** выполняется `query symbol --name dlName --type component`
- **THEN** возвращена сущность типа `component` с указанием формы, файла и строки

#### Scenario: Поиск SMF-инструмента по имени

- **GIVEN** проиндексированный SMF-файл с инструментом `CreditMassOperation`
- **WHEN** выполняется `query symbol --name CreditMassOperation`
- **THEN** возвращена сущность с `symbol_type = "smf_instrument"` и `entity_type = "smf"` с указанием файла и строки

#### Scenario: Фильтрация SMF-инструментов по типу

- **GIVEN** проиндексированный проект с SMF-инструментом `CreditMassOperation` и SQL-процедурой `CreditMassOperation`
- **WHEN** выполняется `query symbol --name CreditMassOperation --type smf_instrument`
- **THEN** возвращён только SMF-инструмент, SQL-процедура отфильтрована

#### Scenario: Поиск API-контракта по kind-типу

- **GIVEN** проиндексированный проект с сервисным контрактом `API_MyService`
- **WHEN** выполняется `query symbol --name API_MyService --type service`
- **THEN** возвращена сущность типа `service` с `entity_type = "xml"` с указанием файла и строки

#### Scenario: Поиск JS-функции по фактическому типу

- **GIVEN** проиндексированный проект с JS-функцией `myFunc`
- **WHEN** выполняется `query symbol --name myFunc --type function`
- **THEN** возвращена сущность типа `function` с `entity_type = "js"` с указанием файла и строки

## ADDED Requirements

### Requirement: Нормализация алиасов и валидация типа фильтра

Система SHALL принимать в фильтре `--type` (CLI) и `type` (MCP `codebase_query_symbol`) алиасы в стиле relations-словаря, транслируя их в канонические типы `symbols`: `sql_procedure`→`procedure`, `sql_table`→`table`, `pas_method`→`method`, `js_function`→`function`, `dfm_form`→`form`, `dfm_component`→`component`; алиас `api_contract` SHALL разворачиваться во все kind-типы контрактов (`service`, `event`, `callback_event`, `used_service`). Значение фильтра SHALL сравниваться без учёта регистра и пробелов. Если значение не является ни алиасом, ни каноническим типом, система SHALL возвращать ошибку со списком валидных значений, а не молчаливый пустой результат.

#### Scenario: Фильтр по алиасу relations-стиля

- **GIVEN** проиндексированный проект с процедурой `MassAccrual_Start`
- **WHEN** выполняется `query symbol --name MassAccrual_Start --type sql_procedure`
- **THEN** процедура найдена, результат эквивалентен фильтрации `--type procedure`

#### Scenario: Алиас api_contract покрывает все kind-типы

- **GIVEN** проиндексированный проект с контрактами `API_MyService` (kind `service`) и `API_MyEvent` (kind `event`)
- **WHEN** выполняется `query symbol --name API_My --type api_contract --like`
- **THEN** возвращены оба контракта всех kind-типов

#### Scenario: Неизвестный тип — ошибка со списком

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query symbol --name MassAccrual_Start --type proc`
- **THEN** возвращена ошибка с сообщением о неизвестном типе и списком валидных значений
- **AND** пустой результат без диагностики не возвращается

#### Scenario: Пустой тип — отсутствие фильтра

- **GIVEN** проиндексированный проект
- **WHEN** выполняется `query symbol --name MassAccrual_Start` без `--type`
- **THEN** фильтр по типу не применяется, возвращены сущности всех типов с этим именем

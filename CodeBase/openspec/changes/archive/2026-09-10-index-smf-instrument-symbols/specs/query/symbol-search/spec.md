## MODIFIED Requirements

### Requirement: Поддерживаемые типы символов

Система SHALL индексировать в `symbols` следующие типы сущностей: `procedure`, `table`, `index`, `column_definition`, `define`, `unit`, `class`, `method`, `js_function`, `constant`, `form`, `component`, `report_form`, `report_param`, `vb_function`, `api_business_object`, `smf_instrument`, и XML/API symbols.

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

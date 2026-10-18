## ADDED Requirements

### Requirement: Fallback поиска report_form по полям DFM-формы

Система SHALL при явном фильтре типа `report_form` (CLI `--type report_form`, MCP `codebase_query_symbol` `type="report_form"`) искать символы отчётных форм не только по имени отчёта (`report_name`), но и по полям DFM-формы: `form_name` и `form_class`. Без `--like` сравнение SHALL выполняться без учёта регистра по любому из трёх полей; с `--like` — поиск по подстроке в любом из трёх полей. Без явного фильтра типа fallback к полям DFM-формы SHALL НЕ применяться: поиск без `--type` ведётся только по имени символа и прежним fallback-полям.

#### Scenario: Точный поиск по имени DFM-формы

- **GIVEN** проиндексированный проект с шестью отчётными формами, использующими DFM-форму `FormRepBCHPeriod`
- **WHEN** выполняется `query symbol --name FormRepBCHPeriod --type report_form`
- **THEN** возвращены все шесть символов типа `report_form` с указанием файлов и строк

#### Scenario: Поиск по классу DFM-формы

- **GIVEN** проиндексированный проект с отчётными формами, у которых `form_class = TFormRepBCHPeriod`
- **WHEN** выполняется `query symbol --name TFormRepBCHPeriod --type report_form`
- **THEN** возвращены все отчётные формы с этим классом DFM-формы

#### Scenario: Поиск по имени отчёта без изменений

- **GIVEN** проиндексированный проект с отчётной формой `BCH_UCH_Period`
- **WHEN** выполняется `query symbol --name BCH_UCH_Period --type report_form`
- **THEN** возвращён один символ `report_form`
- **AND** результат не зависит от наличия fallback-полей

#### Scenario: Поиск по подстроке в полях DFM-формы

- **GIVEN** проиндексированный проект с отчётными формами, у которых `form_name` содержит `RepBCHPeriod`
- **WHEN** выполняется `query symbol --name RepBCH --type report_form --like`
- **THEN** возвращены все отчётные формы с `form_name` или `form_class`, содержащими подстроку

#### Scenario: Без фильтра типа fallback не применяется

- **GIVEN** проиндексированный проект с отчётными формами, использующими DFM-форму `FormRepBCHPeriod`
- **WHEN** выполняется `query symbol --name FormRepBCHPeriod` без `--type`
- **THEN** отчётные формы не найдены по `form_name`
- **AND** поведение поиска без типа не изменилось

## MODIFIED Requirements

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

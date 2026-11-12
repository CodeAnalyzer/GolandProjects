## MODIFIED Requirements

### Requirement: Вызов процедур из других продуктов

Система SHALL обнаруживать вызов процедур из других продуктов Diasoft и формировать finding `foreignProcedureUsing`. Имена вызываемых процедур с префиксом `API_` (без учёта регистра) MUST исключаться из этого правила: прямой вызов чужой API-процедуры покрывается deploy-stopper правилом `apiProcDirectCall` (см. `review/deploy-stoppers`), чтобы на один вызов не формировались два finding'а разной severity.

#### Scenario: Вызов внешней процедуры

- **GIVEN** SQL-файл с `EXEC OtherProduct_ProcName @Param = 1` где `OtherProduct_ProcName` принадлежит другому продукту
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `foreignProcedureUsing`, severity 3

#### Scenario: Прямой вызов чужой API-процедуры не дублируется

- **GIVEN** SQL-файл продукта `fa-contracts` с `EXEC API_FinOper_FindLstByID @ID = 1`, где владелец по индексу — продукт `fa-administrator`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `apiProcDirectCall`, severity 1
- **AND** finding `foreignProcedureUsing` для этого вызова не формируется

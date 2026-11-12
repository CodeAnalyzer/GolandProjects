## ADDED Requirements

### Requirement: Прямой вызов API-процедуры чужого продукта

Система SHALL обнаруживать прямые `EXEC`-вызовы процедур с префиксом `API_` (проверка префикса без учёта регистра), владелец которых — другой продукт Diasoft (определяется по индексу процедур), и формировать finding `apiProcDirectCall` severity 1. Принадлежность API-процедуры MUST определяться на уровне продукта, а не каталога: прямой вызов API-процедуры собственного продукта проверяемого файла легален независимо от расположения её определения в подкаталогах. Макрос `API_EXEC(...)` не является exec-вызовом и данным правилом не рассматривается (см. требование «API_EXEC вне фасадного скрипта»). Finding MUST формироваться не более одного раза на уникальное имя вызванной процедуры в пределах файла (без учёта регистра), на строке первого вхождения. Если вызванная процедура отсутствует в индексе, finding `apiProcDirectCall` MUST NOT формироваться (отсутствие процедуры покрывается правилом `execNotExistsProc`). Одновременное формирование для того же вызова finding `execNotExistsProc` (процедура есть в индексе, но отсутствует в разрешённой области владельца) допустимо: семантики правил разные.

#### Scenario: Прямой exec чужой API-процедуры

- **GIVEN** SQL-файл продукта `fa-contracts` с `EXEC @RetVal = API_FinOper_FindLstByID @ID = 1`, где владелец `API_FinOper_FindLstByID` по индексу — продукт `fa-administrator`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `apiProcDirectCall`, severity 1

#### Scenario: Прямой exec своей API-процедуры

- **GIVEN** SQL-файл продукта `fa-contracts` с `EXEC API_Loan_GetLstTurnDebtByDate @Date = '20260101'`, где владелец `API_Loan_GetLstTurnDebtByDate` по индексу — сам продукт `fa-contracts`
- **WHEN** выполняется review
- **THEN** finding `apiProcDirectCall` не формируется

#### Scenario: Вызов через фасад

- **GIVEN** SQL-файл продукта `fa-contracts` с `EXEC FCD_Con_FO_FindLstByID @ID = 1`, а фасад `FCD_Con_FO_FindLstByID` внутри использует `API_EXEC(API_FinOper_FindLstByID, ...)`
- **WHEN** выполняется review фасада и потребителя
- **THEN** finding `apiProcDirectCall` не формируется ни для фасада, ни для потребителя

#### Scenario: Процедура отсутствует в индексе

- **GIVEN** SQL-файл с `EXEC API_Unknown_Proc @Param = 1`, где `API_Unknown_Proc` не найден в индексе
- **WHEN** выполняется review
- **THEN** finding `apiProcDirectCall` не формируется

#### Scenario: Повторные вызовы одной процедуры

- **GIVEN** SQL-файл с тремя `EXEC API_Foreign_Proc ...` одного и того же имени чужого владельца на разных строках
- **WHEN** выполняется review
- **THEN** сформирован ровно один finding с rule `apiProcDirectCall` для этого имени, на строке первого вхождения

#### Scenario: Вызов в нижнем регистре

- **GIVEN** SQL-файл с `EXEC api_FinOper_FindLstByID @ID = 1`, где владелец по индексу — другой продукт
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `apiProcDirectCall`, severity 1

### Requirement: API_EXEC вне фасадного скрипта

Система SHALL обнаруживать использование макроса `API_EXEC` в скриптах, имя файла которых (без учёта регистра) не начинается с префикса `FCD_`, `Facade_`, `API_` или `_fcd_`, и формировать finding `apiExecOutsideFacade` severity 1. Обоснование: препроцессор Diasoft раскрывает `API_EXEC` только в скриптах с этими префиксами (регистронезависимо — боевые фасады поставки используют lowercase `fcd_*.sql`); в остальных скриптах макрос остаётся нераскрытым и вызов не выполняется. Вхождения макроса в комментариях и строковых литералах не учитываются. Префикс `_fcd_` включён для seed-шаблонов генерации фасадов (`_fcd_tpl_seed.sql`). Файл целиком либо является фасадным скриптом, либо нет, поэтому finding MUST формироваться не более одного раза на файл — на строке первого вхождения макроса.

#### Scenario: API_EXEC в обычном скрипте

- **GIVEN** SQL-файл `CON_STP_SaveObject.sql` (префикс не `FCD_`/`Facade_`/`API_`) с вызовом `API_EXEC(API_FinOper_FindLstByID, @RetVal, ...)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `apiExecOutsideFacade`, severity 1

#### Scenario: API_EXEC в фасаде

- **GIVEN** SQL-файл `FCD_Con_FO_FindLstByID.sql` с вызовом `API_EXEC(API_FinOper_FindLstByID, @RetVal, ...)`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

#### Scenario: API_EXEC в lowercase-фасаде

- **GIVEN** SQL-файл `fcd_Log.sql` (lowercase) с вызовом `API_EXEC(...)`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

#### Scenario: API_EXEC в Facade-скрипте

- **GIVEN** SQL-файл `Facade_Con_LoanData.sql` с вызовом `API_EXEC(...)`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

#### Scenario: API_EXEC в seed-шаблоне фасада

- **GIVEN** SQL-файл `_fcd_tpl_seed.sql` с вызовом `API_EXEC(...)`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

#### Scenario: API_EXEC в комментарии

- **GIVEN** SQL-файл `CON_STP_SaveObject.sql` (не фасад), где `API_EXEC` встречается только внутри комментария `-- API_EXEC(...)` или блока `/* API_EXEC(...) */`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

#### Scenario: Несколько вхождений в одном файле

- **GIVEN** SQL-файл `CON_STP_SaveObject.sql` (не фасад) с тремя вызовами `API_EXEC(...)` на разных строках
- **WHEN** выполняется review
- **THEN** сформирован ровно один finding с rule `apiExecOutsideFacade`, на строке первого вхождения

#### Scenario: API_EXEC в API-скрипте

- **GIVEN** SQL-файл `API_Credit_InitEvent.sql` с вызовом `API_EXEC(...)`
- **WHEN** выполняется review
- **THEN** finding `apiExecOutsideFacade` не формируется

## MODIFIED Requirements

### Requirement: Извлечение упоминаний кода

Система SHALL извлекать упоминания код-сущностей из текстов спек (Related code с подсекциями `### Серверные процедуры` / `### API-процедуры` / `### События` / `### Клиент (Delphi)` / `### Отчёты` и т.п., inline-тексты требований, GIVEN/WHEN/THEN-строки сценариев, delta-тексты changes) в staging-таблицу `spec_code_mentions` с привязкой к сущности-источнику (capability|requirement|scenario|usecase), номером строки и эвристическим `mention_kind`: `API_*` → api_contract; `FCD_*` → фасад (sql_procedure); `t[A-Z]*` / `p[A-Z]*` → sql_table, за исключением префикса `pAPI_` — контрактные таблицы API → api_table; пути `*.smf` → smf_instrument; `*.dfm` → dfm_form; `*.pas` → pas_method; `*.sql` → sql_procedure; `*.tpr` / `*.rpt` → report (report_forms); идентификаторы `On(After|Before)[A-Z]*` и пути `.../Event/<имя>.xml` → event (api_contract события); прочие идентификаторы → sql_procedure с fallback unknown. Inline-режим (тексты требований и сценариев) SHALL дополнительно распознавать report-пути и event-идентификаторы вне бэктиков как однозначные маркеры.

#### Scenario: Подсекция Related code

- **GIVEN** capability со секцией `## Related code`, подсекцией `### API-процедуры` и буллетом `` `API_Depo/Server/DepoAccount/API_DepoAccount_MassInsert.sql` — массовое добавление ...``
- **WHEN** выполняется извлечение
- **THEN** в `spec_code_mentions` создана запись: имя `API_DepoAccount_MassInsert`, kind api_contract, источник — capability, строка буллета

#### Scenario: Inline-упоминание в сценарии

- **GIVEN** сценарий со строкой WHEN, содержащей `` процедуру `API_DpAcc_MassUpdateAccount` ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention с источником — scenario и номером строки

#### Scenario: Отчётная форма в Related code

- **GIVEN** capability со секцией `## Related code`, подсекцией `### Отчёты` и буллетом `` `ReservePortfolio/Reporting/rbr.tpr` ``
- **WHEN** выполняется извлечение
- **THEN** в `spec_code_mentions` создана запись: имя `rbr`, kind report, источник — capability

#### Scenario: Отчётный путь в сценарии вне бэктиков

- **GIVEN** сценарий со строкой THEN, содержащей «формируется отчет на основании отчетной формы form651.tpr»
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `form651`, kind report, источник — scenario

#### Scenario: Событийный идентификатор в THEN

- **GIVEN** сценарий со строкой THEN, содержащей «публикуется событие OnAfterPrtf_UpdateNorm»
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `OnAfterPrtf_UpdateNorm`, kind event, источник — scenario

#### Scenario: Путь к event-контракту в Related code

- **GIVEN** Related code с буллетом `` `API_Reserv/DSArchitectData/BObject/RsvPortfolio/Event/OnAfterPrtf_UpdateNorm.xml` — событие изменения нормы ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `OnAfterPrtf_UpdateNorm`, kind event (путь сведён к последнему сегменту без расширения, сегмент `Event` определяет kind)

#### Scenario: GIVEN-строка сканируется

- **GIVEN** сценарий со строкой GIVEN, содержащей `` расчёт по схеме `Rpt_Sheme_KRS_754P.sql` ``, и без повторения этого имени в WHEN/THEN
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `Rpt_Sheme_KRS_754P`, kind procedure, источник — scenario

#### Scenario: Контрактная таблица pAPI классифицируется как api_table

- **GIVEN** сценарий со строкой WHEN, содержащей `` передает в `pAPI_Accrual_ObjDate` дату расчета ``
- **WHEN** выполняется извлечение
- **THEN** создана запись mention: имя `pAPI_Accrual_ObjDate`, kind api_table (контрактная таблица API, не табличная эвристика `t`/`p`)

### Requirement: Пост-обработка — резолв упоминаний в references_code

Система SHALL резолвить записи `spec_code_mentions` в глобальной пост-обработке индексации (по образцу существующих callback/retcode-постпроцессоров): упоминание сопоставляется с сущностями кода по имени с учётом `mention_kind` — procedure → sql_procedures, table → sql_tables, form → dfm_forms, smf → smf_instruments, method → pas_methods, api → api_contracts, report → report_forms, event → api_contracts, api_table → api_contracts через `api_contract_tables` (таблица сопоставляется всем DISTINCT контрактам-владельцам, и service, и callback_event); разрешённые упоминания пишутся в `relations` с `relation_type = references_code`. Упоминания kind method при промахе в `pas_methods` SHALL дополнительно сопоставляться с `dfm_forms` по имени юнита (упоминание формы через `.pas`-путь); при хите создается ребро к `dfm_form`. Упоминания kind unknown SHALL проходить second-chance сопоставление с `sql_procedures`; при хите создается ребро к `sql_procedure` (покрывает процы в нижнем регистре: `r8938_prc`, `rpt_f1417u_proc`). Резолв двухфазен и НЕ зависит от порядка индексации файлов: спека может ссылаться на код, проиндексированный позже. Нерезолвнутые упоминания остаются в `spec_code_mentions` как измеримые пробелы покрытия и доступны запросам.

#### Scenario: Упоминание резолвится в процедуру

- **GIVEN** спека упоминает `API_DpAcc_MassUpdateAccount`; соответствующая SQL-процедура проиндексирована (до или после спеки)
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `sql_procedures` с указанием строки

#### Scenario: Отчет резолвится в report_form

- **GIVEN** спека упоминает `form651` с kind report; отчетная форма `form651.tpr` проиндексирована
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `report_forms`

#### Scenario: Событие резолвится в api_contract

- **GIVEN** спека упоминает `OnAfterPerson_Update` с kind event; событийный контракт проиндексирован
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` от сущности спеки к `api_contracts`

#### Scenario: Контрактная таблица резолвится в контракты-владельцы

- **GIVEN** спека упоминает `pAPI_Accrual_ObjDate` с kind api_table; таблица декларирована в контрактах `CON_Accrual_Process` и `CON_AfterAccrual_Process`
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` созданы рёбра `references_code` от сущности спеки к обоим контрактам в `api_contracts` (по одному на DISTINCT-владельца)

#### Scenario: .pas-упоминание фолбэком в форму

- **GIVEN** спека содержит буллет `` `ReservePortfolio/CLIENT/ReservePortfolio/RPPortfolio_f.pas` ``; метод с именем `RPPortfolio_f` в `pas_methods` отсутствует, форма `RPPortfolio_f` есть в `dfm_forms`
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` к `dfm_forms`

#### Scenario: Unknown-упоминание second-chance в процедуру

- **GIVEN** спека упоминает `` `r8938_prc` ``, классифицированный как unknown (нижний регистр); SQL-процедура `r8938_prc` проиндексирована
- **WHEN** выполняется пост-обработка
- **THEN** в `relations` создано ребро `references_code` к `sql_procedures`

#### Scenario: Unknown-упоминание без цели остаётся пробелом

- **GIVEN** спека упоминает `` `f123_proc` `` (unknown), процедуры с таким именем в дереве нет
- **WHEN** выполняется пост-обработка
- **THEN** ребро не создаётся, запись остаётся в `spec_code_mentions` и доступна запросу покрытия

#### Scenario: Упоминание без цели остаётся пробелом

- **GIVEN** спека упоминает `API_DpAcc_FindListGroupAccByID`, но файл процедуры отсутствует в дереве
- **WHEN** выполняется пост-обработка
- **THEN** ребро не создаётся, запись остаётся в `spec_code_mentions` и доступна запросу покрытия

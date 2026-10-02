## MODIFIED Requirements

### Requirement: Пост-обработка — резолв упоминаний в references_code

Система SHALL резолвить записи `spec_code_mentions` в глобальной пост-обработке индексации (по образцу существующих callback/retcode-постпроцессоров): упоминание сопоставляется с сущностями кода по имени с учётом `mention_kind` — procedure → sql_procedures, table → sql_tables, form → dfm_forms, smf → smf_instruments, method → pas_methods, api → api_contracts, report → report_forms, event → api_contracts, api_table → api_contracts через `api_contract_tables` (таблица сопоставляется всем DISTINCT контрактам-владельцам, и service, и callback_event); разрешённые упоминания пишутся в `relations` с `relation_type = references_code`. Упоминания kind method при промахе в `pas_methods` SHALL дополнительно сопоставляться с `dfm_forms` по имени юнита (упоминание формы через `.pas`-путь); при хите создается ребро к `dfm_form`. Упоминания kind unknown SHALL проходить second-chance сопоставление с `sql_procedures`; при хите создается ребро к `sql_procedure` (покрывает процы в нижнем регистре: `r8938_prc`, `rpt_f1417u_proc`). При нескольких одноимённых кандидатах резолв SHALL применять приоритет: не-генерируемый источник (`is_generated = false`) → источник того же продукта, что и файл, из которого извлечено упоминание (`ds_product_id` файла упоминания) → стабильный детерминированный tie-break для остаточных дублей; генерируемая копия (`*/UPLOAD/*`, `*.t01`) и одноимённая сущность чужого продукта MUST NOT перехватывать резолв у канонического источника продукта спеки. Для api_table мультикарта владельцев SHALL фильтровать контракты-копии, если среди владельцев есть хотя бы один не-копия. Резолв двухфазен и НЕ зависит от порядка индексации файлов: спека может ссылаться на код, проиндексированный позже. Нерезолвнутые упоминания остаются в `spec_code_mentions` как измеримые пробелы покрытия и доступны запросам.

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

#### Scenario: Генерируемая копия не перехватывает резолв

- **GIVEN** спека продукта fa-contracts упоминает `BaseAlg_ConsMinRest`; в индексе канонический источник `Consumer/SERVER/Accrual/BaseAlg_ConsMinRest.sql` (`is_generated = false`) и копия `LoanBureau/Server/UPLOAD/BaseAlg_ConsMinRest.sql` (`is_generated = true`), id копии больше
- **WHEN** выполняется пост-обработка упоминаний
- **THEN** ребро `references_code` указывает на процедуру канонического источника

#### Scenario: Кросс-продуктовая копия проигрывает продукту спеки

- **GIVEN** спека продукта fa-contracts упоминает `BaseAlgAmrtCostSinglePmnt`; одноимённая не-копия процедуры существует в продуктах fa-contracts и fa-other
- **WHEN** выполняется пост-обработка упоминаний
- **THEN** ребро `references_code` указывает на процедуру продукта fa-contracts

#### Scenario: Упоминания разных продуктов одного имени резолвятся раздельно

- **GIVEN** спека продукта A и спека продукта B упоминают одно имя процедуры `P`; не-копии `P` существуют в обоих продуктах
- **WHEN** выполняется пост-обработка упоминаний
- **THEN** ребро из спеки A указывает на кандидата продукта A, ребро из спеки B — на кандидата продукта B

#### Scenario: Контрактные копии не создают шумовых рёбер

- **GIVEN** спека упоминает контрактную таблицу `T`; таблица декларирована в контракте-копии (`is_generated = true`) и в каноническом контракте (`is_generated = false`)
- **WHEN** выполняется пост-обработка упоминаний
- **THEN** ребро `references_code` создано только к каноническому контракту

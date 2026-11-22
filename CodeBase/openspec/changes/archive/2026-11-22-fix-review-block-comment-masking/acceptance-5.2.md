# Приёмка 5.2: корпус fa-contracts (247 файлов с insert в /* */)

Дата: 2026-11-22. Сравниваемые прогоны: build 1712 (база до фикса) и build 1713
(после фикса) — `CodeBase.exe review --files fp_files.txt --json --min-severity 1`,
12 raw-правил (meta.rules обоих прогонов идентичны: insertRowLock, useSelectAll,
truncTbl, columnInsert, deferredUpdate, inSubQuery, apiExecOutsideFacade,
debugProfileEnabled, tooManyJoins, memoryUniqueIndexUpdate, varUseAfterCursor,
shouldBeCP866). Выходы прогонов: fp_review_out.json (752 findings) и
fp_review_out_1713.json (756 findings).

## Итоги

| Метрика | build 1712 | build 1713 |
|---|---:|---:|
| Всего findings | 752 | 756 |
| insertRowLock | 237 | 150 |
| useSelectAll | 32 | 4 |
| truncTbl | 14 | 14 |
| columnInsert | 350 | 350 |
| deferredUpdate | 110 | 231 |
| inSubQuery | 5 | 3 |
| debugProfileEnabled | 4 | 4 |
| findings внутри `/* */`-спанов (string-aware FSM) | 124 | **0** |

- Исчезнувшие: **124** — каждый внутри `/* */`-спана (FP-класс).
- Общие: **628** — идентичны построчно по `rule:line`;
  `Object` (с нормализацией комментариев) и `Message` — 0 расхождений.
- Появившиеся: **128** — только `deferredUpdate`, все верифицированы
  как устранённые FN (см. ниже).

Заявленные FN-подклассы `truncate /* c */ table`, `insert /* c */`,
`select /* c */ *`, `join` через комментарий — 0 вхождений на корпусе;
хвостые `--` FP-классы не материализовались. Дубликаты (rule|line|message)
— прежнее поведение макро-разворачивания (55 групп -> 56), не регрессия.

## Регенерация списка корпуса

Байтовый string-aware FSM (литералы + `/* */` + `--`) по 11 991 `*.sql`
fa-contracts: 246 файлов с `\binsert\b` в блоке + `LoanAccrual_All.sql`
(блок 1892..1896 содержит реальный `insert`; виден только без учёта литералов —
голая кавычка `#define QUOTEVAR '` в строке 6 держит состояние литерала).
Итого 247 = fp_files.txt, использованный в обоих прогонах.

## Появившиеся findings (128) — устранённые FN

Два механизма, оба — следствие не-маскированных комментариев в старом
`splitStatementsWithOffsets` (проверено in-package Go-харнессом: каждый
появившийся `Line` совпадает с ремапом UPDATE-оператора, который новый
сплиттер видит, а старый терял):

1. **Поглощение оператора (127)** — непарные круглые скобки в комментариях (например
   `end --not exists(select 1 from ...`) ломали depth-учёт: `update` не
   становился точкой разреза, оператор не начинался с `update`. Пример:
   API_CON_AfterACCR_MassProcess.sql — старый сплиттер видел 13 из 153 UPDATE
   (поглотивший оператор — 470 KB).
2. **Комментарий внутри оператора (1)** — ContractExtra_Insert.sql:1774,
   хвостые `--` в SET-клаузе (`set ContractID = 0, -- меняем ...`) ломали
   extractSetColumns.

| rule | файл | line | механизм |
|---|---|---:|---|
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4456 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4456 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4456 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4457 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4457 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4457 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4468 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4468 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4468 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4469 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4469 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4469 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4473 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4473 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4473 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4475 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4475 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4475 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4485 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4485 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4485 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4486 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4486 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4486 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4494 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4494 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4494 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4495 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4495 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4495 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4499 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4499 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4499 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4501 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4501 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4501 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4508 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4508 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4508 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4509 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4509 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4509 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4513 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4513 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4513 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4515 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4515 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4515 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4522 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4522 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4522 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4523 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4523 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4523 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4527 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4527 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4527 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4529 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4529 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4529 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4536 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4536 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4536 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4537 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4537 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4537 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4541 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4541 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4541 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4543 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4543 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4543 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4550 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4550 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4550 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4551 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4551 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4551 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4555 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4555 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4555 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4557 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4557 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4557 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4564 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4564 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4564 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4565 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4565 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4565 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4569 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4569 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4569 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4571 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4571 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4571 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4578 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4578 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4578 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4579 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4579 | swallow |
| deferredUpdate | fa-contracts\API_Credit\Server\Callback\API_CON_AfterACCR_MassProcess.sql | 4579 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\Consumer\ContractExtra_Insert.sql | 1774 | set-comment |
| deferredUpdate | fa-contracts\Consumer\SERVER\Consumer\Mass_ContractExtra_Insert2.sql | 3721 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\PaySchedule\PaySchedule_Select_Ench2.sql | 1049 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\PaySchedule\PaySchedule_Select_Ench2.sql | 2214 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 428 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 589 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 668 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 1116 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 1714 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ContractMassLoad.sql | 2293 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_CCred_MassBindAccount.sql | 597 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_CCred_MassBindAccount.sql | 597 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 1923 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2093 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2237 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2527 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2844 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2872 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 3269 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 3552 | swallow |
| deferredUpdate | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 4359 | swallow |
| deferredUpdate | fa-contracts\LoanBureau\Server\LoanBureau\LB_Fill_758P_Resource_Cons_P1.sql | 3706 | swallow |
| deferredUpdate | fa-contracts\LoanBureau\Server\LoanBureau\LB_Fill_758P_Resource_Cons_P1.sql | 3753 | swallow |
| deferredUpdate | fa-contracts\LoanBureau\Server\LoanBureau\LB_Fill_758P_Resource_Cons_P1.sql | 8215 | swallow |
| deferredUpdate | fa-contracts\LoanBureau\Server\LoanBureau\LoanBureau_GetEvent_Cons.sql | 4653 | swallow |
| deferredUpdate | fa-contracts\LoanBureau\Server\LoanBureau\LoanBureau_GetEvent_Cons.sql | 4665 | swallow |

## Исчезнувшие findings (124) — все внутри `/* */`-спанов

| rule | файл | line |
|---|---|---:|
| insertRowLock | fa-contracts\API_Credit\Server\Callback\API_CON_ACCR_FndLstPercentParam.sql | 2524 |
| insertRowLock | fa-contracts\API_Credit\Server\Callback\API_CON_BfrPRD_MassClcNextDate.sql | 81 |
| insertRowLock | fa-contracts\API_Credit\Server\Facade\FCD_Cons_GetCardByState.sql | 52 |
| insertRowLock | fa-contracts\API_Credit\Server\Facade\FCD_RPT_CalcAssets.sql | 30 |
| deferredUpdate | fa-contracts\API_Insurance\Server\InsurancePolicy\API_INSP_MassModifyInsurContr.sql | 556 |
| deferredUpdate | fa-contracts\API_Insurance\Server\InsurancePolicy\API_INSP_MassModifyInsurContr.sql | 569 |
| insertRowLock | fa-contracts\API_Loan\Server\Facade\FCD_Loan_GetAllProtocolList.sql | 86 |
| deferredUpdate | fa-contracts\API_LoanBureau\Server\Facade\FCD_BKI_GR_FindListLawsuitByID2.sql | 134 |
| deferredUpdate | fa-contracts\API_LoanBureau\Server\Facade\FCD_BKI_GR_FindListLawsuitByID2.sql | 185 |
| insertRowLock | fa-contracts\Consumer\SERVER\Accrual\CON_STP_ProcessReserve.sql | 104 |
| deferredUpdate | fa-contracts\Consumer\SERVER\ConsReservePortfolio\ConsRvs_GetRule.sql | 566 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\ConsAnalysis.sql | 232 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\ConsAnalysis.sql | 418 |
| deferredUpdate | fa-contracts\Consumer\SERVER\Consumer\ConsMassExtraInsert_PayInstructTransfer.sql | 146 |
| deferredUpdate | fa-contracts\Consumer\SERVER\Consumer\ConsPortfolio_CalcNominal.sql | 232 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\Consumer_RPAccount.sql | 240 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\Consumer_RPAccount.sql | 445 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\Vehicle_LineInsert.sql | 1019 |
| insertRowLock | fa-contracts\Consumer\SERVER\Consumer\Vehicle_LineInsert.sql | 1053 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch_dml7_2_1416.sql | 73 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch6_8_275.sql | 97 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch6_8_506.sql | 4 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch6_8_531.sql | 4 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 108 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 216 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 399 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 588 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 672 |
| insertRowLock | fa-contracts\Consumer\SERVER\Patch\patch7_2_1056.sql | 729 |
| insertRowLock | fa-contracts\Consumer\SERVER\PaySchedule\Cons_Mass_PrePayment.sql | 664 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_CreditLineUnion.sql | 981 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_InterestConv2.sql | 20 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_RegValue_Fill.sql | 28 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_RoundCorrectProfitPrc.sql | 12 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_RoundCorrectProfitPrc.sql | 24 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\Cons_RoundCorrectProfitPrc.sql | 37 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_CCred_MassBindAccount.sql | 239 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\ICF_Cetelem_MassImportF5.sql | 2444 |
| insertRowLock | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\PaySchedule_PrcMax_MassCreate.sql | 9 |
| useSelectAll | fa-contracts\Consumer\SERVER\SCRIPT_UTILS\PaySchedule_PrcMax_MassCreate.sql | 15 |
| insertRowLock | fa-contracts\Insurance\SERVER\SCRIPT_UTIL\Insurance_Converter.sql | 225 |
| insertRowLock | fa-contracts\Insurance\SERVER\SCRIPT_UTIL\Insurance_Converter.sql | 604 |
| insertRowLock | fa-contracts\Insurance\SERVER\SCRIPT_UTIL\Insurance_Converter.sql | 1361 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\ADLink_CalendarDiff.sql | 193 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\ADLink_CalendarDiff.sql | 257 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\ADLink_CalendarDiff.sql | 985 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\ADLink_CheckScheme.sql | 169 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\DealAccount_Info.SQL | 168 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\DealAccount_Info.SQL | 183 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\DealAccount_Info.SQL | 211 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\DealAccount_Info.SQL | 377 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\DealFilter_One.sql | 312 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\Loan_PaymentSelect.sql | 512 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\Loan_RedemptionPercent.sql | 847 |
| insertRowLock | fa-contracts\Loan\SERVER\Loan\Loan_SetOneParamValue.sql | 36 |
| insertRowLock | fa-contracts\Loan\SERVER\LoanReservePortfolio\LoanRoundQty.sql | 8 |
| useSelectAll | fa-contracts\Loan\SERVER\LoanReservePortfolio\LoanRoundQty.sql | 41 |
| insertRowLock | fa-contracts\Loan\SERVER\SCRIPT_UTIL\Loan_CalcPredCountValueDoc.sql | 211 |
| insertRowLock | fa-contracts\LoanBureau\Server\Data\BCH_Classification.sql | 24 |
| insertRowLock | fa-contracts\LoanBureau\Server\LoanBureau\TmpTbl_bureau.sql | 407 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Archive\!Порядок запуска!.sql | 180 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Archive\tag05_InnerBalance_old.sql | 857 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Archive\teg00_ReloadPS_and_VU.sql | 29 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Conv_MassContractEntityDelete.sql | 135 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Conv_MassContractEntityDelete.sql | 190 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Conv_MassContractEntityDelete.sql | 193 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Conv_MassContractEntityDelete.sql | 227 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\Conv_MassContractEntityDelete.sql | 230 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\ctl\run_ms\run_pscombined.sql | 8 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\tag05_InnerBalance.sql | 804 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\tag07_Aim.sql | 13 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\tag081_WarrObject.sql | 30 |
| inSubQuery | fa-contracts\Test\Script_Utils\conv-consumer\tag13_PaySchedule_Over_procFix.sql | 1073 |
| inSubQuery | fa-contracts\Test\Script_Utils\conv-consumer\tag13_PaySchedule_Over_procFix.sql | 1078 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\test\test_entattr_load.sql | 17 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\test\test_payschedule_load.sql | 15 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\!update!.sql | 499 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\!update!.sql | 556 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\запуск в потоках.sql | 8 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\запуск в потоках.sql | 114 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 137 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 138 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 139 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 140 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 141 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 142 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 143 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 145 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 146 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 449 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 625 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 657 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 740 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 781 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 888 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\Проверки!.sql | 964 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\new.sql | 220 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\new.sql | 227 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\new.sql | 234 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\new.sql | 295 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 137 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 138 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 139 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 140 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 141 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 142 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 143 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 145 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 146 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 449 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 625 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 657 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 740 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 781 |
| useSelectAll | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 888 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!Проверки!.sql | 964 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!update!.sql | 477 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\!update!.sql | 534 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\запуск в потоках.sql | 8 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\запуск в потоках.sql | 114 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\new.sql | 45 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\new.sql | 52 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\difer\old\new.sql | 59 |
| insertRowLock | fa-contracts\Test\Script_Utils\conv-consumer\utils\VU_Generator_avto205.sql | 2066 |

## Методика

- Спаны комментариев: однопроходный байтовый FSM (литералы с `''` и
  переносом через `\n`, блоки, хвосты `--`) — кодировка-независимо для ASCII
  триггеров (CP866/CP1251/UTF-8 ASCII-совместимы).
- Сравнение: мультимножества по `file|rule|line`; исчезнувшие сверены со спанами,
  появившиеся — с пер-файловым прогоном старого/нового сплиттера на expanded-контенте
  (`replaceMacros` + ремап `mapExpandedLineToOriginal`).
- Технический нюанс воспроизведения: JSON-базлайны созданы PS `>`-редиректом и
  хранят кириллические пути в обратимом mojibake (UTF-8 -> CP866) в UTF-16LE;
  скрипты сверки: %TEMP%\opencode\fp_acc\{compare.ps1, blockcheck.ps1, gen_list.ps1,
  report.txt, appeared.txt}.

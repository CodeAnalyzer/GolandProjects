## ADDED Requirements

### Requirement: Закрытие блочного комментария шапки в построчных сканерах

Система SHALL в построчных сканерах deploy-stoppers `indexWrong`, `updateOnlyVar`, `pTableSpid`, `forceOrder2Tbl`, `saveTran`, `useDrop`, `mathOperations`, `existsWithAndInIf`, `indexExistsInDB`, `varcharSize` обрабатывать закрытие активного блочного комментария (`*/`) раньше проверки строчного комментария (`--`). Если строка закрывает блочный комментарий и после `TrimSpace` начинается с `--` (например, `-------------------------------------------------------------------*/`), она MUST закрывать блочный комментарий, а не считаться строчным комментарием; оставшийся после `*/` текст строки и последующие строки SHALL анализироваться правилом обычным образом. Блочный комментарий без `*/` до конца файла MUST по-прежнему скрывать последующий код. Строчный комментарий `--` вне блочного комментария MUST по-прежнему игнорироваться.

#### Scenario: Строка закрытия на дефисах не скрывает DELETE от indexWrong

- **GIVEN** SQL-файл, где шапка открыта `/*---`, а закрыта строкой `-------------------------------------------------------------------*/`, после чего идёт `delete pBPConsCoolingOff from pBPConsCoolingOff M_ROWLOCK_INDEX(XPKpBPConsCoolingOff) where SPID = @@spid and BankProductID = @BankProductID`
- **AND** у таблицы есть индексы `XPKpBPConsCoolingOff (SPID, AutoID)` и `XIE1pBPConsCoolingOff (SPID, BankProductID, AmountMin)`
- **WHEN** выполняется review
- **THEN** сформирован finding `indexWrong`, severity 1, на строке `delete`
- **AND** сообщение указывает `XIE1pBPConsCoolingOff` как более подходящий индекс
- **AND** `Object` finding равен `pbpconscoolingoff.xpkpbpconscoolingoff`

#### Scenario: Код после шапки виден остальным сканерам deploy-stoppers

- **GIVEN** SQL-файл с шапкой, закрытой строкой на `--`, после чего идёт `create procedure MyProc`, а в теле процедуры — `SAVE TRAN MySavePoint`, `DROP TABLE tTemp` и `DECLARE @Var VARCHAR`
- **WHEN** выполняется review
- **THEN** сформированы findings `saveTran`, `useDrop` и `varcharSize`, severity 1

#### Scenario: Незакрытый блочный комментарий по-прежнему скрывает код

- **GIVEN** SQL-файл с `/*` без закрывающего `*/` перед `SAVE TRAN MySavePoint`
- **WHEN** выполняется review
- **THEN** finding `saveTran` не сформирован

#### Scenario: Строчный комментарий вне блока по-прежнему игнорируется

- **GIVEN** SQL-файл с `-- SAVE TRAN MySavePoint`
- **WHEN** выполняется review
- **THEN** finding `saveTran` не сформирован

## ADDED Requirements

### Requirement: Маскировка блочных комментариев в сканерах insertRowLock, useSelectAll, truncTbl, deferredUpdate, memoryUniqueIndexUpdate и inSubQuery

Система SHALL игнорировать текст внутри блочных комментариев `/* ... */` при анализе правилами `insertRowLock`, `useSelectAll`, `truncTbl`, `deferredUpdate`, `memoryUniqueIndexUpdate`, `inSubQuery` — в том числе когда комментарий открывается на предыдущей строке, закрывается на последующей или вложен в середину оператора. Текст после хвостового строчного комментария (`-- ...`) в строке с кодом MUST также игнорироваться этими правилами. Код вне комментариев SHALL анализироваться этими правилами без изменения поведения: finding по оператору с комментарием внутри (например, `INSERT ... /* comment */ SELECT ...`) формируется по кодовым частям оператора. Блочный комментарий без закрывающего `*/` до конца файла MUST скрывать весь последующий код от этих правил.

#### Scenario: INSERT внутри многострочного блочного комментария не даёт insertRowLock

- **GIVEN** SQL-файл, где на отдельной строке открывается `/*`, ниже идёт `insert pGL_ReportFieldValues (SPID, ObjectID) select SPID = 666, ObjectID = 1 union all select SPID = 666, ObjectID = 2` без `M_WITH_ROWLOCK`, и блок закрывается `*/`
- **WHEN** выполняется review
- **THEN** finding `insertRowLock` не сформирован

#### Scenario: Закомментированный UPDATE индексированной p-таблицы не даёт deferredUpdate

- **GIVEN** SQL-файл, где внутри `/* ... */` расположен `update pContract set Note = Note + 'x' from pContract M_ROWLOCK_INDEX(XIE1pContract) where SPID = @@spid`, а колонка `Note` входит в индекс `XIE1pContract`
- **WHEN** выполняется review
- **THEN** finding `deferredUpdate` не сформирован

#### Scenario: Закомментированный SELECT с подзапросом IN не даёт inSubQuery

- **GIVEN** SQL-файл, где внутри `/* ... */` расположен `select 1 where ID in (select ID from tContract)`
- **WHEN** выполняется review
- **THEN** finding `inSubQuery` не сформирован

#### Scenario: Хвостовой строчный комментарий на строке кода игнорируется

- **GIVEN** SQL-файл со строкой `select A, B from tContract -- старый вариант: select * from tContract`
- **WHEN** выполняется review
- **THEN** finding `useSelectAll` не сформирован

#### Scenario: Комментарий внутри оператора не мешает анализу кодовых частей

- **GIVEN** SQL-файл с реальным оператором `insert pContract (SPID, ID) /* колонки и значения */ select SPID = @@spid, ID = 1` без `M_WITH_ROWLOCK`
- **WHEN** выполняется review
- **THEN** finding `insertRowLock` сформирован на строке оператора, `Object` равен `pContract`

#### Scenario: TRUNCATE TABLE внутри блочного комментария не даёт truncTbl

- **GIVEN** SQL-файл, где внутри `/* ... */` расположен `truncate table tContract`
- **WHEN** выполняется review
- **THEN** finding `truncTbl` не сформирован

#### Scenario: Закомментированный UPDATE полей уникального индекса in-memory таблицы не даёт memoryUniqueIndexUpdate

- **GIVEN** SQL-файл, где внутри `/* ... */` расположен `update pTmpObject set SPID = 1`, таблица `pTmpObject` объявлена таблицей в памяти, а колонка `SPID` входит в её уникальный индекс
- **WHEN** выполняется review
- **THEN** finding `memoryUniqueIndexUpdate` не сформирован

#### Scenario: Незакрытый блочный комментарий скрывает последующий код

- **GIVEN** SQL-файл, где `/*` открывается и не закрывается до конца файла, а после него расположены `insert pContract (SPID) select SPID = @@spid` без `M_WITH_ROWLOCK` и `select * from tContract`
- **WHEN** выполняется review
- **THEN** findings `insertRowLock` и `useSelectAll` не сформированы

#### Scenario: Код вне комментариев по-прежнему даёт findings

- **GIVEN** SQL-файл, где после закрытия блочного комментария `*/` идут `select * from tContract` и `truncate table tContract`
- **WHEN** выполняется review
- **THEN** сформированы findings `useSelectAll` и `truncTbl` на соответствующих строках

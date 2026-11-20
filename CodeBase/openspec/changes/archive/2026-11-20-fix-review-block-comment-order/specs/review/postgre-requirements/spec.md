## ADDED Requirements

### Requirement: Закрытие блочного комментария шапки в сканерах useFuncInIndCol и postgreLabelGotoLevel

Система SHALL в построчных сканерах `useFuncInIndCol` и `postgreLabelGotoLevel` обрабатывать закрытие активного блочного комментария (`*/`) раньше обработки строчного комментария (`--`). Если строка закрывает блочный комментарий и после `TrimSpace` начинается с `--` (например, `-------------------------------------------------------------------*/`), она MUST закрывать блочный комментарий, а не считаться строчным комментарием; оставшийся после `*/` текст строки и последующие строки SHALL анализироваться правилом обычным образом. Для `postgreLabelGotoLevel` усечение остатка строки по первому `--` MUST выполняться после закрытия блока и сохранять прежнюю семантику inline-комментария. Блочный комментарий без `*/` до конца файла MUST по-прежнему скрывать последующий код. Строчный комментарий `--` вне блочного комментария MUST по-прежнему игнорироваться.

#### Scenario: Строка закрытия на дефисах не скрывает функцию над индексированной колонкой

- **GIVEN** SQL-файл, где шапка открыта `/*---`, а закрыта строкой `-------------------------------------------------------------------*/`, после чего идёт `select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1`
- **AND** `SPID` входит в индекс `XPKpT`
- **WHEN** выполняется review
- **THEN** сформирован finding `useFuncInIndCol`, severity 2

#### Scenario: Строка закрытия на дефисах не скрывает GOTO с меткой выше

- **GIVEN** SQL-файл, где шапка открыта `/*---`, а закрыта строкой `-------------------------------------------------------------------*/`, после чего идут `MyLabel:` и `goto MyLabel`
- **WHEN** выполняется review
- **THEN** сформирован finding `postgreLabelGotoLevel`, severity 2
- **AND** `Object` finding равен `goto MyLabel`

#### Scenario: Незакрытый блочный комментарий по-прежнему скрывает код

- **GIVEN** SQL-файл с `/*` без закрывающего `*/` перед `select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1`, `MyLabel:` и `goto MyLabel`
- **WHEN** выполняется review
- **THEN** finding `useFuncInIndCol` не сформирован
- **AND** finding `postgreLabelGotoLevel` не сформирован

#### Scenario: Строчный комментарий вне блока по-прежнему игнорируется

- **GIVEN** SQL-файл с активной меткой `MyLabel:`, строками `-- select * from pT t M_NOLOCK_INDEX(XPKpT) where abs(t.SPID) = 1` и `-- goto MyLabel`
- **WHEN** выполняется review
- **THEN** finding `useFuncInIndCol` не сформирован
- **AND** finding `postgreLabelGotoLevel` не сформирован

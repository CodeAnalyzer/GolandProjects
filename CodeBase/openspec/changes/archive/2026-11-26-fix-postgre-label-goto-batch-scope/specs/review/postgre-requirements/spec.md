## MODIFIED Requirements

### Requirement: Использование GOTO и меток

Система SHALL обнаруживать обратные переходы `GOTO` к метке, расположенной в том же батче (сегменте файла между разделителями) выше или на той же строке, что и оператор перехода, и формировать finding `postgreLabelGotoLevel`. Метка, определённая в другом батче, SHALL NOT приводить к формированию finding для оператора `GOTO` из последующих батчей. Разделителем батча считается строка, содержащая только `go` (без учёта регистра, допускается форма `go N`), либо строка-вызов макроса-завершителя процедуры с аргументом в скобках (`ARC_PROC_END(имя)`, `DCL_PROC_END(имя)`, `PROC_END(имя)` — раскрываются в `go`); строка-метка `PROC_END:` (без скобок) разделителем не является; `go` внутри комментария MUST NOT разделять батчи. Файл без разделителей обрабатывается как один батч.

#### Scenario: GOTO в процедуре

- **GIVEN** SQL-файл с оператором `goto MyLabel` в теле процедуры и меткой `MyLabel:` выше оператора перехода в том же батче
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

#### Scenario: Прямой переход с меткой ниже

- **GIVEN** SQL-файл с оператором `goto MyLabel`, за которым ниже по коду следует метка `MyLabel:`, в одном батче
- **WHEN** выполняется review
- **THEN** finding `postgreLabelGotoLevel` не сформирован

#### Scenario: Метка из другой процедуры

- **GIVEN** SQL-файл с двумя процедурами, разделёнными строкой `go`
- **AND** в первой процедуре определена метка `_FINISH:`
- **AND** во второй процедуре есть оператор `goto _FINISH` с собственной меткой `_FINISH:` ниже перехода
- **WHEN** выполняется review
- **THEN** finding `postgreLabelGotoLevel` не сформирован

#### Scenario: Файл без разделителей go

- **GIVEN** SQL-файл без строк-разделителей `go` и меткой `MyLabel:` выше оператора `goto MyLabel`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

#### Scenario: Метка и GOTO в одной строке

- **GIVEN** SQL-файл со строкой `MyLabel: goto MyLabel`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

#### Scenario: Форма go N разделяет батчи

- **GIVEN** SQL-файл с меткой `MyLabel:` выше оператора `goto MyLabel` и строкой `go 5` между ними
- **WHEN** выполняется review
- **THEN** finding `postgreLabelGotoLevel` не сформирован

#### Scenario: go внутри комментария не разделяет батчи

- **GIVEN** SQL-файл с меткой `MyLabel:` выше оператора `goto MyLabel` и строкой `-- go` между ними
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2
- **AND** строка `go` внутри блочного комментария не разделяет батчи аналогично

#### Scenario: Макрос-завершитель процедуры разделяет батчи

- **GIVEN** SQL-файл с двумя процедурами: первая завершается строкой `ARC_PROC_END(Proc1)`, в её теле метка `EXIT_PROC:`; вторая процедура содержит `goto EXIT_PROC` с собственной меткой `EXIT_PROC:` ниже перехода и строк-разделителей `go` в файле нет
- **WHEN** выполняется review
- **THEN** finding `postgreLabelGotoLevel` не сформирован

#### Scenario: Метка PROC_END не разделяет батчи

- **GIVEN** SQL-файл с меткой `PROC_END:` выше оператора `goto PROC_END` (вызовов макроса и строк-разделителей `go` нет)
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

## MODIFIED Requirements

### Requirement: Потенциальная потеря точности

Система SHALL обнаруживать потенциальную потерю точности при assignment (SELECT @var =, INSERT...SELECT) и conversion (convert/cast) между типами данных, и формировать finding `datatype`. При разрешении типа колонки-приёмника из индекса система MUST выбирать определение колонки по приоритету: определение из анализируемого файла, затем определение из продукта анализируемого файла, затем самое свежее глобальное определение. Finding `datatype` MUST формироваться только по определению, выбранному согласно этому приоритету.

#### Scenario: Сужение datetime к date

- **GIVEN** SQL-файл с `SELECT @DateVar = CONVERT(DSOPERDAY, @DateTimeVar)` где `@DateVar` имеет тип `DSOPERDAY`, а `@DateTimeVar` — `DSDATETIME`
- **WHEN** выполняется review с `--min-severity 3`
- **THEN** сформирован finding с rule `datatype`, severity 3, с сообщением о потере точности

#### Scenario: INSERT SELECT с потерей точности

- **GIVEN** SQL-файл с `INSERT INTO tTarget (DateCol) SELECT DateTimeCol FROM tSource` где `DateCol` имеет тип `DSOPERDAY`, а `DateTimeCol` — `DSDATETIME`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

#### Scenario: FETCH INTO с потерей точности

- **GIVEN** SQL-файл с `FETCH NEXT FROM MyCursor INTO @DateVar` где тип курсора шире типа `@DateVar`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

#### Scenario: Отсутствие ложного срабатывания на кросс-продуктовом дубликате DDL

- **GIVEN** p-таблица `pList` объявлена в продукте `fa-contracts` с колонкой `QtyType DSIDENTIFIER` и в продукте `fa-reports` с колонкой `QtyType DSINT_KEY`, при этом DDL из `fa-reports` проиндексирован позже
- **AND** анализируемый файл принадлежит `fa-contracts` и содержит `INSERT INTO pList (QtyType) SELECT ConsID FROM tSource` где `ConsID` имеет тип `DSIDENTIFIER`
- **WHEN** выполняется review
- **THEN** finding с rule `datatype` по колонке `pList.QtyType` не формируется, поскольку тип приёмника разрешён из продукта `fa-contracts`

#### Scenario: Temp-таблица, объявленная в анализируемом файле

- **GIVEN** анализируемый файл содержит `SELECT ... INTO #Rest (RestCol)` с типом `DSBIGMONEY` и далее `INSERT INTO #Rest (RestCol) SELECT ...`
- **AND** колонка `RestCol` таблицы `#Rest` объявлена в других файлах и продуктах с иным типом
- **WHEN** выполняется review
- **THEN** тип приёмника `#Rest.RestCol` разрешается из определения в анализируемом файле

#### Scenario: Fallback на глобальное определение

- **GIVEN** анализируемый файл из продукта `fa-reports` содержит `INSERT INTO tTarget (Col) SELECT ...`, таблица `tTarget` объявлена только в продукте `fa-contracts`
- **WHEN** выполняется review
- **THEN** тип приёмника `tTarget.Col` разрешается из единственного доступного глобального определения, и проверка потери точности выполняется по нему

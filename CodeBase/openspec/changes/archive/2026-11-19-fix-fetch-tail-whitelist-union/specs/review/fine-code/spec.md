## MODIFIED Requirements

### Requirement: Потенциальная потеря точности

Система SHALL обнаруживать потенциальную потерю точности при assignment (SELECT @var =, INSERT...SELECT) и conversion (convert/cast) между типами данных, и формировать finding `datatype`. При анализе `FETCH INTO` спаривание переменных FETCH с выражениями SELECT курсора MUST выполняться по ближайшей декларации курсора, расположенной выше по файлу; выражения деклараций других жизней переобъявлённого курсора в спаривании не участвуют. Список переменных FETCH MUST формироваться без учёта комментариев: переменная с хвостовым однострочным комментарием (`--`) или блочным комментарием (`/* */`) на своей строке участвует в спаривании наравне с остальными; пустые строки (в том числе ставшие пустыми после удаления комментариев, например закомментированная переменная) список переменных не прерывают. Список переменных FETCH MUST завершаться на первой непустой (после удаления комментариев) строке, которая не начинается с `@` или запятой — в том числе standalone-вызов макроса Diasoft (`M_*`), вызов профайлера (`PROFILE_TIME_*`) или функции, — склеенный парсером с FETCH в один фрагмент или нет: переменные до неё участвуют в спаривании полностью. При UNION в SELECT декларации спаривание MUST выполняться с выражениями первой ветки: выражения веток после первого top-level `union` в спаривании не участвуют. При разрешении типа колонки-приёмника из индекса система MUST выбирать определение колонки по приоритету: определение из анализируемого файла, затем определение из продукта и ветки анализируемого файла, затем определение из разрешённой области продукта-кандидата (по профилю dsinfo или default-ветке; для EXT-продуктов — объединение `develop ∪ prj` по `review/review-scope`), затем определение из продукта анализируемого файла, затем из default-ветки продукта-кандидата, затем самое свежее глобальное определение. Определение из ветки того же продукта вне разрешённой области MUST NOT иметь приоритет над определением из разрешённой области. Finding `datatype` MUST формироваться только по определению, выбранному согласно этому приоритету.

#### Scenario: Сужение datetime к date

- **GIVEN** SQL-файл с `SELECT @DateVar = CONVERT(DSOPERDAY, @DateTimeVar)` где `@DateVar` имеет тип `DSOPERDAY`, а `@DateTimeVar` — `DSDATETIME`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3, с сообщением о потере точности

#### Scenario: INSERT SELECT с потерей точности

- **GIVEN** SQL-файл с `INSERT INTO tTarget (DateCol) SELECT DateTimeCol FROM tSource` где `DateCol` имеет тип `DSOPERDAY`, а `DateTimeCol` — `DSDATETIME`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

#### Scenario: FETCH INTO с потерей точности

- **GIVEN** SQL-файл с `FETCH NEXT FROM MyCursor INTO @DateVar` где тип курсора шире типа `@DateVar`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `datatype`, severity 3

#### Scenario: FETCH INTO переобъявлённого курсора спаривается со своей декларацией

- **GIVEN** SQL-файл с первой жизнью курсора `c`: `DECLARE c CURSOR FOR SELECT ID, Amount FROM tX` и `FETCH c INTO @ID, @Amount`, где `Amount` имеет тип `DSIDENTIFIER`, а `@Amount` — `DSINT_KEY`, затем `DEALLOCATE c`; вторая жизнь: `DECLARE c CURSOR FOR SELECT ID FROM tX` и `FETCH c INTO @ID2`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре выражение `Amount` → переменная `@Amount` из ближайшей декларации (первой жизни), а не пропускается из-за смещённого индексного спаривания с декларацией последней жизни

#### Scenario: Переменная с хвостовым комментарием проходит проверку типа

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT ID, Amount FROM tX` и многострочным `FETCH c INTO @ID,` / `@Amount -- комментарий после переменной`, где `Amount` имеет тип `DSIDENTIFIER`, а `@Amount` — `DSINT_KEY`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре `Amount` → `@Amount`, а не пропускается из-за комментария после переменной

#### Scenario: Закомментированная переменная внутри списка не прерывает спаривание

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT ID, Amount FROM tX` и многострочным `FETCH c INTO @ID,` / `--@CommentedVar,` / `@Amount`, где средняя строка — чисто комментарий, `Amount` имеет тип `DSIDENTIFIER`, а `@Amount` — `DSINT_KEY`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре `Amount` → `@Amount`: закомментированная строка стала пустой после удаления комментариев и не прервала список

#### Scenario: Последняя переменная перед макровызовом проходит проверку типа

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT ID, Amount FROM tX` и `FETCH c INTO @ID, @Amount`, за которым в том же фрагменте следует standalone-вызов макроса `M_BUSINESSLOG_BLOCK_END('>>> Итерация курсора <<<')`, где `Amount` имеет тип `DSIDENTIFIER`, а `@Amount` — `DSINT_KEY`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре `Amount` → `@Amount`, а не пропускается из-за макровызова после списка переменных

#### Scenario: Последняя переменная перед вызовом профайлера проходит проверку типа

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT ID, Amount FROM tX` и `FETCH c INTO @ID, @Amount`, за которым в том же фрагменте следует standalone-вызов профайлера `PROFILE_TIME_EX('...')`, где `Amount` имеет тип `DSIDENTIFIER`, а `@Amount` — `DSINT_KEY`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре `Amount` → `@Amount`, а не пропускается из-за вызова профайлера после списка переменных

#### Scenario: UNION в декларации — спаривание по первой ветке

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT ID, Amount UNION SELECT X, Y FROM tX` (from только во второй ветке; колонка `Amount` таблицы `tX` имеет тип `DSIDENTIFIER`) и `FETCH c INTO @ID, @Amount`, где `@Amount` — `DSINT_KEY`
- **WHEN** выполняется review
- **THEN** для `@Amount` формируется finding с rule `datatype`, severity 3, по паре `Amount` → `@Amount`: выражение второй позиции берётся из первой ветки (`Amount`, резолвится через таблицу `tX` из FromClause), а не смещённое спаривание с выражениями второй ветки

#### Scenario: Отсутствие ложного срабатывания на кросс-продуктовом дубликате DDL

- **GIVEN** p-таблица `pList` объявлена в продукте `fa-contracts` с колонкой `QtyType DSIDENTIFIER` и в продукте `fa-reports` с колонкой `QtyType DSINT_KEY`, при этом DDL из `fa-reports` проиндексирован позже
- **AND** анализируемый файл принадлежит `fa-contracts` и содержит `INSERT INTO pList (QtyType) SELECT ConsID FROM tSource` где `ConsID` имеет тип `DSIDENTIFIER`
- **WHEN** выполняется review
- **THEN** finding с rule `datatype` по колонке `pList.QtyType` не формируется, поскольку тип приёмника разрешён из продукта `fa-contracts`

#### Scenario: Отсутствие ложного срабатывания на дубликате DDL из другой ветки

- **GIVEN** колонка `pList.QtyType` объявлена в ветке `fa-contracts-11-01-7` продукта `fa-contracts` как `DSIDENTIFIER`, а в ветке `develop` того же продукта — как `DSINT_KEY`
- **AND** анализируемый файл принадлежит ветке `fa-contracts-11-01-7` и содержит `INSERT INTO pList (QtyType) SELECT ConsID FROM tSource` где `ConsID` имеет тип `DSIDENTIFIER`
- **WHEN** выполняется review
- **THEN** finding с rule `datatype` по колонке `pList.QtyType` не формируется, поскольку тип приёмника разрешён из целевой ветки

#### Scenario: Temp-таблица, объявленная в анализируемом файле

- **GIVEN** анализируемый файл содержит `SELECT ... INTO #Rest (RestCol)` с типом `DSBIGMONEY` и далее `INSERT INTO #Rest (RestCol) SELECT ...`
- **AND** колонка `RestCol` таблицы `#Rest` объявлена в других файлах и продуктах с иным типом
- **WHEN** выполняется review
- **THEN** тип приёмника `#Rest.RestCol` разрешается из определения в анализируемом файле

#### Scenario: Fallback на глобальное определение

- **GIVEN** анализируемый файл из продукта `fa-reports` содержит `INSERT INTO tTarget (Col) SELECT ...`, таблица `tTarget` объявлена только в продукте `fa-contracts`
- **WHEN** выполняется review
- **THEN** тип приёмника `tTarget.Col` разрешается из единственного доступного глобального определения, и проверка потери точности выполняется по нему

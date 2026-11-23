# PostgreSQL Requirements

## Purpose

Статический анализ SQL-файлов для выявления нарушений требований PostgreSQL (severity=2): 24 правила — некорректные сравнения с NULL, кодировки, лимиты JOIN/параметров, управление транзакциями, типизация, курсоры, UNION/ISNULL/алиасы.

## Requirements

### Requirement: Сравнение с NULL через =/<>

Система SHALL обнаруживать сравнение с `NULL` через `=`/`<>` вместо `IS NULL`/`IS NOT NULL` и формировать finding `nullComparison`. Строка объявления параметра или переменной со значением по умолчанию `= null` (в том числе с указанием направления выходного параметра ключевым словом `output` или его синонимом `out`, в любом регистре, с необязательной хвостовой запятой и необязательной размерностью типа в скобках) SHALL классифицироваться как объявление значения по умолчанию, а не сравнение, и finding не формирует.

#### Scenario: Сравнение с NULL через =

- **GIVEN** SQL-файл с `WHERE Col = NULL`
- **WHEN** выполняется review с `--min-severity 2`
- **THEN** сформирован finding с rule `nullComparison`, severity 2

#### Scenario: Объявление OUT-параметра с default null

- **GIVEN** SQL-файл с объявлением выходного параметра процедуры в короткой форме `@CheckClient DSINT_KEY = null out`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление OUT-параметра с default null и хвостовой запятой

- **GIVEN** SQL-файл со строкой объявления `@Message DSCOMMENT = null out,` внутри списка параметров процедуры
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление OUT-параметра с default null в верхнем регистре

- **GIVEN** SQL-файл со строкой объявления `@ProtocolID DSIDENTIFIER = NULL OUT,`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

#### Scenario: Объявление параметра с default null в длинной форме output

- **GIVEN** SQL-файл со строкой объявления `@AmountDelinq DSMONEY = null output,`
- **WHEN** выполняется review
- **THEN** finding с rule `nullComparison` не формируется

### Requirement: Строковые литералы не в CP866

Система SHALL обнаруживать строковые литералы, не соответствующие кодировке CP866, и формировать finding `shouldBeCP866`.

#### Scenario: Литерал не в CP866

- **GIVEN** SQL-файл со строковым литералом, содержащим символы, не входящие в CP866
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `shouldBeCP866`, severity 2

### Requirement: Превышение лимита JOIN

Система SHALL обнаруживать запросы с более чем 12 JOIN и формировать finding `tooManyJoins`.

#### Scenario: Более 12 JOIN

- **GIVEN** SQL-файл с запросом, содержащим 13 JOIN
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `tooManyJoins`, severity 2

### Requirement: Превышение лимита параметров процедуры

Система SHALL обнаруживать процедуры с более чем 90 параметрами и формировать finding `maxProcParam`.

#### Scenario: 91 параметр

- **GIVEN** SQL-файл с процедурой, имеющей 91 параметр
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `maxProcParam`, severity 2

### Requirement: Модификация выходных параметров

Система SHALL обнаруживать модификацию выходных параметров процедуры в теле процедуры и формировать finding `modifyOutProc`.

#### Scenario: Модификация OUTPUT параметра

- **GIVEN** SQL-файл с `CREATE PROCEDURE MyProc @OutParam int OUTPUT AS SET @OutParam = 5`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `modifyOutProc`, severity 2

### Requirement: Пустой RETURN без значения

Система SHALL обнаруживать `RETURN` без значения и формировать finding `emptyReturn`.

#### Scenario: RETURN без значения

- **GIVEN** SQL-файл с `RETURN` без значения в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `emptyReturn`, severity 2

### Requirement: Прямое управление транзакциями

Система SHALL обнаруживать прямое управление транзакциями (`BEGIN TRAN`/`COMMIT`/`ROLLBACK`) и формировать finding `rawTransactionControl`.

#### Scenario: BEGIN TRAN в процедуре

- **GIVEN** SQL-файл с `BEGIN TRAN` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `rawTransactionControl`, severity 2

### Requirement: Использование GOTO и меток

Система SHALL обнаруживать использование `GOTO` и меток и формировать finding `postgreLabelGotoLevel`.

#### Scenario: GOTO в процедуре

- **GIVEN** SQL-файл с `GOTO MyLabel` в теле процедуры
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `postgreLabelGotoLevel`, severity 2

### Requirement: Приведение даты к строке

Система SHALL обнаруживать приведение даты к строке и формировать finding `dateIntoString`.

#### Scenario: CONVERT varchar from datetime

- **GIVEN** SQL-файл с `CONVERT(varchar, @Date)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `dateIntoString`, severity 2

### Requirement: Сравнение даты с пустой строкой

Система SHALL обнаруживать сравнение даты с пустой строкой и формировать finding `emptyStringDate`.

#### Scenario: Сравнение с пустой строкой

- **GIVEN** SQL-файл с `WHERE @Date = ''`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `emptyStringDate`, severity 2

### Requirement: Использование переменной после закрытия курсора

Система SHALL обнаруживать использование переменной после закрытия курсора и формировать finding `varUseAfterCursor`. Окно анализа после DEALLOCATE курсора MUST ограничиваться сверху следующей декларацией того же имени: использование переменных, полученных FETCH'ем из следующей жизни переобъявлённого курсора, finding не формирует. Строки-продолжения многострочного `FETCH ... INTO` (строки с переменными после строки, содержащей имя курсора) MUST NOT классифицироваться как использование переменных; продолжением считается только непустая (после удаления комментариев) строка, начинающаяся с `@` или запятой; строка заголовка `into` — часть оператора, а не новый оператор. Пустые строки (в том числе ставшие пустыми после удаления комментариев) список переменных не прерывают. Любая иная строка после начала списка переменных — в том числе standalone-вызов макроса (`M_*`), функции или профайлера (`PROFILE_TIME_*`) — строкой-продолжением FETCH не является и MUST анализироваться как обычный оператор: переменные в его аргументах классифицируются как использование.

#### Scenario: Переменная после CLOSE CURSOR

- **GIVEN** SQL-файл с `CLOSE MyCursor; SET @Var = @FetchVar` где `@FetchVar` была получена из курсора
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2

#### Scenario: Переобъявление курсора — переменные следующей жизни чисты

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, циклом `FETCH c INTO @A` с использованием `@A`, `DEALLOCATE c`, затем `DECLARE c CURSOR FOR SELECT B FROM tX` и циклом `FETCH c INTO @B` с использованием `@B` в последующих операторах
- **WHEN** выполняется review
- **THEN** finding с rule `varUseAfterCursor` для использований `@B` (и повторных использований `@A` в новой жизни) не формируется

#### Scenario: Строки-продолжения многострочного FETCH новой жизни после DEALLOCATE

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX`, `FETCH c INTO @A, @B`, `DEALLOCATE c`, затем `DECLARE c CURSOR FOR SELECT A, B FROM tX` и многострочным `FETCH c INTO @A,` / `@B`, где переменные расположены на отдельных строках после строки с именем курсора
- **WHEN** выполняется review
- **THEN** строки-продолжения с переменными не формируют finding с rule `varUseAfterCursor`

#### Scenario: Пустая или закомментированная строка внутри списка ниже DEALLOCATE не классифицируется как использование

- **GIVEN** SQL-файл с `DEALLOCATE c`, затем многострочным `FETCH c INTO @A,` / `--@CommentedVar,` / `@B` ниже DEALLOCATE, где средняя строка — чисто комментарий
- **WHEN** выполняется review
- **THEN** закомментированная строка стала пустой после удаления комментариев, список не прерван, и finding с rule `varUseAfterCursor` на ней не формируется

#### Scenario: Макровызов с переменной курсора ниже DEALLOCATE

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A` выше DEALLOCATE, затем `DEALLOCATE c`, FETCH ниже DEALLOCATE и standalone-вызовом макроса `M_INSERT_ELDOC(@A)` после его списка переменных
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2, для `@A` на строке макровызова: аргумент макроса — реальное использование, а не строка-продолжение FETCH

#### Scenario: Вызов профайлера с переменной курсора ниже DEALLOCATE

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A` выше DEALLOCATE, затем `DEALLOCATE c`, FETCH ниже DEALLOCATE и standalone-вызовом профайлера `PROFILE_TIME_VAR1('... @A = %1!', @A)` после его списка переменных
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2, для `@A` на строке вызова профайлера: аргумент — реальное использование, а не строка-продолжение FETCH

#### Scenario: Макровызов без переменных окна ниже DEALLOCATE

- **GIVEN** SQL-файл с `DEALLOCATE c`, FETCH ниже DEALLOCATE и standalone-вызовом макроса `M_BUSINESSLOG_BLOCK_END('>>> Итерация курсора <<<')` после его списка переменных, аргументы которого не содержат переменных курсора
- **WHEN** выполняется review
- **THEN** finding с rule `varUseAfterCursor` на строке макровызова не формируется

### Requirement: Использование курсоров без объявления

Система SHALL обнаруживать использование курсоров (OPEN, FETCH, CLOSE, DEALLOCATE) без предварительного объявления (DECLARE CURSOR) и формировать finding `useOnlyDeclaredCursors`. Объявления и использования курсоров MUST определяться в одной и той же области текста процедуры после подстановки макросов препроцессора (expanded-текст), чтобы объявления, директива `#define` которых вырезана или раскрыта, не терялись. Номер строки finding'а MUST указывать на строку фактического использования курсора в исходном файле.

#### Scenario: OPEN без DECLARE

- **GIVEN** SQL-файл с `OPEN MyCursor` без `DECLARE MyCursor CURSOR FOR`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useOnlyDeclaredCursors`, severity 2

#### Scenario: Объявленный курсор после многострочного #define

- **GIVEN** SQL-файл, где перед процедурами расположен многострочный `#define`, а в теле процедуры курсор объявлен через `__DECLARE_CURSOR__(cur)` и затем используется (`OPEN`, `FETCH`, `CLOSE`)
- **WHEN** выполняется review с правилом `useOnlyDeclaredCursors`
- **THEN** findings по этому курсору не формируются

#### Scenario: Строка реального необъявленного курсора с учётом сдвига макросов

- **GIVEN** SQL-файл с многострочным `#define` перед процедурами и процедура, использующая курсор без объявления в своей области видимости
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useOnlyDeclaredCursors`
- **AND** номер строки finding'а указывает на строку фактического использования в исходном файле, а не на сдвинутую строку expanded-текста

#### Scenario: Курсор предыдущей процедуры не засчитывается

- **GIVEN** SQL-файл с двумя процедурами, где первая объявляет и использует курсор `cur`, а вторая использует `cur` без объявления
- **WHEN** выполняется review
- **THEN** во второй процедуре сформирован finding с rule `useOnlyDeclaredCursors` на использование `cur`

### Requirement: Сравнение выражений разных типов

Система SHALL обнаруживать сравнение выражений разных типов в `WHERE`/`ON`/`IF`/`CASE WHEN` и формировать finding `diffTypesComparison`. Якорь finding'а (line) MUST указывать на строку фактического сравнения внутри оператора, а не на первую строку query-фрагмента; строки standalone-макросов (`M_DELETE_PTABLE(...)`, `M_LOG_TABLE(...)` и т.п.), склеенные парсером с оператором, не смещают якорь.

#### Scenario: Сравнение datetime и date

- **GIVEN** SQL-файл с `WHERE tA.DSDATETIME = tB.DSOPERDAY`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `diffTypesComparison`, severity 2
- **AND** line finding'а указывает на строку сравнения в WHERE

#### Scenario: Якорь при макросе, склеенном с INSERT…SELECT

- **GIVEN** SQL-файл, где строке с `M_DELETE_PTABLE(pX)` непосредственно следует `INSERT INTO pX ... SELECT ... INNER JOIN ... ON a.Col1 = b.Col2` с несовпадающими типами колонок
- **AND** макрос и INSERT…SELECT попадают в один query-фрагмент со стартовой строкой макроса
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `diffTypesComparison`, severity 2
- **AND** line finding'а указывает на строку сравнения `a.Col1 = b.Col2` в ON, а не на строку макроса

### Requirement: Приведение FLOAT к строке

Система SHALL обнаруживать приведение `FLOAT`/`DSFLOAT` к строке через `CONVERT`/`CAST` и формировать finding `floatToStringConvert`.

#### Scenario: CONVERT varchar from FLOAT

- **GIVEN** SQL-файл с `CONVERT(varchar, @FloatVal)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `floatToStringConvert`, severity 2

### Requirement: SELECT после SET ROWCOUNT без ORDER BY

Система SHALL обнаруживать `SELECT` в переменные и `INSERT...SELECT` после `SET ROWCOUNT N` без `ORDER BY` и формировать finding `selectAfterSetRowcount`.

#### Scenario: SET ROWCOUNT без ORDER BY

- **GIVEN** SQL-файл с `SET ROWCOUNT 10; SELECT @ID = ID FROM tContract`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `selectAfterSetRowcount`, severity 2

### Requirement: Избыточные параметры в вызове процедуры

Система SHALL обнаруживать передачу лишних или дублированных параметров в `EXEC` относительно сигнатуры процедуры из индекса и формировать finding `excessProcParams`.

#### Scenario: Лишний именованный параметр

- **GIVEN** проиндексированная процедура `MyProc` с параметрами `@A`, `@B` и SQL-файл с `EXEC MyProc @A = 1, @B = 2, @C = 3`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `excessProcParams`, severity 2

### Requirement: Дублирование OUTPUT-переменной

Система SHALL обнаруживать повторное использование одной и той же переменной для нескольких OUTPUT-параметров в одном `EXEC` и формировать finding `duplicateOutputVariable`.

#### Scenario: Одна переменная на два OUTPUT

- **GIVEN** SQL-файл с `EXEC MyProc @Out1 = @X OUTPUT, @Out2 = @X OUTPUT`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `duplicateOutputVariable`, severity 2

### Requirement: Несовпадение аргументов FETCH с DECLARE CURSOR

Система SHALL обнаруживать `FETCH ... INTO`, где число переменных не совпадает с числом выражений в `DECLARE CURSOR`, и формировать finding `cursorFetchArguments`. Сопоставление FETCH с декларацией MUST быть позиционным: FETCH сравнивается с ближайшей декларацией курсора с тем же именем, расположенной выше по файлу; FETCH, выше которого нет декларации курсора, этим правилом не проверяется. При повторном объявлении курсора (DECLARE → DEALLOCATE → DECLARE того же имени) каждая жизнь курсора MUST оцениваться по своей собственной декларации, а не по последней декларации файла. Подсчёт переменных FETCH MUST выполняться по тексту без комментариев: однострочный комментарий (`--`) на строке переменной, в том числе после последней переменной многострочного FETCH, не исключает переменную из подсчёта; блочные комментарии (`/* */`) внутри списка переменных также не участвуют в подсчёте. Многострочная форма с `into` на строке после имени курсора (`fetch c` / `into @A, @B`) MUST распознаваться как один оператор. Список переменных FETCH MUST завершаться на первой непустой (после удаления комментариев) строке, которая не начинается с `@` или запятой — в том числе standalone-вызов макроса Diasoft (`M_*`), вызов профайлера (`PROFILE_TIME_*`) или функции, — склеенный парсером с FETCH в один фрагмент или нет: переменные до неё участвуют в подсчёте полностью. Пустые строки (в том числе ставшие пустыми после удаления комментариев, например закомментированная переменная) список переменных не прерывают. Арность SELECT-декларации с UNION MUST определяться по первой ветке: выражения веток после первого top-level `union` в подсчёте не участвуют.

#### Scenario: FETCH с другим числом переменных

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и `FETCH NEXT FROM c INTO @A`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `cursorFetchArguments`, severity 2

#### Scenario: Каждая жизнь переобъявлённого курсора совпадает со своей декларацией

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и `FETCH NEXT FROM c INTO @A, @B`, затем `DEALLOCATE c`, `DECLARE c CURSOR FOR SELECT A FROM tX` и `FETCH NEXT FROM c INTO @A2`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется

#### Scenario: Рассинхрон внутри одной жизни после переобъявления

- **GIVEN** SQL-файл с первой жизнью курсора `c` из двух выражений, затем после `DEALLOCATE c` — `DECLARE c CURSOR FOR SELECT A FROM tX` и `FETCH NEXT FROM c INTO @A, @B`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `cursorFetchArguments`, severity 2, где числом выражений декларации является 1 (ближайшая декларация), а не 2

#### Scenario: FETCH выше первой декларации

- **GIVEN** SQL-файл с `FETCH NEXT FROM c INTO @A`, расположенным выше любой декларации `c`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется

#### Scenario: Переменная с хвостовым комментарием участвует в подсчёте

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B, C FROM tX` и многострочным `FETCH NEXT FROM c INTO @A,` / `@B,` / `@C -- комментарий после последней переменной`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется: подсчитаны все три переменные, комментарий не исключил `@C`

#### Scenario: Реальный рассинхрон при наличии комментария в списке переменных

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX` и многострочным `FETCH NEXT FROM c INTO @A,` / `@B -- комментарий`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `cursorFetchArguments`, severity 2, где числом переменных FETCH является 2

#### Scenario: Многострочный заголовок с into на следующей строке распознаётся

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и оператором `fetch c` / `into @A, @B`, где `into` и список переменных расположены на строке после строки с именем курсора
- **WHEN** выполняется review
- **THEN** оператор распознаётся и finding с rule `cursorFetchArguments` не формируется: обе переменные подсчитаны

#### Scenario: Закомментированная переменная внутри списка не прерывает список

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и многострочным `FETCH c INTO @A,` / `--@CommentedVar,` / `@B`, где средняя строка — чисто комментарий
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется: закомментированная строка стала пустой после удаления комментариев, список не прерван, подсчитаны `@A` и `@B`

#### Scenario: Макровызов после списка переменных не исключает последнюю переменную

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX` и `FETCH NEXT FROM c INTO @A, @B`, за которым в том же фрагменте следует standalone-вызов макроса `M_BUSINESSLOG_BLOCK_END('>>> Итерация курсора <<<')`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется: макровызов завершает список переменных, подсчитаны обе переменные

#### Scenario: Вызов профайлера после списка переменных не исключает последнюю переменную

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B, C, D, E FROM tX` и `FETCH c INTO @A, @B, @C, @D, @E`, за которым в том же фрагменте следует standalone-вызов профайлера `PROFILE_TIME_EX('...')`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется: вызов профайлера завершает список переменных, подсчитаны все пять переменных

#### Scenario: UNION в декларации — арность по первой ветке

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT @Perc as Perc, @Date as PrcDate UNION SELECT PrcValue, PrcDate FROM tX` (from только во второй ветке) и `FETCH c INTO @PercCur, @DateCur`
- **WHEN** выполняется review
- **THEN** finding с rule `cursorFetchArguments` не формируется: числом выражений декларации является 2 (первая ветка), а не 3

#### Scenario: Реальный рассинхрон при UNION в декларации

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT @Perc as Perc, @Date as PrcDate UNION SELECT PrcValue, PrcDate FROM tX` и `FETCH c INTO @PercCur`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `cursorFetchArguments`, severity 2, где числом переменных FETCH является 1, а числом выражений декларации — 2

### Requirement: Использование переменной в том же SELECT

Система SHALL обнаруживать чтение переменной в том же `SELECT`, где она присваивается, и формировать finding `usageVarInSameSelect`. Ссылками на переменную НЕ считаются: вхождения переменной в standalone-вызовы макросов (`M_BUSINESSLOG_PARAM(@Var, ...)`, склеенные с SELECT) и вхождения внутри системных переменных `@@` (`@@rowcount`, `@@error`).

#### Scenario: Чтение @Var в том же SELECT

- **GIVEN** SQL-файл с `SELECT @A = Col, @B = @A FROM tX`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `usageVarInSameSelect`, severity 2

#### Scenario: SELECT присваиваний со склеенными макросами

- **GIVEN** SQL-файл с `SELECT @RowCount = @@rowcount, @RetVal = @@error`, за которым без пустой строки следуют `M_BUSINESSLOG_PARAM(@RowCount, DSINT_KEY, INT)` и `M_BUSINESSLOG_PARAM(@RetVal, DSINT_KEY, INT)`
- **WHEN** выполняется review
- **THEN** finding с rule `usageVarInSameSelect` не формируется

#### Scenario: Системная переменная @@rowcount не считается чтением @rowcount

- **GIVEN** SQL-файл с `SELECT @RetVal = @@error, @RowCount = @@rowcount`
- **WHEN** выполняется review
- **THEN** finding с rule `usageVarInSameSelect` не формируется

### Requirement: Присвоение переменной в UPDATE SET

Система SHALL обнаруживать присвоение переменной (`@var = ...`) в списке `UPDATE ... SET` и формировать finding `varAssignInUpdate`.

#### Scenario: SET @Var в UPDATE

- **GIVEN** SQL-файл с `UPDATE tX SET @Var = Col, Name = @Name`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varAssignInUpdate`, severity 2

### Requirement: JOIN без алиасов таблиц

Система SHALL обнаруживать операторы с JOIN, где таблицы не имеют алиасов, и формировать finding `statementsWithJoinsRequireAliases`.

#### Scenario: JOIN без алиаса

- **GIVEN** SQL-файл с `SELECT tA.ID FROM tA JOIN tB ON tA.ID = tB.ID` без алиасов таблиц
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `statementsWithJoinsRequireAliases`, severity 2

### Requirement: Функция в индексированном столбце

Система SHALL обнаруживать применение функции к индексированному столбцу в условии фильтрации и формировать finding `useFuncInIndCol`.

#### Scenario: Функция над индексной колонкой

- **GIVEN** SQL-файл с `WHERE CONVERT(varchar, tX.IndexedCol) = @Val` где `IndexedCol` входит в индекс таблицы
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `useFuncInIndCol`, severity 2

### Requirement: ISNULL с разными типами

Система SHALL обнаруживать вызов `ISNULL(arg1, arg2)`, где типы аргументов не эквивалентны, и формировать finding `isNullSameTypes`. Литерал во втором аргументе finding не формирует.

#### Scenario: ISNULL разных типов

- **GIVEN** SQL-файл с `ISNULL(@DateTimeVar, @DateVar)` где типы `DSDATETIME` и `DSOPERDAY`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `isNullSameTypes`, severity 2

### Requirement: Алиасы столбцов при UNION и ORDER BY

Система SHALL обнаруживать `UNION` с `ORDER BY` по имени, которое не совпадает с алиасом/именем колонки первого `SELECT`, и формировать finding `aliasWhenUsingUnion`.

#### Scenario: ORDER BY по имени из второго SELECT

- **GIVEN** SQL-файл с `SELECT A AS Name FROM t1 UNION SELECT B FROM t2 ORDER BY B`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `aliasWhenUsingUnion`, severity 2

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

### Requirement: Маскировка блочных комментариев в сканерах tooManyJoins и varUseAfterCursor

Система SHALL игнорировать текст внутри блочных комментариев `/* ... */` при подсчёте JOIN правилом `tooManyJoins` и при разборе структуры курсоров правилом `varUseAfterCursor` (комментированные `FETCH`/`DEALLOCATE`/`DECLARE CURSOR` не считаются операторами). Текст после хвостового строчного комментария (`-- ...`) в строке с кодом MUST также игнорироваться. Код вне комментариев SHALL анализироваться без изменения поведения.

#### Scenario: Закомментированный запрос с превышением лимита JOIN не даёт tooManyJoins

- **GIVEN** SQL-файл, где внутри `/* ... */` расположен SELECT, соединяющий более восьми таблиц через JOIN
- **WHEN** выполняется review
- **THEN** finding `tooManyJoins` не сформирован

#### Scenario: Хвостовой строчный комментарий с JOIN не влияет на счётчик

- **GIVEN** SQL-файл со строкой `select A from tContract c join tDoc d on d.ID = c.ID -- join tExtra e on e.ID = d.ID join tExtra2 e2 on e2.ID = d.ID`
- **WHEN** выполняется review
- **THEN** JOIN-ы из комментария не учитываются при подсчёте

#### Scenario: Закомментированный FETCH не создаёт пропуск анализа переменных

- **GIVEN** SQL-файл с курсором, где внутри `/* ... */` закомментирован `fetch cur into @Var`, а ниже по коду `@Var` используется после `deallocate cursor cur` без повторного заполнения
- **WHEN** выполняется review
- **THEN** закомментированный `fetch` не учитывается как заполнение `@Var`, и поведение правила определяется только реальными операторами

## Related code

- `internal/review/review_rules.go` — реализация проверок (PostgreSQL requirements)
- `internal/review/types.go` — константы правил severity 2
- `internal/review/catalog.go` — каталог правил
- `internal/review/review_helpers.go` — вспомогательные функции (resolveArgType, и др.)
- `internal/review/review_lookup.go` — lookup-функции для типов данных

## Notes

- PostgreSQL requirements имеют severity=2 — не блокируют деплой, но требуют исправления
- Правило `useOnlyDeclaredCursors` поддерживает макросы `__DECLARE_CURSOR__` и `__DEALLOCATE_CURSOR__`
- Правило `diffTypesComparison` определяет потерю точности (datetime → date) как finding
- Правило `shouldBeCP866` проверяет строковые литералы на соответствие кодировке CP866
- Правила `deferredUpdate`, `inSubQuery`, `varcharSize`, `columnInsert` относятся к deploy-stoppers (severity 1), не к PostgreSQL requirements
- Execution-слой `internal/reviewsvc/runtime.go` — общая точка входа для CLI (`cmd/review.go`) и MCP-инструмента `codebase_review_sql`; устраняет дублирование оркестрации. Поведение review-команд специфицировано здесь на уровне правил; транспорт MCP — в `mcp-server/mcp-transport-tools`.

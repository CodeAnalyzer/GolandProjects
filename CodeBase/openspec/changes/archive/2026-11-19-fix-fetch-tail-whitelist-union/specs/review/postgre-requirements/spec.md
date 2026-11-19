## MODIFIED Requirements

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

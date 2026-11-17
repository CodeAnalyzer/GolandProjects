# Delta: review/postgre-requirements

## MODIFIED Requirements

### Requirement: Несовпадение аргументов FETCH с DECLARE CURSOR

Система SHALL обнаруживать `FETCH ... INTO`, где число переменных не совпадает с числом выражений в `DECLARE CURSOR`, и формировать finding `cursorFetchArguments`. Сопоставление FETCH с декларацией MUST быть позиционным: FETCH сравнивается с ближайшей декларацией курсора с тем же именем, расположенной выше по файлу; FETCH, выше которого нет декларации курсора, этим правилом не проверяется. При повторном объявлении курсора (DECLARE → DEALLOCATE → DECLARE того же имени) каждая жизнь курсора MUST оцениваться по своей собственной декларации, а не по последней декларации файла.

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

### Requirement: Использование переменной после закрытия курсора

Система SHALL обнаруживать использование переменной после закрытия курсора и формировать finding `varUseAfterCursor`. Окно анализа после DEALLOCATE курсора MUST ограничиваться сверху следующей декларацией того же имени: использование переменных, полученных FETCH'ем из следующей жизни переобъявлённого курсора, finding не формирует. Строки-продолжения многострочного `FETCH ... INTO` (строки с переменными после строки, содержащей имя курсора) MUST NOT классифицироваться как использование переменных.

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

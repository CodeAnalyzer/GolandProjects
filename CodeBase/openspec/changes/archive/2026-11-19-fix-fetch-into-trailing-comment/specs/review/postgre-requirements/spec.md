## MODIFIED Requirements

### Requirement: Несовпадение аргументов FETCH с DECLARE CURSOR

Система SHALL обнаруживать `FETCH ... INTO`, где число переменных не совпадает с числом выражений в `DECLARE CURSOR`, и формировать finding `cursorFetchArguments`. Сопоставление FETCH с декларацией MUST быть позиционным: FETCH сравнивается с ближайшей декларацией курсора с тем же именем, расположенной выше по файлу; FETCH, выше которого нет декларации курсора, этим правилом не проверяется. При повторном объявлении курсора (DECLARE → DEALLOCATE → DECLARE того же имени) каждая жизнь курсора MUST оцениваться по своей собственной декларации, а не по последней декларации файла. Подсчёт переменных FETCH MUST выполняться по тексту без комментариев: однострочный комментарий (`--`) на строке переменной, в том числе после последней переменной многострочного FETCH, не исключает переменную из подсчёта; блочные комментарии (`/* */`) внутри списка переменных также не участвуют в подсчёте.

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

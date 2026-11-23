## MODIFIED Requirements

### Requirement: Использование переменной после закрытия курсора

Система SHALL обнаруживать использование переменной после закрытия курсора и формировать finding `varUseAfterCursor`. Окно анализа после DEALLOCATE курсора MUST ограничиваться сверху следующей декларацией того же имени: использование переменных, полученных FETCH'ем из следующей жизни переобъявлённого курсора, finding не формирует. Строки-продолжения многострочного `FETCH ... INTO` (строки с переменными после строки, содержащей имя курсора) MUST NOT классифицироваться как использование переменных; продолжением считается только непустая (после удаления комментариев) строка, начинающаяся с `@` или запятой; строка заголовка `into` — часть оператора, а не новый оператор. Пустые строки (в том числе ставшие пустыми после удаления комментариев) список переменных не прерывают. Любая иная строка после начала списка переменных — в том числе standalone-вызов макроса (`M_*`), функции или профайлера (`PROFILE_TIME_*`) — строкой-продолжением FETCH не является и MUST анализироваться как обычный оператор: переменные в его аргументах классифицируются как использование.

Переопределение отслеживаемой переменной в окне после DEALLOCATE MUST прекращать её отслеживание: строки ниже переопределения finding для этой переменной не формируют, строки самого переопределения использованием не считаются. Переопределением считается: присваивание `SET @var = ...`, включая составные операторы (`@var += ...` и аналогичные); `SELECT`-присваивание — вхождение `@var` в левой части списка присваиваний оператора SELECT, непосредственно перед `=` после ключевого слова `select` (в том числе `select top N @var = ...`) или после запятой того же списка присваиваний, включая строки-продолжения многострочного списка (продолжением считается строка, начинающаяся с `@var =` или `, @var =`, пока список не завершён; завершают список строка, начинающая новую секцию — `from`, `where` — или новый оператор) и произвольные пробелы вокруг `=`; объявление `declare @var ...` ниже DEALLOCATE; FETCH другого курсора в эту переменную ниже DEALLOCATE. Именованные параметры вызова процедуры (`exec Proc @param = @var [output]`) переопределением не являются и MUST классифицироваться как использование переменной. Текст внутри строковых литералов при определении переопределения MUST игнорироваться. Вхождение переменной в правой части присваивания, а также в сравнении (`if @var = ...`, `where ... and @var = ...`, `select case when @var = ...`) переопределением не является и MUST классифицироваться как использование. Использование переменной до строки переопределения finding формирует как раньше.

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
- **THEN** строки-продолжения с переменными не формируют finding `varUseAfterCursor`

#### Scenario: Пустая или закомментированная строка внутри списка ниже DEALLOCATE не классифицируется как использование

- **GIVEN** SQL-файл с `DEALLOCATE c`, затем многострочным `FETCH c INTO @A,` / `--@CommentedVar,` / `@B` ниже DEALLOCATE, где средняя строка — чисто комментарий
- **WHEN** выполняется review
- **THEN** закомментированная строка стала пустой после удаления комментариев, список не прерван, и finding `varUseAfterCursor` на ней не формируется

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

#### Scenario: Переприсваивание SELECT с выровненным «=» ниже DEALLOCATE

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A, B FROM tX`, `FETCH c INTO @A, @B`, `DEALLOCATE c`, затем многострочным присваиванием `select @A    = isnull(Value,0),` / `       @B = isnull(Brief,'')` и последующими чтениями `@A` и `@B` (аргумент профайлера, условие `if @A in (...)`)
- **WHEN** выполняется review
- **THEN** finding `varUseAfterCursor` не формируется: строки присваивания — переопределение (пробелы вокруг `=` и строка-продолжение списка), чтения ниже — использование уже переопределённых переменных

#### Scenario: SET с отступом ниже DEALLOCATE

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем строкой `      set @A = 5` (ведущие пробелы) и последующим чтением `@A`
- **WHEN** выполняется review
- **THEN** finding `varUseAfterCursor` для `@A` не формируется: присваивание с отступом распознаётся как переопределение

#### Scenario: Именованный параметр EXEC — не переопределение

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем вызовом `exec MyProc` со строкой-продолжением `       @A = @A output` и последующим чтением `@A`
- **WHEN** выполняется review
- **THEN** сформированы finding'и с rule `varUseAfterCursor`, severity 2, для `@A` на строке EXEC-параметра и на строке чтения: именованный параметр — использование, а не переопределение

#### Scenario: Текст строкового литерала — не переопределение

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем `PROFILE_TIME_VAR1('insert into t, @A = %1!', @A)`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2, для `@A` на строке профайлера: вхождение `@A =` внутри литерала переопределением не считается, аргумент — использование

#### Scenario: Объявление переменной ниже DEALLOCATE — переопределение

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем объявлением `declare @A int` (например, в следующей процедуре файла) и использованием `@A` ниже объявления
- **WHEN** выполняется review
- **THEN** finding `varUseAfterCursor` для использований `@A` ниже объявления не формируется: переменная объявлена заново

#### Scenario: Правая часть присваивания — использование

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем строкой `set @Other = @A`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2, для `@A`: вхождение в правой части присваивания — использование

#### Scenario: Переприсваивание через FETCH другого курсора

- **GIVEN** SQL-файл с `DECLARE c1 CURSOR FOR SELECT A FROM tX`, `FETCH c1 INTO @A`, `DEALLOCATE c1`, затем `DECLARE c2 CURSOR FOR SELECT B FROM tX`, `FETCH c2 INTO @A` и последующим чтением `@A`
- **WHEN** выполняется review
- **THEN** finding `varUseAfterCursor` для `@A` не формируется: FETCH другого курсора переопределяет переменную

#### Scenario: Чтение до переопределения флагуется

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем чтением `@A` (аргумент профайлера) и ниже — присваиванием `select @A = 0`
- **WHEN** выполняется review
- **THEN** сформирован finding с rule `varUseAfterCursor`, severity 2, на строке чтения: переопределение прекращает отслеживание только для строк ниже себя

#### Scenario: Сравнение в условии — не переопределение

- **GIVEN** SQL-файл с `DECLARE c CURSOR FOR SELECT A FROM tX`, `FETCH c INTO @A`, `DEALLOCATE c`, затем строкой `if @A = 1` и строкой `select case when @A = 1 then 1 else 0 end`
- **WHEN** выполняется review
- **THEN** сформированы finding'и с rule `varUseAfterCursor`, severity 2, для `@A` на обеих строках: вхождение переменной перед `=` в сравнении (после `if` / `when`) — использование, а не левая часть присваивания

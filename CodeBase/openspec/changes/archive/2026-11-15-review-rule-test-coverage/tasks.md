## 1. Тестовый хелпер

- [x] 1.1 Добавить в `internal/review` table-driven helper для проверки правила (`wantCount`, `wantRule`, `wantSeverity`, опционально `wantLine`/`wantObject`) и убедиться, что helper покрыт собственным тестом
- [x] 1.2 Добавить проверку, что каждый набор кейсов правила содержит и `wantCount>0`, и `wantCount=0` (fail при одностороннем наборе) и проверить её тестом helper-а; guard действует для тестов, использующих helper, а требование спеки — для всех правил
- [x] 1.3 Добавить nil-guard `db == nil` в `findAPITableNames` и unit-тест guard-а; проверить `go test ./internal/review/... -run FindAPITableNames`
- [x] 1.4 Добавить nil-guard `db == nil` в `lookupProcedureCreateFiles` и unit-тест guard-а; проверить `go test ./internal/review/... -run ProcedureCreateFiles`

## 2. Правила без тестов вообще

- [x] 2.1 `procElseCase`: positive (CASE без ELSE) и negative (CASE с ELSE, CASE в комментарии, вложенный CASE), проверить `go test ./internal/review/... -run ProcElseCase`
- [x] 2.2 `truncTbl`: positive (`TRUNCATE TABLE` в процедуре — finding) и negative (процедура без `TRUNCATE TABLE`, `-- TRUNCATE` в комментарии — finding отсутствует), проверить `go test ./internal/review/... -run TruncTbl`
- [x] 2.3 `useSelectAll`: positive (`SELECT *`) и negative (явный список колонок, `SELECT *` в комментарии), проверить `go test ./internal/review/... -run UseSelectAll`
- [x] 2.4 `indexExistsInDB`: positive (индекс отсутствует) и negative (индекс существует, макрос `M_DELETE_PTABLE*`) через тестовую подмену lookup-ов, проверить `go test ./internal/review/... -run IndexExistsInDB`

## 3. Правила без проверки rule-specific логики

- [x] 3.1 `datatype`: positive и negative для веток select-assign, update-set, insert-select, fetch-into и exec-параметров; проверить `go test ./internal/review/... -run Datatype`
- [x] 3.2 `deferredUpdate`: positive (изменение колонки индекса в UPDATE) и negative (колонка вне индекса, без индекса, `#tmp`) через подмену lookup индексов; проверить `-run DeferredUpdate`
- [x] 3.3 `duplicateOutputVariable`: positive (одна переменная на два OUT-параметра) и negative (разные переменные, OUT-параметр один); проверить `-run DuplicateOutputVariable`
- [x] 3.4 `floatToStringConvert`: positive (явный `CONVERT`/`CAST` float-выражения в строковый тип) и negative (целевой тип не строковый, источник не float); проверить `-run FloatToStringConvert`
- [x] 3.5 `selectAfterSetRowcount`: positive (SELECT после `SET ROWCOUNT`) и negative (SELECT без `SET ROWCOUNT`, SELECT до него); проверить `-run SelectAfterSetRowcount`
- [x] 3.6 `indexWrong`: positive и negative на уровне правила с корректными/некорректными индексами через подмену lookup-ов; token-paste алиаса покрыт на уровне анализатора (`TestIndexWrongInputs_MacroParamTokenPasteAlias`), т.к. правило работает по raw-контенту; проверить `-run IndexWrong`
- [x] 3.7 `foreignPTablesUsing`: positive (P-таблица чужого продукта) и negative (своя P-таблица, t-таблица) через `tableProductIDCache` (после nil-guard из 1.3); проверить `-run ForeignPTablesUsing`

## 4. Правила, покрытые только integration-тестами

- [x] 4.1 `execNotExistsProc`: unit positive и negative через предзаполненный кэш каталога процедур по образцу `newAPIRuleRunner`; проверить `go test ./internal/review/... -run ExecNotExists`
- [x] 4.2 `procDuplicate`: unit positive (within-file дубль) и negative (одиночная процедура без дубля) без обращения к БД; cross-file позитив остаётся в integration; после nil-guard из 1.4; проверить `-run ProcDuplicate`
- [x] 4.3 `memoryUniqueIndexUpdate`: unit positive и negative через подмену lookup индексов памяти; проверить `-run MemoryUniqueIndexUpdate`
- [x] 4.4 `foreignTablesUsing`: unit positive и negative через `tableProductIDCache`; проверить `-run ForeignTablesUsing`

## 5. Правила с helper-only покрытием

- [x] 5.1 `aliasWhenUsingUnion`: positive и negative на уровне правила; проверить `-run AliasWhenUsingUnion`
- [x] 5.2 `columnInsert`: positive (`INSERT` без списка колонок) и negative (`INSERT` со списком колонок); проверить `-run ColumnInsert`
- [x] 5.3 `existsWithAndInIf`: positive (`IF EXISTS(...) AND ...`) и negative (без AND, AND внутри подзапроса); проверить `-run ExistsWithAndInIf`
- [x] 5.4 `inSubQuery`: positive (`IN (SELECT ...)`) и negative (список констант, `IN (@var)`, EXISTS); проверить `-run InSubQuery`
- [x] 5.5 `isNullSameTypes`: positive (IS NULL по разным типам) и negative (совместимые типы); проверить `-run IsNullSameTypes`
- [x] 5.6 `mathOperations`: positive (арифметика в BETWEEN/сравнении) и negative (без арифметики, `convert`/`cast`); проверить `-run MathOperations`
- [x] 5.7 `procParamDefValue`: positive (параметр без default при использовании) и negative (есть default/присваивание до использования); проверить `-run ProcParamDefValue`
- [x] 5.8 `saveTran`: positive (`SAVE TRAN` и полная форма `SAVE TRANSACTION`) и negative (нет `SAVE TRAN`, комментарий); проверить `-run SaveTran`
- [x] 5.9 `useDrop`: positive (`DROP` в теле процедуры) и negative (DROP вне процедуры, комментарий); проверить `-run UseDrop`
- [x] 5.10 `useFuncInIndCol`: positive (функция над индексируемой колонкой) и negative (колонка без функции, функция над неиндексируемой); проверить `-run UseFuncInIndCol`
- [x] 5.11 `varcharSize`: positive (`VARCHAR` без размера) и negative (с размером, `NVARCHAR(MAX)`); проверить `-run VarcharSize`
- [x] 5.12 `excessProcParams`: positive (лишние/повторные/позиционные лишние аргументы) и negative (корректный набор) через подмену каталога параметров; проверить `-run ExcessProcParams`

## 6. Односторонние правила

- [x] 6.1 `diffTypesComparison`: добавить negative (сравнение совместимых типов, сравнение с литералом) и проверить `-run DiffTypesComparison`
- [x] 6.2 `foreignProcedureUsing`: добавить unit positive (вызов чужой не-API процедуры) рядом с существующим negative и проверить `-run ForeignProcedures`

## 7. Сквозная проверка

- [x] 7.1 Прогнать `go test ./internal/review/...` и `go vet ./internal/review/...`, зафиксировать успешный результат
- [x] 7.2 Сверить матрицу «59 правил × (unit, positive, negative)» с каталогом и убедиться, что незакрытых правил нет
- [x] 7.3 Проверить, что integration-тесты не изменены по смыслу и по-прежнему запускаются с `-tags integration`

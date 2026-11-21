## 1. Механизм встроенных типов

- [x] 1.1 В `internal/review/review_helpers.go` в `collectVariableTypes` после сбора declare-блоков добавить fill-gaps назначение встроенных типов: при наличии `__BEGIN_PROCEDURE__` (переиспользовать `reBeginProcedure`) — `retval → int`, `trancount → int`; добавить regex для `DCL_PROC_BODY\s*\(` и набор `tran_count → int`, `err_code → int`, `msg → varchar(255)`; существующие ключи не перезаписывать (D2). Верификация: unit-тест из 2.1 зелёный
- [x] 1.2 Добавить комментарий-ссылку на источник таблицы (`fa-commonfiles\CommonFiles\SERVER\Include\MACROS.H:1346` и `:751`) и на эмпирическую проверку по `.t01`. Верификация: `go vet ./...` чистый

## 2. Тесты

- [x] 2.1 Unit-тесты `collectVariableTypes` (по образцу существующих в пакете): `__BEGIN_PROCEDURE__` → retval/trancount = int; `DCL_PROC_BODY` → tran_count/err_code = int, msg = varchar(255); явный `declare @msg varchar(1000)` переопределяет встроенный тип; без макросов встроенные типы не назначаются. Верификация: `go test ./internal/review -run TestCollectVariableTypes` зелёный
- [x] 2.2 Интеграционный тест правила `datatype` (fixture-based, как в `review_rule_coverage_test.go`): `select @RetVal = max(NTFID)` при нераскрытом `__BEGIN_PROCEDURE__` и колонке `DSIDENTIFIER` → finding `datatype` severity 3; `select @RetVal = convert(int, NTFID)` → finding отсутствует. Верификация: `go test ./internal/review -run TestDatatype` зелёный
- [x] 2.3 Прогон полного пакета и статика: `go test ./internal/review`, `go vet ./...`, `go build ./...` — зелёные

## 3. Приёмка на реальных данных

- [x] 3.1 Пересобрать `CodeBase.exe` (buildNumber 1711 → 1712) и проверить эталонный файл: `codebase review D:\GITHUB\GolandProjects\FA\fa-contracts\Consumer\SERVER\Consumer\Cons_BankPartnerInstPrepare.sql --rules datatype --json` — появились findings по строкам 997 и 1026 (`select @RetVal = max(NTFID)`, DSIDENTIFIER → int), по строке 960 (convert) — нет; прежние 3 datatype-находки (208, 273, 522) сохранены
- [x] 3.2 Контроль FP на правилах-потребителях карты типов: прогнать `CodeBase.exe review` до/после на выборке реальных файлов FA (не менее 10 macro-based процедур, включая Cons_BankPartnerInstPrepare.sql и patch-файл с инлайновым `declare @retVal int`), сравнить наборы findings по правилам `datatype`, `dateIntoString`, `emptyStringDate`, `isNullSameTypes`, `diffTypesComparison`, `floatToStringConvert`; каждое новое срабатывание классифицировать TP/FP; для patch-файла поведение неизменно
- [x] 3.3 Зафиксировать итог приёмки (числа findings до/после, TP/FP-разбор) в tasks.md ниже чек-бокса 3.3

    Итог приёмки (build 1711 → 1712, 13 файлов: 12 macro-based из `fa-contracts\Consumer\SERVER\Consumer` + `Patch\patch7_2_267.sql`; правила: datatype, dateIntoString, emptyStringDate, isNullSameTypes, diffTypesComparison, floatToStringConvert):

    - Cons_BankPartnerInstPrepare: 3 → 5 findings (datatype). Новые: `datatype:997:@RetVal` и `datatype:1026:@RetVal` — оба **TP** (DSIDENTIFIER (15,0) → int (10,0), та же категория потери точности, что существовавшие находки 208/522). Строка 960 (явный `convert(int, NTFID)`) — finding отсутствует, как и прежде.
    - 5 правил-потребителей (dateIntoString, emptyStringDate, isNullSameTypes, diffTypesComparison, floatToStringConvert): **0 findings до и после на всех 13 файлах** — новых FP нет.
    - Остальные 11 macro-based файлов: наборы findings идентичны построчно (rule:line:object). ContractExtVerify_Accept (содержит `select @RetVal = max(NTFID) from pContractPaySchedule`): 5 → 5, новой находки нет — FN из-за неизвестного индексу типа NTFID в p-таблице (в индексе от `pContractPaySchedule_TmpTbl.sql` известна только SPID DSSPID); давнее ограничение индексации p-таблиц, вне scope изменения.
    - patch7_2_267 (инлайновый `declare @retVal int`): 0 → 0, поведение неизменно.

## 4. Спеки и сборка

- [x] 4.1 `openspec validate fix-review-macro-service-vars --strict` и `openspec validate --specs --strict` — без ошибок; после archive — синхронизация основных спек (`/opsx-archive`, Sync now) и запись summary в project memory

# План покрытия парсеров и pure helpers unit-тестами

План — поэтапно добавить unit-тесты сначала для самых критичных парсеров индексатора, затем для остальных парсеров с нулевым/низким покрытием и pure helpers без зависимости от PostgreSQL.

## Цели

- Поднимать покрытие без подключения PostgreSQL и без интеграционных тестов.
- Приоритизировать код, от которого зависит качество семантического индекса: SQL/PAS/DFM/RPT, затем SMF/API/XML/TPR и indexer helpers.
- Каждый этап должен завершаться `go test ./... -coverprofile .\\coverage.out` и сравнением покрытия по затронутым пакетам.

## Стадия 1. SQL parser — `internal/parser/sql`

**Цель:** укрепить самый важный парсер процедур, таблиц, индексов и SQL-фрагментов.

**Файлы:**
- `internal/parser/sql/sql_parser.go`
- `internal/parser/sql/sql_parser_test.go`

**Сценарии тестов:**
- `ParseContent` для `CREATE PROCEDURE` / `CREATE PROC`.
- Параметры процедур и `hasProcedureParam`.
- `EXEC` / `EXECUTE` вызовы процедур.
- `SELECT`, `JOIN`, `INSERT INTO`, `UPDATE`, `DELETE FROM`.
- Временные таблицы `#tmp` и игнорируемые имена.
- `CREATE INDEX`, `CREATE UNIQUE INDEX`, поля индекса, `ASC/DESC`, `INCLUDE`.
- Не извлекать таблицы/процедуры из строковых литералов, `--` и `/* */` комментариев.
- Helper-функции: `computeBodyHash`, `normalizeIndexFields`, `splitIndexFields`, `isInsideSingleQuotedString`, `isInsideInlineComment`, `isInsideBlockComment`.

**Ожидаемый эффект:** повысить `internal/parser/sql` с ~44% до ориентировочно 60-70%.

## Стадия 2. PAS parser — `internal/parser/pas`

**Цель:** покрыть разбор Pascal unit/class/method и извлечение SQL из PAS-кода.

**Файлы:**
- `internal/parser/pas/pas_parser.go`
- `internal/parser/pas/pas_parser_test.go`

**Сценарии тестов:**
- `unit ...`, `interface`, `implementation`, `uses ...`.
- `parseUsesList` для single-line и multi-line списков.
- Class declarations, fields, methods, properties.
- `resolveQualifiedOwnerFallback`, `currentMethodName`, `currentMethodLine`.
- Implementation methods с qualified owner: `TClass.Method`.
- `stripInlinePasComments` для `{}`, `//`, `(* *)`.
- `extractSQLFromLine` и `isLikelySQL`.
- SQL fragments в строках и конкатенациях.
- Таблицы из SQL: `SELECT/JOIN/INSERT/UPDATE/DELETE`.

**Ожидаемый эффект:** повысить `internal/parser/pas` с ~46% до 60%+.

## Стадия 3. DFM parser — `internal/parser/dfm`

**Цель:** закрыть текущие 0% покрытия для форм, компонентов и SQL внутри DFM.

**Файлы:**
- `internal/parser/dfm/dfm_parser.go`
- создать `internal/parser/dfm/dfm_parser_test.go`

**Сценарии тестов:**
- `object MainForm: TMainForm` и `inherited MainForm: TBaseForm`.
- Root form: `FormName`, `FormClass`, `Caption`, `LineStart`, `LineEnd`.
- Вложенные компоненты: `ComponentName`, `ComponentType`, `ParentName`.
- `Name = '...'` переопределяет имя объекта.
- DFM captions со строками и numeric char literals `#1040#1041`.
- `Lines.Strings = (...)`, `Lines = (...)`, `SQL.Strings = (...)`.
- Inline SQL в property value.
- `isLikelySQLText`, `extractTablesFromSQL`, `isKeyword`, `isIgnoredTableName`.
- Escaped quotes `''`, пустые строки, комментарии `{ ... }`.

**Ожидаемый эффект:** поднять `internal/parser/dfm` с 0% до 70%+ за счёт pure parsing tests.

## Стадия 4. RPT parser — `internal/parser/rpt`

**Цель:** закрыть 0% покрытия для отчётов, параметров, SQL-фрагментов и VB-функций.

**Файлы:**
- `internal/parser/rpt/rpt_parser.go`
- создать `internal/parser/rpt/rpt_parser_test.go`

**Сценарии тестов:**
- Имя отчёта из пути: `reportNameFromPath`.
- Root object: `FormName`, `FormClass`, `ReportName`, `LineStart`, `LineEnd`.
- Параметры отчёта: `DsDateTimePicker`, `DsFormLookup`, `TCheckBox`, `TMaskEdit`, `TComboBox`.
- Свойства параметров: `LookupForm`, `FieldName`, `DataType`, `Required`, `Mask`.
- `trimQuoted` и escaped quotes.
- `Lines.Strings = (...)` и `SQL.Strings = (...)` как `QueryFragment`.
- `TDsHugeBox` должен давать context `rpt_named_sql`, прочие — `rpt_textbox_sql`.
- `Script.Strings = (...)` с несколькими `Sub`/`Function`.

**Ожидаемый эффект:** поднять `internal/parser/rpt` с 0% до 70%+.

## Стадия 5. SMF parser — `internal/parser/smf`

**Цель:** усилить уже частично покрытый SMF-парсер в местах с низким покрытием.

**Файлы:**
- `internal/parser/smf/smf_parser.go`
- `internal/parser/smf/smf_parser_test.go`

**Сценарии тестов:**
- `ParseContentWithBasePath` с include-файлами во временной директории.
- `buildIncludeCandidates` для относительных/абсолютных путей и Windows-разделителей.
- `isValidEncoding`.
- `readIncludeFile` для существующего и отсутствующего файла.
- `extractSMFVarsFromIncludeFile`.
- `extractWithBlock` для корректного и незакрытого блока.
- `findHelperFunctionBody`.

**Ожидаемый эффект:** повысить `internal/parser/smf` с ~64% до 75%+.

## Стадия 6. API macro parser — `internal/parser/apimacro`

**Цель:** быстро покрыть компактный parser с текущими 0%.

**Файлы:**
- `internal/parser/apimacro/parser.go`
- создать `internal/parser/apimacro/parser_test.go`

**Сценарии тестов:**
- `API_CREATE_PROC(...)` → `create_proc`.
- `API_INIT_EVENT(...)` → `init_event`.
- `API_EXEC(...)` → `exec_contract`.
- T01 `exec ... @ProcessID = @GlobalProcessID` → `dispatches_to`.
- Исключение `GetAPIProcessID`.
- `detectProcedureName`: `__BEGIN_PROCEDURE__(...)`, `create procedure ...`, fallback из имени файла.
- `MustParseContent` happy path.

**Ожидаемый эффект:** поднять пакет с 0% до 80-90%.

## Стадия 7. TPR parser — `internal/parser/tpr`

**Цель:** закрыть 0% покрытия компактного parser-а привязок отчётов.

**Файлы:**
- `internal/parser/tpr/tpr_parser.go`
- создать `internal/parser/tpr/tpr_parser_test.go`

**Сценарии тестов:**
- `ParseContent` для типовых TPR строк.
- `splitCSVLike` с кавычками, запятыми внутри кавычек, пустыми полями.
- `normalizeReportToken`.
- `reportNameFromPath` для Windows/Unix путей.
- `isValidEncoding`.

**Ожидаемый эффект:** поднять пакет с 0% до 70-85%.

## Стадия 8. DSXML parser — `internal/parser/dsxml`

**Цель:** покрыть XML/API/BO parser без реальной БД.

**Файлы:**
- `internal/parser/dsxml/parser.go`
- создать `internal/parser/dsxml/parser_test.go`

**Сценарии тестов:**
- `xmlRootName` для разных XML roots.
- `classifyPath` для API contract/business object путей.
- `ownerModuleFromPath`.
- `ParseContent` для минимального XML contract.
- `ParseContent` для минимального business object XML.
- Обработка charset через `xmlCharsetReader`/`unmarshalXML`.
- Невалидный XML должен возвращать ошибку.

**Ожидаемый эффект:** поднять пакет с 0% до 60%+.

## Стадия 9. Indexer pure helpers — `internal/indexer`

**Цель:** покрыть чистую логику индексатора без PostgreSQL и без моков store.

**Файлы:**
- `internal/indexer/indexer.go`
- `internal/indexer/indexer_sql_pas.go`
- `internal/indexer/indexer_postprocess_pas.go`
- `internal/indexer/indexer_relations.go`
- существующие/новые `*_test.go` в `internal/indexer`

**Сценарии тестов:**
- `ScanStats.Add`, `ScanStats.Snapshot`, `mergeScanStats`.
- `normalizeParallel` edge cases.
- `cloneInt64Map` deep copy.
- `addPendingSQLCalls` / `snapshotPendingSQLCalls`.
- PAS pending helpers: `addPendingMethod`, `addPendingClass`, `addPendingField`.
- `buildIncludePathCandidates`.
- Дополнительные edge cases для `extractProcedureCallsFromQuery`.
- `mapQueryFragmentParentRelationType` для всех parent types.
- `findProcedureForLine`.
- `hasGlobalProcessIDBinding`.
- Дополнительные `parseSelectIntoFragmentInfo`, `inferSelectIntoOutputName`, `splitSQLByTopLevelCommaLocal` кейсы.

**Ожидаемый эффект:** повысить `internal/indexer` без БД, ориентировочно до 20-30%.

## Стадия 10. Финальная проверка и фиксация прогресса

**Действия:**
- Запустить `go test ./... -coverprofile .\\coverage.out`.
- Сформировать `go tool cover -func .\\coverage.out`.
- Сравнить покрытие с базой: общий baseline `25.4%`.
- Обновить краткий отчёт: какие пакеты выросли, какие остались DB-зависимыми.

## Не включать в этот план

- Integration tests для `internal/store` и DB-heavy методов `internal/query`.
- Рефакторинг под mock DB.
- Тестирование CLI end-to-end.
- Изменение production-кода без необходимости для тестируемости.

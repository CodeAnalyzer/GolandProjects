# Proposal: fix-review-block-comment-masking

## Why

Правила `review`, сканирующие сырой текст файла, не отслеживают состояние блочного комментария `/* ... */` и формируют findings по закомментированному SQL-коду. Реальный кейс: `FA/fa-generalledger/.../TmpTbl_GLReports.sql:68` — `insertRowLock` срабатывает на INSERT внутри `/* ... */` (строки 67–79). Измерение на fa-contracts (11 991 SQL-файл): 525 файлов содержат SQL-ключевые слова внутри блочных комментариев; прогон raw-правил по 247 файлам с `insert` в комментариях дал 87 ложных срабатываний на 54 файлах: `insertRowLock` 76, `deferredUpdate` 7, `inSubQuery` 2, `useSelectAll` 2. Класс зафиксирован как кандидат-последователь в change `fix-review-block-comment-order` (build 1711, Risks), который тогда правил только порядок обработки уже существующих state-machine.

## What Changes

- Ввести сквозную маскировку блочных комментариев (и хвостовых строчных `--` внутри строк кода) в raw-сканерах review, у которых её нет:
  - построчные сканеры: `insertRowLock`, `useSelectAll`, `truncTbl`;
  - операторные сканеры на общем хелпере `splitStatementsWithOffsets` (маскирует строки и скобки, но не комментарии): `deferredUpdate`, `memoryUniqueIndexUpdate`, `inSubQuery`, `tooManyJoins`;
  - курсорные парсеры `varUseAfterCursor` (`parseAllFetchIntoStatements`, `parseDeallocateStatements`, `parseCursorDeclarations` матчат сырые строки без состояния блока — пробел подтверждён по коду): маскировка входа `checkVarUseAfterCursor`.
- Поведение для кода вне комментариев не меняется: 665 настоящих findings на измеренном корпусе должны сохраниться построчно (`rule:line` идентичен; для `Object`, производного от текста оператора — например первая строка у `tooManyJoins` — допускается нормализованная сверка); находки, которые маскировка впервые делает видимыми (устранение FN вида `truncate /* c */ table t`), перечисляются явно как ожидаемые новые.
- Правила с уже реализованной маскировкой не затрагиваются: `apiExecOutsideFacade` и `debugProfileEnabled` (санитизация лексером `sanitizeSQLForMacroScan`), `columnInsert` (`findInsertWithoutColumns` — char-level FSM), `ansiInJoin` и `modifyOutProc` (`maskBlockCommentsKeepLines`), 12 сканеров из build 1711; `shouldBeCP866` не затрагивается принципиально (проверяет байты файла целиком, байты комментария легитимно входят в кодировку).
- Scope определён эмпирически по измеренному корпусу fa-contracts (247 файлов с `insert` в комментариях): прочие raw-сканеры вне измеренного набора (например `emptyStringDate`) и ~27 правил на `parsed *sqlparser.ParseResult` — отдельные кандидаты, не покрытые этим change-ом.
- Регрессионные тесты: многострочный `/* ... */` с триггером каждого правила; комментарий внутри оператора (между `insert` и `select`); хвостовой `-- ...` на строке кода; незакрытый блок; e2e на реальном `TmpTbl_GLReports.sql` (0 findings).
- Поднять `buildNumber` с 1712 до 1713.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/deploy-stoppers`: новое требование о маскировке блочных (и хвостовых строчных) комментариев в шести сканерах без отслеживания состояния: `insertRowLock`, `useSelectAll`, `truncTbl`, `deferredUpdate`, `memoryUniqueIndexUpdate`, `inSubQuery`.
- `review/postgre-requirements`: аналогичное требование для `tooManyJoins` и `varUseAfterCursor` (пробел последнего подтверждён по коду: парсеры курсора матчат сырые строки).

## Impact

- `internal/review/review_rules.go` — циклы `checkInsertRowLock`, `checkUseSelectAll`, `checkTruncTbl`, `checkVarUseAfterCursor` (маскировка входа).
- `internal/review/review_parser.go` — `splitStatementsWithOffsets` (маскировка до разбиения на операторы), новый length-preserving mask-хелпер либо переиспользование существующих.
- `internal/review/*_test.go` — регрессионные тесты по правилам.
- `cmd/root.go` — `buildNumber` 1712 → 1713.
- Схема БД, публичные команды и MCP-контракт не меняются.

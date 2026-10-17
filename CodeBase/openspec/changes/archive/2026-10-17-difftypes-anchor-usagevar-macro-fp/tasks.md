## 1. Helper: @@-guard в containsVarReference

- [x] 1.1 Переписать `containsVarReference` (`internal/review/review_helpers.go`) на `FindAllStringIndex` с отбраковкой вхождений, которым предшествует байт `@` (RE2 не поддерживает lookbehind); проверить unit-тестом `TestContainsVarReference_SystemVarNoMatch`: `"@@rowcount"` не матчится как `@rowcount`, `@RowCount + 1` матчится
- [x] 1.2 Тест правила: SQL с `SELECT @RetVal = @@error, @RowCount = @@rowcount` → finding `usageVarInSameSelect` не формируется (раньше — FP из-за подстроки)

## 2. usageVarInSameSelect: вырезание standalone-макросов

- [x] 2.1 Добавить helper `stripStandaloneMacroLines` (построчный regexp `(?i)^\s*M_[A-Z0-9_]+\s*\(.*\)\s*$`, замена строки на пустую с сохранением `\n`) в `internal/review/review_helpers.go`; unit-тест: строка `M_BUSINESSLOG_PARAM(@RowCount, DSINT_KEY, INT)` вырезается, строка `from tX M_NOLOCK_INDEX(XPK)` — нет, `M_FORCEORDER` — нет, нумерация строк сохраняется
- [x] 2.2 Применить helper в `checkUsageVarInSameSelect` (`review_rules.go`) до `truncateAtStatementBoundary`/`parseSelectAssignStatement`; тест: фрагмент `SELECT @RowCount = @@rowcount, @RetVal = @@error` + glued `M_BUSINESSLOG_PARAM(@RowCount, ...)` / `M_BUSINESSLOG_PARAM(@RetVal, ...)` → finding не формируется
- [x] 2.3 Прогнать существующие тесты `TestCheckUsageVarInSameSelect_*` (CrossRef, FromClause, ReverseOrder, Multiline, SingleAssignment, CaseExpression, NoCrossRef) — все зелёные

## 3. diffTypesComparison: реанкор строки сравнения

- [x] 3.1 Добавить helper offset-поиска: первая строка `fragment.QueryText` (case-insensitive), содержащая оба trimmed-операнда comparison; fallback 0; unit-тест на многострочном QueryText
- [x] 3.2 Применить в fragment-пути `checkDiffTypesComparison` (обе точки: fast-path переменных review_rules.go:5920 и DB-путь :5949) — `Line = fragment.LineNumber + offset`; IF-построчный путь не трогать; тест: `M_DELETE_PTABLE(pX)` + glued `INSERT INTO pX ... INNER JOIN ... ON a.Col1 = b.Col2` (разные типы колонок) → finding с line строки ON, а не строки макроса
- [x] 3.3 Прогнать существующие тесты `diffTypesComparison` — все зелёные (WHERE/IF/CASE-пути не сместились)

## 4. Контроль и сборка

- [x] 4.1 `go build ./...`, `go vet ./...`, `go test ./...` — без ошибок
- [x] 4.2 Контрольный прогон review на `FA/fa-contracts/Consumer/SERVER/Accrual` (базлайн build 1600): дифф содержит только line-shift `diffTypesComparison` и исчезнувшие FP `usageVarInSameSelect`; каждый пункт диффа разобран и объяснён
- [x] 4.3 Поднять build number до 1603 (`cmd/root.go`), пересобрать `CodeBase.exe`; контрольный `codebase review .../CreditTurnoverLink_MassInsert.sql` → ровно 1 finding `diffTypesComparison` с line=242 (строка ON), FP `usageVarInSameSelect` отсутствует

# Proposal: fix-cursor-redeclare-positional-scope

## Why

Паттерн Diasoft «переобъявление курсора» (`DECLARE → OPEN → FETCH → CLOSE → DEALLOCATE → DECLARE` того же имени в той же процедуре, типично для `tmp_*`-курсоров) ломает три правила review: `parseCursorDeclarations` возвращает `map[имя]→декларация`, где последняя декларация затирает предыдущие, а `checkVarUseAfterCursor` сканирует до EOF без границы переобъявления. На `FA/fa-administrator/.../InstrumentSO_LoadStates.sql` это даёт 45 ложных находок из 56: 2 FP `cursorFetchArguments`, 41 FP `varUseAfterCursor` и 2 mispairing-FP `datatype` (FETCH-переменные спариваются с выражениями чужой декларации по индексу). Отдельная находка `varUseAfterCursor` для `@ChangedBrief` (строка 652) остаётся: это известный substring-дефект (`@ChangedBriefs` содержит `@ChangedBrief` как подстроку), вне скоупа этого изменения.

## What Changes

- `parseCursorDeclarations` возвращает упорядоченный список деклараций с номером строки (`cursorDeclaration.Line`); map-представление строится поверх для name-set потребителей.
- `parseAllFetchIntoStatements` возвращает упорядоченный список FETCH-операторов со спаном строк (`LineStart`/`LineEnd`) вместо map со склеенными переменными.
- `checkCursorFetchArguments`: каждый FETCH сопоставляется с ближайшей декларацией того же имени выше по файлу (позиционное сопоставление), а не с единственной записью map.
- `checkDatatypeFetchInto`: FETCH-переменные спариваются с выражениями ближайшей декларации выше по файлу (устраняет mispairing).
- `checkVarUseAfterCursor`: окно анализа после DEALLOCATE ограничивается сверху следующей декларацией того же курсора; строки-продолжения многострочных `FETCH ... INTO` не считаются использованием переменных.

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требование «Несовпадение аргументов FETCH с DECLARE CURSOR» — сопоставление FETCH с декларацией становится позиционным (ближайшая декларация выше); требование «Использование переменной после закрытия курсора» — окно анализа ограничивается переобъявлением курсора, строки-продолжения FETCH исключаются.
- `review/fine-code`: требование «Потенциальная потеря точности» — спаривание переменных FETCH с выражениями SELECT cursорной декларации выполняется по ближайшей декларации выше по файлу.

## Impact

- Код: `internal/review/review_parser.go` (`parseCursorDeclarations`, `parseAllFetchIntoStatements`, типы), `internal/review/review_rules.go` (`checkCursorFetchArguments`, `checkDatatypeFetchInto`, `checkVarUseAfterCursor`), `internal/review/types.go` (`cursorDeclaration`, новый `fetchStatement`), адаптация name-set потребителей (`checkTableExistsInMetadata`, `checkViewUsageInServerCode`, `isTableExistsNoiseName`), тесты `internal/review/review_rules_test.go`.
- Поведение: три правила перестают давать FP на файлах с переобъявлением курсоров; реальная семантика правил (детект рассинхрона аритмии, использование переменных после dealloc, потеря точности FETCH) сохраняется.
- Зависимости: нет новых внешних зависимостей; схема БД и CLI-контракты не меняются.

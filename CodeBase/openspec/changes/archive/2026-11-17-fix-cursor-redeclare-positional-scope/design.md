# Design: fix-cursor-redeclare-positional-scope

## Context

Текущая реализация (см. proposal.md — Why) опирается на два name-keyed представления:

- `parseCursorDeclarations` → `map[normalizedName]cursorDeclaration` (review_parser.go:280): при повторном `__DECLARE_CURSOR__`/`DECLARE ... CURSOR` последняя декларация затирает предыдущие; в `cursorDeclaration` (types.go:384) нет номера строки.
- `parseAllFetchIntoStatements` → `map[normalizedName][]string` (review_parser.go:376): переменные всех FETCH'ей всех жизней курсора склеиваются в один список.

Потребители: `checkCursorFetchArguments` (review_rules.go:5711) и `checkDatatypeFetchInto` (:90) сравнивают/спаривают FETCH с единственной записью map; `checkVarUseAfterCursor` (:5044) сканирует от DEALLOCATE до EOF и skip'ает строки только при совпадении «ключевое слово + имя курсора» в одной строке. `checkTableExistsInMetadata` (:2006) и `checkViewUsageInServerCode` (:2098) используют map только как name-set (`isTableExistsNoiseName`, :1980).

Эталонный кейс — `FA/fa-administrator/Administrator/Server/Core/InstrumentSO_LoadStates.sql`: `tmp_Node_cursor` объявлен 3 раза (138/185/213), `StateTransition_cursor` — 2 раза (292/390); полный review даёт 56 находок, из которых 45 — FP трёх правил (2 `cursorFetchArguments`, 41 `varUseAfterCursor`, 2 `datatype`-mispairing). Находка `varUseAfterCursor` для `@ChangedBrief` (652) — отдельный substring-дефект (`@ChangedBriefs` содержит `@ChangedBrief`), остаётся как есть.

Контент: все курсорные правила уже работают на **одном expanded-тексте** — `fileContent()` возвращает `r.exec.content` = `macroResult.Content` (runner.go:473), `fileProcessedContent()` возвращает тот же `macroResult.Content` (runner.go:609). `fragment.LineNumber` и строки деклараций/dealloc находятся в одних координатах. `rawFileContent()` (исходник до `replaceMacros`) в курсорных правилах не используется и использовать его нельзя — это создало бы рассинхрон нумерации.

## Goals / Non-Goals

**Goals:**

- Позиционная модель: декларации курсоров — упорядоченный список с номерами строк; FETCH-операторы — упорядоченный список со спаном строк.
- `cursorFetchArguments` и `datatype` (ветка FETCH INTO) сопоставляют FETCH с ближайшей декларацией того же имени выше по файлу.
- `varUseAfterCursor`: окно после DEALLOCATE ограничивается следующей декларацией того же имени; строки-продолжения многострочных FETCH не считаются использованием переменных.
- Name-set потребители (`tableExistsInMetadata`, `viewUsageInServerCode`) продолжают работать без изменения поведения.

**Non-Goals:**

- Межпроцедурный/межфайловый scoping курсоров — review анализирует файл автономно.
- Изменение правила `useOnlyDeclaredCursors` (per-procedure name-set: собирает все DECLARE процедуры в map и проверяет наличие имени; позиционным не является).
- Переписывание `varUseAfterCursor` на AST — остаётся построчной эвристикой с уточнёнными границами.
- Фиксы не связанных с курсорами правил этого файла (`inSubQuery`, `useFuncInIndCol`, `execNotExistsProc`).

## Decisions

1. **Первичное представление — упорядоченный список, map — производное.**
   `cursorDeclaration` получает поле `Line` (1-based, строка DECLARE/макроса); `parseCursorDeclarations` возвращает `[]cursorDeclaration` в порядке следования. Для name-set потребителей добавляется тонкая обёртка (например, `cursorDeclarationSet(decls) map[string]cursorDeclaration`), сигнатура `isTableExistsNoiseName` не меняется. Альтернатива — перевести всех потребителей на список — даёт больше правок без изменения поведения.

2. **Сопоставление FETCH ↔ декларация: ближайшая декларация выше.**
   Общий хелпер `nearestCursorDeclaration(decls, name, line)`: последняя декларация того же имени с `decl.Line <= fetch.Line`; FETCH выше первой декларации — skip без finding. DEALLOCATE-граница в сопоставлении не используется: в ASE переобъявление без dealloc невозможно, поэтому ближайшая декларация сверху уже даёт корректную семантику (меньше состояний — меньше FP). Используется и в `checkCursorFetchArguments`, и в `checkDatatypeFetchInto`. Строка самого FETCH вычисляется по смещению префикса внутри `fragment.QueryText` (`reParserFetchIntoPrefix.FindStringSubmatchIndex` уже возвращает offset; `fetchLine = fragment.LineNumber + strings.Count(queryText[:offset], "\n")`) — прецедент `findAssignmentLineOffset`; `fragment.LineNumber` может указывать на склеенный `open`-оператор.

3. **`parseAllFetchIntoStatements` → `[]fetchStatement{CursorName, Variables, LineStart, LineEnd}` с унифицированным разбором.**
   Текущий `reParserFetchNextIntoPrefix` применяется к одной строке и требует `into` в той же строке — формы `__FETCH_NEXT__ c` / `into @A` (строки 158/308 эталона) парсер не видит. Разбор унифицируется: собираем строки оператора (до `isCursorDeclarationBoundary`) и прогоняем через `parseFetchIntoStatement` (его `reParserFetchIntoPrefix` неанкерный, multiline). `LineStart` — строка FETCH; `LineEnd` — номер последней строки, **реально давшей переменную** (не индекс выхода цикла сбора продолжений, иначе спан захватит строку-границу или не-переменные строки и скроет реальные использования). Дедупликация: в `checkVarUseAfterCursor` добавляется `seen` с ключом `line|normalizedVar|cursor` (сегодня дедупликации нет).

4. **`checkVarUseAfterCursor`: границы жизни курсора.**
   Источник деклараций — `parseCursorDeclarations(text)` на том же expanded-тексте, что `parseDeallocateStatements`/`parseAllFetchIntoStatements` (сегодня правило декларации не парсит вовсе). Для каждого DEALLOCATE курсора `c` окно сканирования = строки `(dealloc.Line, следующая декларация c ниже dealloc.Line]` — при 0-based `i` условие `i+1 < nextDecl.Line`, иначе строка следующей декларации попадёт в окно; если декларации ниже нет — до EOF. Множество переменных жизни = переменные FETCH'ей `c` со спаном между предыдущей декларацией `c` (или началом файла) и dealloc. Skip-условия: line-спаны FETCH'ей `c` внутри окна + существующие «ключевое слово + имя» фильтры. Альтернатива — оставить union всех переменных, но ограничить окно — слабее: переменные прошлых жизней продолжали бы отравлять окно текущей.

5. **Единый expanded-контент для всех структур правила.**
   Все курсорные правила парсят один и тот же текст: `fileContent` == `fileProcessedContent` == `macroResult.Content`. Смешение с `rawFileContent` в одном правиле запрещено (рассинхрон нумерации после раскрытия макросов).

6. **Границы при неразобранном SELECT и стоп-строки FETCH (по итогам code review).**
   Декларация курсора фиксируется в списке даже когда `parseSelectSourceStatement` не смог разобрать SELECT (`select 1` без FROM): строка декларации остаётся границей жизни для позиционных окон и name-set, а правила, сравнивающие аритмету (`cursorFetchArguments`), пропускают декларации с пустыми `SelectExpressions` — арность неизвестна. Сбор строк оператора `FETCH ... INTO` останавливается не только на `isCursorDeclarationBoundary`, но и на строке, начинающей новый SQL-оператор (`select`/`exec`/`execute`/`print`/`set`/`insert`/`update`/`delete`/`goto`, предикат `isFetchStatementStopLine`): при склейке через пробел следующий оператор захватывался в `Variables`/`LineEnd` (tail-boundary регэксп ищет `\n`), поэтому строки оператора соединяются через `\n`, а список ключевых слов `reParserFetchIntoBoundary` расширен теми же операторами.

## Risks / Trade-offs

- [Унификация разбора FETCH вскроет ранее невидимые парсеру FETCH'и (`into` на новой строке) и изменит множества переменных] → приёмка по эталону подтверждает итог (окно DEALLOCATE 387 пустеет за счёт границы на decl 390, не за счёт пропуска FETCH 308); спаны покрыты unit-тестами (однострочный, многострочный, `into` на новой строке, FETCH перед `if`, FETCH в конце файла).
- [Спан `LineEnd` «съедет» на строку-границу или не-переменные строки → skip-спаны скроют реальные использования] → `LineEnd` = последняя строка, реально давшая переменную; парсерные тесты на спаны.
- [Регрессия: исчезнут «полезные» находки, пересекавшиеся с окнами] → приёмка по эталонному файлу: из 56 находок должны уйти ровно 45 FP (2+41+2); `varUseAfterCursor` для `@ChangedBrief` (652, substring-дефект вне скоупа) и прочие не-курсорные правила остаются. Плюс unit-тесты на каждый сценарий из дельт; существующие тесты `checkVarUseAfterCursor` (review_rules_test.go:3153–3293, включая `MultilineFetchInto_Finding`) остаются валидными.
- [Nearest-decl эвристика даст FN, если FETCH стоит до декларации валидного SCOPE-блоками кода] → такие FETCH'и сейчас вообще не проверяются (map-поведение); новое поведение строго не хуже.
- [Изменение сигнатуры `parseCursorDeclarations` заденет тест] → тест в review_rules_test.go:1266 обновляется на список в рамках tasks.

## Migration Plan

Изменение локально для `internal/review` + тестов; схема БД и CLI-контракты не затрагиваются. Развёртывание — обычная сборка; откат — git revert без миграций.

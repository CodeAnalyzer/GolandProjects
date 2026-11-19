# Proposal: fix-fetch-tail-whitelist-union

## Why

После fix-fetch-into-macro-tail батч fa-contracts (1049 файлов с FETCH INTO, build 1708) оставляет 5 находок `cursorFetchArguments` — все FP двух pre-existing классов, верифицированных вручную (0 реальных рассинхронов):

- **Класс A — не-`M_` вызовы после FETCH.** `PROFILE_TIME_EX('...')` после `fetch LoanAccountLinks into @ObjectID, ..., @RelType` (API_LN_AfterRuleA_MassCrtAcc.sql:118) — не ключевое слово и не `m_` → не граница → хвост попадает в CSV переменных → анкорный `reParserFetchIntoVar` роняет последнюю переменную («4 vs 5»). Эмпирика: хвосты `PROFILE_TIME_*` (VAR1/EX) — 4 файла fa-contracts + 2 файла fa-administrator; при одно-переменном FETCH дефект скомпенсирован («1 vs 1», невидим), но даёт скрытый FN `varUseAfterCursor`: строка `PROFILE_TIME_VAR1('...', @Date)` продлевает спан FETCH и прячет реальное чтение переменной.
- **Класс B — UNION в DECLARE CURSOR без FROM в первой ветке.** `parseSelectSourceStatement` режет selectPart по первому top-level `from`; при union-декларации, где from только во второй ветке (`select @Perc, @DateTmp1 union select PrcValue, PrcDate from pX`), selectPart захватывает `union select ...` → CSV даёт 3 «выражения» вместо 2 (Cons_CalcAnnScheme8.sql:111/117/200/206, «2 vs 3»). Эмпирика: 7 файлов fa-contracts + 3 файла fa-payments (Rapida); при одноколоночных ветках без запятых арность случайно верна (молчат), но спаривание `datatype` по искажённым выражениям остаётся некорректным.

Корневая общность класса A: распознавание границы по чёрному списку (ключевые слова + `m_`-макросы) бесконечно догоняет зоопарк хвостов (`while`, `M_*`, `PROFILE_TIME_*`, ...). Правильная семантика — белый список: список переменных FETCH INTO состоит только из `@`-переменных, разделённых запятыми, значит любая строка, не начинающаяся с `@` или `,` (после стрипа комментариев), не может быть продолжением списка и является границей оператора.

## What Changes

- `isFetchVariablesContinuationLine` — новый предикат белого списка (строка-продолжение ⟺ после trim начинается с `@` или `,`; пустая после стрипа комментариев строка — прозрачное продолжение, список не прерывает); `isFetchStatementStopLine` переформулируется как инверсия с дополнительным продолжением-заголовком `into` — многострочная форма `fetch c` / `into @A, @B` (238 файлов FA) сохраняется (file-путь `parseAllFetchIntoStatements`).
- `findFetchIntoTailBoundary` переключается с регэкспа `reParserFetchIntoBoundary` на построчный скан по белому списку с пропуском пустых (после стрипа) строк (fragment-путь `parseFetchIntoStatement`); Go RE2 без lookahead не позволяет выразить white-list одним регэкспом.
- Удаляются `reParserFetchIntoBoundary`, `standaloneMacroCallPattern`, `reParserStandaloneMacroCall`, `isStandaloneMacroCallLine` — белый список покрывает `M_*` (в том числе без скобок — `M_FORCEORDER`, `M_CHECK_REQUIRED`) и все прочие вызовы (`PROFILE_TIME_*`, `max(...)`) единым правилом.
- `parseSelectSourceStatement`: арность UNION-запроса = арность первой ветки — граница selectPart = `min(firstTopLevel("from"), firstTopLevel("union"))`; при отсутствии обоих ключевых слов декларация не распознаётся (прежнее поведение); FromClause отсекается от этой границы (алиасы/таблица второй ветки резолвятся прежними `\bfrom\s+`-регэкспами).
- Эффекты: класс A/B FP `cursorFetchArguments` устранены (эталоны API_LN:118, Cons_CalcAnnScheme8 → 0); `datatype` перестаёт спаривать переменные с мусорными union-«выражениями» вида `Amount UNION SELECT X` — там, где первая ветка содержит bare-колонки, резолвимые через таблицу второй ветки, появляются корректные пары (на union-декларациях с `@`-литералами первой ветки, как Cons_CalcAnnScheme8, новых `datatype`-находок может не быть — типы литералов не резолвятся); `varUseAfterCursor` видит аргументы-переменные в не-`M_` хвостах ниже DEALLOCATE (устранение FN, отражено дельтами).

## Capabilities

### New Capabilities

(нет)

### Modified Capabilities

- `review/postgre-requirements`: требование «Несовпадение аргументов FETCH с DECLARE CURSOR» — граница списка переменных обобщена с `M_*`-макросов на любую строку, не начинающуюся с `@`/`,`; добавлена union-арность первой ветки декларации. Требование «Использование переменной после закрытия курсора» — граница строки-продолжения переформулирована по белому списку.
- `review/fine-code`: требование «Потенциальная потеря точности» — те же обобщения границы и union-арности для спаривания.

## Impact

- Код: `internal/review/review_parser.go` (удаление 4 сущностей, `isFetchStatementStopLine`, `findFetchIntoTailBoundary`, `parseSelectSourceStatement`, актуализация doc-комментариев), тесты `internal/review/review_rules_test.go` (замена `TestIsStandaloneMacroCallLine`, новые кейсы).
- Поведение расширяется детерминированно: всё, что было границей, остаётся границей (`M_*`-сценарии из fix-fetch-into-macro-tail сохраняются); новым классом границ становятся любые не-`@`/`,` строки. Сохраняются: многострочный заголовок FETCH с `into` на следующей строке (регрессионные Span-тесты `TestParseAllFetchIntoStatements_Spans`) и прозрачность пустых/чисто-комментарных строк внутри списка переменных (регрессионный тест `TestParseAllFetchIntoStatements_BlockCommentAndEmptyLines`; 12 файлов FA вне fa-contracts с паттерном «запятая, затем закомментированная переменная» — non-contracts регресс в приёмке).
- Эмпирика fa-contracts (100 multiline-хвостов после FETCH INTO): только `while` (~70), `m_businesslog_*` (4), `profile_time_*` (4) — легитимных не-`@`/`,` продолжений не обнаружено.
- Новые находки после фикса (datatype на union-курсорах, varUseAfterCursor на профайл-хвостах ниже DEALLOCATE) — верифицируются вручную как реальные в приёмке; на Cons_CalcAnnScheme8 допустимо отсутствие новых `datatype`-находок (первая ветка из `@`-литералов).
- `isCursorDeclarationBoundary` не меняется: у SELECT декларации свой лексикон продолжений (выражения), белый список там неприменим.
- Зависимости: нет новых; схема БД и CLI-контракты не меняются.

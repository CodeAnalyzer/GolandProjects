# Design: fix-fetch-tail-whitelist-union

## Context

После fix-fetch-into-macro-tail остаются два класса FP `cursorFetchArguments` (батч fa-contracts 1049 файлов, build 1708: 5 находок, 0 реальных рассинхронов): класс A — не-`M_` standalone-вызовы после FETCH (`PROFILE_TIME_EX` в API_LN_AfterRuleA_MassCrtAcc.sql:118 теряет `@RelType`); класс B — UNION в SELECT декларации без from в первой ветке (Cons_CalcAnnScheme8.sql: `parseSelectSourceStatement` режет selectPart по первому top-level `from`, который лежит во второй ветке → CSV даёт 3 «выражения» вместо 2).

Эмпирика (multiline-скан fa-contracts, 100 хвостов после FETCH INTO): `while` ~70 (уже стоп-слово), `m_businesslog_*` 4 (покрыто `m_`), `profile_time_var1`/`profile_time_ex` 4 (не покрыто). fa-administrator: ещё 2 файла с профайл-хвостами; fa-payments: 0. UNION-DECLARE: 7 файлов fa-contracts + 3 fa-payments (Rapida); FP выстреливает только при запятых во 2+ ветках, одноколоночные ветки молчат, но спаривание `datatype` остаётся искажённым. Компенсации: класс A невидим при одно-переменном FETCH (BaseAlg* ×3 файла), но даёт скрытый FN `varUseAfterCursor` (профайл-строка с аргументом-переменной продлевает спан FETCH и прячет реальное чтение).

Решение по классу A (выбор из explore-сессии): **белый список** вместо чёрного. Список переменных FETCH INTO состоит только из `@`-переменных, разделённых запятыми — продолжением может быть только строка, начинающаяся с `@` или `,`; всё остальное — граница. Чёрный список (ключевые слова → `m_` → `PROFILE_TIME_*` → ...) бесконечно догоняет зоопарк хвостов.

Два инварианта существующего поведения, которые наивная инверсия ломает (найдены change-ревью, подтверждены кодом и эмпирикой):

- **Многострочный заголовок FETCH**: `parseAllFetchIntoStatements` применяет stop-предикат ко всем строкам после FETCH-строки, включая строку `into @A, @B` (238 файлов FA / 480 вхождений `fetch`-строка + `into` на следующей; фиксаторы — Span-тесты `fetch next from c into` / `__FETCH_NEXT__ c` / `fetch c`). Наивная инверсия считает `into ...` границей → оператор теряется целиком (тихий FN `varUseAfterCursor`; батч `cursorFetchArguments` его не ловит — идёт по фрагментам).
- **Прозрачность пустых/чисто-комментарных строк внутри списка переменных**: `stripCommentsFromText` превращает `--@CommentedVar,` в пустую строку; наивный построчный скан в `findFetchIntoTailBoundary` считает пустую строку не-`@`/`,` → границей → список обрезается (фиксатор — `TestParseAllFetchIntoStatements_BlockCommentAndEmptyLines`; реальные файлы: 12 файлов FA / 20 вхождений паттерна «запятая, затем пустая или чисто-комментарная строка», все вне fa-contracts — MB_InstBaseReuters_Insert_pp.sql, Card_LoyaltyProgramRep1.sql, Warranty_ChangeAccount.sql, RKO_DebtTransfer_Calc.sql и др.).

## Goals / Non-Goals

**Goals:**

- Граница FETCH-оператора по белому списку: первая непустая (после стрипа комментариев) строка после начала списка переменных, не начинающаяся с `@` или `,`, завершает список — единое правило для макросов, профайлера, функций и любых будущих хвостов.
- Сохранение инвариантов: многострочный заголовок `fetch c` / `into @A, @B` распознаётся; пустые/закомментированные строки внутри списка переменных его не прерывают.
- Арность SELECT-декларации с UNION = арность первой ветки (`min(firstTopLevel(from), firstTopLevel(union))`); при отсутствии обоих ключевых слов декларация не распознаётся (прежнее поведение).
- FN-эффекты устранены и отражены дельтами: `varUseAfterCursor` видит аргументы-переменные в не-`M_` хвостах ниже DEALLOCATE; `datatype` перестаёт спаривать переменные с мусорными union-«выражениями» и проверяет последние переменные перед любыми хвостами.
- Приёмка: 5 остаточных FP → 0 на батче fa-contracts + non-contracts регресс и FN-проверки (батч не ловит потерю операторов и класс B2).

**Non-Goals:**

- `isCursorDeclarationBoundary` — не меняется: у SELECT-декларации свой лексикон продолжений (выражения, алиасы, макросы `M_ISOLAT`/`M_NOLOCK_INDEX`), белый список там неприменим; тест-фиксатор fix-fetch-into-macro-tail остаётся.
- Хвост на той же строке, что и список переменных (`fetch c into @A, @B M_X(...)`): белый список работает построчно, same-line не покрывает — не встречается (эмпирика).
- `M_FORCEORDER`/`M_CHECK_REQUIRED` без скобок как границы: по белому списку они уже границы (строка без `@`/`,`), отдельного паттерна не требуется.
- Резолвинг типов по всем веткам UNION (тип колонки в SQL Server резолвится по веткам): спаривание текстовое по первой ветке — консервативно достаточно; проверка типов второй ветки — отдельная тема.
- `findTopLevelKeywordPosition` не различает строковые литералы и `[]`-квантифицированные идентификаторы: `'union'` в списке выражений даст ложную границу. Ограничение унаследовано от существующего поиска `from`; в DECLARE CURSOR эмпирически не встречается — фиксируется как известный предел, не чинится.
- `parseSelectSourceStatement` вне курсорных деклараций (insert-select и др.): вызывается только из `parseCursorDeclarations`; иные SELECT-парсеры не трогаются.

## Decisions

1. **Белый список продолжений: `isFetchVariablesContinuationLine(s)`: пустая после trim строка → true (прозрачна); иначе `trim(s)` начинается с `@` или `,`.**
   Пустые (в том числе ставшие пустыми после стрипа) строки — не граница и не источник переменных: скан их пропускает. Это сохраняет поведение старого регэкспа и регрессионный тест BlockCommentAndEmptyLines. `, @B` на новой строке — легитимное продолжение.
2. **File-путь: stop-предикат = инверсия продолжения, расширенного заголовком `into` (stateless-эквивалент `sawInto`).**
   `isFetchStatementStopLine(s) = !(trim(lower(s)) начинается со слова "into" || isFetchVariablesContinuationLine(s))`. Глобально инвертировать нельзя: stop-предикат применяется и к строкам до `into` — строка `into @A, @B` многострочного заголовка была бы границей, оператор терялся бы целиком (B1). Между FETCH-строкой и `into`-строкой других строк не бывает, поэтому stateless-проверка префикса `into` эквивалентна stateful `sawInto` и не требует состояния в цикле сбора; строка-`into` ниже уже закрытого списка недостижима (сбор оборван первым не-продолжением). До `into` белый список и прежний чёрный список расходятся только на `into`-строке: всё, что чёрный список считал границей (`while`/`select`/`M_*`), белый тоже считает границей.
3. **Fragment-путь: `findFetchIntoTailBoundary` — построчный скан.**
   Go RE2 не поддерживает lookahead: white-list одним регэкспом невыразим. Скан variablesPart по `\n`: первая строка, где `isFetchVariablesContinuationLine` false (непустая и не `@`/`,`), даёт byte-offset начала этой строки; пустые строки пропускаются (B2). Удаляются `reParserFetchIntoBoundary`, `standaloneMacroCallPattern`, `reParserStandaloneMacroCall`, `isStandaloneMacroCallLine` (единственные потребители — заменяемые точки; grep-фиксация в задачах) и устаревшие doc-комментарии о них. Тесты `TestIsStandaloneMacroCallLine` заменяются на `TestIsFetchVariablesContinuationLine` с инвертированной семантикой: `M_FORCEORDER`/`M_CHECK_REQUIRED` (без скобок), `max(x)`, `PROFILE_TIME_*`, `M_X(@a,` — не-продолжения (границы); `@B,`, `, @B`, `into @A, @B` (для stop-обёртки), пустая — продолжения.
4. **UNION-арность: `boundary = min(firstTopLevel("from"), firstTopLevel("union"))`; ранний выход `fromPos < 0` заменяется на `boundary < 0`.**
   `select a, b union select c, d` (без from вовсе) распознаётся: арность 2, FromClause = `union select c, d` (без табличных ссылок — типы не резолвятся, безопасно). При наличии обоих ключей граница — меньшая позиция; FromClause = `text[boundary:]` — при union-first может начинаться с `union`: `parseAliasMap`/`parseFirstFromTable` (`\bfrom\s+`-матчи) резолвят таблицу второй ветки, `\bunion\b`-токен им не мешает. `findTopLevelKeywordPosition` учитывает word-boundary (`tX_union` не зацепит) и скобочную глубину (`union` внутри подзапроса первой ветки не top-level). Семантика SQL: арность UNION = арность первой ветки.
5. **FN-эффекты — дельты, по прецеденту fix-fetch-into-macro-tail.**
   Переформулировка «Standalone-вызов макроса Diasoft (`M_*`)» → «строка, не начинающаяся с `@` или запятой (макрос, профайлер, функция)» в трёх требованиях + уточнения «первая непустая строка» и «пустые строки список не прерывают»; новые сценарии: профайл-хвост в `cursorFetchArguments` (5 переменных + PROFILE_TIME_EX → 0), union-арность в обе стороны (0 findings / реальный рассинхрон ловится), профайл-аргумент в `varUseAfterCursor` (finding), фиксаторы многострочного заголовка и закомментированной переменной, union-спаривание и профайл-хвост в `datatype`. Union-сценарий `datatype` построен на реально изменяемом поведении: `SELECT ID, Amount UNION SELECT X, Y FROM tX` — до фикса SelectExpressions[1] = `Amount UNION SELECT X` (мусор, bare-колонки нет → FN), после фикса = `Amount` → резолвится через `parseFirstFromTable(FromClause)` = tX. На union-декларациях с `@`-литералами первой ветки (Cons_CalcAnnScheme8) новых `datatype`-находок не будет — типы литералов не резолвятся; ожидание приёмки смягчено.

## Risks / Trade-offs

- [Белый список шире чёрного: любая непустая не-`@`/`,` строка — граница; гипотетические легитимные продолжения FETCH, начинающиеся иначе, сломались бы] → эмпирика fa-contracts (100 multiline-хвостов): только `while`/`m_businesslog_*`/`profile_time_*` — не-`@` продолжений нет; `,`-продолжения поддержаны; регрессия на эталонах (InstrumentSO = 11, Cons_OMCV 0/1/0, Cons_BankPartner 0/3/0) и батч 1049 → 0.
- [Потеря операторов FETCH (B1-класс) не видна батчу `cursorFetchArguments` (идёт по фрагментам)] → Span-тесты многострочного заголовка — обязательные фиксаторы; приёмка 4.5 прогоняет non-contracts файлы с закомментированной переменной (класс B2) до (1708) и после (1709) изменений.
- [Строка `@A = 1` (теоретически) классифицируется как продолжение и расширяет skip-спан `varUseAfterCursor`, маскируя использование] → в T-SQL самостоятельного `@A = 1` практически нет (присваивание через `set`/`select` — оба стоп-слова); принимается как предел построчного подхода.
- [`findTopLevelKeywordPosition("union")` не различает строковые литералы: `'union'` в списке выражений первой ветки даст ложную границу] → ограничение унаследовано от поиска `from`, в DECLARE CURSOR не встречается (эмпирика); Non-Goal.
- [FromClause с префиксом `union`] → потребители `FromClause`/`SelectExpressions` курсорной декларации — только `checkDatatypeFetchInto` (`parseAliasMap` + `parseFirstFromTable`) и `checkCursorFetchArguments` (`SelectExpressions`), плюс тесты; `isTableExistsNoiseName` работает по имени курсора и FromClause не читает. Обязательный тест: `parseFirstFromTable("union select X, Y from tX")` = `tX`, `parseAliasMap` на union-FromClause.
- [Новые находки после фикса: `datatype` на union-курсорах (там, где первая ветка содержит bare-колонки), `varUseAfterCursor` на профайл-хвостах ниже DEALLOCATE] → ожидаемы и корректны; приёмка требует ручной верификации каждого нового finding'а; на Cons_CalcAnnScheme8 допустимо отсутствие новых `datatype`-находок.
- [Batch-режим требует проиндексированных файлов] → приёмка по чистому ASCII-списку (прецедент fix-fetch-into-macro-tail); non-contracts файлы из FA-индекса.

## Migration Plan

Изменение локально для `internal/review` + тестов; схема БД и CLI-контракты не затрагиваются. Развёртывание — обычная сборка (бамп buildNumber 1708→1709); откат — git revert без миграций.

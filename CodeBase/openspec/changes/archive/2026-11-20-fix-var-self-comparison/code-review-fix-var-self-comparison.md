# Code review: fix-var-self-comparison

- **Дата:** 2026-11-20
- **Ревьюер:** opencode (L3/pro)
- **База:** рабочий diff к build 1710 (`git diff`: `review_rules.go` +107, `review_rules_test.go` +148, `review_rule_coverage_test.go` +44, `review_helpers.go` +11/-5, `review_helpers_test.go` +30, `catalog.go`, `cmd/root.go`, `CodeBase.exe`)
- **Вердикт:** **Approve.** Реализация соответствует артефактам change, замечания первого ревью (P1/P2) учтены, регрессов не найдено, приёмка воспроизводится. Ниже — детали и два несуществующих-до-сих ограничения, рекомендованных к запоминанию.

## 1. Соответствие задачам change

| Задача | Реализация | Верификация | Статус |
|---|---|---|---|
| 1.1 `@X = @X` до `@`-skip; `@@`/точка/регистр | `isSelfComparedUserVariable` + `isUserVariableOperand` (review_rules.go:3450-3475): `fullText[start-1] == '@'` → не пользовательская; ровно один ведущий `@`; без точки; `EqualFold` | `TestIsSelfComparedUserVariable` (6 кейсов, включая `@@spid = @@spid` → false по leftStart=1) | Соответствует |
| 1.2 finding severity 1, сообщение, `var:`-ключ, `positionToLine` | Блок самосравнения в `analyzeConditionForEqColumn` (review_rules.go:3369-3404) | `..._VarSelfComparisonLineAndMessage`: Line=11 (многострочное), Message=`Заведомо истинное сравнение: @BankProductID = @BankProductID`, Object=`@bankproductid`, severity=1 | Соответствует |
| 1.3 исключения + mixed 2 findings | — | Табличный тест: `@A = @B`, `@var = column` (оба порядка), `1=1`, bitwise, EXEC/API_EXEC-хвосты, `@X = @X and A.ID = A.ID` → 2 | Соответствует |
| 2.1 `@`-фильтр по индексу матча | `extractUnqualifiedConditionColumns` (review_helpers.go:546-568): `FindAllStringSubmatchIndex`, `expr[start-1] == '@'` → skip | `..._SkipsVariables` (7 кейсов, вкл. обратный порядок `@BankProductID = BankProductID` → пусто) | Соответствует |
| 2.2 реальные ссылки не страдают | — | `p.BankProductID = @BankProductID`, `= 1`, `= cast(@x as int)` → колонка остаётся | Соответствует |
| 2.3 регресс indexWrong XPK/XIE1 | — | `TestCheckIndexWrong_VariableInConditionIsNotColumn`: обе формы DELETE (безымянная переменная → 0; реальная колонка → XIE1) | Соответствует |
| 3.1/3.2 каталог и сообщения | `Description: "Сравнение с самим собой"`; колонковое сообщение не тронуто | `TestRuleUseEqColumnDescription`; живые колонковые finding'ы на корпусе GL с прежним сообщением | Соответствует |
| 4.1–4.5 приёмка | build 1710 | См. раздел 3 | Подтверждено |

Замечания предыдущего ревью артефактов (P1: ложная посылка по Transfer.sql; P2: механизм отсечения `@@`) — устранены: design D2 дополнен механизмом пред-символьной проверки и обоснованием обрезки буфера; D4/Risks переформулированы (Transfer — эталон только для useEqColumn; follow-up на 11 сканеров зафиксирован в tasks 4.3 и Risks).

## 2. Решения сверх исходного плана

### 2.1 `conditionBodyEnd` / `isConditionTerminatorLine` — обрезка буфера условия

Правильное и необходимое добавление: `hasConditionEnded` не разрывает where-буфер на `)`/`begin`/`exec` (в тестах change — кейс `where ... ) begin exec ... @X = @X`), что давало 264 FP на named-args. Обрезка построчная, применяется **только** к var-ветке — колонковая логика не затронута. Оффсет-арифметика (`len(line)+1` за пробел `Join`-а, включая `\r` при CRLF) согласована с `fullText`.

Замечания (не блокируют):

- **R1 (известное ограничение, FN):** эвристика «строка-вызов `Name(...)`» в `isConditionTerminatorLine` считает терминатором любую строку-продолжение условия, начинающуюся с вызова функции (`isnull(@a,0) = @b`, `max(x) from t)` — тело скалярного подзапроса с переноса). Самосравнения после такой строки будут пропущены. На корпусах (батч автора 1000 файлов + независимая выборка ниже) FN не проявился; при разборе будущих FN-репортов помнить об этой эвристике. Возможное уточнение-последователь: не считать терминатором строку, если к её началу в буфере есть незакрытая скобка.
- **R2 (наблюдение):** `Object` var-кейса содержит `@` (`@bankproductid`) в отличие от колонкового (`bankproductid`). Поведение не зафиксировано в спеке; в JSON-выводе выглядит консистентно (переменная как объект). Рекомендуется упомянуть в спеке при архивации (одно предложение), чтобы зафиксировать контракт.

### 2.2 `isInSelectClause` для var-ветки

Корректно отсекает `where col = (select @x = @x from t)` (присваивание в SELECT-list подзапроса — covered тестом). Ограничение (pre-existing семантика `isInSelectClause`): `where exists (select 1 from t where @x = @x)` — вложенный `where` внутри скобок не закрывает select-list (триггеры только на нулевой глубине), самосравнение будет пропущено (FN). Кейс редкий, на корпусах не встретился. Фиксировать не предлагаю — вне scope, отметить как известное ограничение при разборе FN.

## 3. Независимая верификация приёмки (выполнена ревьюером)

1. `go test ./internal/review -count=1` — **ok**; `go vet ./internal/review` — чисто; `openspec validate fix-var-self-comparison --strict` — **valid**.
2. `CodeBase.exe --version` → `0.9.5 build 1710`.
3. `BPConsConditionInsert.sql` (полный набор правил): `total=33`, `useEqColumn:1` (строка 1723, `Заведомо истинное сравнение: @BankProductID = @BankProductID`), `indexWrong:0`, состав остальных 32 без изменений — **в точности 4.1**.
4. `BPConsCoolingOff_Transfer.sql` (`--rules useEqColumn`): `total=0` — **4.2**.
5. **Независимая выборка** (fa-contracts, sorted, skip 2000, 400 файлов — не входили в батч автора): 12 `indexWrong`, **0 `useEqColumn`** — ложных срабатываний нового правила на новом корпусе нет; `indexWrong` жив (агрессивность `@`-фильтра не «обнулила» колонки).
6. **fa-generalledger** (1500 файлов, AML + API_CRM; не входил в батч автора): 4 finding'а `useEqColumn` —
   - `AML/UnreliableObject_Delete.sql:56` и `UnreliableObject_Insert.sql:83`: `and @InterfaceObjectID = @InterfaceObjectID` в WHERE — **живые тавтологии** (глазами подтверждено: условие фильтрации всегда истинно). Независимо подтверждает заявление proposal «GeneralLedger 2»;
   - `API_ICard_FindListType.sql:65` (`where DocTypeID = DocTypeID`) и `API_Person_FindByPhone.sql:83` — колонковые самосравнения со **старым** сообщением «Нельзя сравнивать столбец с самим собой»: колонковое поведение не регрессировало. Попутно: это два живых FA-бага, кандидаты на репорт в FA (вне CodeBase).

## 4. Регресс-анализ

- **Колонковая ветка `useEqColumn`** не изменена (только вставка блока до `@`-skip); сообщение/дедупликация/строки прежние — подтверждено тестами и живыми находками на GL-корпусе.
- **`extractUnqualifiedConditionColumns`** используется и в `extractWhereOnlyColumnsForIndexWrong` (M_FORCEORDER-путь) — фильтр применился к обоим путям, что и требовалось; поведение для колонок без `@` идентично (все прежние тесты пакета зелёные).
- **JSON-формат** ответа не менялся (проверено прогонами `--json`).
- Батч автора (1000 + 2 эталона, 139→139, diff ровно `+1723/-1720`) согласуется с моей независимой выборкой по знаку и масштабу эффекта.

## 5. Качество кода

- Стиль соответствует пакету (табличные тесты, русские комментарии, хелперы с doc-комментариями); дублирования нет; функции малы и названы по делу.
- `isUserVariableOperand` корректно обрабатывает `start == 0` (нет пред-символа — не отсекается), `len(raw) < 2`, точку в имени.
- Тесты покрывают граничные случаи: регистронезависимость, `@@spid`, точка, оба порядка операндов, многострочность, EXEC/API_EXEC/begin-хвосты, mixed var+column, подзапрос-присваивание.

## 6. Итог

Реализация соответствует спроектированному поведению, приёмка воспроизводится на эталонах и независимых корпусах, регрессов не обнаружено. Change готов к архивации (`/opsx-archive` + фиксация в project memory). На память при архивации:

- два известных FN-ограничения var-детекции (строка-вызов функции как терминатор R1; `where` внутри EXISTS-подзапроса R2) — кандидаты в известные-ограничения спеки/памяти, не в этот change;
- follow-up change: порядок `--`/блочного комментария в 11 сканерах (после фикса на Transfer.sql ожидается легитимный `indexWrong@40`);
- попутные находки в FA: `API_ICard_FindListType.sql:65`, `API_Person_FindByPhone:83` (колонковые тавтологии), `UnreliableObject_Delete.sql:56`, `UnreliableObject_Insert.sql:83` (var-тавтологии).

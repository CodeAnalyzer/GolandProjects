## 1. Baseline (до правок)

- [x] 1.1 Corpus baseline: скрипт в `Tests\Temp` прогоняет `codebase review --rules varUseAfterCursor` по плоскому набору FA-продуктов; полный список находок (file:line:object) сохранён в CSV; итоговое количество зафиксировано
- [x] 1.2 Классификация baseline: для каждой находки определено скриптовой эвристикой (+ ручная выборка), есть ли переопределение её переменной между DEALLOCATE и строкой находки (`set`/select-присваивание в контексте оператора SELECT, `declare`, FETCH другого курсора — по правилам design D2/D6: EXEC-параметры и текст литералов переопределением не считаются); класс «переопределено до чтения» количественно зафиксирован как ожидание исчезновения
- [x] 1.3 Эталон FP зафиксирован: `FA/fa-cards/Cards/SERVER/Card/pCardRegRelation_Load.sql` → ровно 4 findings varUseAfterCursor (306 @currObjectTypeID, 307 @currObjectTypeBrief, 312 @currObjectTypeID, 314 @currObjectTypeID)
- [x] 1.4 FN-зонд: в corpus найдены (или зафиксировано отсутствие) кейсы «присваивание переменной в одной ветке if/else, чтение — в соседней» — количество зафиксировано как мера trade-off D1

## 2. Реализация

- [x] 2.1 Kill-логика в `checkVarUseAfterCursor` (review_rules.go): активное множество переменных копируется на окно каждого DEALLOCATE (сет имён — дубликаты FETCH'ей схлопнуты); последовательный скан окна; детекторы переопределения (D2): set-строка `^\s*set\b` с `@var` перед `=` или составным `op=` (`+=` и т.п.); select-присваивание в контексте оператора — заголовок `select [top N] @var =` и продолжения `^\s*,?\s*@var =` только пока список открыт (закрывается строкой, начинающей `from`/`where` или новый оператор/standalone-макрос); EXEC-параметры kill не дают; `declare @var` — kill; переопределённая переменная удаляется из множества и на этой строке не флагуется; старые set/select-гварды (review_rules.go:5236–5242) удалены, declare-гвард и курсорные гварды строк сохранены; проверка: `go test ./internal/review/ -run TestCheckVarUseAfterCursor` зелёный после задач 3.x
- [x] 2.2 Kill по FETCH других курсоров: FETCH ниже DEALLOCATE текущего курсора с пересечением переменных удаляет их из активного множества (по `parseAllFetchIntoStatements`; kill привязан к `fetch.LineStart`, переменные — из `fetch.Variables`); skip-спаны того же курсора не изменены; проверка: тест 3.3
- [x] 2.3 Kill-детектор работает по length-preserving тексту с маскированными строковыми литералами (`maskLexemes(text, true)` / `sanitizeSQLForMacroScan`, review_rules.go:1601) и word-boundary по имени переменной; скан использований остаётся на `maskSQLComments`-тексте и подстрочным (`Contains`); проверка: unit-тесты из группы 3

## 3. Тесты

- [x] 3.1 Тест-эталон: FETCH в @A/@B, DEALLOCATE, многострочный `select @A    = isnull(Value,0),` / `@B = isnull(Brief,'')`, ниже — чтения (профайлер, `if @A in (...)`) → 0 findings
- [x] 3.2 `      set @A = 5` (отступ) + чтение ниже → 0 findings; `set @A += 1` (составной оператор) + чтение ниже → 0 findings; `set @Other = @A` → finding для @A сохраняется (RHS — использование)
- [x] 3.3 `DECLARE c2`, `FETCH c2 INTO @A`, чтение `@A` → 0 findings для @A
- [x] 3.4 Чтение `@A` (профайлер) ниже DEALLOCATE, затем `select @A = 0` → finding на строке чтения, 0 ниже присваивания
- [x] 3.5 `if @A = 1` и `select case when @A = 1 then 1 else 0 end` ниже DEALLOCATE → findings на обеих строках (сравнение — не переопределение)
- [x] 3.6 `declare @A int` ниже DEALLOCATE + чтение ниже → 0 findings для @A
- [x] 3.7 Существующие `TestCheckVarUseAfterCursor_*` (review_rules_test.go:3932–4752) и маскировочные (review_block_comment_masking_test.go) остаются зелёными: `go test ./internal/review/`
- [x] 3.8 EXEC-параметр: `exec MyProc` / `       @A = @A output,` ниже DEALLOCATE не снимает `@A` → findings на строке параметра и на последующем чтении (B1)
- [x] 3.9 Литерал профайлера: `PROFILE_TIME_VAR1('insert into t, @A = %1!', @A)` ниже DEALLOCATE не снимает `@A` → finding на строке профайлера (B2)
- [x] 3.10 Защита substring-якоря: отслеживаемая `@ChangedBrief`, строка `select @ChangedBriefs = 1` ниже DEALLOCATE — kill не срабатывает (word-boundary), finding для `@ChangedBrief` формируется (подстрочный скан, якорь `InstrumentSO_LoadStates.sql:652`)

## 4. Приёмка

- [x] 4.1 bump buildNumber 1716 → 1717 в root.go, пересборка CodeBase.exe, `gofmt -l` пуст, `go test ./...` зелёный
- [x] 4.2 Эталон FP: `codebase review D:\GITHUB\GolandProjects\FA\fa-cards\Cards\SERVER\Card\pCardRegRelation_Load.sql --rules varUseAfterCursor` → 0 findings; источник kill — присваивания 306/307, литерал профайлера на 312 (`@currObjectTypeID=` внутри `'...'`) kill не даёт (маскировка литералов, D6)
- [x] 4.3 Регрессия эталонов: `InstrumentSO_LoadStates.sql` → ровно 1 varUseAfterCursor (@ChangedBrief, 652), общий счёт находок всех правил не изменился; скан использований остаётся подстрочным — якорь `@ChangedBrief` ⊂ `@ChangedBriefs` не меняется; `Cons_BankPartnerInstPrepare.sql` → 0 varUseAfterCursor
- [x] 4.4 Corpus diff: повторный прогон (задача 1.1) на новом билде; исчезли только находки класса «переопределено до чтения» из 1.2 (полное совпадение списков); исчезновения не обусловлены EXEC-параметрами и литералами (B1/B2 — kill только от настоящих присваиваний); новых находок нет, кроме возможно пустого класса «RHS на column-0 `set`» (снят мёртвый whole-line set-гвард) — каждая находка класса верифицирована как реальное чтение; каждая исчезнувшая сверена с фактом присваивания
- [x] 4.5 Верификация FN-зондов на новом билде: кейсы sibling-веток (1.4) больше не флагуются — примеры приложены к отчёту как задокументированный trade-off D1 (при неожиданном масштабе — эскалация на пересмотр решения); зонд B1/B2 — подтверждено, что EXEC-параметры `@X = @X output` и тексты литералов `, @X = %1!` не являются источниками исчезнувших находок
- [x] 4.6 Результаты приёмки зафиксированы в acceptance-файле в каталоге change

## 5. Финализация

- [x] 5.1 Валидация: `openspec validate fix-varuseaftercursor-reassignment-fp --strict` — без ошибок; после ревью и синхронизации — архивация и memory summary по процессу

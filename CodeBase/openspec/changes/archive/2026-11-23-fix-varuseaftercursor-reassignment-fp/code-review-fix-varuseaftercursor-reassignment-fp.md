# Code review: `fix-varuseaftercursor-reassignment-fp`

- **Дата:** 2026-11-23
- **Сборка:** CodeBase 0.9.5 build **1717** (`cmd/root.go:29`)
- **Проверено:** `internal/review/review_rules.go` (kill-логика), `internal/review/review_rules_test.go` (11 новых тестов), `cmd/root.go`; артефакты change (`design.md`, `tasks.md`, дельта-спека, `acceptance-varuseaftercursor.md`).
- **Вердикт:** **Принято.** Реализация соответствует плану (D1–D6), замечания предыдущего ревью B1/B2/B3 закрыты (в т.ч. тестами). Регрессий не обнаружено; независимо воспроизведены эталон и якоря. Замечания ниже — low/info, не блокируют архивацию.

---

## 1. Независимая проверка (не из acceptance)

| Проверка | Результат |
|---|---|
| `CodeBase.exe --version` | `0.9.5 build 1717` |
| `review pCardRegRelation_Load.sql --rules varUseAfterCursor` | **0 findings** (было 4: 306/307/312/314) |
| `review InstrumentSO_LoadStates.sql` | **1** — `@ChangedBrief` line **652** (якорь сохранён) |
| `review Cons_BankPartnerInstPrepare.sql` | **0** |
| `go test ./internal/review/ -run TestCheckVarUseAfterCursor -count=1` | `ok` |
| `go test ./internal/review/ -count=1` | `ok` |
| `gofmt -l review_rules.go review_rules_test.go root.go` | пусто |
| `openspec validate fix-varuseaftercursor-reassignment-fp --strict` | valid |

Артефакты приёмки реальны: `Tests\Temp\vuac_appeared.csv` пуст (новых находок нет), `vuac_disappeared.csv` — 4144 строки, `vuac_*.ps1`/CSV на месте. Арифметика acceptance сходится: `old-uniq 5586 − new-uniq 1442 = disappeared 4144`.

## 2. Соответствие плану (D1–D6)

| Решение | Реализация | Статус |
|---|---|---|
| D1 kill вместо гвардов, активное множество per-DEALLOCATE | `active`/`orderedVars` внутри цикла по `deallocs`, kill→use, `break` при пустом множестве | ✔ |
| D2 позиционный SET/SELECT-детектор в контексте оператора | `reVuacSetStart`/`reVuacAssignCandidate`, `reVuacSelectHeader`/`reVuacSelectCont` + machine состояния `selectListOpen` (закрытие `from/where/...`, новым оператором, standalone-вызовом) | ✔ |
| D2 EXEC-параметры — не переопределение | `exec` закрывает список; параметры вне SELECT-контекста не матчатся | ✔ (тест 3.8) |
| D3 FETCH другого курсора — kill на `LineStart` | `fetchKills[LineStart]`, скип FETCH того же курсора | ✔ |
| D4 `declare @var` — kill | `reVuacDeclareStart` + `containsWord` | ✔ |
| D5 порядок kill→use | kill-блок до поиска использований | ✔ |
| D6 маскировка литералов для kill-детектора | `sanitizeSQLForMacroScan(text)` → `litLines`; использование — по `maskSQLComments` (подстрока) | ✔ (тесты 3.9, 3.10) |
| Удаление старых `set`/`select`-гвардов (5236–5242) | удалены, declare-гвард и курсорные гварды строк сохранены | ✔ |

Покрытие тестами: новые кейсы 3.1–3.10 присутствуют (эталон, set с отступом/compound, RHS, FETCH другого курсора, чтение до reassign, сравнение, declare, EXEC-параметр, литерал профайлера, substring-якорь). Дельта-спека дополнена сценариями на EXEC-параметр, литерал, declare, RHS — закрывает M2 прошлого ревью.

## 3. Замечания

### R1 (Low) — составное присваивание без пробела не распознаётся

`reVuacAssignCandidate`/`reVuacSelectCont` используют группу `@[^\s,)=]+`, которая не исключает `+ - * / % & | ^`. Для `set @a+=1` (без пробела) группа жадно захватывает `@a+`, после чего матчится `=`, и `delete(active,"@a+")` не снимает `@a` → reassignment не распознан (FN).

Эмпирически (временный Go-харнесс в `Tests\Temp`, удалён):
```
"set @a = 5"  -> kill="@a"   ok
"set @a += 1" -> kill="@a"   ok
"set @a+=1"   -> kill="@a+"  ← неверно (нет kill для @a)
"set @a-=1"   -> kill="@a-"
```
Практический риск минимален: в `FA/fa-cards/Cards` составных присваиваний (`op=`) не найдено вовсе (0 вхождений). Но D2/task 2.1 обещают поддержку составных операторов, а тест 3.2 проверяет только вариант с пробелом.

**Рекомендация (не блокирует):** заменить класс имени на `@[A-Za-z_][A-Za-z0-9_]*` во всех трёх регэкспах назначения и добавить кейс `set @a+=1`.

### R2 (Low) — over-kill при инициализации в `declare`

`declare @x int = @a` (T-SQL 2008+, inline-init) ниже DEALLOCATE снимет `@a` как «объявленную», хотя это чтение `@a` в инициализаторе. Паттерн экзотичен для Diasoft-корпуса; при необходимости — ограничить declare-kill только именем, которое реально объявляется (первое имя после `declare`/через запятую).

### R3 (Info) — `selectListOpen` не закрывается «бесскобочными» макросами

`reVuacStandaloneCall` требует `(`. Строки вида `M_ISOLAT`, `M_FORCEORDER`, `M_NOLOCK_INDEX(...)` без скобок список не закрывают. Риск ограничен сценарием, когда сразу после такого макроса идёт строка `@x = ...` до любого ключевого слова-оператора; в реальном коде между ними всегда есть `if/exec/end/...`, которые закрывают состояние. Не наблюдалось.

### R4 (Info) — `IF cond SELECT @v = ...` (select не в начале строки)

D2 требует `select` в начале (после отступа) строки, поэтому reassignment в форме `IF cond SELECT @v = ...` не снимает привязку (FN). Дополнительно: снятие старого широкого `select`-гварда (`Contains("select") && Contains(var+"=")`) теоретически может дать новую находку на `select`-содержащих строках, где `select` не в начале, а `@v=` записан слитно. В корпусе `appeared = 0`, класс не проявился.

### R5 (Info) — неточность в acceptance

`acceptance-varuseaftercursor.md:7` пишет «16 старых + 10 новых»; фактически 15 старых + 11 новых (сумма 26 верна). Косметика.

### R6 (Info) — формулировка дельты-спеки

Текст требования (`spec.md:7`) говорит про переопределение «после `select` **или запятой**» без явного упоминания контекста оператора; реализация корректно ограничивает запятую открытым SELECT-списком, а EXEC-параметры вынесены отдельным предложением. Для однозначности можно добавить в текст «…внутри списка присваиваний оператора SELECT». На валидацию не влияет.

## 4. Анализ регресса

- **Эталон и якоря** (см. §1) — без регресса; `@ChangedBrief:652` держится на подстрочном скане использований (`strings.Contains`), который не менялся; word-boundary применён только в kill-детекторе.
- **Новые находки:** `vuac_appeared.csv` пуст; класс «RHS на column-0 `set`» и inline-`IF ... SELECT @v=` в корпусе не проявились. Удаление whole-line `set`-гварда корректно теперь флагует RHS (`set @Other = @a`) — это исправление FN, а не регресс (тест 3.4/3.2).
- **Спот-чек остаточного класса `no_reassign`:** строка из `vuac_disappeared.csv` — `Cons_PC_AnswerFromBP.sql:2089 @InterfaceObjectID`. Разобрана вручную: макрос `__PC_Answer1__` (invoke на 2084) в expanded-тексте заново объявляет `Cons_PC_Answer_FO_Cur2`, т.е. открывает новую жизнь курсора ниже DEALLOCATE (2058) и ограничивает окно (`nextDeclLine`) — исчезновение законное, не связано с B1/B2. Подтверждает объяснение acceptance для остатка 6.6%.
- **Дубликаты/порядок findings:** дедуп `seen` и ключи не менялись; `orderedVars` лишь схлопывает дубли FETCH'ей (на вывод не влияло — дубли и раньше резались `seen`).

## 5. Заключение

Change реализован в пределах запланированного: kill-семантика внедрена по D1–D6, замечания B1–B3 предыдущего ревью закрыты и покрыты тестами, регрессий по эталонам и корпусу не выявлено (новых находок 0, исчезновения объяснены). Найденные замечания — low/info, к архивации не препятствуют; R1 (compound без пробела) стоит закрыть отдельной мелкой правкой при желании.

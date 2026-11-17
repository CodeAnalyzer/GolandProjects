# Code review: fix-cursor-redeclare-positional-scope

Дата: 2026-11-17
Предмет ревью: реализация change `fix-cursor-redeclare-positional-scope`
Файлы: `internal/review/review_parser.go`, `internal/review/review_rules.go`,
`internal/review/review_rules_test.go`, `internal/review/types.go`

## Вердикт

Реализация **функционально достигает цели change**: позиционная модель работает,
эталонный файл даёт ожидаемые 11 находок вместо 56, 45 курсорных FP устранены,
name-set-потребители не изменили поведение, тесты и сборка зелёные.

Перед merge нужно устранить одно **блокирующее** замечание по качеству кода
(`gofmt` не применён) и одно **блокирующее по корректности** — слишком широкий
сбор строк FETCH, из-за которого `LineEnd`/`Variables` могут захватывать
следующий оператор. Также есть несколько существенных пробелов в тестах,
которые маскируют часть заявленной семантики.

## Выполненные проверки

| Проверка | Результат |
|---|---|
| `go build ./...` | успешно |
| `go test -count=1 ./internal/review/` | `ok` |
| `go test ./...` | все пакеты `ok` |
| `go vet ./internal/review/` | без замечаний |
| `gofmt -l` по изменённым файлам | **`internal/review/review_rules.go` не отформатирован** |
| `openspec validate fix-cursor-redeclare-positional-scope --strict` | `Change ... is valid` |
| Приёмка `review` на `InstrumentSO_LoadStates.sql` | **11 находок**, разбивка совпадает с tasks 3.2 |

Приёмка (фактический вывод `review --json`):

| Rule | До | После |
|---|---:|---:|
| `cursorFetchArguments` | 2 | 0 |
| `datatype` | 5 | 3 |
| `varUseAfterCursor` | 42 | 1 |
| `inSubQuery` | 4 | 4 |
| `useFuncInIndCol` | 2 | 2 |
| `execNotExistsProc` | 1 | 1 |
| **Всего** | **56** | **11** |

Остались ровно строки 214×2, 251, 263 (`inSubQuery`), 350×2, 369
(`datatype`), 434, 451 (`useFuncInIndCol`), 591 (`execNotExistsProc`), 652
(`varUseAfterCursor`, известный substring-дефект `@ChangedBrief`/`@ChangedBriefs`,
явно вне скоупа). Удалено 45 FP = 2 + 41 + 2 — соответствует обновлённым
артефактам.

## Соответствие change

| Пункт change | Реализация | Статус |
|---|---|---|
| 1.1 `Line` в `cursorDeclaration`, список деклараций | `types.go:384-391`, `review_parser.go:287-366`; `declLine := i+1` до мутации `i` | выполнено |
| 1.2 `[]fetchStatement`, `into` на новой строке, корректный `LineEnd` | `review_parser.go:421-489` | выполнено с замечанием (см. B1) |
| 1.3 name-set обёртка + адаптация потребителей + тест | `cursorDeclarationSet` (`:368`), вызовы в `checkTableExistsInMetadata`/`checkViewUsageInServerCode` | выполнено |
| 1.4 `nearestCursorDeclaration` | `review_parser.go:379-394`, тест `:1316` | выполнено |
| 2.1 `checkCursorFetchArguments` позиционно + строка FETCH по offset | `review_rules.go:5810-5812`, `fetchIntoStatementLine` (`review_parser.go:491`) | выполнено |
| 2.2 `checkDatatypeFetchInto` позиционно | `review_rules.go:119-123`, `:160-170` | выполнено, но не отформатировано |
| 2.3 `checkVarUseAfterCursor`: decls, окно, спаны, dedup | `review_rules.go:5069-5198` | выполнено с замечаниями (B2, M1, M2) |
| 3.1 `go test ./...` | пройдено | выполнено |
| 3.2 приёмка 56 → 11 | подтверждено на фактическом прогоне | выполнено |
| 3.3 `openspec validate` | `valid` | выполнено |

Дельта-спеки (`postgre-requirements`, `fine-code`) согласованы с реализацией:
ближайшая декларация выше, окно до следующей декларации, продолжения
многострочного FETCH не считаются использованием.

## Замечания

### B1 (блокирующее, корректность): сбор строк FETCH захватывает следующий оператор

`parseAllFetchIntoStatements` накапливает строки до `isCursorDeclarationBoundary`
(`review_parser.go:443-467`), но этот предикат не считает границей `select`,
`exec`, `print`, `set`, `insert`, `update`, `delete`. Строки склеиваются через
пробел и передаются в `parseFetchIntoStatement`, а `findFetchIntoTailBoundary`
ищет `\n` (`reParserFetchIntoBoundary`), поэтому в склеенном тексте **никогда не
срабатывает**. Последствия:

1. `fetchVariablesLineEnd` (`review_parser.go:476-489`) может выставить `LineEnd`
   на строку следующего оператора, если в ней встречается имя переменной FETCH.
   Пример: `fetch c into @a` + следующая строка `select @a` (без `begin`/`if`/
   пустой строки). `LineEnd` станет строкой `select @a`; если этот FETCH попадёт
   в `skipSpans`, реальное использование `@a` будет скрыто.
2. В `Variables` могут попасть переменные из следующего оператора. Пример:
   `fetch c into @a` + `select @x, @y` → склейка `fetch c into @a select @x, @y`,
   `splitTopLevelCSV` даст часть `@y`, которая пройдёт `reParserFetchIntoVar`.
   Это искажает число переменных (влияет на `cursorFetchArguments` при склейке
   фрагментов) и множество `vars` в `varUseAfterCursor`.

Заявленный в tasks/design инвариант «`LineEnd` — последняя строка, реально давшая
переменную» в этих случаях не выполняется.

Рекомендация: останавливать сбор на строке, начинающей новый SQL-оператор
(`select|exec|execute|print|set|insert|update|delete|goto`), либо собирать только
строки-продолжения переменных (пока предыдущая часть заканчивается запятой или
текущая строка начинается с `@`). Дополнительно: соединять строки через `\n`,
чтобы существующий `findFetchIntoTailBoundary` продолжал работать. Добавить тесты:

- `fetch c into @a` + `select @a` (span не должен захватывать `select`);
- `fetch c into @a` + `select @x, @y` (в `Variables` не должно быть `@y`);
- то же с `into` на новой строке.

### B2 (блокирующее, качество): `gofmt` не применён

`gofmt -l` выдаёт `internal/review/review_rules.go`. В `checkDatatypeFetchInto`
блок `key := ...` / `findings = append(...)` (`review_rules.go:160-174`) имеет
сбитую индексацию. Код компилируется, но это нарушает quality gate репозитория.
Перед merge выполнить `gofmt -w internal/review/review_rules.go` и повторно
прогнать тесты.

### M1 (major, тесты): окно DEALLOCATE фактически не проверяется

`TestCheckVarUseAfterCursor_RedeclaredCursor_NextLifeClean`
(`review_rules_test.go:3392`) во второй жизни использует только `@b`, поэтому
проходит и при отсутствии ограничения окна: `vars` первой жизни содержит только
`@a`, а `select @b` с ним не совпадает. Сценарий дельты «повторное использование
`@a` в новой жизни не даёт finding» не покрыт.

Рекомендация: добавить тест, где после `DEALLOCATE c` и `DECLARE c` вторая жизнь
использует старую переменную, например `select @a`. Без окна это даст finding,
с окном — нет.

### M2 (major, тесты): тест спанов/дедупа опирается на непарсящуюся декларацию

`TestCheckVarUseAfterCursor_MultilineFetchAfterDeallocate_SpansSkippedAndDedup`
(`review_rules_test.go:3420`) объявляет вторую жизнь как
`declare c cursor for select A` — без `FROM`. `parseSelectSourceStatement`
требует `FROM`, поэтому вторая декларация **не попадает** в `decls`. В результате
окно первого DEALLOCATE не ограничивается строкой 9, доходит до EOF и формирует
finding на строке 15 (`select @b`). Тест проверяет skip-спаны и дедуп, но
утверждает finding, который при валидной декларации следующей жизни по спеке
должен отсутствовать.

Рекомендация: сделать декларацию валидной (`select A, B from tX`) и перестроить
ожидания: продолжения FETCH новой жизни не должны давать finding, а для проверки
skip-спанов/дедупа добавить отдельныйDEALLOCATE после этого FETCH (тогда
непересекающиеся окна и дедуп проверяются явно).

### M3 (major, ограничение): декларации без `FROM`/непарсящиеся не дают границ

`parseCursorDeclarations` добавляет запись только если `parseSelectSourceStatement`
успешно выделил `FROM`. Для `DECLARE c CURSOR FOR SELECT 1` / `EXEC ...` запись
отсутствует, поэтому:

- `nearestCursorDeclaration` не найдёт декларацию (FETCH не проверится);
- в `checkVarUseAfterCursor` не будет `nextDeclLine`, и окно уйдёт до EOF, что
  нарушает требование «окно MUST ограничиваться следующей декларацией».

Это не проявляется на эталоне, но спека сформулирована безусловно. Рекомендация:
для позиционных границ сохранять запись декларации даже при неудачном разборе
SELECT (с пустыми `SelectExpressions`/`FromClause`) либо явно зафиксировать
ограничение в design/Notes и добавить тест.

### m1 (minor): skip-спаны могут быть избыточны

После ограничения окна следующей декларацией `skipSpans` в корректном SQL нужны
редко: FETCH следующей жизни обычно уже за пределами окна. Механизм полезен как
fallback при непарсящейся декларации (M3), но тогда его overreach (B1) особенно
опасен. Стоит либо упростить, либо покрыть валидным сценарием, где без спанов
был бы FP.

### m2 (minor): `containsWord` в `fetchVariablesLineEnd` не гарантирует «строку переменной»

Функция ищет вхождение переменной в строке, а не факт принадлежности строки
списку переменных FETCH. Совместно с B1 это и даёт переполнение спана. После
исправления B1 замечание снимается.

### m3 (minor): `checkVarUseAfterCursor` по-прежнему использует `strings.Contains`

Совпадение переменных остаётся подстрочным (`@ChangedBrief` матчит
`@ChangedBriefs`), из-за чего в приёмке остаётся находка на строке 652. Это
осознанно вне скоупа (tasks 3.2), замечание фиксируется только для полноты.

## Анализ регрессий

- **Изменённые правила**: `cursorFetchArguments`, `datatype` (ветка FETCH INTO),
  `varUseAfterCursor`. Поведение изменено намеренно и соответствует дельтам.
- **Name-set потребители** (`tableExistsInMetadata`, `viewUsageInServerCode`):
  обёртка `cursorDeclarationSet` сохраняет прежнюю семантику (важна только
  принадлежность имени), изменений нет.
- **Прочие правила**: не затронуты; `go test ./...` зелёный.
- **Склейка `open`+`fetch`**: новое вычисление `fetchLine` по offset исправляет
  прежнюю привязку к строке `open`; покрыто тестом
  `TestCheckCursorFetchArguments_GluedOpenFetch_LineByOffset`.
- **Позиционный nearest**: для файлов с несколькими процедурами и одинаковыми
  именами курсоров поведение может отличаться от прежнего «последняя декларация
  файла», но в сторону корректности при упорядоченных DECLARE→FETCH; межпроцедурный
  scoping остаётся non-goal.
- **Остаточный риск**: B1/M3 могут давать ложные skip-спаны/неограниченные окна
  на нестандартных формах FETCH/деклараций. На эталоне и в существующих тестах не
  проявляются.

## Итоговые рекомендации

1. Исправить B1: сузить сбор строк FETCH (границы новых операторов / строки
   продолжения переменных), добавить тесты на склейку `fetch` + `select`.
2. Применить `gofmt -w internal/review/review_rules.go`.
3. Усилить тесты: окно с повторным использованием старой переменной (M1),
   валидная декларация в тесте спанов (M2).
4. Зафиксировать/закрыть ограничение деклараций без `FROM` (M3) — хотя бы тестом
   и записью в design Notes.
5. Повторно прогнать `go test -count=1 ./internal/review/`, `go test ./...`,
   `gofmt -l` и приёмку на эталоне (ожидаемо 11 находок).

После устранения B1/B2 change можно считать готовым к архивации.

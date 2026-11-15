# Ревью change: `fix-macro-token-paste`

- **Дата:** 2026-11-15
- **Change:** `CodeBase/openspec/changes/fix-macro-token-paste`
- **Ветка/состояние:** рабочее дерево, change не реализован (`- [ ]` во всех задачах)
- **Валидация:** `openspec validate fix-macro-token-paste --strict` → **valid**
- **Базовый тест-прогон:** `go test ./internal/review/...` → **ok**

## Итоговый вердикт

Диагноз корневой причины точен, выбранное направление (разворачивать token-paste
в препроцессоре, а не расширять `reLookupQualifiedRef`) — правильное. Предложенный
подход подтверждён экспериментально: при имитации схлопывания `##mx##` ложный
`tableFullScan` на `API_CON_RuleDoc_MassGetLimitAcc.sql` исчезает.

**Change не готов к реализации «как написано» без правок.** Главный риск —
формулировка токен-паста в design/spec: буквальная запись `##?<param>##?`
ломает обычную подстановку параметров и/или снимает защиту от подстрок, а
Requirement 2 в spec противоречит принятому решению и в текущей редакции
опасен для `##CURRENT_MODULEID##`, `##PPVARCNT##`. После устранения замечаний
BLOCKER/MAJOR (см. ниже) change можно брать в работу.

## Что проверено

Артефакты:

- `proposal.md`, `design.md`, `tasks.md`, `specs/review/sql-preprocessing/spec.md`;
- структура/валидность через `openspec validate --strict`.

Код (фактическая реализация, а не только заявленная в change):

- `internal/review/review_parser.go` — `collectMacroDefinitions` (625),
  `expandMacrosInLines` (771), `expandFirstMacroCall` (832), `findMacroCall`
  (887, paste перед именем макроса — 910–916), `applyMacroArguments` (985),
  `replaceIdentifierToken` (1007);
- `internal/review/review_lookup.go` — `reLookupQualifiedRef` (30),
  `parseTableWithAlias` (1210), `extractTablesFromFromClause` (1269),
  `extractColumnRefsFromWhere` (1327), `extractColumnRefsFromOn` (1426);
- `internal/review/review_helpers.go` — `isTableFiltered` (875);
- `internal/review/review_rules.go` — `checkTableFullScan` (3451),
  `analyzeStatementForFullScan` (3550), `checkIndexWrong` (244, использует
  `extractTablesFromFromClause`), `checkUseEqColumn` (3226),
  `checkDeferredUpdate` (4302), `checkDiffTypesComparison` (6605);
- `internal/review/runner.go` — `fileProcessedContent` (519),
  `mapExpandedLineToOriginal` (222/529).

## Подтверждение дефекта (evidence)

1. Воспроизведение на реальном файле (индексированная копия):
   `CodeBase.exe review <FA/.../API_CON_RuleDoc_MassGetLimitAcc.sql> --rules tableFullScan --json`
   → `count: 1`, `tableFullScan`, `pcon_cover_checkpaystrategy`, `line: 2092`.
2. Причина подтверждена по коду:
   `replaceIdentifierToken` применяет `(?i)\bmx\b` (1007–1015), `#` не входит
   в `\w`, поэтому `ch##mx##` → `ch##1##`. Далее `reLookupQualifiedRef =
   (?i)(\w+)\.(\w+)` (review_lookup.go:30) не матчит `ch##1##.SPID`, WHERE/ON
   ссылки теряются, `isTableFiltered` (review_helpers.go:875) возвращает
   `false` → finding.
3. Эксперимент с временным in-package harness (удалён после проверки):
   - текущий код: `findings=1` (`pcon_cover_checkpaystrategy`);
   - имитация исправления (`##mx##` → `1` в раскрытом тексте): `findings=0`.
   Это подтверждает, что первопричина именно несхлопнутый paste, а не вторая
   ошибка в правилах.
4. Матрица веса затронутых файлов (важно для Impact):
   - `fa-contracts/Underwriting/SERVER/Underwriting/USR_GET_PAYMENT_COUNT.sql`,
     `USR_GET_MAX_OVER*.sql`, `USR_GET_INSTITUTION_OVER.sql`,
     `USR_GET_CUR_DEBT_TOTAL.sql` — `#define M_pResource_INSERT(..., __N__)`
     и `i##__N__##`, `where i##__N__##.SPID = @@Spid`;
   - `fa-warranty/.../WarrantyInterest_Insert.sql` и др. — `#define
     __InterestValue__(PREFVAR)` и `##PREFVAR##ObjectID`, вызовы
     `__InterestValue__()`, `__InterestValue__(@)`, `__InterestValue__(m.)`
     (критичен кейс **пустого аргумента**);
   - `seed.h` — `##_SHARED_SEED_##t##OBJECT##Seed`, `@##OBJECT##SeedID`;
   - `log.h` — `@val##PPVARCNT##PPLINE` (встроенный маркер, **не** параметр),
     `##M_NPPMACROSBEGIN##...` (объектные макросы);
   - `FCD_##CURRENT_MODULEID##_HM_*.sql` — placeholder уровня файла (не параметр).

   Вывод: правка затрагивает препроцессор, общий для **всех** правил review,
   а не только `tableFullScan`.

## Замечания

Легенда: **BLOCKER** — нельзя реализовывать как есть; **MAJOR** — нужно
исправить/дополнить до реализации; **MINOR** — уточнение.

### B1 (design, BLOCKER). Формула paste `##?<param>##?` некорректна и опасна

`design.md:44-47` и `proposal.md:22-23` предлагают шаблон `##?<param>##?`
(«каждый `##` опционален»). Это неверно трактуется и как regex, и как логика:

- буквальный regex `##?mx##?` — это `#` + необязательный `#`, поэтому **не
  заменяет обычный `mx`** (проверено: `mx` → `mx`, т.е. регрессия базовой
  подстановки аргументов);
- «логическая» реализация `(?:##)?mx(?:##)?` без границ **портит подстроки**:
  параметр `id` превращает `LinkID` → `LinkX` (проверено);
- `\b` по краям всей обёртки (`\b##?mx##?\b`) **не работает** для
  `@##OBJECT##SeedID`: между `@` и `#` нет word-boundary, ведущий `##` не
  съедается.

Рабочая формула (проверена на всех кейсах):

```
(?i)(?:##)?\b<regexp.QuoteMeta(param)>\b(?:##)?
```

Она сохраняет защиту от подстрок (`LinkID` при `id` не меняется), съедает
`##` с любой/обеих сторон (`ch##mx##`, `i##__N__##`, `@##OBJECT##SeedID`,
односторонний `##PREFVAR##ObjectID`), и по-прежнему подставляет обычный `mx`.

Результаты проверки кандидатов (временный стенд, удалён):

| Вход | `##?mx##?` (букв.) | `(?:##)?\b..\b(?:##)?` | `(?:##)?..(?:##)?` (без границ) |
|---|---|---|---|
| `ch##mx##`, mx=1 | `ch1` | `ch1` | `ch1` |
| `mx`, mx=1 | `mx` (**не заменил**) | `1` | `1` |
| `@##OBJECT##SeedID`, OBJECT=User | `@UserSeedID` | `@UserSeedID` | `@UserSeedID` |
| `LinkID`, id=X | `LinkID` | `LinkID` | `LinkX` (**порча**) |
| `i##__N__##`, __N__=2 | `i2` | `i2` | `i2` |

Дополнительно: `replaceIdentifierToken` используется **только** из
`applyMacroArguments` (review_parser.go:999), поэтому правка локальна; заменить
`Replacement` на `ReplaceAllLiteralString` (или `ReplaceAllStringFunc`), чтобы
значения аргументов с `$` не интерпретировались как группы.

### B2 (spec, BLOCKER/MAJOR). Requirement 2 противоречит design и небезопасен

`spec.md:44-52` описывает схлопывание по соседям: «только `##`, соединяющий
части идентификатора (`\w##\w`) либо стоящий перед точкой (`\w##.`)».
Это **глобальное** правило, которое схлопнет `##CURRENT_MODULEID##`,
`##PPVARCNT##` и прочие placeholder'ы, не являющиеся параметрами макроса.
Design (`design.md:40-67`) явно отвергает глобальное схлопывание и привязывает
его к **объявленному параметру**. Spec — источник истины, поэтому сейчас он
описывает другое (и опасное) поведение.

Кроме того, пример `##_TABLENAME_##` (spec.md:49) как «MUST NOT схлопываться»
противоречит param-bound семантике: если `_TABLENAME_` будет объявлен
параметром, он обязан схлопнуться.

Требуется переписать Requirement 2: «схлопывается только `##`, непосредственно
примыкающий к имени **объявленного параметра** раскрываемого макроса; `##`, не
примыкающий к параметру (префикс global temp, placeholder файла, встроенный
маркер, объектный макрос), MUST NOT изменяться». Добавить сценарии:
`##GlobalRest`, `##CURRENT_MODULEID##`, `##PPVARCNT##`, `##_SHARED_SEED_##`.

### B3 (tasks, MAJOR). Не хватает ключевых тест-кейсов и red→green

`tasks.md` покрывает только `ch##mx##`/`sb##mx##` и temp-маркеры. Обязательно
добавить (иначе регрессия B1 пройдёт незамеченной):

- обычный параметр без `##` (`mx` → `1`) — защита от B1;
- подстрочный guard: параметр `id`, текст `LinkID` — не меняется;
- односторонний paste `##PREFVAR##ObjectID` (`m.` → `m.ObjectID`);
- **пустой аргумент** `__InterestValue__()` → `ObjectID`;
- `@##OBJECT##SeedID` (ведущий `##` после `@`);
- `##PPVARCNT##` и `##CURRENT_MODULEID##` — не схлопываются;
- для регрессионного теста явно зафиксировать, что на **старом** коде тест
  падает (red→green), а не просто «зелёный после правки».

Также в `tasks.md` интеграционный тест описан как вызов правила напрямую; при
этом сценарий spec «якорь line=2092» проверяется только через `runner.go`
(`mapExpandedLineToOriginal`). Нужно либо добавить проверку через `Runner`
(как в `runner.go:222`), либо убрать/уточнить этот сценарий.

### B4 (proposal, MAJOR). Impact занижен; «кросс-правильность» требует проверки

`proposal.md:15-17` перечисляет `indexWrong`, `useEqColumn`, `deferredUpdate`,
`diffTypesComparison` как затронутые. По факту `extractColumnRefsFromWhere`/
`extractColumnRefsFromOn`/`isTableFiltered` использует **только**
`checkTableFullScan`. Однако другие правила действительно потребляют
`extractTablesFromFromClause`/`parseAliasMap` (например `checkIndexWrong`,
review_rules.go:353), поэтому искажённый alias `ch##1##` может влиять и на них.
Формулировку нужно уточнить: заявить общий риск препроцессора и добавить
проверку хотя бы 1–2 смежных правил.

`proposal.md:60-61` «текст становится синтаксически корректным» — слишком
широко: для `##CURRENT_MODULEID##`/`##PPVARCNT##` текст остаётся не-SQL; это
вне scope, но формулировку стоит сузить.

Рекомендуется добавить в Impact/приёмку **baseline-диф**ы: прогнать review на
репрезентативной выборке FA (включая `USR_GET_*.sql` fa-contracts и
`WarrantyInterest_*.sql` fa-warranty) до/после и убедиться, что новые findings
не появились, а ожидаемые ложные — исчезли.

### M1 (spec, MINOR). Не указана область раскрытия (same-file only)

Препроцессор review раскрывает `#define` **только из текущего файла**;
`#include` не резолвится (в `review_parser.go` нет обработки include). Для
источника истины это надо отразить в `## Purpose`/Requirement 1, иначе спека
создаёт ложное ожидание раскрытия макросов из `.h`.

### M2 (proposal, MINOR). Формулировка «кросс-правильный дефект»

После уточнения B4 оставить только подтверждённые правила либо пометить
остальные как «требует проверки».

### M3 (tasks, MINOR). Smoke-команда и свежесть бинарника

`codebase review` работает только для **индексированного** файла (проверено:
для копии вне индекса — `file is not indexed`). В задаче 3.2 стоит явно
указать предварительный `go build` (или `go run .`), чтобы smoke не ушёл в
устаревший `CodeBase.exe`.

### M4 (design, MINOR). Идемпотентность и пустые аргументы

Утверждение об идемпотентности (design.md:61-67) верно, но в Risks не отмечен
кейс пустого аргумента (`__InterestValue__()` → `ObjectID`): при пустой
подстановке оба `##` должны быть поглощены, иначе получится `##ObjectID` или
`ObjectID##`. Добавить в тесты и в Risks.

### M5 (design, MINOR, pre-existing). Подстановка внутри строковых литералов

`replaceIdentifierToken` не учитывает строковые литералы: параметр с коротким
именем может замениться внутри `'...'` (актуально при пустом/коротком имени).
Это не регрессия данного change, но при расширении paste-логики стоит
зафиксировать как известное ограничение (или отдельный кандидат-последователь).

## Что сделано хорошо

- Корневая причина найдена точно, с указанием конкретных строк и полной цепочки
  `replaceIdentifierToken → reLookupQualifiedRef → isTableFiltered`.
- Правильно отвергнут соблазн «починить regex `reLookupQualifiedRef` до
  `[\w#]+`»: это лечило бы симптом и оставляло невалидный текст.
- Верное решение — привязка схлопывания к **объявленному параметру**: это
  сохраняет `##CURRENT_MODULEID##`, `##PPVARCNT##`, `##_SHARED_SEED_##`
  (объектные макросы) и не ломает global temp.
- Чётко зафиксированы Non-Goals (FN по веткам `union all` 2–12) — не смешиваются
  с исправлением.
- Дельта-спека и артефакты структурно валидны (`--strict` проходит).

## Обязательный чек-лист доработки change перед реализацией

- [ ] Заменить формулу paste в `design.md`/`proposal.md` на
      `(?:##)?\b<param>\b(?:##)?` (с `QuoteMeta`, literal replacement) и
      объяснить, почему `\b` стоит вокруг имени параметра, а не вокруг `##`.
- [ ] Переписать Requirement 2 в `spec.md` на param-bound семантику; убрать
      глобальное `\w##\w`/`\w##.` и пример `##_TABLENAME_##` как MUST NOT;
      добавить сценарии `##CURRENT_MODULEID##`, `##PPVARCNT##`,
      `##_SHARED_SEED_##`, пустой аргумент.
- [ ] Уточнить Impact в `proposal.md`: общий риск препроцессора, конкретные
      затронутые семейства файлов (Underwriting `__N__`, Warranty `PREFVAR`),
      baseline-диф; сузить формулировку «синтаксически корректный текст».
- [ ] Добавить в `tasks.md` тест-кейсы из B3 + требование red→green, проверку
      якоря через `Runner`, `go build` перед smoke.
- [ ] Добавить в spec примечание про same-file `#define` (без `#include`).

## Рекомендуемая реализация (кратко)

В `applyMacroArguments` (или `replaceIdentifierToken`) для каждого параметра:

1. `pat := "(?i)(?:##)?\\b" + regexp.QuoteMeta(strings.TrimSpace(param)) + "\\b(?:##)?"`
2. `line = cachedRegexp(pat).ReplaceAllLiteralString(line, replacement)`
3. Покрыть тестами матрицу из B1/B3 (обязательно включить `mx` без `##`,
   `LinkID` при `id`, `##PREFVAR##ObjectID` с пустым аргументом,
   `@##OBJECT##SeedID`, `##CURRENT_MODULEID##`, `##PPVARCNT##`, `##GlobalRest`).
4. Smoke: пересобрать бинарник, прогнать `tableFullScan` на индексированном
   `API_CON_RuleDoc_MassGetLimitAcc.sql` (ожидается 0 findings) и baseline-диф
   на `USR_GET_*.sql` / `WarrantyInterest_*.sql`.

## Приложение: ключевые ссылки на код

| Что | Где |
|---|---|
| paste перед именем макроса (уже есть) | `review_parser.go:910-916` |
| подстановка параметров | `review_parser.go:985-1005` |
| текущий `\b<token>\b` | `review_parser.go:1007-1015` |
| regex квалифицированных ссылок | `review_lookup.go:30` |
| парсинг alias | `review_lookup.go:1210-1267` |
| WHERE/ON ссылки | `review_lookup.go:1327-1488` |
| фильтрация таблицы | `review_helpers.go:875-910` |
| check/analyze FullScan | `review_rules.go:3451-3667` |
| маппинг строк | `runner.go:222`, `runner.go:529` |
| кэш чтения контента | `runner.go:519` |

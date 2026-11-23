# Change Review: fix-use-only-declared-cursors-expanded

- **Дата ревью:** 2026-11-23
- **Статус change:** proposed (задачи 0/8, код не менялся)
- **Ревьюер:** opencode (L3/pro)
- **Вердикт:** **GO с условиями** — дизайн корректен и подтверждён кодом/данными, но в tasks.md есть один критичный пробел (F1), который необходимо закрыть до старта apply.

---

## 1. Объект ревью

| Артефакт | Статус проверки |
|---|---|
| `proposal.md` | OK: `## Why` / `## What Changes` / `## Capabilities` / `## Impact` на месте; `### Modified Capabilities` → `review/postgre-requirements` совпадает с путём в `openspec/specs/` |
| `design.md` | OK: `## Context` / `## Goals / Non-Goals` / `## Decisions` (D1–D4) / `## Risks` / `## Migration Plan`; претензий к содержанию нет |
| `tasks.md` | Формат OK (4 группы, чек-боксы), но **пробел в покрытии — см. F1** |
| `specs/review/postgre-requirements/spec.md` (дельта) | OK: `## MODIFIED Requirements`, имя требования «Использование курсоров без объявления» дословно совпадает с основным spec (`openspec/specs/review/postgre-requirements/spec.md:179`); воспроизведён полный текст (первое предложение — исходное тело) + сохранён исходный сценарий «OPEN без DECLARE» + 3 новых сценария в стиле GIVEN/WHEN/THEN |
| `openspec validate --strict` | **valid** (проверено при ревью) |

---

## 2. Анализ кода — подтверждение диагноза

### 2.1 Механика пайплайна (соответствует design.md Context)

- `RunSQLFileCtx` читает файл CP866 → `replaceMacros` → парсит **expanded**-контент (`runner.go:163-172`), складывает его в `r.exec.content`/`r.exec.lines` (`runner.go:181-190`).
- Все `parsed.Procedures[].LineStart/LineEnd` — expanded-координаты.
- В конце прогона `Finding.Line` ремапится expanded → original через `mapExpandedLineToOriginal(macroResult.SourceMap)` (`runner.go:229-236`); findings с `LineInOriginalSpace` не трогаются.
- `fileContent` (`runner.go:473-479`) возвращает `r.exec.content` только для активного файла прогона; вне прогона — ошибка. Это устоявшийся контракт — его уже используют ~30 правил, включая соседнее `checkCursorFetchArguments` (`review_rules.go:5850`).

### 2.2 Правило (`review_rules.go:5657-5841`)

- Читает **сырой** файл: `os.ReadFile(file.Path)` (строка 5663) — единственное место в правиле, требующее правки.
- Окно процедуры: `startLine = proc.LineStart-1`, `endLine = proc.LineEnd`, скан по `lines` от raw-чтения (5685-5692) — та же пара индексов налагается на другую систему координат. Именно источник FP.
- Пер-процедурная область видимости (`declaredCursors` на каждой итерации, 5682) — сохраняется фиксом без изменений (D2).
- `reDeallocCursorMacro` (review_rules.go:46) требует скобки: `__DEALLOCATE_CURSOR__(name)` — форма без скобок не матчится. В `Trunc_GL.sql` dealloc без скобок → ровно 4 FP, а не 5. Соответствует Non-Goal design.

### 2.3 Реальный файл `FA/fa-generalledger/.../Trunc_GL.sql` (888 строк, проверен построчно)

- `#define __BALANCE_CLEAR__(...)` занимает raw-строки **8–63** → сдвиг `expanded = raw − 56` для всего файла ниже define.
- `Truncate_GL_Resource_ResArrest`: raw 177–220, expanded-окно **121–164**. Правило сканирует **raw** 121–164 — это хвост `Truncate_GL_Resource_Oper` (использования `cur_Oper`: open 135, fetch 137, fetch 155, close 161), а объявление `__DECLARE_CURSOR__(cur_Oper)` (raw 118) лежит **выше** окна → 4 finding'а «необъявленный cur_Oper» с `Object = Truncate_GL_Resource_ResArrest`.
- Двойной сдвиг строк: правило возвращает raw-номера (135/137/155/161), централизованный ремап трактует их как expanded и добавляет 56 → findings на строках **191/193/211/217** (в теле ResArrest, где никакого `cur_Oper` нет). Полностью совпадает с симптомами из proposal.
- Остальные процедуры файла (`cur_Oper` 118, `cur_ResArrest` 186, `cur_Resource` 426, `cur_Inst` 777) объявляют и используют курсоры корректно внутри своих тел → ожидание «0 findings» в задаче 3.1 после фикса **реалистично** (окна expanded содержат свои объявления, проверено пострелочно).

### 2.4 Смещённые утверждения проверены

- **Аудит `os.ReadFile` в `internal/review`:** ровно 2 вызова — `review_rules.go:4147` (`checkShouldBeCP866`, анализирует байты кодировки, Line не использует — легитимно, D4 верен) и `review_rules.go:5663` (чинимое). Утверждение proposal об аудите корректно и фактически закрыто.
- **`buildNumber`** сейчас `"1714"` (`cmd/root.go:29`) — задача 3.1 (бамп до 1715) согласована с текущим состоянием.
- **Каталог правила:** единственный call-site — `catalog.go:345`; severity `SeverityPostgreReq = 2` (`types.go:71`) — сценарий дельты «severity 2» точен; формат вывода и severity фиксом не затрагиваются.
- **Курсорные макросы в expanded:** `__DECLARE_CURSOR__`/`__FETCH_NEXT__` в `Trunc_GL.sql` не объявлены через `#define` самого файла (`#include <macros.h>` review не резолвит) — токены остаются в expanded дословно, regex-ы продолжают работать. Риск в design оценён верно.

---

## 3. Находки ревью

### F1. [MUST FIX — пробел в tasks.md] Миграция 9 существующих юнит-тестов не запланирована

9 тестов `TestCheckUseOnlyDeclaredCursors_*` (`review_rules_test.go:4947-5262`) вызывают правило напрямую с `r := &Runner{}` **без `r.exec`** и с temp-файлом на диске. После замены `os.ReadFile` → `r.fileContent` каждый вызов будет возвращать ошибку `file ... is not the active review target` (`runner.go:475-478`) — **все 9 тестов упадут**, задача 4.1 (`go test ./internal/review/...`) не пройдёт.

**Рекомендация:** добавить задачу (в группу 1 или 2) — перевести все 9 тестов на exec-паттерн, как в `TestCheckDatatypeFetchInto_NearestDeclarationPairing` (`review_rules_test.go:4921`):

```go
path := normalizePath("test.sql")
mr := replaceMacros(content)
r.exec = &reviewExecContext{filePath: path, content: []byte(mr.Content),
    lines: strings.Split(mr.Content, "\n"), macroResult: mr}
parsed, _ := parser.ParseContent(mr.Content) // parsed из expanded, не из raw
// ... вызов правила ...
r.exec = nil
```

Попутно: temp-файлы в этих тестах становятся не нужны (`fileContent` не читает диск); для контента без `#define` `mr.Content == content`, поэтому ожидания существующих тестов не изменятся. Удобно вынести хелпер (например `newCursorRuleRunner(t, content)`), чтобы новые тесты 2.1–2.3 использовали тот же паттерн.

### F2. [SHOULD FIX] Задача 2.2 сформулирована с неверным уровнем проверки

Прямой вызов `checkUseOnlyDeclaredCursors` возвращает `Line` в **expanded**-пространстве — ремап выполняется только в `RunSQLFileCtx` (`runner.go:232-236`) и в unit-тесте не происходит. Формулировка «`Line` указывает на строку использования в исходном файле» в рамках прямого вызова невыполнима. Тест должен сам применить централизованный ремап:

```go
origLine := mapExpandedLineToOriginal(findings[0].Line, mr.SourceMap)
// сравнить origLine с raw-строкой использования
```

**Рекомендация:** уточнить формулировку задачи 2.2 (или добавить примечание), чтобы тест явно применял `mapExpandedLineToOriginal` — иначе есть риск написать тест с неверным ожиданием (проверять expanded-строку как «исходную»).

### F3. [MINOR] Задача 1.2: импорт `os` останется в `review_rules.go`

Формулировка «убрать ставший ненужным прямой импорт/чтение файла» вводит в заблуждение: `os` продолжит использоваться в `checkShouldBeCP866` (`review_rules.go:4147`). Удалить можно только вызов `os.ReadFile` на строке 5663. **Рекомендация:** переформулировать: «удалить прямой вызов `os.ReadFile` в `checkUseOnlyDeclaredCursors`; импорт `os` остаётся для `checkShouldBeCP866`».

### F4. [NOTE] Семантическое следствие фикса: курсорные токены внутри раскрытых макросов

После перевода на expanded-текст токены `DECLARE`/`OPEN`/`FETCH`/`CLOSE`, пришедшие из **многострочных тел раскрытых макросов** (в raw — одна строка вызова макроса), начнут участвовать в анализе per-procedure. Это желательное поведение (expanded — то, что реально исполняет БД; объявления и использования сходятся в одном окне), и для `Trunc_GL.sql` нейтрально (`__BALANCE_CLEAR__` курсоров не содержит). Направление изменения корректно, но оно нигде явно не зафиксировано. **Рекомендация (опционально):** упомянуть в design Risks или покрыть тестом-дополнением к 2.1 (макрос, раскрывающийся в `declare`+`open` одного курсора, — 0 findings).

### F5. [NOTE] Микро-вариация реализации (не блокирует)

Правило повторно делает `strings.Split(content, "\n")`, хотя `r.exec.lines` уже готов. Устоявшийся контракт — `fileContent` (как у соседних правил); дублирование Split дёшево (O(n) по памяти, один раз на файл). Оставить как в D1 — нормально; менять на прямой доступ к `exec.lines` не рекомендую ради единообразия контракта.

---

## 4. Оценка дизайна

| Решение | Оценка |
|---|---|
| D1: expanded через `r.fileContent` | **Подтверждена.** Устраняет сам класс рассинхрона, а не конкретный сдвиг; альтернатива (обратная карта raw-координат) отвергнута обоснованно — дублировала бы централизованный ремап |
| D2: scope = процедура | Сохраняется как есть; сценарий дельты «Курсор предыдущей процедуры не засчитывается» это фиксирует; задача 2.3 покрывает тестом |
| D3: синтетика + реальный файл | Достаточно; проверка ожидания «0 findings» на `Trunc_GL.sql` выполнима (см. 2.3) |
| D4: `checkShouldBeCP866` не трогаем | Подтверждена аудитом (единственный второй `os.ReadFile`, Line не использует) |
| Риски | Оценены корректно; параллельный read-only доступ к `r.exec` — тот же контракт, что у остальных правил, гонок нет |

---

## 5. Чек-лист готовности к apply

- [ ] F1: добавить задачу миграции 9 существующих тестов `TestCheckUseOnlyDeclaredCursors_*` на exec-паттерн (рекомендуется хелпер)
- [ ] F2: уточнить задачу 2.2 — проверка исходной строки через `mapExpandedLineToOriginal` в самом тесте
- [ ] F3 (опционально): переформулировать задачу 1.2 про импорт `os`
- [ ] F4 (опционально): зафиксировать семантику токенов внутри раскрытых макросов

После внесения F1 (минимум) change готов к реализации. Повторное полное ревью не требуется — достаточно точечного контроля задач 1.1 и миграции тестов.

---

## 6. Использованные источники

- `internal/review/review_rules.go:5657-5841` (правило), `:40,46` (regex-ы), `:4145-4166` (checkShouldBeCP866), `:5843-5899` (checkCursorFetchArguments — образец)
- `internal/review/runner.go:141-257` (пайплайн, exec, ремап), `:473-490` (fileContent/rawFileContent), `:617-638` (mapExpandedLineToOriginal)
- `internal/review/review_parser.go:902-922` (stripMacroDeclarations), `:931-990` (expandMacrosInLines)
- `internal/review/review_rules_test.go:4921` (exec-паттерн теста), `:4947-5262` (9 существующих тестов правила)
- `internal/review/catalog.go:341-347`, `internal/review/types.go:54,71`
- `FA/fa-generalledger/GeneralLedger/SERVER/SCRIPT_UTIL/CleaningData/Trunc_GL.sql` (raw: #define 8–63; cur_Oper 118–163; cur_ResArrest 186–216; cur_Resource 426–613; cur_Inst 777–839)
- `cmd/root.go:29` (buildNumber 1714)
- `openspec validate fix-use-only-declared-cursors-expanded --strict` → valid

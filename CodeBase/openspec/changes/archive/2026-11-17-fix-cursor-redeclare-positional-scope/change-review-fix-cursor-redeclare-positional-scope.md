# Review change: fix-cursor-redeclare-positional-scope

Дата: 2026-11-17
Ревьюер: opencode (анализ артефактов + фактического кода `internal/review`)

## Вердикт

Change **валиден и в целом готов к реализации**. Направление (позиционная модель
жизни курсора вместо name-keyed map) выбрано верно и покрывает все три источника
FP. Но перед реализацией нужно исправить несколько дефектов в артефактах:

- **блокирующее для приёмки**: арифметика «46 FP / остаётся 10» неверна;
- **блокирующее для корректной реализации**: неверная посылка о «двух конвейерах
  контента» (`fileContent` — это уже expanded-контент, а не сырой файл);
- **major**: `parseAllFetchIntoStatements` не распознаёт `FETCH cursor` с `INTO`
  на следующей строке, а новая модель спанов/окон опирается на его границы;
- **major**: риск переполнения `LineEnd` и off-by-one в окне DEALLOCATE;
- **minor**: несколько неточностей формулировок и пробелов в тестах.

## Что проверено

| Проверка | Результат |
|---|---|
| `openspec validate fix-cursor-redeclare-positional-scope --strict` | `valid` (3 дельты распознаны) |
| Обязательные секции proposal/design/tasks | присутствуют |
| Пути дельт vs `openspec/specs/` | совпадают: `review/fine-code`, `review/postgre-requirements` |
| `MODIFIED`-требования существуют в основных спеках | да, названия совпадают |
| `go build ./...` (CodeBase) | успешно |
| `go test ./internal/review/` | `ok` (baseline зелёный) |
| Baseline `review` на эталонном файле | 56 находок: 2 `cursorFetchArguments`, 5 `datatype`, 42 `varUseAfterCursor`, 4 `inSubQuery`, 2 `useFuncInIndCol`, 1 `execNotExistsProc` |

## Блокирующие замечания

### 1. Неверная арифметика приёмки (proposal, design, tasks 3.2)

Факт по эталонному файлу `FA/fa-administrator/.../InstrumentSO_LoadStates.sql`:

- `cursorFetchArguments` — 2 FP (строки 158, 308);
- `datatype` — 5 находок, из них 2 курсорных mispairing-FP (158, 308) и 3
  не-курсорные (350 ×2, 369);
- `varUseAfterCursor` — 42 находки, из них **41 FP** (`StateTransition_cursor`,
  после DEALLOCATE 387) и **1 легитимная** (`@ChangedBrief`, строка 652).

Итого уходит **45 FP** = 2 + 2 + 41, а не 46. Остаётся **11** находок, а не 10:

- 4 `inSubQuery` (214 ×2, 251, 263);
- 2 `useFuncInIndCol` (434, 451);
- 1 `execNotExistsProc` (591);
- 3 `datatype` (350 ×2, 369);
- 1 `varUseAfterCursor` (`@ChangedBrief`, 652).

При этом список в tasks 3.2 фактически перечисляет 11 пунктов (4+2+1+3+1), но
заголовок говорит «ровно 10». Нужно исправить в трёх местах:

- `proposal.md`: «46 FP» → «45 FP», «10» → «11»;
- `design.md` (Risks): «уйти ровно 46 FP (2+42+2)» → «45 FP (2+41+2)»;
- `tasks.md` 3.2: «ровно 10 (…)» → «ровно 11 (…)», «46 курсорных FP» → «45 FP».

### 2. Неверная посылка о конвейерах контента (design, Context + Decision 5)

В дизайне сказано, что `checkCursorFetchArguments`/`checkDatatypeFetchInto`
работают на «сыром `fileContent`», а `checkVarUseAfterCursor` — на
макрорасширенном `fileProcessedContent`, и что это два разных конвейера.

По факту (`runner.go`):

- `r.exec.content` = `[]byte(macroResult.Content)` — это **expanded**-контент;
- `fileContent()` возвращает именно его;
- `fileProcessedContent()` возвращает тот же `macroResult.Content`;
- сырой исходник доступен только через `rawFileContent()`/`r.exec.rawContent`.

То есть все три правила уже работают на **одном и том же expanded-тексте**, а
`fragment.LineNumber` находится в тех же координатах, что и строки деклараций.
Это хорошо (нет реального рассинхрона), но формулировки дизайна опасны: буквальное
следование им подтолкнёт реализатора перевести курсорные правила на
`rawFileContent`, что как раз создаст межконвейерный рассинхрон.

Рекомендация: переписать Context/Decision 5 — «все три правила парсят один
expanded-текст (`fileContent` == `fileProcessedContent` == `macroResult.Content`);
`rawFileContent` для курсорных правил не использовать». Дополнительно в tasks 2.3
явно указать, что декларации для окна `checkVarUseAfterCursor` надо брать из того
же `text`, а не из raw.

## Major-замечания (влияют на корректность реализации)

### 3. `parseAllFetchIntoStatements` не видит `FETCH cursor` + `INTO` на следующей строке

Регэксп `reParserFetchNextIntoPrefix` применяется к одной строке и требует `into`
в той же строке. В эталонном файле ключевые FETCH устроены иначе:

- 158: `__FETCH_NEXT__ tmp_Node_cursor` / 159: `into @tmpNumber,`
- 197, 235, 279: то же;
- 308: `fetch StateTransition_cursor` / 309: `into @State`.

Поэтому `parseAllFetchIntoStatements` их не находит. В частности, для DEALLOCATE
387 у `StateTransition_cursor` «переменные текущей жизни» вообще пусты — удаление
41 FP частично опирается не на новую позиционную логику, а на этот пропуск.
Дельта-сценарий «Строки-продолжения многострочного FETCH» тоже не проверяет
заявленный механизм: без предшествующего DEALLOCATE правило и так молчит, а форма
`fetch cursor` + `into` на новой строке не покрыта.

Рекомендация:

- унифицировать разбор FETCH: в `parseAllFetchIntoStatements` переиспользовать
  `parseFetchIntoStatement` (он уже умеет multiline `INTO`) либо расширить
  `reParserFetchNextIntoPrefix` на перенос `into` на следующую строку;
- в tasks 1.2 добавить unit-тест на span для формы `fetch c` \n `into @A, @B`;
- в дельту `varUseAfterCursor` добавить сценарий с DEALLOCATE перед многострочным
  FETCH следующей жизни (иначе сценарий вакуумный);
- после этого перепроверить приёмку: с поддержкой `into`-на-новой-строке окно
  DEALLOCATE 387 пустое (до следующей decl 390), регрессии на эталоне быть не
  должно, но это надо подтвердить прогоном.

### 4. Риск неверного `LineEnd` и off-by-one в окне DEALLOCATE

Текущий цикл сбора продолжений `parseAllFetchIntoStatements` идёт до
`isCursorDeclarationBoundary` и может «съесть» лишние строки (например, `exec`/
`select` не являются границей) вплоть до EOF. Если `LineEnd` положить равным
индексу `j` на выходе из цикла, span захватит строку-границу (`if`/`deallocate`)
или уедет далеко вперёд — новые skip-спаны начнут скрывать реальные
использования переменных. Нужно:

- `LineEnd` = номер последней строки, реально давшей переменную, а не индекс
  завершения цикла;
- в `checkVarUseAfterCursor` окно `(dealloc.Line, nextDecl.Line)` считать с
  корректным off-by-one: при 0-based `i` условие `i+1 < nextDecl.Line`, иначе
  строка следующей декларации и её продолжения попадут в окно;
- добавить parser-unit-тест на span (однострочный FETCH, многострочный FETCH,
  FETCH перед `if`, FETCH в конце файла).

### 5. `fragment.LineNumber` — не всегда строка самого FETCH

`checkCursorFetchArguments`/`checkDatatypeFetchInto` берут `fragment.LineNumber`,
но фрагмент может начинаться раньше FETCH: например `open c` и `fetch c into ...`
без `begin`/пустой строки между ними попадут в один фрагмент, и позиция будет
указана по строке `open`. Для nearest-decl это может дать неверную декларацию на
границе переобъявления.

Рекомендация: вернуть из разбора FETCH собственную строку (offset внутри
`QueryText`) или вычислять `fetchLine = fragment.LineNumber + lineOffset`, как уже
делается для других правил (`findAssignmentLineOffset`). Добавить тест на
склеенный `open`+`fetch`.

### 6. Не описан источник деклараций для нового окна `varUseAfterCursor`

Tasks 2.3 говорит «ограничить следующей декларацией того же имени», но не требует
вызвать `parseCursorDeclarations` в `checkVarUseAfterCursor`. Сейчас правило
декларации вообще не парсит. Нужно явно добавить в задачу: `decls :=
parseCursorDeclarations(text)` на том же expanded-тексте, что и
`parseDeallocateStatements`/`parseAllFetchIntoStatements`, плюс хелпер поиска
следующей декларации (`Line > dealloc.Line`). Заодно исправить в design 4 слово
«выше» на «ниже»: следующая декларация имеет `Line > dealloc.Line`.

### 7. Дедупликация находок не реализована и не специфицирована

Design 3 говорит «дедупликация „одна переменная — один finding“ сохраняется на
уровне правила», но в текущем `checkVarUseAfterCursor` никакого `seen` нет —
находки добавляются на каждое совпадение. После перехода на непересекающиеся окна
дубли в основном исчезнут, но при нескольких DEALLOCATE/перекрытиях они возможны.
Нужно либо добавить `seen` (ключ `line|normalizedVar|cursor`) и отразить это в
tasks 2.3, либо убрать утверждение о дедупликации из дизайна.

## Minor-замечания

- **Non-goal про `useOnlyDeclaredCursors` неточен**: правило не позиционное, а
  per-procedure name-set (собирает все DECLARE процедуры в map и проверяет
  наличие). На изменение это не влияет, но формулировку стоит поправить.
- **`checkTableExistsInMetadata`/`checkViewUsageInServerCode`**: обёртка
  `cursorDeclarationSet(decls) map[string]cursorDeclaration` сохранит поведение
  (важна только принадлежность имени), потерь нет. Замечаний нет.
- **`parseCursorDeclarations` + `Line`**: при реализации захватывать номер строки
  декларации до мутации `i = j-1` в цикле продолжений, иначе `Line` уедет на
  строку перед `open`/границей. В tasks 1.1 стоит подчеркнуть.
- **Тесты**:
  - обновить `TestParseCursorDeclarations_ExplicitAndMacro`
    (`review_rules_test.go:1251`, вызов на :1266) на новый тип;
  - добавить unit-тест на `nearestCursorDeclaration` (нет декларации выше / одна /
    несколько / граница равной строки);
  - добавить parser-unit-тест на `[]fetchStatement` (спаны);
  - существующие тесты `checkVarUseAfterCursor` (3153–3293) остаются валидными,
    включая `MultilineFetchInto_Finding` (FETCH до DEALLOCATE — находки после
    DEALLOCATE сохраняются).
- **Приёмка**: команда из tasks 3.2 рабочая (`go run . --config ... review ...`);
  при запуске через `go run` обязателен `--config`, иначе `config not loaded`.
  В отчёте о приёмке фиксировать разбивку по правилам, а не только total.

## Оценка артефактов

| Артефакт | Оценка | Комментарий |
|---|---|---|
| `proposal.md` | Хорошо, требует правки чисел | Why/What/Impact согласованы с кодом; исправить 46→45, 10→11 |
| `design.md` | Требует правки | Верные решения 1–4, но неверный Context про конвейеры; исправить «выше»→«ниже»; уточнить spans/off-by-one |
| `tasks.md` | Почти готов | Исправить 3.2; добавить явные пункты: declarations в `varUseAfterCursor`, span/off-by-one тесты, dedup |
| `specs/review/postgre-requirements/spec.md` | Хорошо | Требования и сценарии согласованы; усилить сценарий continuation через предшествующий DEALLOCATE |
| `specs/review/fine-code/spec.md` | Хорошо | Полный текст MODIFIED воспроизведён; добавлен корректный сценарий переобъявления |
| `.openspec.yaml` | ОК | `skip_specs` не нужен, дельты есть |

## Рекомендуемый порядок правок перед реализацией

1. Исправить числа приёмки в proposal/design/tasks (45 FP, 11 остаётся).
2. Переписать раздел про конвейеры: один expanded-текст; `rawFileContent` не
   использовать.
3. Уточнить в design/tasks модель span (`LineStart`/`LineEnd`), off-by-one окна,
   источник деклараций в `varUseAfterCursor`, дедупликацию.
4. Добавить в tasks тесты: multiline `INTO` на новой строке, span, склеенный
   `open`+`fetch`, nearest/next declaration, сценарий continuation после
   DEALLOCATE.
5. После реализации прогнать `go test ./...`, `openspec validate --strict` и
   приёмку на эталонном файле с ожидаемым результатом **11 находок**.

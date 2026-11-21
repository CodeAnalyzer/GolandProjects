# Ревью change `fix-review-macro-service-vars`

Дата: 2026-11-21
Ревьюер: анализ артефактов change + фактического кода и данных FA.

## 1. Объём ревью

- Артефакты: `proposal.md`, `design.md`, `tasks.md`, `specs/review/fine-code/spec.md`, `.openspec.yaml`.
- Код: `internal/review/review_helpers.go` (`collectVariableTypes`, `enrichVariableTypesFromAPI`, `reBeginProcedure`), `internal/review/review_parser.go` (`replaceMacros`), `internal/review/runner.go` (пайплайн), `internal/review/review_rules.go` (8 правил-потребителей), `cmd/root.go` (buildNumber), `internal/review/review_rules_test.go` (существующие тесты).
- Данные: `FA\fa-commonfiles\CommonFiles\SERVER\Include\MACROS.H`, эталонный файл `FA\fa-contracts\Consumer\SERVER\Consumer\Cons_BankPartnerInstPrepare.sql`, `.t01` из `FA\fa-contracts\Consumer\SERVER\UPLOAD`, полнотекстовые подсчёты по FA.
- Валидатор: `openspec validate fix-review-macro-service-vars --strict` — **passed**.

## 2. Проверка утверждений change по коду

| Утверждение | Факт | Статус |
|---|---|---|
| `collectVariableTypes` собирает типы только из `parsed.Procedures[].Params` и `extractDeclareBlocks` | review_helpers.go:1984–2018: два источника, возврат на :2018. Точка вставки fill-gaps «после declares, до return» корректна | ✔ |
| `enrichVariableTypesFromAPI` — fill-gaps-паттерн | review_helpers.go:2060: `if _, exists := variableTypes[name]; !exists` | ✔ |
| `reBeginProcedure` матчит `__BEGIN_PROCEDURE__(name)` | review_helpers.go:43: `(?i)__BEGIN_PROCEDURE__\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)` — переиспользование для триггера возможно (`MatchString`) | ✔ |
| Правила работают в expanded-контенте после `replaceMacros` | runner.go:169 (`replaceMacros(fileContent)`), :183 (`content: []byte(macroResult.Content)`); `replaceMacros` (review_parser.go:761) собирает `#define` только из самого файла (`collectMacroDefinitions`), `#include` не резолвится — нераскрытый макрос остаётся текстом | ✔ |
| 8 потребителей карты типов по указанным строкам | Все 8 подтверждены точно: checkDatatypeFetchInto :100, checkDatatypeSelectAssign :189, checkDatatypeExecParams :2641, checkDateIntoString :4922, checkEmptyStringDate :5322, checkIsNullSameTypes :6468, checkDiffTypesComparison :6809, checkFloatToStringConvert :6976 | ✔ |
| `checkDatatypeSelectAssign` тихо скипает при неизвестном типе | review_rules.go:204–207: `if !exists || targetType == ""` → continue | ✔ |
| Severity 3 | types.go:72: `SeverityFineCode = 3` | ✔ |
| DSIDENTIFIER → int даёт потерю, DSBIGINT → int тоже | numericPrecisionScale (review_helpers.go:1217): dsidentifier→(15,0), int/dsint_key→(10,0), dsbigint→(19,0); isPotentialPrecisionLoss (:1195): `sourceP > targetP` | ✔ |
| Ключи карты — lowercase без `@` | normalizeVariableName (:2112): trim `@`/скобок + ToLower → `retval`, `trancount`, `tran_count`, `err_code`, `msg` | ✔ |
| D4: `int` и `DSINT_KEY` эквивалентны для детекции | numericPrecisionScale: обе ветки → (10,0) | ✔ |
| Unit-тесты `collectVariableTypes` уже есть | review_rules_test.go:1912 `TestCollectVariableTypes_DoesNotCaptureKeywordAcrossNewline`, :1929 `TestCollectVariableTypes_ExecOutputDoesNotOverwriteDeclareType` | ✔ |
| buildNumber 1711 → 1712 | cmd/root.go:29: `buildNumber = "1711"` | ✔ |
| MACROS.H:1346 — `__BEGIN_PROCEDURE__`: `declare @RetVal int, @trancount int` | Подтверждено точно, строка 1346 | ✔ |
| MACROS.H:751 — `DCL_PROC_BODY`: `declare @tran_count int, @err_code int, @msg varchar(255)` | Подтверждено точно, строка 751 | ✔ |

## 3. Проверка на реальных данных FA

- **Эталонный файл** `Cons_BankPartnerInstPrepare.sql`: строка 108 — `__BEGIN_PROCEDURE__(Cons_BankPartnerInstPrepare)`, `declare @RetVal` в файле нет; :960 — `select @RetVal = convert(int,NTFID)`; :997 и :1026 — `select @RetVal = max(NTFID)`. Совпадает с tasks 3.1 побайтно. Сценарии 1–2 дельты воспроизводимы на нём.
- **Триггер `__BEGIN_PROCEDURE__`**: по fa-contracts `.sql` — 4797 файлов содержат макрос, 4897 вхождений со скобками, **0 вхождений без скобок** → переиспользование `reBeginProcedure` (требует `(...)`) безопасно и покрывает 100% реальных вызовов.
- **Триггер `DCL_PROC_BODY`**: по всем FA `.sql` — 20 вхождений, **все со скобками** (`DCL_PROC_BODY(Name)`), 0 «голых» → regex `DCL_PROC_BODY\s*\(` покрывает все реальные случаи. Замечание: PG-стиль редкий (20 файлов на весь FA) против ~4800 файлов с `__BEGIN_PROCEDURE__` — массовый эффект даёт первый триггер, второй — полнота.
- **Эмпирика `.t01`** (UPLOAD): всего 59 файлов; ровно `declare @RetVal int, @trancount int` — в 43; `@RetVal` упоминается где-либо в 47; 13 файлов вовсе без `@RetVal` (tmp-таблицы, `patch_xxx.t01` — самодостаточные скрипты). В `.t01` **0** вхождений `__BEGIN_PROCEDURE__`/`DCL_PROC_BODY` — препроцессор раскрыл макросы, декларации инлайном: подтверждает и модель «`.t01` самодостаточны», и то, что в нераскрытых `.sql` триггер необходим.
- **Конфликт с явными `declare`**: найденные в FA `declare @RetVal DSINT_KEY` (211 файлов с `@RetVal DS*`, почти все — declare-блоки, напр. `ODPointVal_Verify.sql:35`) защищены fill-gaps (D2); DSINT_KEY≡int по precision — детекция не меняется.

## 4. Согласованность артефактов

- proposal ↔ design ↔ tasks ↔ дельта: набор переменных и типов совпадает во всех четырёх артефактах (`retval/trancount → int`; `tran_count/err_code → int`, `msg → varchar(255)`); 8 правил-потребителей перечислены одинаково.
- Дельта: `## ADDED Requirements` (мн. ч., уровень 2), SHALL/MUST в теле требования, 5 сценариев GIVEN/WHEN/THEN — соответствует правилам openspec/AGENTS.md; `--strict` валидация пройдена; путь `specs/review/fine-code/spec.md` зеркалит существующую основную спеку `openspec/specs/review/fine-code/spec.md` (файл существует).
- Сценарии дельты покрыты задачами: сценарии 1–2 → tasks 2.2/3.1, сценарий 3 → 2.1, сценарий 4 → 2.1, сценарий 5 → 2.1/3.2 (patch-файл).
- Non-Goals (include-резолвинг, индексация `#define`, прочие макросы) — корректно вынесены за scope, дублирования с Proposed нет.

## 5. Замечания

### 5.1. Неточность в эмпирике design.md (minor, рекомендация исправить текст)

design.md Context: «43/46 препроцессированных `.t01` … содержат ровно `declare @RetVal int, @trancount int`». Фактически в каталоге 59 `.t01`: 43 — с точным набором, 47 — с `@RetVal` в любом виде, 13 — без него вовсе (tmp/patch). Числитель 43 верен, знаменатель 46 не воспроизводится (ближе всего 47 файлов с `@RetVal`). На решение не влияет (доминирование точного набора и наличие выбросов подтверждены), но формулировку стоит поправить, чтобы эмпирика воспроизводилась: например «43 из 47 файлов, использующих @RetVal (59 .t01 в каталоге, 13 из них — tmp/patch без @RetVal)».

### 5.2. Взаимодействие встроенных типов с API-fallback (info, принять и задокументировать)

Порядок источников после правки: параметры парсера → declares → **встроенные типы** (внутри `collectVariableTypes`) → API-индекс (`enrichVariableTypesFromAPI`). D2 формулирует приоритет «сигнатура и declares > встроенные» — для сигнатуры из парсера это выполняется. Но для пятого источника — параметров из БД-индекса — встроенные типы теперь заполнят карту раньше, и fill-gaps в `enrichVariableTypesFromAPI` уже не сможет доставить тип для `retval`/`trancount`/`tran_count`/`err_code`/`msg`: API-fallback для этих пяти имён становится недостижим. По данным FA это теоретический кейс (в сигнатурах `@RetVal DS*` как параметр процедуры практически не встречается, все найденные — declare-блоки), но эффект не отражён в design.md. Рекомендация: добавить в D2 одно предложение («встроенные типы применяются до API-enrichment, поэтому для пяти сервисных имён API-fallback не достигается — принято сознательно») — либо, если хочется обратного, встраивать типы в `enrichVariableTypesFromAPI` после API-блока. Реализация «как в tasks 1.1» допустима без изменений.

### 5.3. Покрытие сценария «convert подавляет finding» на unit-уровне (info)

Сценарий 2 дельты («Явная конвертация подавляет finding») проверяется только интеграционным тестом (tasks 2.2). Для unit-теста `collectVariableTypes` это и не нужно (карта типов не зависит от convert), поэтому замечание — только о трассируемости: в tasks 2.1 перечислены 4 из 5 сценариев дельты. Можно оставить как есть — покрытие через 2.2 достаточно.

### 5.4. Подтверждённые нетривиальные детали, учтённые change корректно

- Файл, определяющий макрос локально через `#define` и раскрывающий его, триггером не станет (вызов исчезает в expanded-контенте, декларация из тела появится в контенте и подберётся `extractDeclareBlocks`) — семантика «MUST считаться нераскрытым… после подстановки макросов, определённых в самом файле» в дельте сформулирована точно.
- Отсутствие ложного срабатывания `reDeclareVar` на склейке `@RetVal int, @trancount int` уже покрыто существующим поведением `extractDeclareBlocks`/`reDeclareVar` (см. тест :1929).
- Отсутствие пересечения с `debugProfileCarrierRe` (review_rules.go:1703, содержит голые имена `DCL_PROC_BODY`/`__BEGIN_PROCEDURE__`) — разные правила, interference нет.

## 6. Вывод

**Change утверждён к реализации (approve) с двумя мелкими правками текста.**

- Механика, точки правки, триггеры, типы и семантика приоритетов полностью подтверждены фактическим кодом и данными FA; эталонный кейс (Cons_BankPartnerInstPrepare.sql, строки 108/960/997/1026) воспроизводим побайтно.
- Регулярки-триггеры покрывают 100% реальных вхождений в FA (0 «голых» форм обоих макросов).
- Перед apply рекомендуется: (1) поправить знаменатель эмпирики в design.md (п. 5.1); (2) добавить в D2 предложение о недостижимости API-fallback для пяти сервисных имён (п. 5.2). Обе правки не меняют ни кода, ни дельты спеки.

# Код-ревью реализации change `review-rule-test-coverage`

- **Дата:** 2026-11-15
- **Объект ревью:** реализация change `review-rule-test-coverage`
- **Изменённый код:** `internal/review/review_rule_coverage_test.go` (новый), `review_lookup.go`, `runner.go`, `review_rules.go`
- **Артефакты:** proposal.md, design.md, tasks.md, specs/review/test-coverage/spec.md
- **Вердикт:** **Одобрено.** Все блокеры из change-ревью устранены корректно; найденные замечания — незначительные, не блокируют архивацию.

---

## 1. Верификация проверок реализации

| Проверка | Результат |
|---|---|
| `go test ./internal/review/...` | ✅ ok (1.2s) |
| `go vet ./internal/review/...` | ✅ чисто |
| `openspec validate review-rule-test-coverage --strict` | ✅ valid |
| `go test -tags integration -run '^$' ./internal/review/...` | ✅ компилируется; integration-файлы не изменялись (подтверждено `git status`) |
| Прогон новых coverage-тестов (`-v -run Coverage\|NilDB\|ValidateCoverage`) | ✅ все PASS (96 подтестов) |
| Race-детектор | недоступен в среде (требует cgo); тесты однопоточные (`t.Parallel` не используется) — риск отсутствует |
| Кодировка артефактов | ✅ UTF-8 (кириллица корректна во всех .md) |
| tasks.md | ✅ 36/36 отмечено, формат чек-листа сохранён |

---

## 2. Ревью прод-кода (тестовые seam'ы)

Дифф: 3 файла, +18/−1 строк. Все правки — точечные, поведение при не-nil `db` не меняется.

### 2.1 `lookupProcedureCreateFiles` (review_lookup.go:197) ✅
`if r.db == nil { return nil, nil }` — деградация в «файлы не найдены» → `len(fileIDs) > 1` ложно → cross-file finding не формируется. Семантически эквивалентно «протестированному негативу»: одиночная процедура в реальной БД тоже даёт 1 файл → отсутствие finding. Закрывает блокер F2 change-ревью (вариант (a)).

### 2.2 `findAPITableNames` (review_lookup.go:821) ✅
Guard возвращает пустую карту — API-имена таблиц не резолвятся. Закрывает блокер F1 (вариант (a)). Гward стоит после нормализации имён — корректно (пустой вход и так возвращает пустой результат).

### 2.3 `cachedFindColumnDefinitionType` (runner.go:494) ✅
На cache miss при `db == nil` → `("", nil)`. Эквивалентно существующей ветке `ErrNoRows` (кэширует "" и возвращает nil) — консистентная деградация «тип неизвестен». Выявлено паникой в datatype-ветках при реализации — засчитано, задокументировано в design.

**Примечание:** на cache miss с nil db пустой результат НЕ кэшируется (в отличие от ErrNoRows-пути). Для unit-тестов несущественно; в проде `db` всегда задан.

### 2.4 `checkDatatypeExecParams` (review_rules.go:2625) ✅
Условие выхода заменено на `r.db == nil && r.procParamsCache == nil` — ветка работает на предзаполненном каталоге параметров. Тело проверено: остальные вызовы (`cachedLookupProcedureParams`, `enrichVariableTypesFromAPI`) nil-безопасны, паники нет. В проде (`db != nil`) поведение идентично прежнему.

### 2.5 Соответствие change-ревью

| Находка change-ревью | Разрешение в реализации | Статус |
|---|---|---|
| F1: findAPITableNames паника на nil db | nil-guard + unit-тест guard'а (Task 1.3) | ✅ устранено |
| F2: lookupProcedureCreateFiles паника | nil-guard + unit-тест guard'а (Task 1.4); task 4.2 переформулирован (unit = within-file, cross-file остаётся в integration) | ✅ устранено |
| F3: неверный кэш в tasks 3.7/4.4 | Заменено на `tableProductIDCache` (tasks 3.7, 4.4) | ✅ устранено |
| F4: критерий «прямого теста» не сформулирован | Добавлен в design Context (строка 7) | ✅ устранено |
| F5: граница §3/§5 | Примечание в design Decision 4 | ✅ устранено |
| F6: scope guard'а задачи 1.2 | Уточнение в tasks 1.2 | ✅ устранено |
| F7: proposal Impact не покрывает seam'ы | Impact дополнен перечнем трёх файлов-правок | ✅ устранено |

---

## 3. Ревью тестового кода

### 3.1 Структура `review_rule_coverage_test.go` (988 строк)

- **Хелпер:** `ruleCase` / `validateRuleCases` (guard pos+neg, задача 1.2) / `runFileRuleCases` / `runParsedRuleCases` / `assertRuleFindings` + self-тест `TestRuleCaseHelper_ValidateCoverage` (empty / positive-only / negative-only / both). Задача 1.1–1.2 выполнена.
- **`assertRuleFindings`** проверяет: count, Rule ID, **severity из `ruleCatalog`** (спец-требование 4), Line (опционально, для позиционных), Object (EqualFold). Тексты сообщений не проверяются — соответствует Risks design.
- **29 rule-level тестов** ровно по целевым правилам (27 непокрытых + 2 односторонних), кейсы table-driven, негативы «соседние» (Decision 5 соблюдён: тот же SQL с корректной формой).
- Temp-файлы через `t.Cleanup` — утечек нет; зависимостей от окружения/БД нет.

### 3.2 Корректность подмен (spot-check по коду правил)

| Тест | Подмена | Проверка корректности |
|---|---|---|
| indexExistsInDB | `indexCandCache["pt"]` | `lookupIndexExists` cache-first ✅; M_DELETE_PTABLE-негатив через `lookupIndexExists` ✅ |
| deferredUpdate | `indexFieldsCache["xpkpfoo"]` | нормализация `XPKpFoo`→`xpkpfoo` совпадает с `lookupIndexFieldsByName` ✅ |
| duplicateOutputVariable | `procParamsCache` + OUT-параметры | `cachedLookupProcedureParams` nil-safe ✅ |
| foreignPTables/foreignTablesUsing | `tableProductIDCache` | `cachedLookupTableProductIDs` cache-first ✅; фильтр префиксов t/p в `dedupeTableRefs` ✅ |
| execNotExistsProc | `procProductIDCache` + `ExistsInArea` | точное поле `store.ProcedureScopeInfo.ExistsInArea` ✅ |
| procDuplicate | без кэша (nil-guard) | within-file дубль через `reProcDuplicateHeader`, строка 6 = второе определение ✅ |
| memoryUniqueIndexUpdate | `memoryTableCache`+`memoryTablesLoaded=true` | флаг обязателен — иначе ленивая загрузка с nil db даёт пустое множество ✅ |
| datatype (5 веток) | `colTypeCache` ключи `table\|col`, `alias\|col`; `procParamsCache` | формат ключа совпадает с `cachedFindColumnDefinitionType` (runner.go:487) ✅ |

### 3.3 Замечания к тестам (не блокирующие)

- **N1.** `assertRuleFindings` проверяет Line/Object только у `findings[0]` — для многfinding-позитивов валидируется первый. Сейчас все позитивы односfinding'овые; при появлении многfinding-кейсов стоит расширить хелпер.
- **N2.** `TestCheckSelectAfterSetRowcount` и `TestCheckIsNullSameTypes` собирают `ParseResult` вручную (мимо парсера) — прагматично для fragment-driven правил; синтетические LineNumber. Приемлемо, зафиксировано здесь для истории.
- **N3.** `cachedLookupMacroType` на cache miss идёт в БД (`lookupMacroSignature`) — в nil-guard-схемы не входит. Текущие тесты его не задевают (аргументы — переменные, не вызовы функций). Потенциальный будущий seam-gap для datatype-тестов с функциональными аргументами — кандидат в follow-up, не блокер.

---

## 4. Проверка матрицы покрытия

### 4.1 Коррекция числа правил: 59, а не 57

Реализация скорректировала количество правил каталога с 57 на **59** — проверено точным подсчётом: 59 констант в types.go, 59 записей в `ruleCatalog`, 59 `Build:`-замыканий. Исходный подсчёт 57 (в аудите и в моём change-ревью) был артефактом regex без цифр: `RuleForceOrder2Tbl` и `RuleShouldBeCP866` не попадали в выборку `Rule[A-Za-z]+`. **Коррекция реализации верна**; оба правила имели analyze-level тесты (`TestAnalyzeStatementForForceOrder2Tbl_*`, тесты `checkShouldBeCP866`), поэтому матрица сходится.

### 4.2 Матрица «59 × (unit, positive, negative)»

- 27 ранее непокрытых правил — покрыты новыми тестами (проверено по составу файла: 4+7+4+12 групп);
- `diffTypesComparison` — добавлен negative (3 кейса) к существующему positive; `foreignProcedureUsing` — добавлен unit positive к существующему negative;
- 30 ранее покрытых — выборочно верифицированы на двусторонность (`debugProfileEnabled`: 12 кейсов pos+neg; `emptyReturn`: pos+neg; `maxProcParam`, `tooManyJoins`: pos+neg) — соответствуют design Context;
- **Итог: 59/59 правил имеют rule-level unit-тест с обеими сторонами в обычном прогоне** — требование спеки выполнено.

### 4.3 Соответствие дельта-спеке

| Требование спеки | Статус |
|---|---|
| Unit-тест для каждого правила, без integration/БД | ✅ 59/59, все — checkX/analyze в обычном прогоне |
| Положительный и отрицательный кейсы | ✅ guard `validateRuleCases` + все 29 новых наборов содержат обе стороны |
| Детерминированные подмены вместо БД | ✅ кэши-двойники + nil-guard'ы; PostgreSQL не требуется |
| Проверка идентичности finding (rule, severity, line) | ✅ `assertRuleFindings`: rule+severity всегда, line/object для позиционных кейсов |

---

## 5. Ревью обновлённых артефактов

- **proposal.md:** What Changes дополнен двумя пунктами про nil-guard'ы и seam; Impact перечисляет три файла прод-правок с оговоркой «при не-nil db поведение не меняется». Согласован с кодом.
- **design.md:** Context исправлен на 59 правил + явный критерий прямого теста; Decision 2 документирует три guard'а и seam `checkDatatypeExecParams` как «единственные правки прод-кода»; Non-Goals содержит явное исключение; добавлен риск «nil-guard замаскирует отсутствие БД в проде» с обоснованием (Runner в проде всегда с БД + guard покрыт unit-тестом).
- **tasks.md:** добавлены 1.3/1.4 (guard'ы), исправлены формулировки под фактическую семантику правил: truncTbl (positive — в процедуре), columnInsert (negative — со списком колонок), saveTran (`SAVE TRANSACTION` — тоже finding; проверено по коду правила — намеренно матчит обе формы, согласуется с поведением rawTransactionControl). Все 36 пунктов отмечены.
- **spec.md:** не изменялся, валиден.

### Замечания к артефактам (не блокирующие)

- **M1. tasks.md 3.4 (`floatToStringConvert`) — формулировка не обновлена:** заявлено positive «float в строку **без явного** преобразования» и negative «явный convert/cast». Фактическая семантика правила обратная: правило флагует явный `CONVERT(строка, float_expr)` (review_rules.go:6791-6833), и тест правильно утверждает `convert(varchar, @f)` → finding. Тесты верны, текст задачи стоит выровнять, как это сделано для saveTran/columnInsert.
- **M2. Ряд негативных вариантов из формулировок задач не реализован** (спец-контракт «≥1 pos + ≥1 neg» при этом выполнен везде): 3.2 «без индекса» (UPDATE без хинта), 2.1 «вложенный CASE», 5.7 «присваивание до использования», 5.12 «повторные/позиционные лишние аргументы», 3.6 token-paste алиас на уровне правила (существует только входной тест `TestIndexWrongInputs_*`). Кандидаты на дозаполнение при следующем касании тестов.
- **M3. design.md Migration Plan** всё ещё говорит «прод-код не затрагивается» — противоречит Decision 2 (три seam-правки). Словесная нестыковка внутри одного документа.

---

## 6. Итог

**Одобрено.** Реализация:

1. устранила все блокеры change-ревью выбранными в нём вариантами (nil-guard'ы) и задокументировала их в артефактах;
2. скорректировала фактическую ошибку подсчёта правил (59 вместо 57) с явным критерием покрытия в design;
3. добавила 29 rule-level тестов качества выше среднего: соседние негативы, проверка severity из каталога, nil-safe подмены, self-тест guard'а;
4. не изменила продовое поведение (дифф +18/−1, все правки — тестовые seam'ы при `db == nil`);
5. привела матрицу к «59 правил × (unit, positive, negative) — все закрыты».

**Follow-up кандидаты (вне scope change):** `cachedLookupMacroType` без nil-guard (N3); негатив-варианты из M2; выравнивание формулировки 3.4 (M1) и Migration Plan (M3) — можно поправить при архивации или в следующем тестовом change.

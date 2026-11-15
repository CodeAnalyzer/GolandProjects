# Ревью change `review-rule-test-coverage`

- **Дата:** 2026-11-15
- **Объект ревью:** `openspec/changes/review-rule-test-coverage/` (proposal.md, design.md, tasks.md, specs/review/test-coverage/spec.md, .openspec.yaml)
- **Проверяемый код:** `internal/review/` (types.go, catalog.go, review_rules.go, review_lookup.go, runner.go, scope.go, тесты)
- **Вердикт:** **Условно одобрено — требуется ревизия задач 3.7, 4.2, 4.4 и уточнение proposal/design перед реализацией**

---

## 1. Проверка артефактов

### 1.1 Валидация

- `openspec validate review-rule-test-coverage --strict` — **проходит**.
- Состав change полный: proposal.md (Why/What Changes/Capabilities/Impact), design.md (Context/Goals-Non-Goals/Decisions/Risks/Migration Plan), tasks.md (7 групп, 34 задачи), дельта specs/review/test-coverage/spec.md.
- Дельта-спека корректна по формату: `## Purpose` (новая capability), `## ADDED Requirements` (мн. ч., h2), 4 требования `### Requirement` с телом, содержащим английское `MUST`, минимум один `#### Scenario` на требование, сценарии в GIVEN/WHEN/THEN-стиле.
- Новая capability `review/test-coverage` не конфликтует с существующими `review/*` (deploy-stoppers, fine-code, postgre-requirements, review-scope, sql-preprocessing) — добавляет тестовый контракт, а не меняет поведение правил.

### 1.2 Согласованность артефактов между собой

- Proposal ↔ design ↔ tasks согласованы: классификация 27 непокрытых правил (4 без тестов / 7 без rule-specific логики / 4 integration-only / 12 helper-only) и 2 односторонних правила одинаково отражена во всех трёх документах.
- Задачи покрывают все группы из design Decision 4, порядок задач совпадает с заявленным приоритетом.
- Спека ↔ tasks: требование о pos+neg-кейсах ↔ задача 1.2 (guard на односторонний набор); требование об идентичности finding ↔ `wantRule/wantSeverity/wantLine/wantObject` в хелпере (1.1).

---

## 2. Проверка фактической базы (сверка с кодом)

Все ключевые утверждения Context проверены по коду — **расхождений по сути нет**:

| Утверждение | Проверка | Результат |
|---|---|---|
| 57 правил в каталоге | 57 констант в types.go, 57 записей `Build:` в ruleCatalog | ✅ подтверждено |
| `procElseCase`, `truncTbl`, `useSelectAll`, `indexExistsInDB` без тестов | grep по всем `*_test.go` — ни одного вызова/упоминания check-функций | ✅ подтверждено |
| Тесты `TRUNCATE`/`SAVE TRAN` — не покрытие этих правил | Найденные кейсы принадлежат `TestCheckModifyOutProc` и `TestCheckRawTransactionControl` | ✅ подтверждено (ложного покрытия нет) |
| `diffTypesComparison` — только positive | `TestCheckDiffTypesComparison_AnchorWithGluedMacro` (review_rules_test.go:4020) — только позитив + якорь | ✅ подтверждено |
| `foreignProcedureUsing` — только negative в unit | `TestForeignProceduresSkipsAPINamesCaseInsensitive`; positive только в integration (review_scope_integration_test.go:344) | ✅ подтверждено |
| 4 правила integration-only | `checkProcDuplicate`/`checkExecNotExistsProcedures`/`checkForeignTables` — только в review_scope_integration_test.go; `memoryUniqueIndexUpdate` — только в review_memory_unique_integration_test.go | ✅ подтверждено |
| Группа §3 (helper-тесты без rule-логики) | `TestCheckDatatypeExecParams_NoDB_NoPanic` (только «0 findings, no panic»), `TestIndexWrongInputs_MacroParamTokenPasteAlias` (входные данные, не правило) | ✅ подтверждено |
| Группа §5 (helper-тесты обоих кейсов) | Пример: existsWithAndInIf — `hasIfWithAndAndQuery`/`hasTopLevelAnd` с pos+neg; ни одной вызова check-функций у всех 12 правил | ✅ подтверждено |

**Замечание по подсчёту (низкий приоритет).** Прямой подсчёт вызовов check-функций в unit-тестах даёт 27 rule-level функций (28 уникальных вызовов минус `checkDatatypeExecParams` — это подфункция datatype из §3). Число «30» из proposal/design получается, только если считать прямыми ещё 5 правил, тестируемых на уровне `analyze*`/`extract*` с проверкой Rule ID и Object: `useEqColumn`, `tableHintIsRight`, `updateOnlyVar`, `pTableSpid`, `forceOrder2Tbl` (например, `TestAnalyzeConditionForEqColumn_*`, `TestAnalyzeStatementForHintType_*`). Классификация защитима, но критерий не сформулирован в design — а на нём строится финальная матрица задачи 7.2. Рекомендация: добавить в design одно предложение-критерий («rule-level тестом считается тест, вызывающий check-функцию или её statement-анализатор с проверкой Rule/count/Object; тест чистой helper-функции не считается»).

---

## 3. Проверка реализуемости подхода (design Decisions)

### Decision 1 — тест на уровне check-функции с `reviewExecContext` ✅

- `fileContent`/`fileProcessedContent` (runner.go:388, :519) читают `r.exec` по совпадению пути — БД не нужна.
- Паттерн доказан в существующих тестах (`TestCheckModifyOutProc`, `TestCheckRawTransactionControl`, `TestCheckDiffTypesComparison`, `TestCheckTableFullScan_*`), а также в `newAPIRuleRunner` (review_api_proc_direct_call_test.go:18) — ровно так, как описано в design.

### Decision 2 — подмены через prewarm-кэши ✅ с двумя исключениями (см. §4)

Проверена nil-безопасность и cache-first-механика всех точек подмены:

| Lookup | Кэш | Cache-first | `db == nil` |
|---|---|---|---|
| `cachedLookupProcedureScopeInfo` | `procProductIDCache` | да (review_lookup.go:90) | ✅ деградирует в «не найдено» |
| `cachedLookupProcedureParams` | `procParamsCache` | да (:69) | ✅ возвращает nil |
| `cachedLookupTableProductIDs` | `tableProductIDCache` | да (:110) | ✅ возвращает nil |
| `lookupIndexExists` | `indexCandCache` | да (:243) | ✅ при предзаполнении |
| `lookupTableIndexCandidates` | `indexCandCache` | да (:782) | ✅ при предзаполнении |
| `lookupIndexFieldsByName` | `indexFieldsCache` | да (:277) | ✅ при предзаполнении |
| `isMemoryTable` / `isViewTable` | `memoryTableCache`+flag / `viewNameCache`+flag | ленивая загрузка | ✅ предзаполнение обоих полей (same package) |
| `findAPITableNames` | — нет кэша | — | ❌ **паника на nil db** |
| `lookupProcedureCreateFiles` | — нет кэша | — | ❌ **паника на nil db** |

Zero-value `ReviewScope{}` безопасен (`Pairs()` → `NewProductBranchPairs(nil)`).

### Проверка задач по отдельности

| Задача | Правило | Верификация реализуемости |
|---|---|---|
| 2.1–2.3 | procElseCase / truncTbl / useSelectAll | ✅ чисто контентные; негативы «в комментарии» соответствуют реальным guard'ам (review_rules.go:2455, :2498, :2533) |
| 2.4 | indexExistsInDB | ✅ предзаполнение `indexCandCache`; негативы «индекс существует» и «M_DELETE_PTABLE*» реализуемы (`lookupIndexExists` cache-first) |
| 3.1 | datatype | ✅ список веток задачи точно соответствует фактическим подфункциям (SelectAssign/UpdateSet/InsertSelect/FetchInto/ExecParams, review_rules.go:90/175/2619/2779/2842) |
| 3.2 | deferredUpdate | ✅ предзаполнение `indexFieldsCache`; негатив «без индекса» = реальный continue при пустом indexName (:4346) |
| 3.3–3.5 | duplicateOutputVariable / floatToStringConvert / selectAfterSetRowcount | ✅ контентные/parsed-зависимые, БД не требуют |
| 3.6 | indexWrong | ✅ через `indexCandCache`+`indexFieldsCache`; кейс token-paste алиаса обоснован комментарием существующего `TestIndexWrongInputs_*` |
| **3.7** | **foreignPTablesUsing** | ❌ см. находку F1, F3 |
| 4.1 | execNotExistsProc | ✅ полностью по образцу `newAPIRuleRunner`: `cachedLookupProcedureScopeInfo` с полем `ExistsInArea` |
| **4.2** | **procDuplicate** | ❌ см. находку F2 (within-file путь ✅, cross-file — нет) |
| 4.3 | memoryUniqueIndexUpdate | ✅ предзаполнение `memoryTableCache`+`memoryTablesLoaded` и `indexCandCache` |
| **4.4** | **foreignTablesUsing** | ⚠️ реализуемо, но в задаче не тот кэш — см. F3 |
| 5.x, 6.x | 12 helper-only + 2 односторонних | ✅ контентные, прямая реализация по существующим образцам |

### Baseline

- `go test ./internal/review/...` — **проходит** (ok, 0.7s); `go vet ./internal/review/...` — чисто. Change строится на здоровом baseline.

---

## 4. Находки

### F1. [Блокер для 3.7] `findAPITableNames` паникует на nil db — конфликт с ограничением «только тестовый слой»

`checkForeignPTables` (review_rules.go:1508) при наличии хотя бы одной p-table вызывает `findAPITableNames`, которая безусловно делает `r.db.QueryContext` (review_lookup.go:818). `store.DB` встраивает `*sql.DB` (db.go:16); вызов `QueryContext` на nil-приёмнике dereferencирует `db.boundTx` (db_tx.go:80) → **nil pointer panic**. Позитивный кейс задачи 3.7 по определению содержит p-table → юнит-тест с `&Runner{}` упадёт с паникой.

Proposal утверждает «код правил не меняется», design в Non-Goals — «не менять поведение». Добавление guard'а — не изменение поведения, а тестовый seam, но в change это не запланировано.

**Варианты:**
- (a) добавить nil-guard в `findAPITableNames` (`if r.db == nil { return map, nil }`) — 2 строки, согласуется с деградацией остальных cached*-lookup'ов; зафиксировать в proposal Impact и design как явное исключение;
- (b) тестировать на уровне под-seam (не рекомендуется — уходит от проверки правила целиком).

**Рекомендация:** вариант (a) с явной фиксацией исключения в change.

### F2. [Блокер для 4.2 в текущей формулировке] `lookupProcedureCreateFiles` не имеет кэша и nil-guard'а

`checkProcDuplicate` (review_rules.go:2190) для каждого `parsed.Procedures` вызывает `lookupProcedureCreateFiles` → безусловный `r.db.QueryContext` (review_lookup.go:210) → паника на nil db. Within-file-путь (дубль в одном файле) тестируем без БД, но позитивный кейс с двумя `create proc` гарантированно заполнит `parsed.Procedures` и упрётся в cross-file ветку.

**Варианты:**
- (a) nil-guard (+ опционально prewarm-кэш) для `lookupProcedureCreateFiles` — prod-обвязка, требует фиксации исключения как в F1;
- (b) переформулировать 4.2: unit = within-file дубль (positive) + негатив «разные ветки» → перенести «разные ветки» в негативы within-file/namespace-логики, а cross-file позитив оставить в integration;
- (c) оставить как есть — реализация заблокируется на панике.

**Рекомендация:** (b) — минимальное изменение change, не трогает прод; (a) — если хочется полного rule-level юнита.

### F3. [Средний] В задачах 3.7 и 4.4 указан неверный кэш-подмена

Обе задачи ссылаются на `procProductIDCache`. Фактически владение **таблицей** резолвится через `cachedLookupTableProductIDs` → **`tableProductIDCache`** (runner.go:54; вызовы review_rules.go:60 и :1526). `procProductIDCache` — это кэш владельцев **процедур** (foreignProcedureUsing/apiProcDirectCall/execNotExistsProc). Реализация «по тексту задачи» не даст positive-кейса.

**Рекомендация:** заменить в 3.7 и 4.4 на `tableProductIDCache` (для 4.4 этого достаточно; для 3.7 дополнительно см. F1). Для `foreignTablesUsing` паники нет — правило не вызывает `findAPITableNames`.

### F4. [Низкий] Критерий «прямого unit-теста» не сформулирован

Число «30 прямых» держится на неявном включении 5 analyze*-уровневых тестов (см. §2 замечание). Для воспроизводимости матрицы 7.2 сформулировать критерий в design (одним предложением).

### F5. [Низкий] Граница групп §3/§5 размыта

Обе группы — «нет rule-level теста»; различие («helper-тесты есть, но не проверяют логику правила» vs «helper-only покрытие») не меняет объём работы. Не блокирует; допустимо оставить как есть (группы соответствуют аудиту), либо объединить примечанием.

### F6. [Инфо] Guard задачи 1.2 покрывает только хелпер

Проверка «набор содержит pos и neg» сработает для тестов на новом хелпере; легаси-тесты вне хелпера (например, `TestCheckDatatypeExecParams_NoDB_NoPanic`) guard'ом не контролируются — они переписываются в рамках §3/§5. Это соответствует спеке, но стоит явно указать в 1.2, что guard — про хелпер, а требование спеки — про все правила.

### F7. [Инфо] Proposal Impact не покрывает исключения F1/F2

Формулировка «код правил не меняется, кроме случаев, когда тест выявляет дефект» не описывает тестобъективные seam'ы (nil-guard'ы). При выборе варианта (a) в F1/F2 дополнить Impact и design Decisions 2 перечнем двух точечных прав прод-обвязки.

---

## 5. Положительные стороны change

1. **Фактура проверена и точна:** аудит покрытия (57 правил, группы, односторонние правила) полностью подтверждается кодом, включая ловушку «кажущегося покрытия» (TRUNCATE/SAVE TRAN кейсы в тестах соседних правил).
2. **Подход выбран верный и доказанный:** `reviewExecContext` + prewarm-кэши — существующий, проверенный в коде паттерн; design честно фиксирует альтернативы (sqlmock/реальная БД отклонены обоснованно).
3. **Хорошая инженерная гигиена:** negative-кейс «соседний» (Decision 5), проверка идентичности finding вместо текста сообщений (Risks), явная политика при обнаружении дефекта правила (Risks, п.5 — блокировать задачу, а не помечать expected-fail).
4. **Спека качественная:** требования проверяемы, сценарии behavior-oriented, границы (без БД, без тега integration, double-объекты) сформулированы строго.
5. **Разумные Non-Goals:** не дублировать 28 существующих тестов, не трогать integration-слой и CI.
6. **Правильный порядок работ:** первыми закрываются правила без тестов вообще.

---

## 6. Итоговое заключение и действия перед реализацией

| № | Действие | Приоритет |
|---|---|---|
| 1 | Исправить tasks 3.7 и 4.4: `procProductIDCache` → `tableProductIDCache` (F3) | Обязательно |
| 2 | Разрешить конфликт F1 для 3.7: либо nil-guard в `findAPITableNames` + фиксация исключения в proposal/design, либо переформулировать задачу | Обязательно |
| 3 | Разрешить конфликт F2 для 4.2: переформулировать (unit = within-file + namespace-негативы) либо добавить nil-guard/кэш для `lookupProcedureCreateFiles` с фиксацией исключения | Обязательно |
| 4 | Добавить в design критерий «прямого unit-теста» для матрицы 7.2 (F4) | Желательно |
| 5 | Уточнить 1.2: guard действует для тестов на хелпере (F6) | Желательно |
| 6 | (Опционально) примечание о границе §3/§5 (F5) | Опционально |

После ревизии задач 3.7/4.2/4.4 и (при выборе guard-вариантов) дополнения proposal/design change полностью готов к реализации: остальная часть задач (§1–§6 кроме указанных, §7) верифицирована как реализуемая на текущем baseline без изменения прод-кода.

# Код-ревью реализации `fix-review-macro-service-vars`

Дата: 2026-11-21
Предмет: реализация change `fix-review-macro-service-vars` (build 1711 → 1712).

## 1. Объём ревью

- Diff по git: `cmd/root.go`, `internal/review/review_helpers.go`, `internal/review/review_rules_test.go`, `internal/review/review_rule_coverage_test.go` (+ пересобранный `CodeBase.exe`). Других изменений кода нет — посторонних правок не обнаружено.
- Артефакты: обновлённые `design.md`/`tasks.md` change (учёт замечаний предыдущего ревью, фиксация приёмки).
- Проверки: `go build ./...`, `go vet ./...`, `go test ./...`, `openspec validate --strict`, независимый прогон `CodeBase.exe review` на эталонном файле.
- Анализ потребителей карты типов: 8 правил, использующих `collectVariableTypes` (FP-поверхность, `areEquivalentTypes`, `typeGroup`, `isPotentialPrecisionLoss`).

## 2. Состав изменений

`internal/review/review_helpers.go`:

- `reDclProcBody = (?i)DCL_PROC_BODY\s*\(` — триггер PG-стиля (review_helpers.go:45).
- `collectVariableTypes`: перед return добавлен вызов `applyMacroServiceVarDefaults(result, content)` (review_helpers.go:2019).
- `applyMacroServiceVarDefaults`: при `reBeginProcedure.MatchString(content)` → `retval=int`, `trancount=int`; при `reDclProcBody.MatchString(content)` → `tran_count=int`, `err_code=int`, `msg=varchar(255)` (review_helpers.go:2022–2034).
- `setDefaultVariableType`: fill-gaps (`if _, exists := result[name]; exists { return }`) (review_helpers.go:2036–2041).
- Комментарий-блок: ссылки на `MACROS.H:1346`/`:751`, эмпирика по `.t01` в исправленной формулировке «43 из 47» (замечание 5.1 предыдущего ревью учтено).

`cmd/root.go:29`: `buildNumber = "1712"`.

Тесты:

- `TestCollectVariableTypes_MacroServiceVarDefaults` (review_rules_test.go:1950) — 5 кейсов: оба макроса, переопределение `@msg`/`@RetVal` явным declare, отсутствие макросов.
- `TestCheckDatatype_MacroServiceVars` (review_rule_coverage_test.go:595) — 3 кейса: `max`→finding, `convert`→нет finding, без макроса→нет finding; `macroTypeCache["max"]=""` — легитимный приём (консистентен с существующими тестами, review_rules_test.go:7112+), исключает обращение к nil-БД через `cachedLookupMacroType` (review_lookup.go:2013).

## 3. Соответствие запланированному change

| Пункт change | Реализация | Статус |
|---|---|---|
| D1: встроенная таблица + триггер по тексту макроса, переиспользование `reBeginProcedure` | Ровно так; новый `reDclProcBody`; семантика «нераскрытый после replaceMacros» соблюдена — все потребители передают expanded-контент (`fileContent` → `r.exec.content`, runner.go:473–479) | ✔ |
| D2: fill-gaps, приоритет declares/параметров | `setDefaultVariableType` не перезаписывает существующие ключи; вызов после params+declares | ✔ |
| D2 (доп. из ревью 5.2): недостижимость API-fallback задокументирована | design.md:36 — «встроенные типы применяются до API-enrichment … принято сознательно» с обоснованием по FA | ✔ |
| D3: точка встраивания — чистая функция | Все 8 потребителей (`:100,:189,:2641,:4922,:5322,:6468,:6809,:6976`) получают эффект автоматически; БД/кэши не задействованы | ✔ |
| D4: `int`, не `DSINT_KEY` | `int`; эквивалентность по precision (10,0) подтверждена | ✔ |
| Дельта, сценарии 1–3, 5 | Покрыты unit/integration-тестами; сценарий 1 дополнительно подтверждён на реальном файле | ✔ |
| tasks 1.1–1.2, 2.1–2.3, 3.1–3.3, 4.1 | Все отмечены `[x]`, итог приёмки зафиксирован в tasks.md | ✔ |
| Эмпирика design.md (замечание 5.1) | Исправлено: «из 59 .t01 — 47 используют @RetVal (13 tmp/patch); 43 из 47 — ровно declare» | ✔ |

Расхождений с планом нет: правка локализована в `collectVariableTypes`, препроцессинг, индексация, схема БД и CLI-контракты не затронуты.

## 4. Регресс-анализ

- Изменённых функций, кроме `collectVariableTypes`, нет; поведение при отсутствии макросов в контенте неизменно (ранний возврат из `applyMacroServiceVarDefaults`), что подтверждено кейсом «no macros».
- **`datatype`** (3 точки): единственный ожидаемый прирост — `select @X = <expr>` с сервисными переменными; литерные/convert-пути не затронуты (`literalFitsType`, `hasExplicitConversion` выполняются после разрешения типа, порядок не менялся).
- **`checkDatatypeExecParams`** (:2641): `@RetVal` (int) как аргумент параметра — при передаче в более узкие int-типы (`dstinyint`/`dssmallint`) возможны новые findings; это корректная TP-семантика (int 10 > target 3/5), в выборке FA не проявилась.
- **`dateIntoString`** (:4922), **`emptyStringDate`** (:5322): срабатывают только при `typeGroup(target)=="string"`/`"datetime"`; `int` в эти группы не входит — новые findings невозможны.
- **`floatToStringConvert`** (:6976): `isFloatType` строгий (только `float`/`dsfloat`); `int` не float — новые findings невозможны.
- **`isNullSameTypes`** (:6468) и **`diffTypesComparison`** (:6805): `typeGroup` относит `int` к группе «int», а `dsidentifier` — к «float» (review_helpers.go:126–150), т.е. `areEquivalentTypes(int, DSIDENTIFIER)==false`. Теоретическая поверхность: `isnull(@RetVal, @DSIdentifierVar)` или сравнение `@RetVal = @DSIdentifierVar` могут дать новое срабатывание. На выборке 13 файлов — 0/0 (приёмка 3.2); поверхность узкая и уже отражена в Risks design.md.
- Полный `go test ./...` — зелёный; `go vet ./...`, `go build ./...` — чистые. Изменений DDL/схемы нет — bump `CurrentSchemaVersion` не требуется.

## 5. Независимая проверка приёмки (tasks 3.1)

Прогон текущего `CodeBase.exe` (build 1712) на `Cons_BankPartnerInstPrepare.sql`, `--rules datatype --json`:

```
success=True count=5
line=208  sev=3 obj=@OpenAccBP              DSIDENTIFIER -> DSINT_KEY
line=273  sev=3 obj=Cons_PC_FindDataByClient DSINT_KEY -> dstinyint
line=522  sev=3 obj=@CntFundID              DSIDENTIFIER -> DSINT_KEY
line=997  sev=3 obj=@RetVal                 DSIDENTIFIER -> int   (новый)
line=1026 sev=3 obj=@RetVal                 DSIDENTIFIER -> int   (новый)
```

Строка 960 (`convert(int, NTFID)`) отсутствует, прежние 208/273/522 сохранены — совпадает с заявленным в tasks.md. `openspec validate fix-review-macro-service-vars --strict` — passed.

## 6. Замечания

### 6.1. Сценарий 4 дельты (PG-набор → finding) покрыт только на уровне карты типов (minor, рекомендуется закрыть)

Дельта требует: нераскрытый `DCL_PROC_BODY(...)` + `select @err_code = max(ErrID)` при `DSBIGINT` → finding `datatype` severity 3. Unit-тест проверяет лишь наличие `err_code=int` в карте; сквозного rule-теста на PG-ветку нет. `DCL_PROC_BODY` в FA редок (20 вхождений), поэтому риск низок, но для полной трассировки дельты стоит добавить 4-й под-кейс в `TestCheckDatatype_MacroServiceVars` (content с `DCL_PROC_BODY(...)`, `colTypes: {"ptable|errid": "DSBIGINT"}`, `wantCount: 1`). Правка только тестовая.

### 6.2. Тест правила не проверяет объект/сообщение finding (info)

`assertRuleFindings` в `TestCheckDatatype_MacroServiceVars` проверяет count/rule/severity, но не `Object == "@RetVal"` и не сообщение `DSIDENTIFIER -> int`. Риск ложноположительного теста (finding по другой причине) невелик — на том же сценарии эталонный файл даёт ожидаемое; можно усилить ассертом `wantObject`.

### 6.3. Остаточная FP-поверхность `isNullSameTypes`/`diffTypesComparison` (info, принять)

Описана в п. 4: `int` vs `dsidentifier` — разные группы, поэтому `isnull`/сравнение `@RetVal` с DSIDENTIFIER-переменной теоретически могут сработать. Полевая выборка — 0/0; зафиксировано в Risks design.md. Действий не требуется, но полезно знать при будущих жалобах на шум этих правил.

## 7. Вывод

**Реализация принята (approve).** Код минимален, локализован в одной центральной точке, полностью соответствует D1–D4 и дельте спеки; регрессов не выявлено (полный набор тестов, вет/билд зелёные; 5 правил-потребителей без нового шума, 2 — с узкой теоретической поверхностью, полевой контроль 0/0). Приёмка 3.1 воспроизведена независимо, артефакты change валидны. Единственная рекомендация перед archive — добавить сквозной PG-кейс для сценария 4 дельты (п. 6.1); остальные замечания информационные.

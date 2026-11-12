# Code Review: add-review-api-proc-direct-call-rule

- **Дата**: 2026-11-12
- **Ревьюер**: opencode (L3/pro/fast)
- **Вердикт**: ✅ **РЕАЛИЗАЦИЯ СООТВЕТСТВУЕТ СПЕКЕ. БЛОКИРУЮЩИХ ДЕФЕКТОВ НЕТ.** Замечания R1–R3 — косметика/крайние случаи и расхождения документации; их желательно закрыть до архивирования, но повторное ревью не требуется. Семантика обоих правил, дедуп, регистронезависимость, сужение `foreignProcedureUsing` и отсутствие двойного ремапа строк подтверждены кодом, тестами и живыми прогонами на FA.

## Что проверялось

1. Код: `internal/review/types.go`, `catalog.go`, `review_rules.go` (новые `isAPIProcedure`, `isFacadeScriptName`, `apiExecMacroRe`, `checkAPIProcDirectCall`, `checkAPIExecOutsideFacade`, сужение `checkForeignProcedures`), `review_api_proc_direct_call_test.go`, `review_scope_integration_test.go`, `runner.go` (ремап строк), `review_lookup.go` (`cachedLookupProcedureScopeInfo`, `prewarmProcCaches`, `removeComments`), `review_parser.go` (`maskSingleQuotedStringContent`, `replaceMacros`), `internal/parser/sql/sql_parser.go` (execRe, CalleeName).
2. Артефакты change: `proposal.md`, `design.md`, `tasks.md`, дельты `specs/review/deploy-stoppers`, `specs/review/fine-code`, `README.md`, `cmd/root.go`.
3. Живой индекс FA (localhost:5435) — выборочные прогоны `codebase review` на реальных файлах (см. таблицу ниже). Полный свуп 2730 файлов повторно не запускался; масштаб 336/233 принят по отчёту реализации и выборочно сверен.

## Выполненные проверки

| Команда | Результат |
|---|---|
| `go build ./...` | ok |
| `go vet ./internal/review/...` | ok |
| `go test -count=1 ./internal/review/...` | ok (1.124s) |
| `go test -count=1 -tags integration ./internal/review/...` | ok (34.670s) |
| `openspec validate add-review-api-proc-direct-call-rule --strict` | valid |
| `CodeBase.exe review ... --rules apiProcDirectCall,apiExecOutsideFacade --json` | фильтр работает, вернул ожидаемый 1 finding |
| `CodeBase.exe review --help` | build 1692, оба rule ID в списке |
| `gofmt -l` по изменённым файлам | **`review_api_proc_direct_call_test.go` не отформатирован** (R1) |

Живые проверки на FA (build 1692):

| Файл | Ожидание | Факт |
|---|---|---|
| `fa-reports/.../API_rpt_CCred_FindListBaseAttrByID.sql` | `apiProcDirectCall` sev=1, target=fa-administrator, без дубля `foreignProcedureUsing` | ✅ finding на строке 451, target_product_id=138; `foreignProcedureUsing` только на FCD-вызовах (401, 883) |
| `fa-contracts/Consumer/SERVER/Accrual/BaseAlgAmrtCostSinglePmnt.sql` | свой API — чисто | ✅ 0 findings |
| `fa-contracts-ext/API_Credit_Ext/Server/Facade/fcd_Log.sql` | lowercase-фасад — чисто | ✅ 0 findings |

## Подтверждено положительно

- **Семантика `apiProcDirectCall`** воспроизводит проверенный образец `checkForeignProcedures`: `dedupeProcedureCalls` → `cachedLookupProcedureScopeInfo` → skip `ProductID == 0` / `== file.DsProductID`. Product-level семантика обеспечена существующим lookup'ом, новых запросов/DDL нет.
- **Дедуп строго по имени** (F3): rule-local `seen` по `normalizeIdentifier(call.Name)`, finding на строке первого вхождения; `dedupeProcedureCalls` используется только как первичный фильтр. Покрыто unit- и интеграционным тестом (три вызова, один finding, строка 3).
- **Регистронезависимость** (F1, F5): `isAPIProcedure` на базе `normalizeIdentifier`; `isFacadeScriptName` через `strings.ToLower(filepath.Base(path))`. Тесты покрывают lowercase `api_*` и `fcd_*`.
- **Seed-шаблон** (F2): `_fcd_` включён в `facadeScriptPrefixes`; тест `_fcd_tpl_seed.sql`/`_FCD_TPL_SEED.SQL` зелёный.
- **Textual-scan `apiExecOutsideFacade`** (F7, F8): regex `(?i)\bAPI_EXEC\s*\(`, скан по expanded-контенту, строка возвращается в expanded-пространстве, централизованный ремап — в `runner.go:197`. Повторного ремапа внутри правила нет, `getIndexedFile` не используется.
- **Сужение `foreignProcedureUsing`** выполнено тем же предикатом до lookup'а; на живом примере дубля нет, семантика не потеряна (переквалификация в severity 1).
- **Каталог/CLI/MCP**: оба правила автоматически попадают в `AllRuleIDs`/`RuleListString`/`DefaultEnabledRules`; `--rules` подтверждён.
- **Severity** (F10): выставляется явно `SeverityDeployStopper` в `Finding`, `ruleMeta.Severity` — описательное поле.
- **Коллизия с `execNotExistsProc`** (F12) зафиксирована в требовании и design.
- **README** (F11): оба ID добавлены в секцию Deploy stoppers, отмечено сужение `foreignProcedureUsing`.

## Замечания

### R1 [Low] — Новый тест-файл не отформатирован `gofmt`

`internal/review/review_api_proc_direct_call_test.go:232-233` — выравнивание полей структуры:

```diff
-		path    string
-		facade  bool
+		path   string
+		facade bool
```

`gofmt -l` возвращает этот файл. Правка: `gofmt -w internal/review/review_api_proc_direct_call_test.go`. На работу не влияет, но нарушает гигиену репозитория.

### R2 [Low] — Крайние случаи лексики в `checkAPIExecOutsideFacade`

`internal/review/review_rules.go:1627-1639`:

```go
clean := maskSingleQuotedStringContent(removeComments(string(content)))
...
line := 1 + strings.Count(clean[:loc[0]], "\n")
```

Два крайних случая (оба дают **false negative**, не false positive):

1. **Порядок «сначала комментарии, потом строки»**: `removeComments` не знает о строковых литералах. Строка `select 'foo -- bar'` перед `API_EXEC` приведёт к тому, что `--` внутри литерала вырежет остаток строки вместе с закрывающей кавычкой; затем `maskSingleQuotedStringContent` посчитает строку незакрытой и замаскирует весь дальнейший текст, включая реальный `API_EXEC`. Аналогично для `/*` внутри литерала.
2. **Подсчёт строк по маскированному тексту**: `maskSingleQuotedStringContent` заменяет **все** символы внутри кавычек на `?`, включая `\n`. Если перед `API_EXEC` есть многострочный строковый литерал, номер строки будет занижен (новая строка внутри литерала не посчитается).

Рекомендация: заменить пару вызовов на один проход-лексер (кавычки + `--` + `/* */` одновременно), либо как минимум:
- считать строки по тексту после `removeComments` (позиция `loc[0]` валидна, т.к. маскирование сохраняет длину), а не по `clean`;
- маскировать строки до удаления комментариев и сохранять `\n` при маскировании.

Добавить регресс-кейсы: `select 'a -- b'` и многострочный литерал перед `API_EXEC`. Вероятность на реальном FA низкая (вхождений не найдено), поэтому Low.

### R3 [Low] — Расхождения артефактов с реализацией

- `proposal.md:29` указывает `internal/review/runner_test.go`, `tasks.md:25` (task 5.1) — `review_rules_test.go`; фактически тесты вынесены в новый `internal/review/review_api_proc_direct_call_test.go`. Сам выбор файла корректен и соответствует проектной практике (`table_exists_metadata_test.go`, `empty_string_date_test.go`), но артефакты надо синхронизировать.
- `tasks.md:35` (task 6.3) сохраняет оценку «~274 findings на 188 файлах», тогда как финальные числа — 336/233 (внесены в proposal Impact и design Risks). Обновить task 6.3.
- `proposal.md:32`: формулировка «18 файлов неиндексированы (Test conv-consumer, нечитаемые имена)» расходится с отчётом о реализации («+18 — Test-файлы»), а арифметика прироста `274 + 96 + 18 = 388 ≠ 336`. Стоит унифицировать формулировку и явно пояснить методику (пересечения branch/Test-множеств), иначе release notes будут противоречивы.

### R4 [Info] — Неточный комментарий к `isAPIProcedure`

`internal/review/review_rules.go:1555-1556` утверждает, что предикат используется в `checkAPIExecOutsideFacade` «для префикса имени файла». Фактически там используется отдельный `isFacadeScriptName` со своим списком префиксов (включая `api_`/`_fcd_`). Поправить комментарий, чтобы он не вводил в заблуждение.

### R5 [Info] — Лишняя работа для фасадных файлов

`internal/review/review_rules.go:1631-1638`: regex и две полнобъёмные копии контента (`removeComments` + `maskSingleQuotedStringContent`) выполняются до проверки `isFacadeScriptName`. Фасадные файлы составляют большинство (`FCD_*` ~9 204 + `fcd_*` ~8 149 по данным change-review), а finding для них невозможен. Перенос проверки имени в начало функции — дешёвая оптимизация полного свупа.

### R6 [Info] — Пробелы в тестах (не блокируют)

- нет кейса `info.ProductID == 0` (в индексе найден, владелец не определён);
- нет кейса schema-qualified/`[bracketed]` имени (`exec dbo.API_Foo`, `exec [API_Foo]`) — парсер их нормализует, но регресс не зафиксирован;
- нет теста ремапа строки через `replaceMacros` для `API_EXEC` (сейчас проверяется только прямая строка);
- нет кейсов R2 (`--`/`/*` в литерале, многострочный литерал);
- интеграционный `TestCheckAPIExecOutsideFacadeScope` не проверяет `Rule`/`Object` для facade-кейса.

### R7 [Info] — Поведение «один finding на файл» для `apiExecOutsideFacade` не зафиксировано в спеке

Требование `API_EXEC вне фасадного скрипта` не оговаривает дедуп, а реализация сообщает только первое вхождение (`FindStringIndex`). Поведение разумное (файл целиком не фасадный), но для симметрии со спекой `apiProcDirectCall` стоит либо явно записать «не более одного finding на файл, на строке первого вхождения», либо осознанно подтвердить.

## Статус замечаний предыдущего change-review (F1–F12)

| # | Статус | Комментарий |
|---|---|---|
| F1 регистронезависимый префикс | ✅ закрыто | `isFacadeScriptName`, тесты `fcd_*`/`facade_*` |
| F2 `_fcd_tpl_seed.sql` | ✅ закрыто | префикс `_fcd_`, тест |
| F3 дедуп по имени | ✅ закрыто | rule-local `seen`, тест на первое вхождение |
| F4 числа в Impact/Risks | ✅ закрыто (кроме task 6.3) | 336/233 в proposal/design; task 6.3 устарел (R3) |
| F5 общий предикат `API_` | ✅ закрыто | `isAPIProcedure` |
| F6 расхождения по `Facade_` | ✅ закрыто | формулировки синхронизированы |
| F7 двойной ремап / `getIndexedFile` | ✅ закрыто | expanded-строка, ремап в runner |
| F8 `(?i)`, комментарии/строки | ✅ закрыто (с оговоркой R2) | `(?i)`, `removeComments` + маска, тесты |
| F9 расположение тестов | ⚠️ частично | интеграционные кейсы разделены; unit-тесты в новом per-rule файле — артефакты устарели (R3) |
| F10 severity в `Finding` | ✅ закрыто | явно `SeverityDeployStopper` |
| F11 README-каталог | ✅ закрыто | оба ID + пометка про `foreignProcedureUsing` |
| F12 коллизия с `execNotExistsProc` | ✅ закрыто | требование + design Risks |

## Резюме

Реализация полная и корректная: 16/16 задач, сборка и оба набора тестов зелёные, `openspec validate --strict` — valid, живые прогоны на FA подтверждают негативный кейс и отсутствие ложных срабатываний на своих API/фасадах. Дефектов, влияющих на боевое поведение, не выявлено. К архивированию — после `gofmt` (R1) и синхронизации артефактов (R3); R2/R4–R7 — рекомендации на будущее и не блокируют.

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| R1 | `review_api_proc_direct_call_test.go` не отформатирован | Low | `gofmt -w` |
| R2 | Крайние случаи `removeComments`/маскирования: false negative и сдвиг строки | Low | лексер одним проходом или маска с сохранением `\n` + подсчёт по немаскированному тексту; тесты |
| R3 | Артефакты ссылаются на `runner_test.go`/`review_rules_test.go`, task 6.3 устарел, арифметика чисел не сходится | Low | синхронизировать proposal/tasks |
| R4 | Неточный комментарий `isAPIProcedure` | Info | поправить текст |
| R5 | Regex/маскирование до проверки фасада | Info | ранний return для фасадов |
| R6 | Пробелы в тестах (ProductID=0, schema/brackets, ремап, литералы) | Info | добавить кейсы |
| R7 | «Один finding на файл» не зафиксирован в спеке | Info | уточнить требование |

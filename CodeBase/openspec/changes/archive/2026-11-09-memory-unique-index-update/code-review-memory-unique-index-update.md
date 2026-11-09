# Code review: memory-unique-index-update

Дата ревью: 2026-11-09. Ревью выполнено без правок кода (только этот файл).
Ревьюится реализация относительно `proposal.md`, `design.md`, `tasks.md` и
дельты `specs/review/deploy-stoppers/spec.md`; pre-implementation review
(`change-review-memory-unique-index-update.md`, п.1–12 и раздел «Контроль
полноты») учтён как чек-лист.

**Проверено (код):**

- `internal/review/types.go` — `RuleMemoryUniqueIndexUpdate` (`:41`),
  `tableIndexCandidate.IsUnique` (`:263-265`).
- `internal/review/catalog.go` — запись правила, `SeverityDeployStopper`
  (`:250-256`), `DefaultEnabledRules` (`:437-443`).
- `internal/review/runner.go` — поля кэша `memoryTableCache`/`memoryTablesLoaded`
  (`:32-37`), инициализация в `NewRunner` (`:61`), сброс в `RunSQLFileCtx`
  (`:90-93`).
- `internal/review/review_lookup.go` — `IsUnique` в обоих SQL
  `batchLoadIndexCandidates` (`:495`, `:509`), агрегация в кандидата (`:534`),
  `isMemoryTable` (`:565-595`).
- `internal/review/review_rules.go` — `uniqueIndexHit`/`matchUniqueIndexColumns`
  (`:3750-3791`), `checkMemoryUniqueIndexUpdate` (`:3793-3861`).
- Тесты: `review_rules_test.go:6162` (unit), `runner_test.go:109`
  (enabled-rule-set), `review_lookup_integration_test.go:15` (store),
  `review_memory_unique_integration_test.go:57` (E2E).
- Документация/сборка: `README.md:1057-1066,1098`, `cmd/root.go`
  (`version 0.9.5`, `buildNumber 1681`), `cmd/review.go` (только reorder
  импортов gofmt).

**Автоматическая проверка (выполнена в этом ревью):**

- `go build ./...` — ok.
- `go vet ./internal/review/...` — ok.
- `gofmt -l internal/review cmd` — пусто (чисто).
- `go test ./internal/review/` — PASS (1.5s).
- `go test ./internal/review/ -tags integration -run
  "TestBatchLoadIndexCandidatesIsUnique|TestMemoryUniqueIndexUpdateRuleE2E" -v`
  — оба PASS (6.35s, реальная временная БД).
- `openspec validate memory-unique-index-update --strict` — valid.

## Что реализовано (соответствие задачам)

- **1.1** `IsUnique` добавлен в `tableIndexCandidate`; SQL#1 (`sql_index_definitions`)
  селектит `i.is_unique`, SQL#2 (`api_business_object_table_indexes`) — выражение
  `(i.index_type IN (2, 3))`; признак пронесён в aggregate и в кандидата. Ключ
  агрегата `(tableLower, indexID, normName)` — уникальность берётся из строки
  индекса, дубли поле́й не размножают. Соответствует D4.
- **1.2** Ленивое множество `memory_tables`: `memoryTableCache` +
  `memoryTablesLoaded`, отдельный `memoryTableMu`, сброс в `RunSQLFileCtx`.
  Флаг ставится и при пустом результате, и при ошибке (правило инертно до конца
  прогона, без шторма запросов). Реализует D3 и учёт п.7 pre-review.
- **2.1** Константа правила, запись каталога с severity 1 и описанием, тест
  `TestEnabledRuleSet_MemoryUniqueIndexUpdate`.
- **2.2** `checkMemoryUniqueIndexUpdate` структурно повторяет
  `checkDeferredUpdate`: `splitStatementsWithOffsets` → отбор `update` →
  `extractUpdateTargetTable` → skip `#` → **резолв алиаса через
  `extractTablesFromFromClause` + `sameTableReference` ДО `isMemoryTable`** →
  `extractSetColumns` → уникальные поля через `lookupTableIndexCandidates`.
  Порядок и нормализация соответствуют D2 и п.8 pre-review.
- **3.1–3.3** Unit-тест хелпера, store-интеграционный тест `IsUnique`
  (SQL true/false, API type 0/2/3, дубль индекса), E2E-тест полного пути
  (индексация `.lst` + `M_CRT_INDEX('UNIQUE',...)` → review) — добавлены и
  зелёные. Учёт п.6 pre-review.
- **4.1** README: правило добавлено в **Deploy stoppers**; `deferredUpdate`,
  `inSubQuery`, `varcharSize`, `columnInsert` перенесены из PostgreReq; текст
  дефолта `--rules` исправлен на «все правила каталога». Учёт п.5 pre-review.
- **4.3** `buildNumber` 1679→1681, `version` 0.9.4→0.9.5.

## Замечания

### 1. [Low] Рассогласование нормализации цели между проверкой памяти и lookup индексов

- `isMemoryTable` нормализует имя через `normalizeIdentifier` (LOWER + снятие
  `[]`/`"`) **и** отрезает схемный префикс по последней точке
  (`review_lookup.go:566-569`). А `lookupTableIndexCandidates` нормализует ключ
  кэша/запроса только как `strings.ToLower(strings.TrimSpace(tableName))`
  (`review_lookup.go:599`) — без `normalizeIdentifier`, без снятия скобок и
  схемы.
- Следствие: для `UPDATE dbo.pMemUniq SET KeyCol = ...` или
  `UPDATE [pMemUniq] SET KeyCol = ...` проверка in-memory пройдёт, но
  lookup индексов запросит `lower(table_name) = 'dbo.pmemuniq'` /
  `'[pmemuniq]'` и не найдёт кандидатов → **ложноотрицательный результат**
  (в FA имена без схемы и скобок, поэтому на живом индексе не проявляется).
- Рекомендация: привести `realTable` к тому же виду перед lookup (например,
  использовать уже нормализованное имя или нормализовать в
  `lookupTableIndexCandidates`); добавить E2E-кейс с `dbo.`/`[ ]`.
  Не блокирует: сценарии дельты и реальные данные используют простые имена.

### 2. [Low] `isMemoryTable` держит мьютекс всё время блокирующего DB-запроса; ошибка кэшируется как «пусто»

- Первый вызов правила на файл захватывает `memoryTableMu` и выполняет
  `SELECT DISTINCT LOWER(table_name) FROM memory_tables` под замком
  (`review_lookup.go:573-593`). Остальные worker-горутины правил на этом файле
  ждут. Это осознанный компромисс (запрос по 26 строкам, один раз на файл), не
  дефект, но при медленной/недоступной БД сериализует параллельные правила.
- Ошибка запроса кэшируется как «загружено-пусто» (флаг `memoryTablesLoaded`
  выставляется в любом случае), то есть при транзиентной ошибке правило
  становится инертным до конца файла. Это прямо зафиксировано в D3
  (толерантность к lookup-ошибкам), поэтому принято как есть.
- Рекомендация: при желании вынести запрос за пределы критической секции
  (double-checked locking) — не обязательно.

### 3. [Info] Union/name-семантика при дедупликации — риск из design, но реальный

- `matchUniqueIndexColumns` дедуплицирует по `(normColumn, normIndexName)`
  (`review_rules.go:3775-3779`). Кандидаты одного имени индекса из SQL- и
  API-источников/веток — разные записи (разный `indexID`), но дедуп идёт по
  **имени**. Если одноимённый индекс уникален хотя бы в одном источнике/ветке,
  finding сформируется. Это наследуемая union-семантика кэша (`Risks` в
  `design.md`); код соответствует зафиксированному решению, действий не требует.

### 4. [Info] Пробелы в тестовом покрытии (не блокируют)

- Нет E2E-кейса на `dbo.`/`[ ]`-квалификацию цели (п.1 выше) — самый заметный
  непокрытый край.
- Нет теста «пустая `memory_tables` → один SELECT, правило инертно» (поведение
  реализовано флагом `memoryTablesLoaded`, но не зафиксировано тестом).
- Нет отдельного теста `isMemoryTable` (нормализация схемы/скобок/регистра).
- Store-тест покрывает `index_type` 0/2/3 и дубль имени; кейс «одноимённый
  уникальный в API и неуникальный в SQL» в E2E не выделен (покрыт логикой
  дедупа unit-теста).
- Рекомендация: при следующем касании добавить E2E-кейс
  `UPDATE dbo.[pMemUniq] SET KeyCol = ...` — он сразу поймал бы п.1.

### 5. [Info] Гигиена рабочего дерева / артефакты

- В рабочем дереве изменён `CodeBase.exe` (результат задачи 4.3) и присутствуют
  sidecar-файлы (`desc_lsa_*`, `spec_lsa_*`). По конвенции репо бинарник и
  сгенерированные sidecar'ы не должны попадать в коммит изменения; включать в
  коммит только `*.go`, `README.md` и артефакты `openspec/changes/...`.
- Изменение `version` 0.9.4→0.9.5 не упомянуто в `tasks.md` (там только
  `buildNumber` 1679→1681). Безвредно, но при следующем bump — фиксировать в
  задаче явно.

## Вывод

Реализация соответствует proposal/design/tasks и корректно закрывает все
замечания pre-implementation review (критерий `is_unique`/`index_type IN (2,3)`,
lazy-кэш с флагом загруженности, резолв алиаса до проверки памяти, дедуп по
паре «колонка+имя индекса», store/E2E-интеграция, README и bump build).
Контракты и инварианты соблюдены: `IsUnique` — из общего batch-кэша без новых
N+1-запросов; `memoryTableCache` и его флаг сбрасываются на каждый файл;
severity 1 согласована между каталогом и finding'ом; строка finding'а корректно
ремапится runner'ом (`runner.go:168-169`), сама rule строку не пересчитывает.

`go build`/`go vet`/`gofmt`/unit и оба интеграционных теста — зелёные;
`openspec validate --strict` — valid.

Блокирующих дефектов нет. П.1 [Low] — единственное функциональное замечание
(ложноотрицание на схемно-квалифицированных/закавыченных именах цели); на
текущих данных FA не проявляется. П.2–5 — заметки на будущее.

---

## Post-review: разбор замечаний и правки (2026-11-09)

- **п.1 [Low] — ПРИНЯТ, исправлен.** Новый хелпер `normalizeTableKey`
  (`review_lookup.go`): нижний регистр → отрезание схемного префикса → снятие
  `[]`/`"` — порядок обязателен (снятие скобок до отрезания схемы оставляло бы
  `[tbl` от `dbo.[tbl]`; прежний код `isMemoryTable` имел этот латентный дефект
  порядка). `isMemoryTable` переведён на хелпер; `checkMemoryUniqueIndexUpdate`
  передаёт в `lookupTableIndexCandidates` нормализованное `normTable`
  (Object/имя таблицы в сообщении — тоже `normTable`, единый вид для прямой/
  alias/схемной форм). E2E дополнен оператором `update dbo.[pMemUniq]` —
  третий finding (строка 9), до фикса давал false negative.
- **п.2 [Low] — отклонён** (самим ревью как необязательный): мьютекс на время
  одного 26-строчного SELECT — осознанный компромисс D3; double-checked locking
  не усложняет оправданно. Без правки.
- **п.3 [Info] — без действий**: union-семантика зафиксирована в design Risks.
- **п.4 [Info] — принят частично**: добавлены unit-тест
  `TestMemoryUniqueNormalizeTableKey` (простое/скобки/кавычки/dbo/смешанное),
  E2E-кейс `dbo.[pMemUniq]` и ассерт «пустая `memory_tables` → `isMemoryTable`
  false, повторный вызов стабилен» в store-тесте. Отдельный тест «ровно один
  SELECT» не добавлен — требует счётчика запросов; поведение обеспечено флагом
  `memoryTablesLoaded` и покрыто логикой D3.
- **п.5 [Info] — к сведению**: при коммите НЕ включать `CodeBase.exe` и
  sidecar'ы (`*_lsa_*`); при следующем bump фиксировать в задаче и `version`.

Верификация после правок: `go build`/`go vet`/`gofmt -l internal/review` —
чисто; unit `TestMemoryUnique*` — PASS; интеграционные
`TestBatchLoadIndexCandidatesIsUnique` + `TestMemoryUniqueIndexUpdateRuleE2E`
(3 finding'а: прямая, alias, `dbo.[pMemUniq]`) — PASS; `CodeBase.exe`
пересобран, build остался 1681 (ревью-фиксы не бампают по прецеденту).

## Ручная верификация на живом индексе (2026-11-09)

- Отрицательный случай: исходный `CreateDocument.sql` (обновления
  `pRuleDoc_QtyStack` только по `ID`/`Qty`) — `Findings: 0` (ожидаемо).
- Положительный случай: в реальном UPDATE (line 946) добавлено
  `Brief = 'test'` — `Findings: 1`:
  `memoryUniqueIndexUpdate line=946 object=pruledoc_qtystack`, сообщение с
  колонкой `brief` и индексом `XPKpRuleDoc_QtyStack` — корректно.
  Тестовая правка файла FA после проверки откачена (`git checkout`).

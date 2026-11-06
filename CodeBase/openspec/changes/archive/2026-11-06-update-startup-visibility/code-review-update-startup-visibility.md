# Code review: update-startup-visibility

Дата ревью: 2026-11-06. Ревью выполнено без правок кода (только этот файл).

**Проверено:** реализация относительно `proposal.md`, `design.md`, `tasks.md` и
трёх дельт `specs/`. Просмотрены изменения и новые тесты:
`cmd/init.go`, `cmd/update.go`, `cmd/root.go`, `internal/indexer/runner.go`,
`internal/indexer/source_registry.go`, `internal/indexer/source_git.go`,
`internal/indexer/indexer.go` (`statsCollector`, `startProgressReporter`,
`normalizeParallel`), `internal/model/model.go`, `internal/store/db_schema.go`,
`db_files.go`, `db_products.go`, `db_reconcile_relations.go`, `db_schema_version.go`,
`internal/indexer/progress_reporter_test.go`,
`internal/indexer/source_discovery_parallel_integration_test.go`,
`internal/store/db_files_path_predicates_integration_test.go`.

**Автоматическая проверка (выполнена):**
`gofmt -l` по изменённым файлам — чисто; `go build ./...` — ok; `go vet ./...` — ok;
`go test ./internal/store -run 'TestEscapeLikeAll|TestRootLikePattern'` и
`go test ./internal/indexer -run TestProgressReporterStageRendering` — PASS;
интеграционные `-tags integration`:
`TestRootPredicates_EscapedLikeScope`, `TestBackfillFilesFromSources_LikePredicate`,
`TestIdxFilesPath_MigrationV6`, `TestIdxFilesPath_ExplainUsesIndex`,
все `TestDiscovery_*` (включая четыре новых) — PASS;
`openspec validate update-startup-visibility --strict` — valid;
`openspec validate --all --strict` — 35 passed, 0 failed.

**Живой `EXPLAIN` на локальной БД** (`localhost:5435`, база `codebase`,
PostgreSQL 18, `files` = 206 042 строки; только `EXPLAIN`, данные не менялись,
индекс `idx_files_path` уже в БД):

- узкий префикс с параметром (`WHERE path LIKE $1`, `$1 = '<root>/fa-contracts/…/%'`)
  → `Bitmap Index Scan on idx_files_path` (и `Index Scan using idx_files_path` для
  более узкого префикса) — индекс доступен планировщику в custom plan
  (lib/pq, один прогон на процесс);
- полнокорневой `LIKE '<root>/%'` → `Seq Scan` + `Sort`/`HashAggregate` — как и
  заявлено в design (Non-Goals);
- **production-форма `BackfillFilesFromSources` (N-строчный `VALUES` + `UPDATE …
  FROM src … WHERE f.path LIKE s.prefix || '/%' OR f.path LIKE s.prefix || '\\%'`)
  → `Nested Loop` с `Seq Scan on files f` и `Join Filter`** — индекс не
  используется (см. замечание 1).

## Замечания

1. **[High] `BackfillFilesFromSources` не использует `idx_files_path` — MUST дельты `database-schema` не выполняется.**

   - Дельта (`specs/infrastructure/database-schema/spec.md:18-21`) требует:
     «Узкопрефиксные запросы — backfill привязок источников (префиксы каталогов
     источников) и `update --path <подпуть>` — MUST использовать индекс вместо
     полного последовательного скана таблицы». Proposal (`proposal.md:38-40`) и
     design (`design.md:21-22`, D3) называют backfill одним из главных
     потребителей индекса.
   - Фактический запрос — один `UPDATE files f … FROM src s … WHERE (f.path LIKE
     s.prefix || '/%' OR f.path LIKE s.prefix || '\\%')` (`db_products.go:156-161`),
     где `s.prefix` — значение из `VALUES`-строки. Postgres не умеет строить
     индексный диапазон из LIKE-паттерна, вычисляемого из другой таблицы (для
     `text_pattern_ops` нужен план-константный префикс). `EXPLAIN` production-формы
     (80 `VALUES`-строк) даёт `Nested Loop` + `Seq Scan on files f`, т.е. ровно
     полный скан, который спека обещает устранить. Индексный скан получается
     только тогда, когда паттерн вырождается в константу (единичная `VALUES`-строка
     после планирования) — в бою источников десятки.
   - `update --path <подпуть>` действительно получает выигрыш (подтверждено
     `EXPLAIN`: narrow `path LIKE $1` → Bitmap/Index Scan). То есть индекс полезен,
     но не для backfill — заявленная спекой гарантия для backfill не выполнена.
   - Тест `TestIdxFilesPath_ExplainUsesIndex`
     (`db_files_path_predicates_integration_test.go:217-258`) проверяет
     литеральный узкий префикс (`WHERE path LIKE 'D:/bulk/narrow/%'`), а не
     production-форму backfill, поэтому проблему не ловит. Тест
     `TestBackfillFilesFromSources_LikePredicate` проверяет корректность
     результата, но не план.
   - Рекомендация (одно из):
     - переписать backfill на per-source запросы (`WHERE f.path LIKE $1 || '/%' …`
       с параметром-префиксом, в цикле/`LATERAL` с константой на итерацию), чтобы
       каждый подзапрос получал параметризованный LIKE-prefix; либо
     - честно скорректировать дельту `database-schema`: индекс SHALL
       использоваться только для `update --path`/узкопрефиксных `LIKE` с
       параметром; backfill-привязок допускает Seq Scan (как уже допускает
       полнокорневую загрузку). Плюс добавить EXPLAIN-тест production-формы
       backfill, чтобы зафиксировать фактическое поведение.
   - Важно: функциональной регрессии нет (прежний `left()`-предикат тоже давал
     Seq Scan), но цель «сжать окно» для backfill'а не достигнута, а дельта после
     архивации будет утверждать обратное.

2. **[Medium] Тайминги подготовительного окна не покрывают всё время для `update --modified=false` (rebuild) — нарушение требования `progress-reporting`.**

   - Требование (`specs/indexing/progress-reporting/spec.md:81-88`): тайминги фаз
     SHALL суммарно покрывать «всё время от старта прогона до начала обхода
     файлов».
   - `UpdateCtxStartedAt` при `!onlyModified` уходит в `fullRebuildCtx`
     (`runner.go:212-214`), который **игнорирует переданный `prepStartedAt`** и
     вызывает `runInitPipeline(…, time.Now())` (`runner.go:481`). Между `time.Now()`
     и началом `runInitPipeline` выполняются `ResetCodebaseTables` (TRUNCATE…
     CASCADE, `runner.go:475`) и `removeLSASidecars` (`runner.go:480`) — они не
     таймируются ни одним полем. Время `InitSchema` (включая разовый `CREATE
     INDEX` v6) в `PrepMiscMs` rebuild тоже не попадает, хотя для `update`/`init`
     попадает через внешнюю точку отсчёта. Печать `Full rebuild: reset …` при этом
     идёт до старта репортера, т.е. отдельной именованной стадией это не видно.
   - Рекомендация: прокинуть `prepStartedAt` в `fullRebuildCtx` (сделать точку
     отсчёта до `ResetCodebaseTables`) и, при желании, показать стадию
     `reset tables` / учесть её в `PrepMiscMs`.

3. **[Low] В `update` между `StateLoadMs` и `walkStart` есть неатрибутированный участок — построение fingerprint-карты.**

   - `stateLoadDone` фиксируется сразу после `GetLatestFilesByRootPath`
     (`runner.go:254`), затем `Stage` сбрасывается (`runner.go:259`), после чего
     строится `fingerprints := make(map[string]fswalk.FileFingerprint, len(existing))`
     по всем строкам предыдущего состояния (`runner.go:269-275`), и только потом
     `walkStart := time.Now()` (`runner.go:277`).
   - Для корня ~624k файлов это заметная работа (аллокация/итерация map), которая
     не входит ни в `StateLoadMs`, ни в `WalkSaveMs`, т.е. «четыре тайминга
     суммарно покрывают всё окно» (комментарий `runner.go:425-427`) буквально не
     выполняется. Рекомендация: либо включить построение fingerprints в
     `StateLoadMs`/отдельное поле, либо сдвинуть `stateLoadDone` после
     `SetPreFilter`.

4. **[Low] Счётчик `git detection: N/M` может «прыгнуть назад» в кадре: `setStage` вызывается вне `stageMu`, хотя комментарий утверждает обратное.**

   - `source_registry.go:178-182`: под `stageMu` инкрементируется `detected` и
     копируется `n`, но `setStage(fmt.Sprintf("git detection: %d/%d", n, …))`
     вызывается уже **после** `stageMu.Unlock()`. Возможное чередование:
     горутина A получает `n=1` и приостанавливается; B получает `n=2` и пишет
     `2/M`; A возобновляется и пишет `1/M`. Комментарий «под мьютексом с записью
     Stage, чтобы кадры были монотонны» не соответствует коду.
   - Функционально безвредно (следующие завершения снова увеличат счётчик), но
     для change'а про «честную видимость» стоит либо перенести `setStage` под
     `stageMu`, либо в репортере выводить монотонный максимум. Тест
     `TestDiscovery_StageSequence` (`source_discovery_parallel_integration_test.go:220-248`)
     гоняет `pool=1` и этот случай не проверяет.

5. **[Low] При ошибке `NewDB`/`InitSchema` строка индикации схемы остаётся без перевода строки.**

   - `cmd/update.go:55-65`, `cmd/init.go:114-126`: печатается
     `Schema: check/migrations... `, а `done\n` — только после успешного
     `InitSchema`. При ошибке возвращается `fmt.Errorf(...)`, и сообщение об ошибке
     (stderr) визуально приклеивается к незавершённой строке stdout. Рекомендация:
     печатать `done\n`/`failed\n` и в ветке ошибки (или использовать `\r`-очистку).

6. **[Info] `CreateScanRun` и FTS-backfill не имеют именованной стадии.**

   - Design D1 (`design.md:56-58`) осознанно относит их к `PrepMiscMs`, а не к
     отображению. На легаси-БД FTS-backfill (`UPDATE api_contracts … WHERE
     search_vector IS NULL`) может быть заметным; в это время репортер показывает
     не стадию, а строку счётчиков `scanned=0`. Формально требование
     `progress-reporting` («каждая фаза — именованной стадией», `spec.md:33-47`)
     перечисляет git-детекцию, safety-net и load state, поэтому это не нарушение,
     но на первом прогоне после смены версии окно остаётся безликим. Можно
     завести стадию `fts backfill: contracts` (дёшево) либо оставить как есть.

7. **[Info] Прочие наблюдения.**
   - `db_reconcile_relations.go:287` — попутный gofmt-фикс отступа `query :=`
     (не относится к change, безвреден).
   - В рабочем дереве изменены `CodeBase.exe` и `desc_lsa_state.json` (побочные
     артефакты локального прогона/сборки) — при коммите включать только исходники,
     тесты и `openspec/changes/update-startup-visibility/`.
   - Новые тесты не проверяют значения `PrepMiscMs`/`DiscoveryMs`/`SafetyNetMs`/
     `StateLoadMs` (только рендер стадий и планы запросов). Для регрессии
     семантики `WalkSaveMs` полезен хотя бы smoke-тест `WalkSaveMs < сумма фаз`.

## Что подтверждено (без замечаний)

- Параллельная git-детекция: пул `normalizeParallel(parallel)` с защитой от `-j 0`
  (`source_registry.go:146`), результаты собираются по индексу кандидата, порядок
  `ReadDir` сохранён, upsert'ы последовательны и детерминированы, кэш детектора
  потокобезопасен (`source_git.go:50-67`), ошибка одного воркера не валит
  остальные. Все четыре новых интеграционных теста и прежние `TestDiscovery_*`
  зелёные.
- Ранний старт репортёра в `UpdateCtx`/`runInitPipeline` до подготовительных фаз;
  стадии строго в форме «фаза: деталь» (`git detection: N/M`,
  `source registry: upsert`, `safety-net: duplicates`, `load state: files`),
  переход к счётчикам walk штатный; тест рендера проверяет отсутствие пустого
  префикса.
- Sargable-предикаты и единое экранирование: `rootLikePattern` (нормализация
  `\`→`/`, trim хвостового `/`, `escapeLikeAll`), убрана мёртвая ветка `path = $1`,
  backfill использует `'\\%'` (литеральный `\` + wildcard) — семантика границы
  подтверждена unit- и интеграционными тестами (включая ловушки `%`/`_`,
  sibling-префиксы и backslash-границу).
- Индекс `idx_files_path` c `text_pattern_ops` на `TEXT`, bump
  `CurrentSchemaVersion` v5→v6, миграция штатным `applyMigrationTx` (в транзакции,
  с advisory-lock) — `TestIdxFilesPath_MigrationV6` PASS; `buildNumber` 1676.
- `WalkSaveMs` действительно отсчитывается от `walkStart` перед
  `WalkParallelCtx` и больше не включает подготовительные фазы; новые поля
  `ScanStats` заполняются в обоих пайплайнах; `printPipelineTimings` печатает их
  при `> 0` и подпись `Walk+save files` обновлена.
- Баннер и строка индикации схемы печатаются до `NewDB`/`InitSchema` в обеих
  командах; `gofmt`/`vet`/`build`/`openspec --all --strict` — чисто.

## Вывод

Направление реализовано аккуратно и в основном соответствует плану: параллельная
детекция, ранний репортёр со стадиями, sargable-предикаты с корректным
экранированием, индекс с bump'ом версии и тесты — всё работает, автоматические
проверки зелёные. Блокирующих функциональных дефектов нет.

Перед `/opsx-archive` рекомендуется устранить **п.1** (главное): либо сделать
backfill индексно-используемым (per-source `LIKE` с параметром), либо привести
дельту `database-schema` к фактическому поведению и добавить EXPLAIN-тест
production-формы backfill. **П.2** желательно закрыть прокидыванием
`prepStartedAt` в rebuild и таймингом сброса таблиц, чтобы требование «тайминги
покрывают всё окно» выполнялось буквально. Пункты 3–5 — небольшие улучшения
точности/UX; 6–7 — информационные.

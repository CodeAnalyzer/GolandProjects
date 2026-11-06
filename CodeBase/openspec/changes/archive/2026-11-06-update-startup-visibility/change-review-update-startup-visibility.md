# Change review: update-startup-visibility

Дата ревью: 2026-11-05. Ревью выполнено без правок кода (только этот файл).

**Проверено:** proposal.md, design.md, tasks.md, 3 дельты `specs/`, `.openspec.yaml`; сверка с реальным кодом `internal/indexer/runner.go`, `source_registry.go`, `source_git.go`, `indexer.go`, `internal/store/db_schema.go` / `db_schema_version.go` / `db_files.go` / `db_products.go` / `db_reconcile_relations.go` / `db_fts.go`, `internal/model/model.go`, `internal/config/config.go`, `internal/fswalk/fswalk.go`, `cmd/update.go`, `cmd/init.go`; сверка дельт с основными спеками (названия требований и полный текст MODIFIED воспроизведены корректно, все прежние сценарии сохранены; `indexing/progress-reporting` в основных спеках отсутствует — новая capability заявлена правомерно). `openspec validate update-startup-visibility --strict` — valid; `openspec validate --all --strict` — 35 passed, 0 failed.

Дополнительно снят живой `EXPLAIN` на локальной БД (`localhost:5435`, база `codebase`, PostgreSQL 18, `files` = 206 042 строки) на временной таблице, повторяющей распределение путей `files` (200 000 строк, индекс `varchar_pattern_ops`), всё в `BEGIN … ROLLBACK` — БД и код не изменялись.

## Замечания

1. **[High] Дельта `database-schema` требует Index Scan там, где design сам ожидает Seq Scan — и `EXPLAIN` это подтверждает.**
   - Дельта: «запросы … загрузка предыдущего состояния файлов, поиск дубликатов поколений, backfill привязок источников — MUST использовать индекс вместо полного последовательного скана таблицы» (`specs/infrastructure/database-schema/spec.md:12-13`), сценарий «Миграция индекса на существующей БД» прямо утверждает: «запрос `SELECT … FROM files WHERE path LIKE '<root>/%'` использует Index Scan по `idx_files_path`, а не Seq Scan» (`spec.md:54-59`).
   - Design же в Non-Goals фиксирует обратное: при `root` = весь FA `LIKE 'root/%'` покрывает все ~624k строк, «seq scan + sort там легитимен, стоимость фазы в передаче строк, а не в скане; индекс помогает backfill'у (80 узких префиксов) и подпутевым `update -p`» (`design.md:31-34`).
   - Живой `EXPLAIN` (PG18): полнокорневой `path LIKE '/root/%'` → `Seq Scan` + `Sort` (Unique), как и предсказывает design; узкий префикс (`/root/bucket7/%`, ~1.25% строк) → `Bitmap Index Scan`; backfill-форма с `OR` → `BitmapOr` из двух индексных сканов. На рабочем ПК `GetLatestFilesByRootPath` получает `root_path` = весь FA → Seq Scan неизбежен, `DISTINCT ON … ORDER BY path` добавляет Sort.
   - Следствие: после архивации спека будет утверждать заведомо невыполнимое. `tasks.md:54-58` делает `EXPLAIN`-проверку «опциональной», хотя сценарий спеки — не опциональный.
   - Рекомендация: в требовании оставить «индекс SHALL существовать + предикаты sargable», а «вместо полного Seq Scan» квалифицировать узкими префиксами (backfill источников, `update -p`); сценарий переписать на узкий префикс/репрезентативные данные; убрать «опционально» у `EXPLAIN` либо явно указать `SET enable_seqscan = off` для структурной проверки. В proposal (Why/What Changes) не продавать индекс как исправление всех трёх seq-scan — design признаёт, что выигрыш только у backfill.

2. **[Medium] Для колонки `TEXT` указан opclass `varchar_pattern_ops`; канонический — `text_pattern_ops`. Плюс неверны утверждения design об операторах.**
   - `files.path` объявлен как `TEXT NOT NULL` (`db_schema.go:88-93`), а proposal/design/tasks/дельта единодушно пишут `varchar_pattern_ops` (`proposal.md:31-33`, `design.md:81-82`, `tasks.md:41-42`, `spec.md:48-52`).
   - На PG18 `CREATE INDEX … (path varchar_pattern_ops)` на `TEXT` принимается (text/varchar бинарно-совместимы) и даже используется планировщиком — проверено `EXPLAIN`: и `LIKE`, и `=` идут по такому индексу, `OR` — через `BitmapOr`. Но это не каноническая связка (для `TEXT` — `text_pattern_ops`), и поведение зависит от версии PG/путей планировщика (продукт заявляет PG14+, на рабочем ПК версия может отличаться). Рекомендация: использовать `text_pattern_ops` и зафиксировать это в 4.6.
   - Design D3 (`design.md:87-93`) утверждает, что pattern-opclass «НЕ поддерживает обычные операторы сравнения (`=`, `<`)» и поэтому надо убрать ветку `path = $1`. Фактически на PG18 `=` обслуживается этим индексом (`Index Only Scan`), а `OR` даёт `BitmapOr`; `<` действительно уходит в Seq Scan. То есть мотивировка удаления `= $1` («OR заставляет planner выбрать Seq Scan целиком») не подтверждается. Само удаление семантически безвредно (root — каталог, файл всегда глубже), но обоснование в design стоит поправить: ветка мертва по семантике, а не по требованию планировщика.

3. **[Medium] Стадия `git discovery: N/M` перестаёт быть правдивой после детекции: счётчик дойдёт до N/M, а фаза продолжится.**
   - Счётчик по design/D1 инкрементируется только по завершении `detector.Detect` (`design.md:44-49`, `tasks.md:20`). Но `discoverGitSources` после детекции последовательно делает `resolveDSProductID` + `UpsertDSProductBranch` (~160 roundtrip), `SetDefaultBranch` по продуктам, `sourceRegistry.set` и `BackfillFilesFromSources` — тот самый UPDATE с seq-scan по `files` (`source_registry.go:140-190`).
   - В результате строка «git discovery: 80/80» будет висеть всё время backfill'а, а `DiscoveryMs` (D4) включит и его. Для change'а, весь смысл которого — честная видимость, это вводит в заблуждение.
   - Рекомендация: разделить стадии — `git discovery: N/M` (только детекция) и отдельная стадия для resolve/upsert/backfill (например, `source backfill`), либо включить эти шаги в M. Спека (`specs/indexing/progress-reporting/spec.md:33-42`) пост-детекционную работу не описывает — согласовать.

4. **[Medium] Первый `update` после апгрейда получает новое «тихое окно» до баннера — вне зоны действия репортера.**
   - `db.InitSchema()` вызывается в `cmd/update.go:55` и `cmd/init.go:98` — до печати «Updating started»/«Indexing started» (`update.go:63`, `init.go:113`). Именно там на существующей БД выполнится `CREATE INDEX` без `CONCURRENTLY` по ~624k строк (`design.md:84-86`), взяв SHARE-блокировку на `files`.
   - Change борется с «минутой тишины после баннера», но добавляет/сохраняет одномоментное тихое окно **до** баннера, которое репортер принципиально не покрывает (он стартует внутри `UpdateCtx`/`runInitPipeline`). Это ровно тот первый прогон после замены бинаря, когда пользователь уже настроен на «зависло».
   - Рекомендация: печатать баннер/строку «schema check/migration…» до `InitSchema` (или перенести баннер выше `InitSchema`), добавить окно в Risks/Migration Plan и замерить; заодно уточнить формулировку proposal «без блокировки записи сверх штатного» (`proposal.md:70-71`) — `CREATE INDEX` пишет блокировку записи `files` на время построения (design это честно говорит).

5. **[Medium] `load state: 624457 files` в proposal противоречит design/дельте: счётчик внутри блокирующего SELECT невозможен.**
   - Proposal (`proposal.md:27`) даёт конкретный счётчик; design прямо относит это к Non-Goals: «стриминг-счётчик внутри `load state` (блокирующий SELECT): именованное ожидание без счётчика» (`design.md:35-36`), D1 — «load state: files — статичная деталь» (`design.md:50`), дельта — «именованное ожидание без счётчика» (`spec.md:50-54`).
   - Рекомендация: привести пример в proposal к `load state` (или `load state: previous files`). Если хочется показать число — только после завершения загрузки отдельной строкой/стадией, но это уже другое поведение.

6. **[Medium] Bump схемы блокирует все read-only команды до полного `update` — для одного индекса это нужно проговорить явно.**
   - `CheckSchemaVersion` ищет точный маркер (`db_schema_version.go:15-44`): после замены бинаря на v6 при БД v5 все `query`/`stats`/`health`/`review`/`rti`/`trc` вернут `ErrSchemaUpdateRequired`, пока не выполнится `update`. Дельта формулирует это как «read-only команды остаются на fast-path проверки версии» (`spec.md:13`), что маскирует фактический пользовательский эффект.
   - Bump обязателен по readonly-schema-policy для любого DDL, так что решение приемлемо, но Impact/Risks должны явно сказать: «после обновления бинаря read-only команды требуют однократного `update`», и что это цена за чисто оптимизационный индекс.

7. **[Low] Стадия без «: » отрисуется с пустым префиксом (` | safety-net`).**
   - Репортер режет `Stage` по `": "`; без разделителя `prefix=""`, `detail=Stage`, `line = "%s %c %s"` (`indexer.go:1938-1945`, `1970`). Для `Stage="safety-net"` получится `" | safety-net"` — не как `load state | files`.
   - Design D1 перечисляет разнородные формы: `git discovery: N/M`, `safety-net`, `load state: files` (`design.md:44-49`). Стоит либо всегда задавать `phase: detail` (`safety-net: dedup`), либо научить репортер пустому префиксу. Добавить в тест рендера (task 3.4) кейс стадии без детали.

8. **[Low] Пул discovery должен использовать существующий `normalizeParallel`, иначе `-j 0` подвесит прогон.**
   - В проекте уже есть `normalizeParallel` (`indexer.go:199`, `<=0 → 1`), его используют оба worker-пула; `fswalk.WalkParallelCtx` тоже клампит (`fswalk.go:164`). При `-j 0` `effectiveParallel=0` уходит и в `discoverGitSources`, и пул из 0 воркеров не завершится. В design D2/задаче 2.1 указать `normalizeParallel(parallel)`.

9. **[Low] Сценарий «Пул из одного воркера» содержит неверную предпосылку.**
   - «`parallel = 1` (или конфигурация без `indexer.parallel`)» (`spec.md:41-45`): конфигурация без ключа даёт `Parallel = 4` (`config.go:352-353`), а не 1. Оставить только `-j 1` / явный `parallel = 1`.

10. **[Low] Сценарий «Параллельная детекция 80 каталогов» ассертит тайминговое соотношение — недетерминированное и непроверяемое.**
    - «суммарное время детекции порядка времени `ceil(80/12)` самых медленных детекций, а не суммы всех» (`spec.md:34-39`) — это не поведенческая гарантия, а оценка, зависящая от AV/нагрузки; как критерий приёмки она флейкует.
    - Заменить на наблюдаемое свойство: максимум одновременно выполняющихся `Detect` равен `parallel` (счётчик в подставном детекторе) + идентичность результата. Для этого `discoverGitSources` принимает конкретный `*gitDetector` (`source_registry.go:105`) — для теста 2.4 нужен интерфейс/хук, о чём в задаче не сказано.

11. **[Low] Impact не перечисляет `internal/store/db_files.go` и `db_reconcile_relations.go`.**
    - Задачи 4.2/4.3 правят `GetLatestFilesByRootPath` и `FindDuplicateFileGenerations`, а Impact (`proposal.md:67-68`) называет только `db_schema.go`/`db_products.go`.

12. **[Low] Переякорение `WalkSaveMs` меняет смысл «Parallel efficiency» — это стоит отметить в задачах и замере.**
    - Отношение считается как `(SaveMs+ParseMs)/(WalkSaveMs+ProcessMs)` (`cmd/init.go:34-38`); после переноса старта `WalkSaveMs` на фактический walk знаменатель перестаёт включать подготовку (для init — корректнее, для сравнения с историей — другая база). Design Risk это упоминает общо («старые сравнения поплывут», `design.md:118-119`); task 1.2/5.2 полезно дополнить явным «зафиксировать до/после на одном корне».

13. **[Info] Экранирование LIKE непоследовательно, и хелпер уже существует.**
    - `GetLatestFilesByRootPath` подставляет `normalizedRoot+"/%"` без экранирования (`db_files.go:24`), `FindDuplicateFileGenerations` — через `escapeLikeAll` (`db_reconcile_relations.go:124`). При root с `%`/`_` первый запрос перематчивает. Раз предикаты «выравниваются», уместно применить `escapeLikeAll` в обоих (существующий хелпер `db_reconcile_relations.go:103`; для 4.4 новый не нужен — сослаться на него).

14. **[Info] Migration Plan: откат старым бинарём описан неверно (повтор замечания из прошлого change-review).**
    - `applyMigrationTx` только вставляет маркеры и никогда их не удаляет (`db_schema.go:1309-1326`); после v6 маркер `codebase_schema_v5` остаётся, поэтому старый v5-бинарь пройдёт `CheckSchemaVersion` и будет работать (индекс обратно совместим). Утверждение «после применения v6 старый бинарник откажется работать» (`design.md:131-135`) неверно — откат фактически бесшовный. Это же уже отмечалось в `change-review-symbols-owner-key-materialization.md` (п.7) — текст стоит поправить, а не копировать дальше.

15. **[Info] Тайминги «трёх фаз» не покрывают FTS-backfill, а его стоимость не измерена.**
    - `WalkSaveMs` перестанет включать `CreateScanRun` и `BackfillAPIContractSearchVectors` (`db_fts.go:43-50` — `UPDATE api_contracts … WHERE search_vector IS NULL`), при этом отдельного тайминга у них нет. Design называет их «millisecond-фазами» без замера (`design.md:51-53`), а дельта обещает, что три тайминга «суммарно объясняют подготовительное окно» (`spec.md:68-80`) — на легаси-БД после смены версии FTS-backfill может затронуть много строк. Кроме того, в `runInitPipeline` репортер по task 3.1 стартует только перед discovery — FTS в `init` останется без стадии. Либо замерить и завести тайминг/стадию, либо квалифицировать формулировку спеки.

## Вывод

Направление верное и хорошо сверено с кодом: тихое окно действительно образуют фазы до `startProgressReporter` (`runner.go:187-214`), `WalkSaveMs` действительно считается от старта `UpdateCtx` (`runner.go:132`, `runner.go:380`), параллельная детекция с сохранением порядка ReadDir и последовательными upsert'ами реализуема, механизм `Stage` переиспользуется без изменений, миграция соответствует паттерну `applyMigrationTx`/fast-path, дельты структурно корректны и проходят `--strict`. Блокирующих дефектов нет.

Перед `/opsx-apply` рекомендуется устранить п.1–6: (1) согласовать требование/сценарий индекса с реальным планом (Seq Scan при полном корне) и убрать «opциональный» EXPLAIN; (2) перейти на `text_pattern_ops` для `TEXT` и поправить обоснование в D3; (3) не показывать `N/M` во время upsert/backfill; (4) закрыть новое тихое окно миграции до баннера; (5) убрать счётчик из примера `load state`; (6) явно задокументировать блокировку read-only команд после bump. Пункты 7–15 — уточнения формулировок/тестов, не блокирующие реализацию.

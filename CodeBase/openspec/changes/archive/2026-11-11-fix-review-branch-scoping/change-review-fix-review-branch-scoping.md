# Change Review: fix-review-branch-scoping

- **Дата**: 2026-11-10
- **Ревьюер**: opencode (L3/pro)
- **Вердикт**: ⚠️ **ГОТОВ К РЕАЛИЗАЦИИ ПОСЛЕ РЕШЕНИЯ F1** (модель поставки EXT-продуктов) и небольших уточнений F2–F4. Артефакты change структурно корректны, все измерения proposal воспроизведены с точным совпадением, каждое заявление design о текущем коде проверено и подтверждено. F1 — единственный содержательный пробел: спроектированная модель «одна разрешённая ветка на продукт» не выражает поставку EXT-продуктов (develop ∪ prj/<банк>), что создаёт новый класс ложных severity-1 findings уже на текущем индексе.

## Что проверялось

1. Структура: `openspec validate fix-review-branch-scoping --strict` — valid; 25 задач; все 5 дельт распознаются; каждое `MODIFIED`-имя существует в основном спеке (`deploy-stoppers` ×4, `fine-code` ×1, `mcp-transport-tools` ×2, `dsinfo-resolution` ×2); MODIFIED-тексты — полные замены с корректными добавлениями; новая capability `review/review-scope` имеет Purpose/Requirements/Related code/Notes.
2. Код: `internal/review/{types,runner,review_lookup,review_rules}.go`, `internal/store/{db_lookup_sql,lookup_scope}.go`, `internal/dsinfosvc/context.go`, `internal/reviewsvc/runtime.go`, `cmd/review.go`, `cmd/{query,rti,trc}.go` (паттерн `analysisDsInfoID`), `internal/mcp/registry.go`; перечислены **все** DB-lookup call sites правил review для проверки полноты списка затрагиваемых правил.
3. Живая БД (localhost:5435/codebase, SELECT-only через временную Go-программу, удалена): воспроизведение всех четырёх измерений proposal по запросам автора + дополнительные замеры (настоящие дубли, новые existence-находки, EXT-специфика, индексы `files`).

## Подтверждённые положительно

- **Измерения proposal точны**: 10 635 пар (имя, продукт) с копиями >1 ветки; 9 489 с идентичным `body_hash`; 771 расхождение типов колонок; 81 расхождение сигнатур — все четыре числа воспроизведены точно. Контекст: 1 987 настоящих межфайловых дублей в пределах одной ветки (пул реальных `procDuplicate` после скоупа) и 11 within-file (file, имя)-пар, которые `SELECT DISTINCT f.id` маскирует.
- **Все заявления design Context верны** (проверено по коду): `indexedFile` без ветки и `getIndexedFile` без её выбора (types.go:253, review_lookup.go:34); `FindLatestSQLColumnDefinitionType` уже принимает scope, review передаёт `BranchID: 0` (runner.go:442); `LookupScope` существует (lookup_scope.go); `ResolveAnalysisContext` даёт `Branches map[int64]int64` (dsinfosvc/context.go:47); `cmd/review.go` без `--dsinfo`, `codebase_review_sql` без `dsinfo_id` (registry.go:372), `analysisDsInfoID` — общий var на query/rti/trc (query.go:12, rti.go:703, trc.go:765); batch-функции `db_lookup_sql.go` без scope; `checkProcDuplicate` через `DISTINCT f.id` (review_rules.go:1800 → review_lookup.go:207); first-wins дедуп в `indexFieldsByName` (review_lookup.go:613).
- **Факты БД**: 0 файлов с NULL product/branch; 15/15 продуктов с default-веткой; `idx_files_ds_product_id`, `idx_files_ds_product_branch_id` существуют.
- **Список затрагиваемых правил полон**: перечисление всех lookup call sites в review_rules.go показало, что каждое правило с DB-lookup'ом входит в список proposal (existence/value/identity). Единственное исключение — `statementsWithJoinsRequireAliases` → `FindHDefineExistsByName`, см. F2.
- **`postgre-requirements` действительно не задаёт scope** (grep по ветка/branch/scope/продукт — пусто) — заявление proposal о неизменности их текстов корректно.
- **Три режима согласованы**: таблица D3 = требование «Режимы DB-lookup review» = цепочка приоритета в дельте fine-code; «свой продукт раньше чужого» для identity соответствует текущему дефекту `lookupProcedureProductID` (один продукт по `id DESC`, review_rules.go:1551).
- **Инфраструктурно чисто**: схема БД не меняется — bump `CurrentSchemaVersion` не нужен; откат возвратом бинаря; конкурентность не ухудшается (один запуск = один файл = один scope, кэши остаются keyed-by-name, scope строится до prewarm'ов).
- **Цепочка dsinfo** в дельтах = существующая семантика `ResolveAnalysisContext` (explicit > env > active > none; несуществующий id — ошибка).

## Findings

### F1 [High] — EXT-продукты: модель «одна ветка на продукт» не выражает поставку develop ∪ prj/<банк>

Уточнение пользователя (2026-11-10): у EXT-продуктов develop содержит общую логику, `prj/*` — уникальный функционал конкретного банка, общих файлов с develop обычно нет; **в банк поставляется develop + ветка банка**. Подтверждено индексом:

- `fa-contracts-ext` = `develop` (default, 1029 процедур) + `prj/novikom-persform` (16 процедур); пересечение имён — **0** (ветки полностью дизъюнктны). `fa-dbmaintenance-ext` = `develop` + `prj/tblinmemory` — та же схема.
- Следствие для **existence strict**: файл prj-ветки, обращающийся к сущности из ext-develop, получит ложный severity-1 finding (`execNotExistsProc`/`tableExistsInMetadata`), т.к. own-product scope = только файловая ветка. Замер на текущем индексе: **6 таблиц** в DML-контексте prj-файлов имеют создателя только в ext-develop → 6 ложных `tableExistsInMetadata` уже сегодня; call-рёбер prj→develop-only процедур сейчас 0 (51 SQL-файл в prj-ветках), но это свойство текущего маленького банка, не модели.
- Следствие для **чужих продуктов**: fallback EXT-продукта на default-ветку (= develop) прячет 16 prj-имён от файлов других продуктов; обратный выбор (prj) спрятал бы 1029 develop-имён. Обе стороны одной проблемы.
- **Не страдают**: `procDuplicate` (дизъюнктные имена в разных ветках не дубли по спеке), value-режим (тир «продукт» ловит develop-определения), identity-режим (ось продукта).

**Требуемое решение до реализации** (design D2/D3/D8 + дельта review-scope + tasks синхронно):

1. Для own-product existence-скоупа файлов в `prj/*`-ветках: область = **{файловая ветка} ∪ {default-ветка продукта}** — отражает фактическую поставку «develop + банка». Детект по `ds_products_branch.branch_key LIKE 'prj/%'` (оба EXT-продукта следуют паттерну; без изменения схемы).
2. Для чужих EXT-продуктов: разрешённая область = default ∪ все `prj/*`-ветки (имена дизъюнктны — ложных дублей и конфликтов value не возникает; либо default ∪ профильная prj, если профиль указывает линию банка).
3. Сценарий в дельту review-scope: «prj-файл использует таблицу/процедуру, созданную в ext-develop → findings нет» + зеркальный «develop-файл ext-продукта использует prj-таблицу → findings нет» и задача-тест.

Альтернатива — осознанно принять FP для prj-файлов и зафиксировать в Risks, но это противоречит цели change (убрать ложные severity-1) и уже даёт 6 ложных находок на текущих данных.

### F2 [Med] — review-scope over-claim: «все DB-lookup'и» в трёх режимах, но два lookup'а остаются глобальными и не названы

Требование «Режимы DB-lookup review» предписывает «Система SHALL применять к DB-lookup'ам review один из трёх режимов», однако:

- `isMemoryTable` → `memory_tables` — остаётся глобальным по имени (задача 4.5 это явно сохраняет, но **спека об этом не говорит**);
- `filterKnownNames` → `FindHDefineExistsByName` → `h_files_defines` (правило `statementsWithJoinsRequireAliases`, severity 2) — глобальная проверка существования define, **нигде в change не упомянута**.

После реализации оба lookup'а формально нарушат спеку. Дёшево закрыть: предложение в review-scope об исключениях — «декларации in-memory таблиц (`memory_tables`) и константы `#define` (`h_files_defines`) проверяются глобально по имени: физическая сущность одна на БД / константы общие» (по образцу ignore-листа deploy-stoppers). NB: `lookupMacroSignature` (тип макроса) при этом остаётся в value-режиме — это корректно и асимметрия («существование — глобально, значение — с приоритетом») стоит одной фразы в Notes.

### F3 [Low] — tasks 2.3 уже́, чем design D3/D4: batch-функциям нужна карта, а не только BranchID

Полный value-приоритет D3 содержит тир «branch=resolved(product)» для **чужих** продуктов — он не выражается парой (ProductID, BranchID): нужна карта `ProductBranch` через `unnest` (как D4 и говорит про batch-функции). Но задача 2.3 формулирует «Добавить `BranchID` в `BatchFindColumnDefinitionTypes`, `FindLatestSQLColumnDefinitionType` … и `FindAPIColumnDefinitionType`». Реализация по букве задачи даст: batch-путь = file → product → id (без веточных тиров), ленивый путь = file → branch(own) → product → default → id — расхождение поймает тест эквивалентности 2.4, но правильнее синхронизировать формулировку 2.3 (карта/unnest; для `FindAPIColumnDefinitionType` — веточный приоритет через join `files`/`ds_products_branch`, сейчас только `product_match`).

### F4 [Low] — value-режим при `BranchID = 0` (branch_id NULL) не специфицирован

D8 и спека определяют product-scope для existence («поиск по всем веткам продукта») и совместимость («как до многоветочной модели»), но не описывают, как деградируют **тиры** value-режима: «из продукта и ветки» при branch 0 не матчится никогда, «разрешённая ветка продукта» = все ветки? Нужен один абзац в design (например: branch 0 → веточные тиры схлопываются, own-product кандидаты всех веток получают приоритет тира «продукт», далее default-ветки чужих, далее id) — иначе реализатор выберет произвольно, а тест 1.2 проверяет лишь заполнение scope, не порядок.

### F5 [Info] — Масштаб новых «корректных» existence-находок (зафиксировать в Impact/notes)

Замер «имя существует в продукте, но отсутствует в конкретной ветке» (потенциальные новые findings, которых сегодня нет, т.к. lookup глобальный):

| Ветка | Имён в продукте | Отсутствует в ветке |
|---|---|---|
| fa-contracts-11-01-7 | 6 494 | 28 |
| fa-contracts-11-01-6 | 6 494 | 63 |
| fa-cards-11-01-9 | 4 223 | 53 |
| develop (прочие продукты) | — | 0–16 |

Пул мал относительно 10 635 ложных — курс change верный; но числа стоит внести в design (Impact «появляются корректные findings» → с масштабом) и в release-notes (см. Risks: «Отразить в notes к релизу»).

### F6 [Info] — Новое пересечение execNotExistsProc ∩ foreignProcedureUsing

По новой семантике вызов процедуры чужого продукта, существующей только в его неразрешённой ветке, даст **обе** находки: `execNotExistsProc` (severity 1) и `foreignProcedureUsing` (severity 3). Каждая сторона покрыта сценариями по отдельности, комбинация — следствие. Рекомендация: добавить в дельту deploy-stoppers сценарий «процедура чужого продукта только в неразрешённой ветке → execNotExistsProc формируется, foreignProcedureUsing не маскирует его» и по желанию замерить класс на живом индексе (в F1-замере 0 рёбер prj→develop — класс пуст на текущих данных, но не для чужих EXT-продуктов при default=develop).

### F7 [Info] — Верифицированные факты (для протокола)

- MODIFIED-дельта dsinfo-resolution добавляет `review` в список команд анализа и в границы применения профиля; MCP-дельта переносит review из исключений `dsinfo_id` во включения + «Review SHALL NOT добавлять warnings о ветках» — ответ review (`review.Result`) warnings-поля не имеет, требование тривиально выполняется.
- `--dsinfo` через `reviewCmd.PersistentFlags().Int64Var(&analysisDsInfoID, ...)` — установившийся паттерн (query/rti/trc вешают тот же var); ошибку dsinfo можно классифицировать в `classifyReviewError` по аналогии с query, но это опционально.
- Классификация ошибок «file is not indexed» / «ds_product_id is not set» — поведение не меняется, сценарии дельты соответствуют коду (runner.go:109,113).
- `unnest($n::bigint[], $m::bigint[])` + `pq.Array` — параметризовано, соответствует прецедентам кодовой базы; риски производительности оценены в design корректно (индексы `files` и `sql_*(file_id)` существуют).
- Prewarm-гейт `ruleSet[RuleTableExistsInMetadata]` сохраняется — scope не влияет на условия гейта.

## Резюме

| # | Суть | Приоритет | Действие |
|---|---|---|---|
| F1 | EXT-поставка develop ∪ prj не выражается одной веткой; 6 ложных tableExists уже сейчас | High | Решение в design D2/D3/D8 + сценарии дельты + задача-тест (union для prj-веток) |
| F2 | Спека требует режимы для «всех» lookup'ов, но memory_tables/h_files_defines глобальны | Med | Явные исключения в review-scope + Notes |
| F3 | Задача 2.3 «только BranchID» vs D4 «карта» — расхождение prewarm/lazy | Low | Синхронизировать формулировку 2.3 |
| F4 | Value-тиры при BranchID=0 не определены | Low | Абзац в design |
| F5 | Масштаб новых existence-находок (28/63/53 имён) | Info | Зафиксировать в design/notes |
| F6 | execNotExists ∩ foreignProcedure на одном вызове | Info | Сценарий в дельту по желанию |
| F7 | Прочие верифицированные факты | Info | — |

Change исключительно хорошо подготовлен: измерения точны, заявления о коде достостоверны, режимы согласованы между design и спеками, покрытие правил полное. После решения F1 (EXT-union) и правок F2–F4 готов к реализации без повторного ревью.

## Resolution (2026-11-10, разбор автором change)

Вердикт: F1–F6 — **приняты и применены к артефактам change**; F7 — подтверждение без действий. Замечаний, отклонённых с обоснованием, нет.

### Исправлено

- **F1 [High]** — модель EXT-поставки `develop ∪ prj/<банк>` встроена во все артефакты:
  - design: Context дополнен фактами EXT (дизъюнктность имён, 51 prj-файл, 6 ложных `tableExistsInMetadata`); D1 — карта `ProductBranch map[int64][]int64` (множество веток = область); D2 — правила EXT-областей (prj-файл → {ветка, default}; default-файл → {default} ∪ prj; чужой EXT → default ∪ prj либо default ∪ профильная prj; детект `branch_key LIKE 'prj/%'`); D3 — предикат existence `= ANY(область)`, value-тир `branch IN область(product)`; D5 — область `procDuplicate` едина с existence (имя в prj и develop одного EXT-продукта — дубль в поставке банка; на текущих данных пересечение 0, случай теоретический); D8 — EXT-совместимость; Risks — эвристика `prj/%` (риск FN, не FP); Migration Plan — проверка на prj-файлах.
  - review-scope: новое требование «Область видимости EXT-продуктов» (4 сценария: prj-файл × таблица/процедура из ext-develop, develop-файл × prj-таблица, prj другого банка вне области); формулировки existence/value/дублей переведены с «ветки» на «область»; в требовании профиля — результат разрешения для чужого EXT = область.
  - deploy-stoppers: все 4 MODIFIED-требования переформулированы на «разрешённую область» («существующая только в другой ветке» → «вне разрешённой области»); добавлены сценарии «Процедура ext-develop в prj-файле» и «Таблица ext-develop в prj-файле». fine-code: EXT-объединение в value-приоритете.
  - tasks: 1.3 — карта областей; новая 1.5 — построение EXT-областей с интеграционным тестом на `fa-contracts-ext`; 4.1/4.2 — EXT-случаи; новая 6.6 — прогон на prj-файлах (6 таблиц из замера). proposal: What Changes/Impact дополнены.
  - Независимая верификация факта: `FA/fa-contracts-ext-novikom-persform` существует, 51 SQL-файл — совпадает с замером ревью.
- **F2 [Med]** — исключения зафиксированы: требование «Режимы DB-lookup review» (параграф об `memory_tables` и `h_files_defines` глобально по имени) + Notes об асимметрии «существование константы глобально, тип макроса — value»; design D3 — блок исключений; proposal — строка об исключениях; задача 4.5 расширена на `FindHDefineExistsByName`.
- **F3 [Low]** — задача 2.3 переформулирована под D3/D4: `BatchFindColumnDefinitionTypes` — карта областей через unnest-пары (включая тир «разрешённая область чужого продукта»), `FindLatestSQLColumnDefinitionType` — `scope.BranchID` + область, `FindAPIColumnDefinitionType` — join `files`/`ds_products_branch`.
- **F4 [Low]** — деградация value-тиров при `BranchID = 0` описана в design D8 (веточные тиры своего продукта схлопываются до тира «продукт»; далее область чужих → default чужих → `id DESC`) и в Notes review-scope.
- **F5 [Info]** — масштаб новых existence-находок (28/63/53/0–16) внесён в proposal Impact и design Risks с отсылкой к release-notes.
- **F6 [Info]** — сценарий «Процедура чужого продукта только в неразрешённой ветке» добавлен в требование «Режимы DB-lookup review»: `execNotExistsProc` (severity 1) формируется, `foreignProcedureUsing` (severity 3) не маскирует его.

### Отклонено

- Ничего: все замечания ревью приняты. Единственное расширение сверх ревью — распространение EXT-union на `procDuplicate` (D5, спека, задача 4.1): ревью сочло `procDuplicate` «не страдающим», но единая область существования и дублей семантически корректнее (в поставке банка develop+prj — одна кодовая база); на текущих данных поведение не меняется (пересечение имён веток = 0).

### Подтверждено без правок

- **F7 [Info]** — верифицированные факты протокола; изменений артефактов не требует.

### Верификация после правок

`openspec validate fix-review-branch-scoping --strict` — valid; `openspec validate --all --strict` — 37/37 passed, 0 failed. Change готов к реализации.

Code review: branch-aware-lookup

Заключение
Реализация в целом соответствует замыслу среза B, но к архивированию не готова. Каркас (LookupScope, tier-фильтр, batch-группировка по (product, branch), детерминированный ORDER BY) сделан последовательно, сборка и unit-тесты проходят, openspec validate --strict — valid. Однако есть 4 дефекта уровня P1 (два из них ломают пользовательские данные, один — стык со срезом C) и несколько расхождений спеки с кодом.

Что проверено
- Все артефакты change (proposal, design, tasks, 6 дельт) и весь дифф по internal/{store,query,specsvc,indexer,model,review} + новые файлы.
go build ./... — exit 0.
- go test ./internal/query ./internal/specsvc ./internal/store ./internal/indexer ./internal/review — ok (cached; исходники не менялись).
- openspec validate branch-aware-lookup --strict — valid.
- Integration-тесты и smoke на FA не запускались (read-only режим; БД не трогал). Задача 5.3 в tasks.md не отмечена.

P1 — исправить до smoke/архива
1. branch_used означает разное: branch_name в query/API и branch_key в specsvc. internal/query/query_branch.go:21, internal/store/db_lookup_retcode.go:29 → br.branch_name; internal/specsvc/specsvc.go:462,542,587,... и internal/store/db_lookup_spec_profile.go:90 → br.branch_key. Для SP-линии это даст FA-Contracts-11-01-7-SP против fa-contracts-11-01-7. Срез C (branch_requested из dsinfo — это branch_key) будет сравнивать их напрямую → ложные fallback-warnings и неверная семантика. Нужно унифицировать (рекомендую branch_key, при необходимости добавить отдельное поле branch_name) и покрыть тестом копию не из default-ветки.
2. SearchTable: обогащение строк ломается при нескольких продуктах. internal/query/query_sql.go:80-85 строит tableResults по TableName, хотя запрос теперь законно возвращает по строке на продукт. Обе строки имеют один ключ → loadTableFiles/Columns/Procedures (:100-212) наполняют только последнюю, первая остаётся пустой; сами enrichment-запросы к тому же не ограничены продуктом/веткой. Сценарий «таблица в fa-contracts + fa-payments» формально проходит, но данные строк неверны. Нужен ключ (table_name, product) (лучше — (name, product, branch)) и product/branch-фильтр в enrichment.
3. FindLatestEventContractIDsByNames: module_key стоит перед приоритетом. internal/store/api_store.go:431: ORDER BY name_key, module_key, not_generated DESC, branch_match DESC, .... Для fallback ByName первая строка выбирается из алфавитно первого модуля, а не по приоритету — non-default копия модуля aaa побьёт default-копию модуля zzz. Убрать module_key из ORDER BY (для ByNameAndModule приоритет внутри пары сохранится) либо перейти на DISTINCT ON.
4. Спеки обещают то, чего в срезе B нет (важно для корректного архива). entity-queries/spec.md:7,36 требует выбора по «запрошенной ветке / branch_match / product_match», api-relations-queries/spec.md:9 — тот же приоритет. В query-слое ветку запросить нечем (у GetProcedureResult нет scope, списки дедупят только по default). Эти оси фактически работают лишь во внутренних FindLatest* пост-обработки. Либо перенести эти пункты в dsinfo-integration через openspec-update-change, либо реализовать scope в B. Иначе после archive основная спека станет ложной.

P2 — исправить или осознанно зафиксировать
1. spec_history: branch_used записи берётся от последней строки. internal/specsvc/specsvc.go:1079-1082: result.BranchUsed перезаписывается каждым Scan, затем entry.BranchUsed = result.BranchUsed — все записи получают ветку последней строки. В by-change режиме result.BranchUsed вообще не заполняется (:1089-1094). Нужно сканировать в entry.BranchUsed и отдельно определить семантику поля на уровне результата.
2. desc-search: нарушен явный запрет менять состав на одно-веточном индексе. description-search/spec.md:13 («дедупликация не SHALL изменять состав»), но dedup-ключ (internal/query/desc_query.go:143-148, internal/query/query_branch.go:24-27) — (имя, продукт) без ветки. Одноимённые процедуры одной ветки/продукта схлопываются, чего раньше не было. Тот же паттерн в списках противоречит proposal.md:74 («поведение идентично»). Либо паттерн «выбрать ветку → сохранить все строки этой ветки» (как сделано для query fragments), либо скорректировать спеку.
3. spec_by_code: в ключ дедупа попал m.line_number. internal/specsvc/specsvc.go:447-449. Копии capability в develop/SP дедупятся только при совпадении номера строки упоминания; при сдвиге строк получим дубль capability, что нарушает spec-queries/spec.md:26. Нужен тот же двухшаговый выбор ветки/конфигурации.
4. apiChosenContractsCTE не учитывает contract_kind. internal/query/api_query.go:334,340 — DISTINCT ON (contract_name, product). Для одноимённых event/callback_event/service может быть выбрана не та копия; в SearchAPIPublishers (:424-425) это приводит к потере event-строки. В SearchAPIContract kind учтён (:127) — здесь забыт.
5. FindLatestSQLColumnDefinitionType не реализует tier-фильтр. internal/store/db_lookup_sql.go:176-193 только сортирует, но не исключает чужие ветки того же продукта (в отличие от остальных FindLatest*). При этом review/runner.go:416 передаёт только ProductID, без BranchID, так что review принципиально не может предпочесть ветку проверяемого файла. Нужно либо выровнять по D4, либо задокументировать исключение.
6. query tables: изменилась семантика usages и context. internal/query/query_sql.go:28-39: COUNT(*) теперь считает копии всех веток продукта (тест это фиксирует: query_branch_dedup_integration_test.go:166), а DISTINCT ON (table_name, product) схлопывает разные context одной ветки в одну произвольную строку. Это изменение пользовательского поведения не отражено в спеке/README — решить и задокументировать.

P3 — мелкие замечания
1. loadAPIContractParams/loadAPIContractTables (internal/query/api_query.go:492-530) не заполняют новые Product/BranchUsed во вложенных params/tables, хотя standalone-запросы заполняют.
resolvePASClassTargets (internal/indexer/indexer_postprocess_pas.go:231,253) всегда возвращает non-nil map, поэтому проверка classIDByScope == nil (:88) мёртвая; при ошибке lookup обработка продолжается и генерирует лавину «class not found».
2. resolveAPIMacroNames (indexer_postprocess_fragments.go:360) изменил обработку ошибок: раньше — пустая map и частичные рёбра, теперь — пропуск всей scope-группы.
SearchDFMComponent (internal/query/query.go:646-688) схлопывает несколько pas_fields одного компонента в одну строку (часть связей теряется).
3. SearchSymbol: коррелированные подзапросы в symbolOwnerKeyExpr (query.go:353-358) выполняются на каждую совпавшую строку; при широком LIKE стоит проверить план.
4. В FindLatest* ось not_generated стоит выше tier-ярусов: сгенерированная копия в ветке источника проиграет не-сгенерированной в другом продукте. Это соответствует design D2, но расходится с формулировкой ярусов в relations-postprocessing/spec.md:9-11.
5. Порядок строк в SearchTable/SearchAPIParam и т.п. при равных ключах не детерминирован (выбор копии — детерминирован, порядок выдачи — нет).

Пробелы в тестах
1. Нет ни одного assertion branch_used для не-default ветки (все фикстуры проверяют develop) — поэтому P1.1 не ловится.
2. Нет теста SearchTable с двумя продуктами (обогащение/файлы) — P1.2.
3. Нет теста event-контрактов с разными owner_module — P1.3.
4. Нет теста API-графа с одноимёнными контрактами разных kind — P2.8.
5. Нет теста spec_history с изменениями из двух веток — P2.5.
6. Нет регрессионного теста «одно-веточный индекс до/после» (обещан в 5.1, фактически не показан).

Соответствие требованиям
Требование	Статус
entity-queries / «Детали SQL-процедуры»	✅ детерминизм и branch_used есть; ⚠️ branch_used = branch_name
entity-queries / приоритет и списковый дедуп	⚠️ branch_match/product_match не применимы без scope; дедуп только default
api-relations-queries	⚠️ branch_used есть, граф без дублей; ⚠️ нет contract_kind в CTE, нет оси ветки
relations-postprocessing	✅ tier-фильтр, группировка по (product, branch), batch сохранён
symbol-search	✅ ключ (тип, имя, владелец, продукт)
description-search	⚠️ дедуп есть, но нарушен no-op на одно-веточном индексе
spec-search (exact/trgm)	✅; LSA-хиты не несут branch_used (вне сценария)
spec-queries	⚠️ spec_by_code (line в ключе), spec_history (branch записи)
spec_config / перечисление конфигураций	✅

Рекомендации
Исправить P1.1–P1.3, добавив тесты на SP-копию и на два продукта.
Решить судьбу P1.4: либо update-change и перенос пунктов про branch_match/запрошенную ветку в dsinfo-integration, либо реализация scope в B. До этого не архивировать.
Закрыть P2.5–P2.8 (это либо прямая ложь в данных, либо нарушение текста дельты).

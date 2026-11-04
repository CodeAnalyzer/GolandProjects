# Code review: optimize-query-search-bottlenecks

Дата ревью: 2026-11-04. Ревью выполнено без правок кода.

**Проверено:** diff 23 файлов (+794/−130) + 6 новых файлов; `openspec validate --strict` — valid; `go build/vet` — чисто; `go test ./...` — ok; `go test -tags integration` (query/store/specsvc/mcp/indexer) — ok (235–279 с).

## Верификация реализации (по задачам)

| Задача | Статус |
|---|---|
| B1 pushdown FTS | ✅ предикат во всех 4 ветках CTE, внешние фильтры в `chosen`/финальном SELECT сохранены (`internal/specsvc/specsvc.go:1316–1372`); equivalence-тест против legacy-запроса |
| B2 гейтинг body-trgm | ✅ `specfts.LexicalUnitCount` (переиспользование, своей токенизации нет), tech-ветви всегда, порог — константа `specTrgmMaxQueryTokens=2`; порядок веток сохранён; конкатенация только статического SQL |
| B3 report_form UNION ALL | ✅ 3 индексные ветви + `DISTINCT` в финальном SELECT; INNER JOIN от `report_forms` обоснован комментарием; `buildReportFormLookupCondition` удалена полностью; equivalence-тест против legacy OR + тест дедупа мультиполевого совпадения |
| B5 cap + truncated | ✅ внутренний `LIMIT $3`=cap+1, `count(*) OVER ()` строго снаружи, tri-state конфиг (nil/0/<0), дефект $2/$3 исправлен и закрыт регресс-тестом limit=1 (`internal/query/query_fragment_cap_integration_test.go:129–138`); сигнатура обновлена во всех вызовах (cmd, mcp, rti-интерфейс) |
| B8 кеш displaced | ✅ ключ «generation + MAX(scan_run)», singleflight по образцу `descSemanticHolder`, double-check гонки; тесты покрывают все 4 сценария дельты, включая F1 («update без переобучения») |
| ANALYZE | ✅ единый `ANALYZE` по 14 таблицам (`internal/store/db_analyze.go`), ошибка — warning, статус прогона не меняется; вызов только в init/update-хвостах после `UpdateScanRun` |
| 7.x | ✅ build 1669, help CLI/MCP/README обновлены, отчёт верификации `Bug-reports/BUG-query-search-performance-verification-20261104.md` на месте |

## Находки

1. **[Medium] B8: гонка «запрос во время активного update»** — `MaxScanRunID` (`internal/store/db_scan_runs.go:38`) берёт `MAX(id)` без фильтра статуса, а scan_run создаётся со статусом `'running'` в начале прогона. Semantic-вызов, выполненный **во время** update, снимет displaced-множество с частичных данных и привяжет его к эпохе R; после завершения update `MAX(id)` останется R — кеш будет считаться свежим и раздавать снимок середины прогона до следующего update. Per-call SQL такого не допускал (регресс семантики в узком окне). Рекомендация: считать эпохой `MAX(id) WHERE status IN ('completed','completed_with_errors')` — идиома уже есть в `HasCompletedInit`.
2. **[Low] Proposal противоречит факту** — proposal.md:34 «внешние API не меняются», но MCP-ответ `codebase_query_sql_fragment` изменил форму: массив → объект `{items, truncated, hint}` (`internal/mcp/registry.go:535`). Изменение задокументировано в описании инструмента и закреплено юнит-тестом (`internal/mcp/sql_fragment_test.go`), но для существующих MCP-клиентов это breaking change; в дельте entity-queries смена формы MCP-ответа явно не зафиксирована. Перед архивацией стоит уточнить формулировку.
3. **[Low] gofmt** — `internal/config/config.go:557–558`: выравнивание `DefaultLimit`/`MaxLimit` в `CreateDefault` (остальные изменённые файлы чистые; шум `gofmt -l` по репо — CRLF).
4. **[Low] Shadowing builtin** — переменная `cap := ...` (`internal/query/query.go:802`) и параметр `cap` в тесте затеняют встроенную функцию `cap`. Легально, vet молчит, но не идиоматично.
5. **[Info]** Тест `internal/indexer/pipeline_integration_test.go` называет ANALYZE «B6», хотя в bug-репорте B6 = пробы MCP warmup (выведен из scope); ANALYZE в change не нумерован.
6. **[Info]** `analyzeAfterRun` выполняется и при `completed_with_errors` (корректно по духу — индекс обновлён), при `canceled` пропускается; NDJSON-режим CLI не несёт `truncated`/`hint` (для построчного контракта осознанно, но нигде не оговорено); `descDisplacedHolder.get` отдаёт shared map без копии — безопасно, пока все потребители read-only.

## Вывод

Реализация соответствует proposal/design/tasks, качество высокое (equivalence-тесты против legacy-SQL для B1/B3, регресс на найденный при верификации дефект). Блокирующего нет. Перед `/opsx-archive` рекомендуется: принять решение по п.1 (фикс одной строки или оформить known limitation в design Risks), уточнить п.2, поправить п.3–4 (косметика).

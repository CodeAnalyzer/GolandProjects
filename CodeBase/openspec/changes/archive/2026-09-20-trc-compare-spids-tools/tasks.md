# Tasks: trc-compare-spids-tools

## 1. Домен: compare-агрегация (internal/trc)

- [x] 1.1 Реализовать в `internal/trc/aggregate.go` compare-агрегацию file-mode: Top-N только по focus, peer-метрики для выбранных имён, nullable-пары для отсутствующих, `peer_combined` взвешенный, ratios `avg_vs_peers`/`max_vs_peers`/`count_vs_peers` (null при нулевом знаменателе), `rank`. Проверка: unit-тесты в `aggregate_test.go` — сценарии спеки (взвешенность 10×100+30×200=175, nullable-отличимость от нуля, Top-N только focus, нулевой знаменатель)
- [x] 1.2 Реализовать `internal/trc/store.go` серверный compare: `GROUP BY (spid, procedure)` с `event_name = ANY` + `procedure = ANY(topNames)` + `spid = ANY(focus+peers)`, метрики по `(spid, procedure)` для сборки результата одним проходом. Проверка: `store_integration_test.go` — совпадение с контрольным `GROUP BY` и с file-mode на одинаковых данных

## 2. Домен: сводка SPID (internal/trc)

- [x] 2.1 Реализовать file-mode сводку SPID в `internal/trc/aggregate.go`: `event_count`, `first/last_time` по ненулевым `Columns[14]`, счётчики `SP/RPC/BatchCompleted`, `error_count` по `Columns[31]`, `max_duration_ms`, мода `Columns[10,11,8]` с tie-break по первому встреченному. Проверка: unit-тесты на синтетике (мода с tie, NULL-время, счётчики)
- [x] 2.2 Реализовать серверную сводку: `GROUP BY spid` с `COUNT(*) FILTER`, `min/max(start_time)`, `max(duration_ms)`, мода identity-полей `(value, count, min(id)) ORDER BY count DESC, min(id) ASC`; фильтры `spid = ANY`, временной полуинтервал. Проверка: `store_integration_test.go` — parity с file-mode, временнóй фильтр исключает события без `start_time`
- [x] 2.3 Реализовать эмпирическую проверку доступности длительностей: `max(duration_ms)` по источнику (SQL + file-mode проход). Проверка: unit/integration-тесты — предупреждение при всех нулях, отсутствие при ненулевых

## 3. Сервисный слой (internal/trcsvc)

- [x] 3.1 Типы и валидация: `CompareParams` (`focus_spid > 0`, `compare_spids` непустой после дедупликации и удаления focus, `top` 1..100 с default 20, `sort_by` из части 1), `SpidsParams` (`spids` опц., время, `sort_by` enum с `error_count`, `limit` 1..1000 с default 100); результаты с `warnings`. Проверка: unit-тесты валидации в `runtime_test.go` (пустой peer-список, focus в peers, границы top/limit)
- [x] 3.2 Оркестрация `ExecuteCompareProcedures` / `ExecuteSpids`: saved-session серверно, file-mode в памяти; сборка warnings (completed-only, elapsed ≠ wall-clock, duration-unavailable). Проверка: parity-тесты saved/file на синтетике

## 4. CLI/MCP контракты

- [x] 4.1 Зарегистрировать MCP-инструменты `codebase_trc_compare_procedures` и `codebase_trc_spids` в `internal/mcp/registry.go` (схемы с array-параметрами, handlers с переиспользованием helpers части 1; `focus_spid` скаляр) + профиль `trc`. Проверка: тесты схем и handler-ошибок в `server_test.go`
- [x] 4.2 Добавить CLI `trc compare-procedures` (--focus-spid, --compare-spids, --top, --event-names, --sort) и `trc spids` (--spids, --time-from, --time-to, --sort, --limit) в `cmd/trc.go` с CSV-валидацией. Проверка: тесты парсинга в `cmd/trc_test.go`

## 5. Производительность и регрессия

- [x] 5.1 EXPLAIN-проверка compare и сводки SPID на большой сессии (≥ 500K событий: nbki) — существующие индексы используются либо измерения подтверждают приемлемость; partial-индекс `SP:Completed` включается только при подтверждении планами (отдельное решение, не в этой поставке). Проверка: замеры на nbki-сессии, зафиксированные в PR/commit
- [x] 5.2 Полный набор: `go build ./...`, `go test ./internal/trc ./internal/trcsvc ./internal/mcp ./cmd -count=1`, `go test -tags=integration ./internal/trc -count=1`; smoke на nbki через CLI и MCP (compare с `sort_by=count` — предупреждение о длительностях + count_vs_peers; spids с `sort_by=error_count`)
- [x] 5.3 Обновить README: новые подкоманды и MCP-инструменты, семантика ratios/warnings/nullable-пар

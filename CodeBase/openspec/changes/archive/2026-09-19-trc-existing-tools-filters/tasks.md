# Tasks: trc-existing-tools-filters

## 1. Модель фильтров и валидация

- [x] 1.1 Перевести `TRCEventFilter` (`internal/trc/store.go`) на массивы и указатели: `SPIDs []int`, `EventNames []string`, `TimeFrom/TimeTo *time.Time`, `MinDurationMs *int64`, `AfterID *int64`; расширить `buildEventFilterWhere` условиями `spid = ANY`, `event_name = ANY`, `start_time >= / <`, `duration_ms >=`, `procedure =`; обновить существующие вызовы фильтра. Проверка: `go build ./...` и `go test ./internal/trc ./internal/trcsvc -count=1`
- [x] 1.2 Добавить `StoreID int64` (`json:"id,omitempty"`) в `TRCEvent` (`internal/trc/model.go`) и проекцию с `id` в `scanEventRow`, не затрагивая tree-запросы. Проверка: unit-тест сканирования строки с `id`
- [x] 1.3 Реализовать в `trcsvc` нормализацию и валидацию параметров: положительные SPID, дедупликация с сохранением порядка, запрет пустых строк, RFC3339, `time_from < time_to`, `min_duration_ms >= 0`, `top` 0..1000, enum `sort_by`; ошибки содержат имя параметра (и индекс элемента для массивов). Проверка: unit-тесты валидации в `runtime_test.go`

## 2. Список событий: пагинация и short-формат

- [x] 2.1 Реализовать keyset-запрос в `internal/trc/store.go`: `WHERE session_id=$1 AND id > $cursor ORDER BY id LIMIT $limit+1`, курсор опционален (`AfterID *int64`); `has_more` по лишней строке; вернуть курсор последнего события. Проверка: `store_integration_test.go` — страницы не пересекаются, не теряют события, последняя страница `has_more=false`
- [x] 2.2 Реализовать short-проекцию без чтения `params`/`columns` JSONB: SELECT выделенных колонок (`id, event_class, event_name, spid, procedure, start_time, end_time, duration_ms`); ввести `TRCEventView` в `trcsvc/types.go` (full — доменные `TRCEvent` c `id`, short — view без `params`/`columns`). Проверка: интеграционный тест полного и short-набора с одинаковыми ID, порядком и числом событий
- [x] 2.3 Оркестрация `ExecuteEvents` (`internal/trcsvc/runtime.go`): применение всех фильтров в saved-session и file-mode (file-курсор `EventIndex + 1`), `filtered_count` на каждой странице, `total_count` только при отсутствии `after_id` (поле опускается в JSON на продолжениях), `has_more`/`next_after_id` в ответе. Проверка: parity-тесты saved/file в `runtime_test.go`
- [x] 2.4 File-mode фильтры в памяти: множественные SPID (источник — `Columns[12]`), множественные event names, временной полуинтервал по `start_time`, `min_duration_ms`. Проверка: unit-тесты фильтрации на синтетическом наборе событий

## 3. Агрегация процедур: фильтры, top, сортировка, группировка

- [x] 3.1 Расширить серверную агрегацию `LoadProceduresAggregated` (`internal/trc/store.go`): параметры `spids`, `event_names` (default `SP:Completed`), `group_by_spid` (`GROUP BY spid, procedure` + `WHERE spid IS NOT NULL`), `sort_by` с secondary `procedure, spid`, `top` при > 0; пустое имя процедуры исключается. Проверка: `store_integration_test.go` — агрегаты совпадают с контрольным `GROUP BY`
- [x] 3.2 Расширить `TRCProcAgg` полем `SPID int` (`json:"spid,omitempty"`) и `AggregateByProcedure` (`internal/trc/aggregate.go`) теми же правилами для file-mode. Проверка: unit-тесты в `aggregate_test.go` — группировка `(spid, procedure)`, NULL-spid исключение, top/sort, детерминированные tie-breakers
- [x] 3.3 Оркестрация `ExecuteProcedures` (`internal/trcsvc/runtime.go`): передача новых параметров, enrichment сохраняется для обоих режимов. Проверка: unit-тест parity метрик saved/file

## 4. CLI/MCP контракты

- [x] 4.1 Добавить helpers `optionalIntSlice`/`optionalStringSlice` в `internal/mcp` (только массивы соответствующего типа, строгая валидация элементов, ошибка с именем аргумента и индексом). Проверка: unit-тесты helpers в `server_test.go`
- [x] 4.2 Расширить схему и handler `codebase_trc_events` (`internal/mcp/registry.go`): `spids[]`, `event_names[]`, `time_from`, `time_to`, `min_duration_ms`, `after_id`, `format`; legacy `spid`/`event_name` нормализуются в массивы, scalar+array — ошибка. Проверка: тесты схемы и handler в `server_test.go`
- [x] 4.3 Расширить схему и handler `codebase_trc_procedures`: `spids[]`, `event_names[]`, `top`, `sort_by`, `group_by_spid` (без scalar-алиасов). Проверка: тесты схемы в `server_test.go`
- [x] 4.4 Добавить CLI-флаги (`cmd/trc.go`): `trc events` — `--spids`, `--event-names`, `--after-id`, `--time-from`, `--time-to`, `--min-duration-ms`, `--format`; `trc procedures` — `--spids`, `--event-names`, `--top`, `--sort`, `--group-by-spid`; CSV-списки валидируются (имя флага + некорректное значение в ошибке), `--spid`+`--spids` — ошибка, новый singular `--event-name` не добавляется. Проверка: тесты парсинга в `cmd/trc_test.go`

## 5. Индекс

- [x] 5.1 В `internal/store/db_schema.go`: создать `idx_trc_events_session_spid_id (session_id, spid, id)`, добавить `idx_trc_events_session_spid` в legacy-drop; проверить `EXPLAIN (ANALYZE, BUFFERS)` пагинации по одному и нескольким SPID на сессии ≥ 500K событий (индекс используется либо измерения подтверждают выигрыш). Проверка: `go test -tags=integration ./internal/store -count=1`

## 6. Регрессия и документация

- [x] 6.1 Прогнать полный набор: `go build ./...`, `go test ./internal/trc ./internal/trcsvc ./internal/mcp ./cmd -count=1`, `go test -tags=integration ./internal/trc -count=1`; убедиться, что вызовы без новых параметров возвращают прежний результат (`top=0` — все процедуры, `event_names` отсутствует — `SP:Completed`, `total_count` на первой странице)
- [x] 6.2 Обновить README: новые флаги `trc events`/`trc procedures`, параметры MCP-инструментов, семантика `after_id`/`has_more`/`next_after_id` и `total_count` только на первой странице

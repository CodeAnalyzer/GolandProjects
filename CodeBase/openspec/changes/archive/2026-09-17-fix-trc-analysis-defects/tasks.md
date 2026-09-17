## 1. Корректная агрегация процедур

- [x] 1.1 Добавить в `internal/trc/aggregate_test.go` regression-тесты для смешанных `SP:Completed`/statement events и длительностей `[0, 100]`, реализовать completed-only фильтрацию и единую формулу Go-метрик; проверить целевые тесты командой `go test ./internal/trc -run "TestAggregateByProcedure" -count=1`.
- [x] 1.2 Обновить `LoadProceduresAggregated` для `event_name='SP:Completed'` и включения нулей в min/avg, добавить SQL integration-тест parity метрик и отсутствия statement-задвоения; проверить командой `go test ./internal/trc -run "Test.*ProceduresAggregated" -count=1` при доступной тестовой БД.

## 2. Точные счётчики выборки событий

- [x] 2.1 Выделить общий SQL-предикат существующего `TRCEventFilter`, добавить server-side подсчёт полного числа совпадений и integration-тест совпадения count с тем же фильтром при `limit` меньше результата; проверить командой `go test ./internal/trc -run "Test.*EventsFiltered|Test.*EventCount" -count=1`.
- [x] 2.2 Добавить `returned_count` в `EventsResult`, обновить saved-session и file-mode так, чтобы `filtered_count` вычислялся до limit, ошибки count-запросов не игнорировались, а file-mode продолжал подсчёт после заполнения результата; проверить сценарии без фильтра, с фильтром, пустым результатом и превышением limit командой `go test ./internal/trcsvc -run "TestExecuteEvents" -count=1`.
- [x] 2.3 Обновить текстовый вывод `trc events`, чтобы отдельно показывать возвращённое и полное число совпадений, и проверить формат существующим либо новым тестом пакета `cmd` через `go test ./cmd -run "Test.*TRCEvents" -count=1`.

## 3. Полное enrichment агрегатов

- [x] 3.1 Выделить в `internal/trc/enrich.go` enrichment уникального списка имён с переиспользованием текущего worker pool, перевести `EnrichEvents` на новый helper и добавить unit-тесты дедупликации, найденной и отсутствующей процедуры; проверить командой `go test ./internal/trc -run "TestEnrich" -count=1`.
- [x] 3.2 Перевести оба пути `ExecuteProcedures` на enrichment имён итоговых агрегатов и удалить saved-session sample первых 1000 событий; добавить тест, где процедура агрегата отсутствует в первой тысяче событий, и проверить командой `go test ./internal/trcsvc -run "TestExecuteProcedures" -count=1`.

## 4. Валидация TRC MCP-аргументов

- [x] 4.1 Усилить числовые optional helpers: отвергать неверный тип, дробное JSON-число и переполнение без усечения; добавить unit-тесты корректных integer-представлений и каждой ошибки командой `go test ./internal/mcp -run "TestOptionalInt|TestOptionalInt64" -count=1`.
- [x] 4.2 Проверить ошибки каждого optional-аргумента во всех существующих `codebase_trc_*` handlers, заменить молчаливый `optionalLimit` в TRC handlers на error-returning разбор и добавить табличные handler-тесты, подтверждающие ошибку до обращения к сервису; проверить командой `go test ./internal/mcp -run "TestTRC.*Argument|TestTRC.*Handler" -count=1`.
- [x] 4.3 Обновить описания `codebase_trc_events`, `codebase_trc_procedures`, CLI help и TRC-раздел README: зафиксировать `filtered_count`/`returned_count` и агрегацию только `SP:Completed`; проверить отсутствующие старые формулировки поиском по изменённым файлам и командой `go test ./internal/mcp ./cmd -count=1`.

## 5. Итоговая проверка

- [x] 5.1 Выполнить регрессионные тесты затронутых пакетов `go test ./internal/trc ./internal/trcsvc ./internal/mcp ./cmd -count=1` и устранить все новые сбои.
- [x] 5.2 Выполнить `go build ./...` и подтвердить успешную сборку без изменения схемы БД, парсеров и новых зависимостей.
- [x] 5.3 Выполнить `openspec validate fix-trc-analysis-defects --strict` и подтвердить валидность proposal, design, tasks и обеих delta-spec.

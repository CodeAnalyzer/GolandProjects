## 1. Снапшот статистики (A2)

- [x] 1.1 Добавить таблицу `stats_snapshot` в `InitSchema` (`internal/store/db_schema.go`) с одной актуальной строкой (счётчики + `created_at`); проверить integration-тестом, что `InitSchema` создаёт таблицу
- [x] 1.2 Реализовать в store запись и чтение снапшота (`SaveStatsSnapshot`/`LoadStatsSnapshot`) с привязкой к поколению LSA; проверить integration-тестом перезапись одной строки
- [x] 1.3 Переписать `GetStats` (`internal/store/db_stats.go:13`): читать снапшот при совпадении поколения LSA; иначе — живой подсчёт (фолбэк) и сохранить результат в `stats_snapshot` с запрошенным поколением; проверить тестом оба пути (включая смену поколения)
- [x] 1.4 Добавить `systemsvc.RefreshStats(db)` (резолв LSA-поколения + запись снапшота) и вызвать по завершении `init` (`cmd/init.go`) и `update` (`cmd/update.go`); при ошибке записи — предупреждение, индексация не падает
- [x] 1.5 Проверить, что `systemsvc.ExecuteStats` (CLI + MCP) возвращает те же поля из снапшота; существующие тесты `cmd`/`systemsvc` зелёные

## 2. `query procedure`: «не найдено» — пустой результат (P7)

- [x] 2.1 В CLI `query procedure` (`cmd/query_commands.go:133`) перехватывать `errors.Is(err, sql.ErrNoRows)` и возвращать пустой срез `[]SQLProcedureResult{}`; контракт `GetProcedureResult` для RTI/TRC не менять
- [x] 2.2 В MCP `codebase_query_procedure` (`internal/mcp/registry.go:447`) аналогично перехватывать `sql.ErrNoRows` и возвращать пустой результат
- [x] 2.3 Тесты: несуществующая процедура даёт `count: 0`, `items: []` и не даёт `sql: no rows in result set` (CLI + MCP); существующая — как раньше

## 3. Логирование аргументов MCP (A1)

- [x] 3.1 Переписать `formatToolArgs` (`internal/mcp/server.go:189`): все аргументы, сортировка по ключу, формат `key:value` через запятую, маскировка значений `text`/`sql`; пустой набор — `args=-`
- [x] 3.2 Тесты `formatToolArgs`: детерминированный многоаргументный вывод, маскировка `text`/`sql`, пустой набор; обновить `profiles_test.go` при необходимости

## 4. Верификация

- [x] 4.1 `go test ./...` и `go vet ./...` — без ошибок
- [x] 4.2 Замер `codebase stats` до/после на реальном индексе: после `init`/`update` (снапшот) — сотни ms вместо ~14.8s; фолбэк без снапшота работает
- [x] 4.3 Ручная проверка: `query procedure --name <несуществующая> --json` → `count: 0`, `items: []`; `codebase_query_procedure` — успех без error
- [x] 4.4 Ручная проверка формата лога MCP: вызов с несколькими аргументами даёт `args=k1:v1,k2:v2`, `text`/`sql` замаскированы
- [x] 4.5 `openspec validate harden-stats-errors-and-logging --strict` — без ошибок

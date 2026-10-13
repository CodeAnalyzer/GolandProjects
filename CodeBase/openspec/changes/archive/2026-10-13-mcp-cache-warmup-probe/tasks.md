# Задачи: warm-up probe при старте MCP

## 1. Конфигурация

- [x] 1.1 В `MCPConfig` (`internal/config/config.go`) добавить поля `WarmupEnabled *bool` (toml: `warmup_enabled`) и `WarmupTimeoutSec *int` (toml: `warmup_timeout_sec`); в `applyDefaults` задать дефолты (enabled=true, 60 с). Проверка: `go build ./...`
- [x] 1.2 В валидацию конфигурации (по образцу `query_timeout_sec`, `config.go:370`) добавить проверку `warmup_timeout_sec >= 0` с ошибкой, упоминающей имя параметра. Проверка: тест в `internal/config/config_timeout_test.go` (отрицательное значение → ошибка; nil → дефолт; явный 0 сохраняется)

## 2. Оркестратор проб

- [x] 2.1 Создать `internal/mcp/warmup.go`: тип `warmupProbe{tool string; args map[string]interface{}}`, таблицу `warmupProbesByProfile` (только `query`, 9 проб в порядке из design.md п.8; args проходят required-валидацию хендлеров, имя callers = `FCD_GL_Audit_Insert`) и `warmupForProfile(ctx, profile, registry, logger, timeoutSec)`: no-op при отсутствии проб/инструмента в registry; `context.WithTimeout` при timeout > 0; последовательный вызов `rt.Handler(ctx, args)` с игнорированием ошибок; итоговая строка лога `profile=<p> warmup probes=N done=M elapsed=... errors=K`. Проверка: `go build ./...`
- [x] 2.2 Создать `internal/mcp/warmup_test.go`: (а) no-op для профиля без проб (ни одного вызова хендлера), (б) ошибки хендлеров не прерывают пачку и учитываются в errors, (в) таймаут пачки прерывает оставшиеся пробы (хендлер-заглушка со sleep), (г) logger == nil — функция не вызывается/не падает, (д) отсутствующий в registry инструмент пропускается. Проверка: `go test ./internal/mcp/... -count=1`

## 3. Запуск в RunStdio

- [x] 3.1 В `internal/mcp/server.go` (`RunStdio`, после `buildToolRegistryForProfile`, до `server.Run`) добавить асинхронный запуск: `if logger != nil && warmupEnabled(cfg) { go warmupForProfile(...) }` (nil → дефолт true; таймаут через `secOrDefault(cfg.MCP.WarmupTimeoutSec, 60)`). Комментарий фиксирует решение «без логгера probe не запускается». Проверка: `go build ./...` + существующие тесты `internal/mcp` проходят
- [x] 3.2 Убедиться, что существующие тесты логирования/хендшейка (`server_test.go`, `profiles_integration_test.go`) не сломались от одной дополнительной лог-строки (при необходимости — фильтр по формату warmup-строки). Проверка: `go test ./internal/mcp/... -count=1`

## 4. Верификация и регресс

- [x] 4.1 `go vet ./...` и `go test ./... -count=1` — чисто
- [x] 4.2 Smoke на живой БД FA (профиль query, лог включён): в логе присутствует строка `warmup probes=9 ...`; первые вызовы `desc_search`/`spec_search`/`callers` после старта существенно дешевле холодных из `codebase_20261011.log` (порядка тёплых: ~1.5 с / ~6 с / десятки мс)
- [x] 4.3 Отключаемость: конфиг с `warmup_enabled = false` → warmup-строки в логе нет, поведение прежнее
- [x] 4.4 Обновить исходное предложение `Modifications/mcp-cache-warmup-probe-b7e4d1.md` фактами исследования (Q1–Q4, shared_buffers, порядок проб, имя пробы, отказ от pg_prewarm больших индексов) и пометкой «реализовано в change mcp-cache-warmup-probe»
- [x] 4.5 `openspec validate mcp-cache-warmup-probe --strict` — без ошибок


## 1. Конфиг: добавление `ParseTimeoutSec` в RTIConfig и TRCConfig

- [x] 1.1 Добавить поле `ParseTimeoutSec int` в структуру `RTIConfig` в `internal/config/config.go` с тегом `toml:"parse_timeout_sec"`. Проверка: `go build ./internal/config/...` проходит.
- [x] 1.2 Добавить поле `ParseTimeoutSec int` в структуру `TRCConfig` в `internal/config/config.go` с тегом `toml:"parse_timeout_sec"`. Проверка: `go build ./internal/config/...` проходит.
- [x] 1.3 Добавить дефолт `ParseTimeoutSec = 300` для RTI в `Load()` (после блока дефолтов `cfg.RTI.TopSlowCount`). Проверка: конфиг без `[rti] parse_timeout_sec` даёт 300.
- [x] 1.4 Добавить дефолт `ParseTimeoutSec = 300` для TRC в `Load()` (после блока дефолтов `cfg.TRC.MinProcsForParallelEnrich`). Проверка: конфиг без `[trc] parse_timeout_sec` даёт 300.
- [x] 1.5 Добавить `ParseTimeoutSec: 300` в секции `RTI` и `TRC` в `CreateDefault()`. Проверка: `CreateDefault()` возвращает конфиг с `ParseTimeoutSec = 300` для обеих секций.

## 2. MCP-сервер: маршрутизация таймаута через switch

- [x] 2.1 Заменить блок `if tool.Definition.Name == "codebase_review_sql"` в `registerSDKCoreTools` (`internal/mcp/server.go:72-75`) на `switch`: `codebase_review_sql` → `cfg.MCP.ReviewTimeoutSec`, `codebase_trc_parse` → `cfg.TRC.ParseTimeoutSec`, `codebase_rti_parse` → `cfg.RTI.ParseTimeoutSec`, default → `cfg.MCP.QueryTimeoutSec`. Проверка: `go build ./internal/mcp/...` проходит.
- [x] 2.2 Добавить unit-тест в `internal/mcp/server_test.go`, проверяющий что `codebase_trc_parse` и `codebase_rti_parse` получают `ParseTimeoutSec` из конфига TRC/RTI соответственно, а non-parse инструменты получают `QueryTimeoutSec`. Проверка: `go test ./internal/mcp/...` проходит.

## 3. codebase.toml: добавление новых параметров

- [x] 3.1 Добавить `parse_timeout_sec = 300` с комментарием в секцию `[rti]` в `codebase.toml`. Проверка: файл парсится корректно через `go run . config` или ручную проверку.
- [x] 3.2 Добавить `parse_timeout_sec = 300` с комментарием в секцию `[trc]` в `codebase.toml`. Проверка: файл парсится корректно.

## 4. Верификация

- [x] 4.1 Выполнить `go build ./...` и проверить чистую компиляцию. Проверка: нет ошибок.
- [x] 4.2 Выполнить `go vet ./...` и проверить чистоту. Проверка: нет предупреждений.
- [x] 4.3 Выполнить `go test ./internal/config/... ./internal/mcp/... -count=1` и проверить что все тесты проходят.
- [x] 4.4 Выполнить `openspec validate --changes add-parse-timeout-config` и проверить что валидация проходит.

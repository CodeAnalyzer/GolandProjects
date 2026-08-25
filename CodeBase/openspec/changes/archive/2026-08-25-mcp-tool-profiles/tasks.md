# MCP Tool Profiles — Tasks

## 1. Profile definitions и фильтрация registry

- [x] 1.1 В `internal/mcp/registry.go` добавить `baseTools` — map с именами базовых инструментов (`codebase_ping`, `codebase_health`, `codebase_stats`, `codebase_read_more`)
- [x] 1.2 В `internal/mcp/registry.go` добавить `profileToolSets` — map профиль → whitelist имён инструментов (query, rti, trc, review)
- [x] 1.3 В `internal/mcp/registry.go` добавить функцию `buildToolRegistryForProfile(db *store.DB, profile string) (map[string]registeredTool, error)` — при пустом profile возвращает `buildToolRegistry(db)`, при известном — фильтрует по whitelist, при неизвестном — ошибка
- [x] 1.4 Добавить helper `mergeMaps(a, b map[string]bool) map[string]bool` для объединения baseTools с профильным набором

## 2. Интеграция в server.go

- [x] 2.1 Изменить сигнатуру `RunStdio(serverVersion string, logger *log.Logger)` → `RunStdio(serverVersion string, profile string, logger *log.Logger)`
- [x] 2.2 Заменить вызов `buildToolRegistry(db)` на `buildToolRegistryForProfile(db, profile)` в `RunStdio`
- [x] 2.3 Передать `profile` в `registerSDKCoreTools` для использования в логировании
- [x] 2.4 Добавить `profile` в `logMCPToolCall` — поле `profile=%s` в формате записи лога

## 3. CLI команда mcp

- [x] 3.1 В `cmd/mcp.go` добавить строковый флаг `--profile` (переменная `mcpProfile`, default пустая строка)
- [x] 3.2 Передать `mcpProfile` в `mcp.RunStdio(version, mcpProfile, commandLogger)`
- [x] 3.3 Обновить `Short`/`Long` описание команды `mcp` с упоминанием `--profile`

## 4. Логирование

- [x] 4.1 В `logMCPToolCall` добавить параметр `profile string`
- [x] 4.2 В формат строки лога добавить `profile=%s` (при пустом profile — `profile=all`)
- [x] 4.3 Обновить все вызовы `logMCPToolCall` в `registerSDKCoreTools`

## 5. Тесты

- [x] 5.1 Тест: `buildToolRegistryForProfile` с пустым profile → все инструменты
- [x] 5.2 Тест: `buildToolRegistryForProfile` с `profile=rti` → только base + rti инструменты
- [x] 5.3 Тест: `buildToolRegistryForProfile` с `profile=query` → только base + query инструменты
- [x] 5.4 Тест: `buildToolRegistryForProfile` с `profile=trc` → только base + trc инструменты
- [x] 5.5 Тест: `buildToolRegistryForProfile` с `profile=review` → только base + review инструменты
- [x] 5.6 Тест: `buildToolRegistryForProfile` с неизвестным profile → ошибка
- [x] 5.7 Тест: все инструменты из `buildToolRegistry` присутствуют хотя бы в одном профиле
- [x] 5.8 Тест: `logMCPToolCall` с profile пишет `profile=rti` в запись лога

## 6. Сборка и верификация

- [x] 6.1 `go build ./...` — без ошибок
- [x] 6.2 `go vet ./...` — без ошибок
- [x] 6.3 `go test ./internal/mcp/...` — все тесты PASS
- [x] 6.4 Проверка размера `tools/list` для каждого профиля

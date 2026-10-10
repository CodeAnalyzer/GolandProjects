# Tasks: fix-timeout-zero-unreachable

## 1. Конфиг: типы полей и Load()

- [x] 1.1 Перевести `RTIConfig.ParseTimeoutSec` и `TRCConfig.ParseTimeoutSec` на `*int` в `internal/config/config.go`; обновить комментарии полей на «nil = 300, явный 0 = без таймаута». Проверка: `go build ./internal/config/...` проходит.
- [x] 1.2 Перевести `MCPConfig.QueryTimeoutSec` и `MCPConfig.ReviewTimeoutSec` на `*int`; комментарии полей «nil = 30/120, явный 0 = без таймаута». Проверка: `go build ./internal/config/...` проходит.
- [x] 1.3 В `Load()` удалить 4 блока нормализации `<= 0` → дефолт (`cfg.RTI.ParseTimeoutSec`, `cfg.TRC.ParseTimeoutSec`, `cfg.MCP.QueryTimeoutSec`, `cfg.MCP.ReviewTimeoutSec`); вместо них добавить валидацию явных отрицательных значений по образцу `spec.lsa_retrain_threshold` (`config.go:388–390`): ошибки вида `rti.parse_timeout_sec must be >= 0, got %d` (аналогично trc.parse_timeout_sec, mcp.query_timeout_sec, mcp.review_timeout_sec). Проверка: конфиг с `parse_timeout_sec = -5` даёт ошибку Load с именем параметра.
- [x] 1.4 В `CreateDefault()` инициализировать 4 поля явными дефолтами через указатели (`intPtr(300)` и т.д.). Проверка: `TestCreateDefault` проходит, поля не nil.

## 2. Потребитель: timeoutForTool

- [x] 2.1 В `internal/mcp/server.go` (`timeoutForTool`) разрешать `nil` в дефолт (300/300/30/120), явный `0` возвращать как 0 (без таймаута). Проверка: `go build ./internal/mcp/...` проходит.
- [x] 2.2 Обновить литералы `config.Config` в `internal/mcp/server_test.go` (тесты `TestTimeoutForTool_*`, строки ~676–752) на указатели — локальный хелпер `intSec(n int) *int`, т.к. `config.intPtr` непроэкспортирован. Существующие 5 тестов сохраняются как defensive-контракт. Проверка: `go test ./internal/mcp/ -run TestTimeoutForTool` проходит.

## 3. Тесты три-состоятельной семантики (TOML → Load)

- [x] 3.1 Добавить тест «явный 0 сохраняется после Load» для всех 4 параметров: TOML с `[rti] parse_timeout_sec = 0`, `[trc] parse_timeout_sec = 0`, `[mcp] query_timeout_sec = 0, review_timeout_sec = 0` → после `Load()` поля не nil и `*field == 0` (методология `writeTestConfig` из `config_spec_test.go`; при необходимости обобщить хелпер на произвольные секции — сейчас он уже принимает строку секции). Проверка: новый тест проходит.
- [x] 3.2 Добавить тест «отсутствие параметра → nil → дефолт в timeoutForTool»: TOML без таймаут-ключей → `Load()` → `timeoutForTool` для trc/rti/review/default возвращает 300/300/120/30с. Проверка: новый тест проходит.
- [x] 3.3 Добавить тест «отрицательное значение отклоняется» для каждого из 4 параметров (4 кейса) — ошибка Load содержит имя параметра (образец: `TestSpecDefaults_NegativeThresholdRejected`). Проверка: новый тест проходит.

## 4. Интеграционная проверка и валидация

- [x] 4.1 Прогнать пакеты целиком: `go test ./internal/config/... ./internal/mcp/...` — все тесты зелёные, включая существующие `TestLoadAppliesDefaults` (таймаут-поля не проверяет — менять не должен) и `TestSaveWritesCurrentConfig`.
- [x] 4.2 Ручная проверка end-to-end: временный `codebase.toml` с `[trc] parse_timeout_sec = 0` → `go run . mcp` стартует без ошибки конфига (запрос к БД не требуется для проверки загрузки — достаточно отсутствия ошибки config load; допустимо проверить через любую CLI-команду с `--config` и `--json`, ожидающую только загрузки конфига). Проверка: явный 0 не превращается в 300 (нет ошибки, поведение «конфиг принят»).
- [x] 4.3 `go vet ./internal/config/... ./internal/mcp/...` и `openspec validate fix-timeout-zero-unreachable` — без ошибок.

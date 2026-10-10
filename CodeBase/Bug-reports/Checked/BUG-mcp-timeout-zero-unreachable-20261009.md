# Bug Report: «timeout = 0 отключает таймаут» недостижимо через codebase.toml — Load() нормализует 0 в дефолт

**Дата:** 2026-10-09
**Файл:** `internal/config/config.go` (`Load`, строки ~328–341, ~366–370); спеки: `openspec/specs/infrastructure/configuration/spec.md:57`, `openspec/specs/mcp-server/mcp-transport-tools/spec.md:208,240–244`
**Версия CodeBase:** 0.9.2
**Статус:** Исправлено (в рамках \openspec\changes\archive\2026-10-10-fix-timeout-zero-unreachable)

## Summary

Две спеки документируют возможность отключить per-tool таймаут MCP-инструментов значением `0`:

- `infrastructure/configuration` (строка 57): «`parse_timeout_sec` (по умолчанию 300) …; `0` означает отсутствие таймаута»;
- `mcp-server/mcp-transport-tools` («Таймауты tool-вызовов», строка 208): «При `timeout = 0` таймаут не применяется (вызов выполняется без ограничения)» + сценарий «Таймаут отключён» (строки 240–244): `GIVEN` конфигурация с `[trc] parse_timeout_sec = 0` → `THEN` `context.WithTimeout` не создаётся.

Реально сценарий **недостижим**: `Load()` нормализует `<= 0` в значения по умолчанию, поэтому `parse_timeout_sec = 0` в `codebase.toml` превращается в `300`, и таймаут **создаётся**. Механизм отключения при этом в коде есть и покрыт тестом — но тест конструирует конфиг-структуру напрямую, минуя `Load()`, то есть проверяет путь, который через конфигурационный файл не воспроизводится.

Критичность: **MEDIUM** (документированное поведение противоположно фактическому; затрагивает 4 параметра в двух спеках).

---

## Environment

- CodeBase 0.9.2 (build 1539), Go 1.25
- MCP-сервер (`codebase mcp`), конфигурация `codebase.toml`

---

## Reproduction Steps

1. В `codebase.toml` установить:
   ```toml
   [trc]
   parse_timeout_sec = 0
   ```
2. Запустить `codebase mcp` и вызвать `codebase_trc_parse` с большим файлом.
3. Наблюдать: спустя 300 секунд вызов прерывается по таймауту, в логе `tool=codebase_trc_parse ... status=error` — таймаут применён, хотя спека обещает «вызов выполняется без ограничения».

Аналогично для `[rti] parse_timeout_sec = 0`, `[mcp] query_timeout_sec = 0`, `[mcp] review_timeout_sec = 0`.

## Expected Result

При `parse_timeout_sec = 0` (и аналогичных) `context.WithTimeout` не создаётся — вызов выполняется без ограничения по времени (сценарий «Таймаут отключён» из `mcp-transport-tools`).

## Actual Result

`Load()` переписывает `0` → дефолт (300/30/120). Отключение таймаута конфигурацией невозможно ни в одной секции.

---

## Root Cause Analysis

### 1. Нормализация в `Load()` затирает семантику нуля

`internal/config/config.go`, `Load()`:

```go
if cfg.RTI.ParseTimeoutSec <= 0 {
    cfg.RTI.ParseTimeoutSec = 300
}
...
if cfg.TRC.ParseTimeoutSec <= 0 {
    cfg.TRC.ParseTimeoutSec = 300
}
...
if cfg.MCP.QueryTimeoutSec <= 0 {
    cfg.MCP.QueryTimeoutSec = 30
}
if cfg.MCP.ReviewTimeoutSec <= 0 {
    cfg.MCP.ReviewTimeoutSec = 120
}
```

Те же значения дублируются в `CreateDefault()` (строки ~494–512). Отсутствие параметра в TOML и явный `0` неразличимы (оба дают `0`), поэтому «явный ноль = отключить» выразить нечем.

### 2. Потребитель ноль поддерживает

`internal/mcp/server.go`, `timeoutForTool` (строки 108–119) возвращает длительность из конфига; `registerSDKCoreTools` (строки 88–94) создаёт `context.WithTimeout` только при `timeout > 0`:

```go
timeout := timeoutForTool(tool.Definition.Name, cfg)
if timeout > 0 {
    ctx, cancel = context.WithTimeout(ctx, timeout)
    ...
}
```

То есть «0 = без таймаута» реализовано на уровне потребителя — оно просто не может доехать туда из конфига.

### 3. Тест проверяет недостижимый путь

`internal/mcp/server_test.go:735` — `TestTimeoutForTool_ZeroDisablesTimeout`:

```go
cfg := &config.Config{
    MCP: config.MCPConfig{QueryTimeoutSec: 0, ...},
    TRC: config.TRCConfig{ParseTimeoutSec: 0},
    ...
}
```

Конфиг собран литералом в тесте, `Load()` не вызывается — тест зелёный, а документированный сценарий через `codebase.toml` падает. Это типичный разрыв «unit-тест на структуре ≠ контракт конфигурационного файла».

---

## Impact

- **MEDIUM:** документированное в двух спеках поведение («0 = без таймаута») недостижимо; пользователь, рассчитывающий на неограниченный парсинг огромного `.trc`, получает обрыв через 300 секунд с невнятной диагностикой.
- Обратный риск ниже, но тоже есть: правка «в лоб» (убрать из спек) закрепит невозможность отключения таймаута, при том что механизм в `timeoutForTool` уже написан и протестирован.
- Затронуты 4 параметра: `RTIConfig.ParseTimeoutSec`, `TRCConfig.ParseTimeoutSec`, `MCPConfig.QueryTimeoutSec`, `MCPConfig.ReviewTimeoutSec`.

---

## Suggested Fix

### Вариант A (рекомендуется): указатель вместо int — `nil` = дефолт, `0` = отключено

1. `internal/config/config.go` — перевести 4 поля на `*int`:
   ```go
   ParseTimeoutSec *int `toml:"parse_timeout_sec"`
   ```
2. `Load()`: `nil` → дефолт (300/30/120); явный `0` сохраняется как «отключено».
3. `CreateDefault()` — инициализировать поля дефолтами (не `nil`).
4. `internal/mcp/server.go`, `timeoutForTool` — разыменование с nil-чеком (nil → дефолт, 0 → 0). Потребитель один — `timeoutForTool`, других чтений этих полей в проекте нет.
5. Тест: в `config_test.go` добавить `TestLoad_ParseTimeoutZeroDisablesTimeout` — конфиг-файл с `parse_timeout_sec = 0` → после `Load()` значение остаётся 0.

### Вариант B (минимальный): правка спек под фактическое поведение

1. `infrastructure/configuration:57` — убрать «`0` означает отсутствие таймаута», заменить на «при значении `0` или отсутствии параметра применяется значение по умолчанию».
2. `mcp-transport-tools` — убрать предложение про `timeout = 0` (строка 208) и сценарий «Таймаут отключён» (строки 240–244), либо переформулировать его в терминах прямого конструирования конфига (что бессмысленно для пользователя).
3. `TestTimeoutForTool_ZeroDisablesTimeout` сохранить как defensive-тест внутреннего контракта `timeoutForTool`.

Вариант A предпочтителен: семантика «0 = без ограничения» полезна для разового парсинга гигантских трейсов и уже реализована в потребителе; правка спек навсегда её хоронит.

### Файлы для изменения (вариант A)

1. **`internal/config/config.go`** — `RTIConfig`/`TRCConfig`/`MCPConfig` (строки ~57–80): поля → `*int`; `Load()` (строки ~328–370): нормализация nil→дефолт; `CreateDefault()`.
2. **`internal/mcp/server.go`** — `timeoutForTool` (строки 108–119): nil-safe разыменование.
3. **`internal/config/config_test.go`** — сценарий «0 из файла доезжает до конфига как 0».
4. **Спеки не меняются** (вариант A делает их правдивыми).

## MODIFIED Requirements

### Requirement: Динамическое применение TTL из конфига

Система SHALL применять TTL из секции `[mcp] pagination_ttl` конфигурации: bootstrap CLI (`cmd/root.go`) вызывает `SetPaginationTTL(d)` при загрузке конфигурации, до входа в `RunStdio` (который затем инициализирует `globalPages` и запускает фоновый GC). Значение `<= 0` игнорируется (остаётся прежний TTL). `SetPaginationTTL` меняет пакетную переменную `paginationTTL`, которую используют и фоновый GC (интервал пересчитывается как `TTL / 2`), и `gc()` (cutoff = now − TTL).

#### Scenario: Кастомный TTL из конфига

- **GIVEN** конфигурация с `[mcp] pagination_ttl = "5m"`
- **WHEN** bootstrap CLI (`cmd/root.go`) вызывает `SetPaginationTTL(5 * time.Minute)` при загрузке конфигурации, затем `RunStdio` инициализирует `globalPages` и запускает фоновый GC
- **THEN** фоновый GC запускается с интервалом 2.5 минуты
- **AND** записи старше 5 минут считаются просроченными

#### Scenario: Некорректный TTL игнорируется

- **GIVEN** конфигурация с `pagination_ttl = "0"` или невалидным значением
- **WHEN** вызывается `SetPaginationTTL(0)`
- **THEN** TTL остаётся прежним (по умолчанию 15 минут), новое значение не применяется

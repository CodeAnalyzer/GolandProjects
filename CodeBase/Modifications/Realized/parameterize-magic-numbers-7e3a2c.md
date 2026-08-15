# Параметризация магических чисел в конфигурацию

Вынос жёстко закодированных числовых констант из кода в `codebase.toml` для гибкой настройки без перекомпиляции. Затрагивает пул соединений БД, batch-размеры для TRC/RTI, лимиты запросов, пороги "медленности", TTL пагинации MCP, интервал прогресс-репортера и лимиты enrich-воркеров.

---

## Контекст проблемы

В коде разбросано ~8 категорий магических чисел, влияющих на производительность, потребление памяти и поведение запросов. Они захардкожены как `const` или inline-литералы, хотя естественным местом для них является `codebase.toml`. Пользователь уже параметризовал `pagination_chunk_size` и `batch_size` — остальные числа остались в коде.

| # | Значение | Где | Влияние |
|---|----------|-----|---------|
| 1 | `25` / `5m` | `store/db.go:120-122` | Пул коннектов БД (max open/idle, conn lifetime) |
| 2 | `50000` | `trc/store.go:55,320`, `trc/parse_to_db.go:15`, `rti/store.go:626` | Batch insert/delete размер для TRC/RTI |
| 3 | `100` / `1000` | ~30 мест в `cmd/rti.go`, `cmd/trc.go`, `rti/store.go`, `trc/store.go`, `mcp/registry.go` | Default/max limit запросов |
| 4 | `100` мс / `0.1` сек | `rti/parser.go:487`, `rti/parser_client.go:318`, `mcp/registry.go:762,1518` | Порог "медленности" для RTI/TRC |
| 5 | `10` | `rti/parser.go:488`, `rti/parser_client.go:326` | Top-N медленных вызовов в summary |
| 6 | `15m` | `mcp/pagination.go:14` | TTL пагинированных MCP-ответов |
| 7 | `250ms` | `indexer/indexer.go:1648` | Интервал вывода прогресса в консоль |
| 8 | `16` / `16` | `trc/enrich.go:52,57` | Лимит enrich-воркеров, порог параллельности |

---

## Затронутые файлы

- `internal/config/config.go` — новые поля в структурах + дефолты
- `internal/config/config_test.go` — обновление тестов дефолтов
- `internal/store/db.go` — чтение пула коннектов из конфига
- `internal/trc/store.go` — batch-размер из конфига
- `internal/trc/parse_to_db.go` — streamBatchSize из конфига
- `internal/trc/store_test.go` — обновление теста константы
- `internal/rti/store.go` — batch-размер из конфига
- `internal/rti/store_test.go` — обновление теста константы
- `internal/rti/parser.go` — slow threshold, top-N из конфига
- `internal/rti/parser_client.go` — client slow threshold, top-N из конфига
- `internal/mcp/pagination.go` — TTL из конфига
- `internal/mcp/registry.go` — default/max limit, slow threshold из конфига
- `internal/indexer/indexer.go` — progress interval из конфига
- `internal/trc/enrich.go` — enrich workers из конфига
- `codebase.toml` — новые секции и параметры

---

## План (8 шагов)

### Шаг 1: Пул соединений к БД — `max_open_conns`, `max_idle_conns`, `conn_max_lifetime`

**Файлы:** `internal/config/config.go`, `internal/store/db.go`, `codebase.toml`

**Конфиг:**
```toml
[database]
max_open_conns = 25
max_idle_conns = 25
conn_max_lifetime = "5m"
```

**Изменения:**

1. В `DBConfig` добавить поля:
   ```go
   MaxOpenConns    int    `toml:"max_open_conns"`
   MaxIdleConns    int    `toml:"max_idle_conns"`
   ConnMaxLifetime string `toml:"conn_max_lifetime"` // Go duration: "5m", "30s", "1h"
   ```

2. В `Load()` добавить дефолты:
   ```go
   if cfg.DB.MaxOpenConns <= 0 {
       cfg.DB.MaxOpenConns = 25
   }
   if cfg.DB.MaxIdleConns <= 0 {
       cfg.DB.MaxIdleConns = 25
   }
   if cfg.DB.ConnMaxLifetime == "" {
       cfg.DB.ConnMaxLifetime = "5m"
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   MaxOpenConns:    25,
   MaxIdleConns:    25,
   ConnMaxLifetime: "5m",
   ```

4. В `store/db.go:120-122` заменить:
   ```go
   db.SetMaxOpenConns(25)
   db.SetMaxIdleConns(25)
   db.SetConnMaxLifetime(5 * time.Minute)
   ```
   на:
   ```go
   db.SetMaxOpenConns(cfg.MaxOpenConns)
   db.SetMaxIdleConns(cfg.MaxIdleConns)
   lifetime, err := time.ParseDuration(cfg.ConnMaxLifetime)
   if err != nil || lifetime <= 0 {
       lifetime = 5 * time.Minute
   }
   db.SetConnMaxLifetime(lifetime)
   ```

**Проверка:**
```
go build ./internal/config/... ./internal/store/...
go test ./internal/config/... ./internal/store/...
```

---

### Шаг 2: Batch insert/delete размер для TRC/RTI — `batch_insert_size`

**Файлы:** `internal/config/config.go`, `internal/trc/store.go`, `internal/trc/parse_to_db.go`, `internal/rti/store.go`, `internal/trc/store_test.go`, `internal/rti/store_test.go`

**Конфиг:**
```toml
[indexer]
batch_insert_size = 50000  # Размер батча для TRC/RTI insert/delete (строк)
```

**Изменения:**

1. В `IndexerConfig` добавить поле:
   ```go
   BatchInsertSize int `toml:"batch_insert_size"`
   ```

2. В `Load()` добавить дефолт:
   ```go
   if cfg.Indexer.BatchInsertSize <= 0 {
       cfg.Indexer.BatchInsertSize = 50000
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   BatchInsertSize: 50000,
   ```

4. В `internal/trc/store.go` заменить `const batchInsertSize = 50000` и `const batchDeleteSize = 50000` — убрать константы, принимать параметр через функцию или пакетную переменную. Поскольку `trc/store.go` не имеет доступа к `config.Config` напрямую, добавить пакетную функцию:
   ```go
   // SetBatchSize устанавливает размер батча для insert/delete.
   // Вызывается из cmd при инициализации.
   var trcBatchSize = 50000 // дефолт

   func SetBatchSize(size int) {
       if size > 0 {
           trcBatchSize = size
       }
   }
   ```
   Заменить все использования `batchInsertSize` и `batchDeleteSize` на `trcBatchSize`.

5. Аналогично в `internal/rti/store.go` — убрать `const batchDeleteSize = 50000`, добавить `SetBatchSize`.

6. В `internal/trc/parse_to_db.go` — убрать `const streamBatchSize = 50000`, использовать `trcBatchSize` из `store.go`.

7. В `cmd/root.go` (или `cmd/init.go`) после `config.Load()` вызывать:
   ```go
   trc.SetBatchSize(cfg.Indexer.BatchInsertSize)
   rti.SetBatchSize(cfg.Indexer.BatchInsertSize)
   ```

8. Обновить тесты: `TestBatchDeleteSizeConstant` → `TestSetBatchSize` (проверка что `SetBatchSize` меняет значение).

**Проверка:**
```
go build ./...
go test ./internal/trc/... ./internal/rti/... ./internal/config/...
```

---

### Шаг 3: Default/max limit для запросов — `default_query_limit`, `max_query_limit`

**Файлы:** `internal/config/config.go`, `internal/mcp/registry.go`, `cmd/rti.go`, `cmd/trc.go`, `internal/rti/store.go`, `internal/trc/store.go`

**Конфиг:**
```toml
[query]
default_limit = 100   # Лимит по умолчанию для query/rti/trc вызовов
max_limit = 1000      # Максимальный лимит (hard cap)
```

**Изменения:**

1. Добавить новую структуру `QueryConfig`:
   ```go
   type QueryConfig struct {
       DefaultLimit int `toml:"default_limit"`
       MaxLimit     int `toml:"max_limit"`
   }
   ```
   Добавить поле `Query QueryConfig `toml:"query"`` в `Config`.

2. В `Load()` добавить дефолты:
   ```go
   if cfg.Query.DefaultLimit <= 0 {
       cfg.Query.DefaultLimit = 100
   }
   if cfg.Query.MaxLimit <= 0 {
       cfg.Query.MaxLimit = 1000
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   Query: QueryConfig{
       DefaultLimit: 100,
       MaxLimit:     1000,
   },
   ```

4. Создать пакетную переменную в `internal/mcp/registry.go`:
   ```go
   var queryDefaultLimit = 100
   var queryMaxLimit = 1000

   func SetQueryLimits(defaultLimit, maxLimit int) {
       if defaultLimit > 0 { queryDefaultLimit = defaultLimit }
       if maxLimit > 0 { queryMaxLimit = maxLimit }
   }
   ```
   Заменить `const defaultLimit = 100` и все inline `limit = 100` / `limit = 1000` в `optionalLimit` и handler-функциях на `queryDefaultLimit` / `queryMaxLimit`.

5. В `internal/rti/store.go` и `internal/trc/store.go` — заменить inline `limit = 100` / `limit = 1000` на пакетные переменные, задаваемые через `SetQueryLimits` (или принимать как параметр от caller). Предпочтительный вариант: оставить clamping в store-функциях, но значения брать из пакетных переменных.

6. В `cmd/rti.go` и `cmd/trc.go` — заменить inline `limit = 100` / `limit = 1000` на `cfg.Query.DefaultLimit` / `cfg.Query.MaxLimit`.

7. В `cmd/root.go` после `config.Load()` вызывать:
   ```go
   mcp.SetQueryLimits(cfg.Query.DefaultLimit, cfg.Query.MaxLimit)
   ```

**Проверка:**
```
go build ./...
go test ./internal/mcp/... ./internal/config/...
```

---

### Шаг 4: Порог "медленности" для RTI/TRC — `slow_threshold_ms`

**Файлы:** `internal/config/config.go`, `internal/rti/parser.go`, `internal/rti/parser_client.go`, `internal/mcp/registry.go`

**Конфиг:**
```toml
[rti]
slow_threshold_ms = 100  # Порог медленности серверных вызовов (мс)

[trc]
slow_threshold_ms = 100  # Порог медленности событий (мс)
```

**Изменения:**

1. Добавить структуры:
   ```go
   type RTIConfig struct {
       SlowThresholdMs int `toml:"slow_threshold_ms"`
       TopSlowCount    int `toml:"top_slow_count"`
   }
   type TRCConfig struct {
       SlowThresholdMs int `toml:"slow_threshold_ms"`
   }
   ```
   Добавить поля `RTI RTIConfig `toml:"rti"`` и `TRC TRCConfig `toml:"trc"`` в `Config`.

2. В `Load()` добавить дефолты:
   ```go
   if cfg.RTI.SlowThresholdMs <= 0 {
       cfg.RTI.SlowThresholdMs = 100
   }
   if cfg.RTI.TopSlowCount <= 0 {
       cfg.RTI.TopSlowCount = 10
   }
   if cfg.TRC.SlowThresholdMs <= 0 {
       cfg.TRC.SlowThresholdMs = 100
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   RTI: RTIConfig{SlowThresholdMs: 100, TopSlowCount: 10},
   TRC: TRCConfig{SlowThresholdMs: 100},
   ```

4. В `internal/rti/parser.go` — заменить `countSlowCalls(allCalls, 100)` на пакетную переменную `rtiSlowThresholdMs` (дефолт 100, задаётся через `SetSlowThresholdMs`).

5. В `internal/rti/parser_client.go` — заменить `const clientSlowSQLThresholdSec = 0.1` на вычисление из `rtiSlowThresholdMs / 1000.0`.

6. В `internal/mcp/registry.go` — заменить inline `threshold = 100` (строки 762, 1518) на `rtiSlowThresholdMs` / `trcSlowThresholdMs`.

7. В `cmd/root.go` после `config.Load()`:
   ```go
   rti.SetSlowThresholdMs(cfg.RTI.SlowThresholdMs)
   rti.SetTopSlowCount(cfg.RTI.TopSlowCount)
   trc.SetSlowThresholdMs(cfg.TRC.SlowThresholdMs)
   ```

**Проверка:**
```
go build ./...
go test ./internal/rti/... ./internal/config/...
```

---

### Шаг 5: Top-N медленных вызовов в summary — `top_slow_count`

**Файлы:** `internal/config/config.go` (уже в шаге 4), `internal/rti/parser.go`, `internal/rti/parser_client.go`

**Изменения:**

1. В `internal/rti/parser.go` — заменить `topSlowCalls(allCalls, 10)` на `topSlowCalls(allCalls, rtiTopSlowCount)` с пакетной переменной:
   ```go
   var rtiTopSlowCount = 10
   func SetTopSlowCount(n int) { if n > 0 { rtiTopSlowCount = n } }
   ```

2. В `internal/rti/parser_client.go` — заменить `topSlowClientSQLEvents(events, 10)` на `topSlowClientSQLEvents(events, rtiTopSlowCount)`.

**Проверка:**
```
go build ./...
go test ./internal/rti/...
```

---

### Шаг 6: MCP pagination TTL — `pagination_ttl`

**Файлы:** `internal/config/config.go`, `internal/mcp/pagination.go`, `internal/mcp/server.go`

**Конфиг:**
```toml
[mcp]
pagination_ttl = "15m"  # Время жизни пагинированных ответов
```

**Изменения:**

1. В `MCPConfig` добавить поле:
   ```go
   PaginationTTL string `toml:"pagination_ttl"`
   ```

2. В `Load()` добавить дефолт:
   ```go
   if cfg.MCP.PaginationTTL == "" {
       cfg.MCP.PaginationTTL = "15m"
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   PaginationTTL: "15m",
   ```

4. В `internal/mcp/pagination.go` — убрать `const paginationTTL = 15 * time.Minute`, заменить на пакетную переменную:
   ```go
   var paginationTTL = 15 * time.Minute

   func SetPaginationTTL(d time.Duration) {
       if d > 0 { paginationTTL = d }
   }
   ```

5. В `internal/mcp/server.go` — в `RunStdio` после инициализации `globalPages` парсить TTL:
   ```go
   ttl, err := time.ParseDuration(cfg.MCP.PaginationTTL)
   if err != nil || ttl <= 0 {
       ttl = 15 * time.Minute
   }
   SetPaginationTTL(ttl)
   ```

**Проверка:**
```
go build ./...
go test ./internal/mcp/...
```

---

### Шаг 7: Интервал прогресс-репортера — `progress_interval_ms`

**Файлы:** `internal/config/config.go`, `internal/indexer/indexer.go`

**Конфиг:**
```toml
[indexer]
progress_interval_ms = 250  # Интервал вывода прогресса (мс)
```

**Изменения:**

1. В `IndexerConfig` добавить поле:
   ```go
   ProgressIntervalMs int `toml:"progress_interval_ms"`
   ```

2. В `Load()` добавить дефолт:
   ```go
   if cfg.Indexer.ProgressIntervalMs <= 0 {
       cfg.Indexer.ProgressIntervalMs = 250
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   ProgressIntervalMs: 250,
   ```

4. В `internal/indexer/indexer.go:1648` — заменить `time.NewTicker(250 * time.Millisecond)` на чтение из конфига. Поскольку `startProgressReporter` не имеет доступа к конфигу, добавить пакетную переменную:
   ```go
   var progressInterval = 250 * time.Millisecond

   func SetProgressInterval(ms int) {
       if ms > 0 { progressInterval = time.Duration(ms) * time.Millisecond }
   }
   ```
   Использовать `time.NewTicker(progressInterval)`.

5. В `cmd/root.go` после `config.Load()`:
   ```go
   indexer.SetProgressInterval(cfg.Indexer.ProgressIntervalMs)
   ```

**Проверка:**
```
go build ./...
go test ./internal/indexer/...
```

---

### Шаг 8: Enrich workers и порог параллельности — `max_enrich_workers`, `min_procs_for_parallel_enrich`

**Файлы:** `internal/config/config.go`, `internal/trc/enrich.go`

**Конфиг:**
```toml
[trc]
max_enrich_workers = 16
min_procs_for_parallel_enrich = 16
```

**Изменения:**

1. Добавить поля в `TRCConfig` (созданную в шаге 4):
   ```go
   type TRCConfig struct {
       SlowThresholdMs           int `toml:"slow_threshold_ms"`
       MaxEnrichWorkers          int `toml:"max_enrich_workers"`
       MinProcsForParallelEnrich int `toml:"min_procs_for_parallel_enrich"`
   }
   ```

2. В `Load()` добавить дефолты:
   ```go
   if cfg.TRC.MaxEnrichWorkers <= 0 {
       cfg.TRC.MaxEnrichWorkers = 16
   }
   if cfg.TRC.MinProcsForParallelEnrich <= 0 {
       cfg.TRC.MinProcsForParallelEnrich = 16
   }
   ```

3. В `CreateDefault()` добавить:
   ```go
   TRC: TRCConfig{
       SlowThresholdMs:           100,
       MaxEnrichWorkers:          16,
       MinProcsForParallelEnrich: 16,
   },
   ```

4. В `internal/trc/enrich.go` — убрать `const maxEnrichWorkers = 16` и `const minProcsForParallelEnrich = 16`, заменить на пакетные переменные:
   ```go
   var maxEnrichWorkers = 16
   var minProcsForParallelEnrich = 16

   func SetEnrichWorkers(max, minParallel int) {
       if max > 0 { maxEnrichWorkers = max }
       if minParallel > 0 { minProcsForParallelEnrich = minParallel }
   }
   ```

5. В `cmd/root.go` после `config.Load()`:
   ```go
   trc.SetEnrichWorkers(cfg.TRC.MaxEnrichWorkers, cfg.TRC.MinProcsForParallelEnrich)
   ```

**Проверка:**
```
go build ./...
go test ./internal/trc/...
```

---

## Порядок выполнения

1. **Шаг 1** (пул коннектов) → сборка + тесты config/store
2. **Шаг 2** (batch insert/delete) → сборка + тесты trc/rti/config
3. **Шаг 3** (query limits) → сборка + тесты mcp/config
4. **Шаг 4** (slow threshold) → сборка + тесты rti/config
5. **Шаг 5** (top slow count) → сборка + тесты rti
6. **Шаг 6** (pagination TTL) → сборка + тесты mcp
7. **Шаг 7** (progress interval) → сборка + тесты indexer
8. **Шаг 8** (enrich workers) → сборка + тесты trc

Каждый шаг независим. Шаги 4+5 можно комбинировать (один коммит). Шаги 1-3 — отдельные коммиты (наибольший риск).

После всех шагов — обновить `codebase.toml` новыми секциями и параметрами.

---

## Финальный вид codebase.toml

```toml
root_path = "D:/GITHUB/GolandProjects/FA"

[database]
host = "localhost"
port = 5435
database = "codebase"
user = "postgres"
password = "123456"
sslmode = "disable"
connect_timeout = 10
max_open_conns = 25
max_idle_conns = 25
conn_max_lifetime = "5m"

[indexer]
parallel = 12
batch_size = 1000
batch_insert_size = 50000
progress_interval_ms = 250
include_patterns = ["*.sql", "*.h", "*.pas", "*.inc", "*.js", "*.smf", "*.dfm", "*.tpr", "*.rpt", "*.xml"]
exclude_patterns = ["*/.*", "*~", "*.bak", "*.old", "*/archive/*", "*/backup/*"]

[query]
default_limit = 100
max_limit = 1000

[rti]
slow_threshold_ms = 100
top_slow_count = 10

[trc]
slow_threshold_ms = 100
max_enrich_workers = 16
min_procs_for_parallel_enrich = 16

[logging]
command_enabled = true

[mcp]
pagination_chunk_size = 8000
pagination_ttl = "15m"
```

---

## Откат

Все изменения обратно совместимы: при отсутствии новых полей в `codebase.toml` применяются дефолты, идентичные текущим захардкоженным значениям. Существующие `codebase.toml` без новых секций продолжат работать без изменений.

---

## Ожидаемый результат

| Параметр | Было | Стало |
|----------|------|-------|
| Пул коннектов БД | захардкожено `25`/`5m` | `[database] max_open_conns` / `conn_max_lifetime` |
| Batch insert/delete | захардкожено `50000` в 4-х файлах | `[indexer] batch_insert_size` |
| Query limits | inline `100`/`1000` в ~30 местах | `[query] default_limit` / `max_limit` |
| Slow threshold | захардкожено `100` мс / `0.1` сек | `[rti] slow_threshold_ms` / `[trc] slow_threshold_ms` |
| Top-N slow | захардкожено `10` | `[rti] top_slow_count` |
| Pagination TTL | захардкожено `15m` | `[mcp] pagination_ttl` |
| Progress interval | захардкожено `250ms` | `[indexer] progress_interval_ms` |
| Enrich workers | захардкожено `16`/`16` | `[trc] max_enrich_workers` / `min_procs_for_parallel_enrich` |
| Магических чисел в коде | ~8 категорий | 0 (все в конфиге) |

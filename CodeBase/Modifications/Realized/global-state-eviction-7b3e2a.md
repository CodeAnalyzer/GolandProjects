# Исправление глобального изменяемого состояния: eviction для regexpCache и globalPages

Устранение неограниченного роста двух глобальных кэшей в long-running MCP-процессе: (1) bounded LRU для `util.regexpCache` вместо `sync.Map` без лимита, (2) proactive gc для `mcp.globalPages` вместо lazy-очистки только при новой пагинации.

## Контекст проблемы

Замечание №7 из ревью кода: глобальное изменяемое состояние и невытесняемые кэши.

1. **`util.regexpCache`** (`internal/util/regexp_cache.go`) — `sync.Map` хранит скомпилированные `*regexp.Regexp` с ключами-паттернами, построенными из идентификаторов исходного кода (`regexp.QuoteMeta(tableName)`, `QuoteMeta(objectName)`, и т.д.). Кэш никогда не очищается. В CLI-режиме процесс завершается после работы — утечки нет. В MCP-сервере (long-running) кэш накапливается при каждом `review`/`update` для новых файлов, ограничен только числом уникальных идентификаторов в кодовой базе.

2. **`mcp.globalPages`** (`internal/mcp/pagination.go`) — `gc()` вызывается только из `maybePaginate` (строка 65), т.е. только при новой пагинации большого ответа. Если MCP-клиент «бросает» пагинированный ответ (не запрашивает оставшиеся чанки) и дальше идут только маленькие запросы — просроченные записи живут неограниченно долго.

## Затронутые файлы

| Файл | Действие |
|---|---|
| `internal/util/regexp_cache.go` | замена `sync.Map` на bounded LRU с `sync.Mutex` |
| `internal/util/regexp_cache_test.go` | **новый** — unit-тесты LRU eviction |
| `internal/mcp/pagination.go` | proactive gc: `time.AfterFunc` + вызов `gc()` из `readChunk` |
| `internal/mcp/pagination_test.go` | новые тесты для proactive gc и abandoned-entry cleanup |
| `internal/config/config.go` | добавить `RegexpCacheMaxEntries` в `MCPConfig` |

---

## Шаг 1 — Bounded LRU для `util.regexpCache`

### 1.1. Конфиг: `RegexpCacheMaxEntries`

В `internal/config/config.go`:
- Добавить поле в `MCPConfig`:
  ```go
  type MCPConfig struct {
      PaginationChunkSize   int `toml:"pagination_chunk_size"`
      RegexpCacheMaxEntries int `toml:"regexp_cache_max_entries"`
  }
  ```
- В `Load()` — дефолт: `cfg.MCP.RegexpCacheMaxEntries = 2048` если `<= 0`
- В `CreateDefault` — `MCP: MCPConfig{PaginationChunkSize: 8000, RegexpCacheMaxEntries: 2048}`

### 1.2. Замена `sync.Map` на bounded LRU

Полностью переписать `internal/util/regexp_cache.go`:

```go
package util

import (
    "regexp"
    "sync"
)

const defaultRegexpCacheMax = 2048

type regexpCacheEntry struct {
    re   *regexp.Regexp
    prev *regexpCacheEntry
    next *regexpCacheEntry
    key  string
}

type regexpCache struct {
    mu         sync.Mutex
    maxEntries int
    entries    map[string]*regexpCacheEntry
    head       *regexpCacheEntry // most recently used
    tail       *regexpCacheEntry // least recently used
}

var globalRegexpCache = newRegexpCache(defaultRegexpCacheMax)

func newRegexpCache(max int) *regexpCache {
    if max <= 0 {
        max = defaultRegexpCacheMax
    }
    return &regexpCache{
        maxEntries: max,
        entries:    make(map[string]*regexpCacheEntry),
    }
}

// SetRegexpCacheMaxEntries устанавливает лимит записей в глобальном кэше.
// При уменьшении лимита лишние записи (от tail) вытесняются немедленно.
func SetRegexpCacheMaxEntries(max int) {
    globalRegexpCache.setMax(max)
}

func (c *regexpCache) setMax(max int) {
    if max <= 0 {
        return
    }
    c.mu.Lock()
    defer c.mu.Unlock()
    c.maxEntries = max
    c.evict()
}

// CachedRegexp возвращает скомпилированный regexp для pattern, используя
// глобальный LRU-кэш. Потокобезопасен. При превышении maxEntries вытесняет
// наиболее давно используемые записи. Для статических паттернов предпочтительнее
// package-level var.
func CachedRegexp(pattern string) *regexp.Regexp {
    return globalRegexpCache.get(pattern)
}

func (c *regexpCache) get(pattern string) *regexp.Regexp {
    c.mu.Lock()
    defer c.mu.Unlock()

    if e, ok := c.entries[pattern]; ok {
        c.moveToFront(e)
        return e.re
    }

    re := regexp.MustCompile(pattern)
    entry := &regexpCacheEntry{re: re, key: pattern}
    c.entries[pattern] = entry
    c.addToFront(entry)
    c.evict()
    return re
}

func (c *regexpCache) addToFront(e *regexpCacheEntry) {
    e.next = c.head
    if c.head != nil {
        c.head.prev = e
    }
    c.head = e
    if c.tail == nil {
        c.tail = e
    }
}

func (c *regexpCache) moveToFront(e *regexpCacheEntry) {
    if e == c.head {
        return
    }
    c.remove(e)
    c.addToFront(e)
}

func (c *regexpCache) remove(e *regexpCacheEntry) {
    if e.prev != nil {
        e.prev.next = e.next
    } else {
        c.head = e.next
    }
    if e.next != nil {
        e.next.prev = e.prev
    } else {
        c.tail = e.prev
    }
    e.prev = nil
    e.next = nil
}

func (c *regexpCache) evict() {
    for len(c.entries) > c.maxEntries && c.tail != nil {
        oldest := c.tail
        c.remove(oldest)
        delete(c.entries, oldest.key)
    }
}

// CacheLen возвращает текущее количество записей в кэше (для тестов).
func CacheLen() int {
    globalRegexpCache.mu.Lock()
    defer globalRegexpCache.mu.Unlock()
    return len(globalRegexpCache.entries)
}
```

### 1.3. Интеграция в `cmd/root.go`

В `applyConfigToPackages()` (после строки 283) добавить:
```go
if cfg.MCP.RegexpCacheMaxEntries > 0 {
    util.SetRegexpCacheMaxEntries(cfg.MCP.RegexpCacheMaxEntries)
}
```
Добавить import `"github.com/codebase/internal/util"` в `cmd/root.go` (если ещё нет).

### 1.4. Тесты: `internal/util/regexp_cache_test.go`

| Тест | Что проверяет |
|---|---|
| `TestCachedRegexp_SamePatternReturnsSamePointer` | одинаковый паттерн → тот же `*regexp.Regexp` |
| `TestCachedRegexp_DifferentPatternReturnsDifferentPointer` | разные паттерны → разные указатели |
| `TestCachedRegexp_FunctionalMatch` | скомпилированный regexp корректно матчит |
| `TestRegexpCache_LRUEviction` | при превышении maxEntries вытесняется LRU (tail) |
| `TestRegexpCache_LRUPromotionOnAccess` | повторный доступ перемещает запись в head (не вытесняется) |
| `TestRegexpCache_SetMaxEvicts` | уменьшение лимита через `SetRegexpCacheMaxEntries` вытесняет лишние |
| `TestRegexpCache_CacheLen` | `CacheLen()` корректно отражает число записей |

**Примечание:** существующие тесты `TestCachedRegexp_*` в `internal/review/review_helpers_test.go` продолжают работать без изменений (API `cachedRegexp` → `util.CachedRegexp` не меняется).

---

## Шаг 2 — Proactive gc для `mcp.globalPages`

### 2.1. Вызов `gc()` из `readChunk`

В `internal/mcp/pagination.go`, метод `readChunk` — добавить вызов `ps.gc()` перед возвратом (после `ps.mu.Unlock()`):

```go
func (ps *pageStore) readChunk(id string, chunkIdx int) (rawMCPText, error) {
    // ... существующий код ...
    ps.mu.Unlock()
    ps.gc()   // ← добавить: очищаем просроченные при любом обращении
    // ... существующий код (формирование header + return) ...
}
```

Это гарантирует, что даже если все последующие запросы маленькие (не вызывают `maybePaginate`), но клиент запрашивает чанки — gc срабатывает.

### 2.2. Фоновая очистка по таймеру

Добавить в `pageStore` метод `startGCLoop`, который запускает периодическую очистку через `time.AfterFunc`:

```go
// startGCLoop запускает периодическую фоновую очистку просроченных записей.
// Интервал = TTL / 2. Останавливается через stopGCLoop.
func (ps *pageStore) startGCLoop() {
    ps.stopGCLoop() // остановить предыдущий таймер, если был
    interval := paginationTTL / 2
    if interval < time.Minute {
        interval = time.Minute
    }
    ps.gcTimerMu.Lock()
    ps.gcTimer = time.AfterFunc(interval, ps.gcTick)
    ps.gcTimerMu.Unlock()
}

func (ps *pageStore) gcTick() {
    ps.gc()
    ps.gcTimerMu.Lock()
    defer ps.gcTimerMu.Unlock()
    if ps.gcTimer != nil {
        ps.gcTimer.Reset(paginationTTL / 2)
    }
}

func (ps *pageStore) stopGCLoop() {
    ps.gcTimerMu.Lock()
    defer ps.gcTimerMu.Unlock()
    if ps.gcTimer != nil {
        ps.gcTimer.Stop()
        ps.gcTimer = nil
    }
}
```

Добавить поля в структуру `pageStore`:
```go
type pageStore struct {
    chunkSize  int
    mu         sync.Mutex
    entries    map[string]*pageEntry
    gcTimerMu  sync.Mutex
    gcTimer    *time.Timer
}
```

### 2.3. Интеграция в `RunStdio`

В `internal/mcp/server.go`, функция `RunStdio` — после инициализации `globalPages` из конфига:
```go
globalPages = newPageStore(cfg.MCP.PaginationChunkSize)
globalPages.startGCLoop()   // ← добавить
defer globalPages.stopGCLoop()
```

### 2.4. Тесты: `internal/mcp/pagination_test.go`

Добавить тесты:

| Тест | Что проверяет |
|---|---|
| `TestPageStore_GCOnReadChunk` | просроченная запись удаляется при вызове `readChunk` (без `maybePaginate`) |
| `TestPageStore_GCLoopCleansExpired` | фоновый gc удаляет просроченные записи без явного вызова (с коротким TTL и `time.Sleep`) |
| `TestPageStore_StopGCLoop` | `stopGCLoop` останавливает таймер (через проверку `gcTimer == nil`) |

**Примечание:** для `TestPageStore_GCLoopCleansExpired` использовать TTL ~200ms и `time.Sleep(300ms)` — тест быстрый, детерминированный.

---

## Проверка

```
go build ./...
go vet ./...
go test ./internal/util/... ./internal/mcp/... ./internal/config/... -count=1
```

## Ожидаемый эффект

- **`regexpCache`**: рост ограничен `maxEntries` (по умолчанию 2048). LRU вытесняет давно неиспользуемые паттерны. Память в long-running MCP-процессе ограничена ~2048 × ~5KB ≈ 10MB вместо неограниченного роста.
- **`globalPages`**: просроченные пагинированные ответы удаляются (1) при любом `readChunk`, (2) фоновым таймером каждые `TTL/2` — независимо от того, поступают ли новые большие ответы.
- Конфигурируемость: `regexp_cache_max_entries` в `codebase.toml` → `[mcp]` секция.
- API `util.CachedRegexp` и `cachedRegexp` не меняются — существующий код и тесты работают без изменений.

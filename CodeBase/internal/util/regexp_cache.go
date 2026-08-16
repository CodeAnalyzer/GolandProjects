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

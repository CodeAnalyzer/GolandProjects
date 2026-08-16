package util

import (
	"regexp"
	"testing"
)

func TestCachedRegexp_SamePatternReturnsSamePointer(t *testing.T) {
	c := newRegexpCache(64)
	re1 := c.get(`(?i)\bfoo\b`)
	re2 := c.get(`(?i)\bfoo\b`)
	if re1 != re2 {
		t.Error("same pattern should return same *regexp.Regexp")
	}
}

func TestCachedRegexp_DifferentPatternReturnsDifferentPointer(t *testing.T) {
	c := newRegexpCache(64)
	re1 := c.get(`(?i)\bfoo\b`)
	re2 := c.get(`(?i)\bbar\b`)
	if re1 == re2 {
		t.Error("different patterns should return different *regexp.Regexp")
	}
}

func TestCachedRegexp_FunctionalMatch(t *testing.T) {
	c := newRegexpCache(64)
	re := c.get(`(?i)@\w+\b`)
	if !re.MatchString("select @a from t") {
		t.Error("compiled regexp does not match expected input")
	}
}

func TestRegexpCache_LRUEviction(t *testing.T) {
	c := newRegexpCache(3)
	c.get("p1")
	c.get("p2")
	c.get("p3")
	c.get("p4") // should evict p1 (tail)

	if len(c.entries) != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", len(c.entries))
	}
	if _, ok := c.entries["p1"]; ok {
		t.Error("p1 should have been evicted as LRU")
	}
	if _, ok := c.entries["p4"]; !ok {
		t.Error("p4 should be in cache")
	}
}

func TestRegexpCache_LRUPromotionOnAccess(t *testing.T) {
	c := newRegexpCache(3)
	c.get("p1")
	c.get("p2")
	c.get("p3")
	// Access p1 to promote it to head
	c.get("p1")
	// Now add p4 — should evict p2 (tail), not p1
	c.get("p4")

	if _, ok := c.entries["p1"]; !ok {
		t.Error("p1 should still be in cache after promotion")
	}
	if _, ok := c.entries["p2"]; ok {
		t.Error("p2 should have been evicted as LRU")
	}
}

func TestRegexpCache_SetMaxEvicts(t *testing.T) {
	c := newRegexpCache(10)
	for i := 0; i < 10; i++ {
		c.get(string(rune('a' + i)))
	}
	if len(c.entries) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(c.entries))
	}

	c.setMax(5)
	if len(c.entries) != 5 {
		t.Fatalf("expected 5 entries after setMax(5), got %d", len(c.entries))
	}
	// First 5 entries (a..e) should be evicted as LRU, f..j kept
	if _, ok := c.entries["a"]; ok {
		t.Error("a should have been evicted")
	}
	if _, ok := c.entries["j"]; !ok {
		t.Error("j should still be in cache")
	}
}

func TestRegexpCache_CacheLen(t *testing.T) {
	// CacheLen operates on the global cache; we test it indirectly
	// by using SetRegexpCacheMaxEntries and CachedRegexp.
	saved := globalRegexpCache
	globalRegexpCache = newRegexpCache(64)
	defer func() { globalRegexpCache = saved }()

	CachedRegexp("test1")
	CachedRegexp("test2")
	if n := CacheLen(); n != 2 {
		t.Fatalf("expected CacheLen=2, got %d", n)
	}
	CachedRegexp("test1") // same pattern, no new entry
	if n := CacheLen(); n != 2 {
		t.Fatalf("expected CacheLen=2 after duplicate, got %d", n)
	}
}

func TestCachedRegexp_GlobalAPI(t *testing.T) {
	// Verify the global API works end-to-end
	re := CachedRegexp(`(?i)\btest\b`)
	if _, ok := regexp.Compile(`(?i)\btest\b`); ok != nil {
		t.Fatal("baseline regexp.Compile failed")
	}
	if !re.MatchString("this is a Test") {
		t.Error("global CachedRegexp returned non-matching regexp")
	}
}

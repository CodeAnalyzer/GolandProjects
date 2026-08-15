package util

import (
	"regexp"
	"sync"
)

// regexpCache хранит динамически скомпилированные regexps (с переменными паттернами),
// чтобы избежать повторной компиляции для одинаковых pattern-строк.
var regexpCache sync.Map

// CachedRegexp возвращает скомпилированный regexp для pattern, используя глобальный кэш.
// Потокобезопасен. Для статических паттернов предпочтительнее package-level var.
func CachedRegexp(pattern string) *regexp.Regexp {
	if v, ok := regexpCache.Load(pattern); ok {
		return v.(*regexp.Regexp)
	}
	re := regexp.MustCompile(pattern)
	regexpCache.Store(pattern, re)
	return re
}

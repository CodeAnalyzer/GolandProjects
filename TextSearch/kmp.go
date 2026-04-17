package main

type kmpAlgorithm struct {
	pattern    string
	patternLen int
	pi         []int
}

func newKmpAlgorithm(pattern string) *kmpAlgorithm {
	kmp := &kmpAlgorithm{
		pattern:    pattern,
		patternLen: len(pattern),
	}
	kmp.pi = kmp.createPi()
	return kmp
}

func (kmp *kmpAlgorithm) createPi() []int {
	var pi = make([]int, kmp.patternLen+1)
	pi[0] = 0
	pi[1] = 0
	for q := 1; q < kmp.patternLen; q++ {
		len := pi[q]
		for len > 0 && kmp.pattern[len] != kmp.pattern[q] {
			len = pi[len]
		}
		if kmp.pattern[len] == kmp.pattern[q] {
			len++
		}
		pi[q+1] = len
	}
	return pi
}

func (kmp *kmpAlgorithm) kmpSearch(text string) []int {
	var results []int
	var q int = 0

	for pos, char := range []byte(text) {
		for q > 0 && kmp.pattern[q] != char {
			q = kmp.pi[q]
		}
		if kmp.pattern[q] == char {
			q++
		}
		if q == kmp.patternLen {
			results = append(results, pos-kmp.patternLen+1)
			q = kmp.pi[q]
		}
	}

	return results
}

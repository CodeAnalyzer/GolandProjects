package main

import "strings"

func simpleSearch(text string, pattern string) []int {
	var results []int
	pos := 0
	for {
		index := strings.Index(text[pos:], pattern)
		if index == -1 {
			break
		}
		results = append(results, pos+index)
		pos += index + 1
	}
	return results
}

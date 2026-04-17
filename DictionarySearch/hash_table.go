package main

import (
	"fmt"
	"math"
	"strings"
)

type word struct {
	english       string
	transcription string
	russian       string
	next          *word
}

func newWordByLine(line string) *word {
	var items = strings.Split(line, "\t")
	return &word{english: items[0], transcription: items[1], russian: items[2]}
}

func getHashCodeOld(key string) int {
	var result int = 0
	var byte0, byte1, byte2, byte3 byte
	for index, value := range key {
		if (index+1)%4 == 0 {
			byte0 = byte0 + (byte)(value)
		}
		if (index+1)%4 == 1 {
			byte1 = byte1 + (byte)(value)
		}
		if (index+1)%4 == 2 {
			byte2 = byte2 + (byte)(value)
		}
		if (index+1)%4 == 3 {
			byte3 = byte3 + (byte)(value)

		}
	}
	result = (int)(((int64)(byte0) + ((int64)(byte1) << 8) + ((int64)(byte2) << 16) + ((int64)(byte3) << 24)) % math.MaxInt32)
	return result
}

func getHashCode(key string) int {
	hash := 0
	for _, c := range key {
		hash = hash*31 + int(c)
	}
	return hash
}

type hDictionary struct {
	size  int
	table []*word
}

func newHDictionary(size int) *hDictionary {
	return &hDictionary{size: size, table: make([]*word, size)}
}

func putWord(dict *hDictionary, line string) {
	nextWord := newWordByLine(line)
	hashCode := getHashCode(nextWord.english) % dict.size
	if dict.table[hashCode] == nil {
		dict.table[hashCode] = nextWord
	} else {
		root := dict.table[hashCode]
		for root != nil {
			if root.next == nil {
				root.next = nextWord
				break
			}
			root = root.next
		}
	}
}

func printHDictionary(dict *hDictionary) {
	if dict.size > 0 {
		for index, currWord := range dict.table {
			if dict.table[index] != nil {
				fmt.Println("english: ", currWord.english, ", transcription: ", currWord.transcription, ", russian: ", currWord.russian)
				if currWord.next != nil {
					nextWord := currWord.next
					for nextWord != nil {
						fmt.Println("english: ", nextWord.english, ", transcription: ", nextWord.transcription, ", russian: ", nextWord.russian)
						nextWord = nextWord.next
					}
				}
			}
		}
	}
}

func getWord(dict *hDictionary, key string) *word {
	hashCode := getHashCode(key) % dict.size
	if dict.table[hashCode] != nil {
		root := dict.table[hashCode]
		if root.english == key {
			return root
		} else if root.next != nil {
			root = root.next
			for root != nil {
				if root.english == key {
					return root
				} else {
					root = root.next
				}
			}
		}
	}
	return nil
}

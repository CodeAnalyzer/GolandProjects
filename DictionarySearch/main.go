package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"
)

func readFileLines(filename string) ([]string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	return lines, nil
}

const nums = 50000

func main() {
	fmt.Println("------------------------------------------------------")
	fmt.Println("|                   DictionarySearch                  |")
	fmt.Println("------------------------------------------------------")

	var lines, _ = readFileLines("dictionary.txt")
	fmt.Printf("Read file dictionary.txt: %d lines \n", len(lines))

	var randomNum [nums]int
	for i := 0; i < nums; i++ {
		randomNum[i] = rand.Intn(len(lines))
	}

	fmt.Println("------------------------------------------------------")
	fmt.Println("Bucket sort dictionary")

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	var dict = newBuckets(len(lines), len(lines)*2)
	for i, line := range lines {
		addItem(dict, i+1, line)
	}
	runtime.ReadMemStats(&m2)
	fmt.Printf("Bucket sort memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)
	// printDictionary(dict)

	runtime.Gosched()
	start := time.Now().UnixMicro()
	for i := 0; i < nums; i++ {
		var _ = findItem(dict, randomNum[i])

	}
	end := time.Now().UnixMicro()
	fmt.Printf("Bucket sort search: %d µs for %d times (%d ns per one) \n", end-start, nums, 1000*(end-start)/nums)

	fmt.Println("------------------------------------------------------")
	fmt.Println("Binary search tree")

	runtime.ReadMemStats(&m1)
	var root = newBstNode(len(lines)*5, lines[0])
	for i, line := range lines {
		if i > 0 {
			key := rand.Intn(len(lines) * 10)
			for finBstNode(root, key) != nil {
				key = rand.Intn(len(lines) * 10)
			}
			addBstNode(root, key, line)
		}
	}
	runtime.ReadMemStats(&m2)
	fmt.Printf("Binary search tree memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)
	// printBstTree(root)

	runtime.Gosched()
	start = time.Now().UnixMicro()
	for i := 0; i < nums; i++ {
		var _ = finBstNode(root, randomNum[i])
	}
	end = time.Now().UnixMicro()
	fmt.Printf("Binary search tree: %d µs for %d times (%d ns per one) \n", end-start, nums, 1000*(end-start)/nums)

	fmt.Println("------------------------------------------------------")
	fmt.Println("Hash table")

	runtime.ReadMemStats(&m1)
	var hDict = newHDictionary(4000)
	for _, line := range lines {
		putWord(hDict, line)
	}
	runtime.ReadMemStats(&m2)
	fmt.Printf("Hash table memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)
	// printHDictionary(hDict)

	runtime.Gosched()
	start = time.Now().UnixMicro()
	for i := 0; i < nums; i++ {
		var _ = getWord(hDict, lines[randomNum[i]][:strings.Index(lines[randomNum[i]], "\t")])
	}
	end = time.Now().UnixMicro()
	fmt.Printf("Hash table search: %d µs for %d times (%d ns per one) \n", end-start, nums, 1000*(end-start)/nums)

	fmt.Println("------------------------------------------------------")
	fmt.Println("Prefix tree")

	runtime.ReadMemStats(&m1)
	var pDict = newPrefixTree()
	for _, line := range lines {
		putTreeNodeByLine(pDict, line)
	}
	runtime.ReadMemStats(&m2)
	fmt.Printf("Prefix tree memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)
	// printPDictionary(pDict)

	runtime.Gosched()
	start = time.Now().UnixMicro()
	for i := 0; i < nums; i++ {
		var _ = getTreeNode(pDict, lines[randomNum[i]][:strings.Index(lines[randomNum[i]], "\t")])
	}
	end = time.Now().UnixMicro()
	fmt.Printf("Prefix tree search: %d µs for %d times (%d ns per one) \n", end-start, nums, 1000*(end-start)/nums)
}

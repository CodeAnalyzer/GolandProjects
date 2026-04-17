package main

import (
	"fmt"
	"os"
	"regexp"
	"runtime"
	"sort"
	"sync"
	"time"
)

func findLongestWords(text string, count int) []string {
	// Находим все слова (последовательности русских букв)
	re := regexp.MustCompile(`[а-яА-Я]+`)
	words := re.FindAllString(text, -1)

	// Создаем карту для уникальных слов с сохранением оригинального регистра
	uniqueWords := make(map[string]bool)
	for _, word := range words {
		if len(word) > 3 { // Игнорируем очень короткие слова
			uniqueWords[word] = true // Сохраняем оригинальный регистр
		}
	}

	// Преобразуем в слайс и сортируем по длине
	var wordList []string
	for word := range uniqueWords {
		wordList = append(wordList, word)
	}

	sort.Slice(wordList, func(i, j int) bool {
		return len(wordList[i]) > len(wordList[j])
	})

	// Возвращаем count самых длинных слов
	if len(wordList) < count {
		count = len(wordList)
	}

	return wordList[:count]
}

func readFileAsLine(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func main() {
	fmt.Println("------------------------------------------------------")
	fmt.Println("|                     TextSearch                     |")
	fmt.Println("------------------------------------------------------")

	text, _ := readFileAsLine("Garri-Potter-i-uznik-Azkabana.txt")
	fmt.Printf("Read file Garri-Potter-i-uznik-Azkabana.txt: %d characters \n", len(text))

	// Находим 10 самых длинных слов и используем их как паттерны
	var patterns = findLongestWords(text, 10)
	fmt.Println("Found longest words:")
	for i, pattern := range patterns {
		fmt.Printf("%d. [%s] (length: %d)\n", i+1, pattern, len(pattern))
	}

	fmt.Println("------------------------------------------------------")
	fmt.Println("Simple search (parallel)")

	runtime.Gosched()
	start := time.Now()
	foundSS := make([][]int, len(patterns))
	var wg sync.WaitGroup

	for i, pattern := range patterns {
		wg.Add(1)
		go func(index int, pat string) {
			defer wg.Done()
			foundSS[index] = simpleSearch(text, pat)
		}(i, pattern)
	}

	wg.Wait()
	duration := time.Since(start)

	for i, pattern := range patterns {
		fmt.Printf("Found [%s] %d times\n", pattern, len(foundSS[i]))
	}
	fmt.Printf("Parallel search time: %v\n", duration)

	fmt.Println("------------------------------------------------------")
	fmt.Println("Finite-state machine search (parallel)")

	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	stateMachines := make([]*finiteStateMachine, len(patterns))
	for i, pattern := range patterns {
		stateMachines[i] = newFiniteStateMachine(pattern)
	}
	runtime.ReadMemStats(&m2)
	fmt.Printf("FSM memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)

	runtime.Gosched()
	start = time.Now()
	foundSM := make([][]int, len(patterns))
	var wg2 sync.WaitGroup

	for i, stateMachine := range stateMachines {
		wg2.Add(1)
		go func(index int, sm *finiteStateMachine) {
			defer wg2.Done()
			foundSM[index] = sm.search(text)
		}(i, stateMachine)
	}

	wg2.Wait()
	duration = time.Since(start)

	for i, pattern := range patterns {
		fmt.Printf("Found [%s] %d times\n", pattern, len(foundSM[i]))
	}
	fmt.Printf("Parallel FSM search time: %v\n", duration)

	fmt.Println("------------------------------------------------------")
	fmt.Println("Knuth–Morris–Pratt search (parallel)")

	runtime.ReadMemStats(&m1)
	kmpAlgorithms := make([]*kmpAlgorithm, len(patterns))
	var wg3 sync.WaitGroup

	// Создаем экземпляры kmpAlgorithm параллельно
	for i, pattern := range patterns {
		wg3.Add(1)
		go func(index int, pat string) {
			defer wg3.Done()
			kmpAlgorithms[index] = newKmpAlgorithm(pat)
		}(i, pattern)
	}
	wg3.Wait()
	runtime.ReadMemStats(&m2)
	fmt.Printf("KMP memory: %d KB\n", (m2.Alloc-m1.Alloc)/1024)

	runtime.Gosched()
	start = time.Now()
	foundKMP := make([][]int, len(patterns))

	// Выполняем поиск параллельно
	for i, kmp := range kmpAlgorithms {
		wg3.Add(1)
		go func(index int, kmpAlg *kmpAlgorithm) {
			defer wg3.Done()
			foundKMP[index] = kmpAlg.kmpSearch(text)
		}(i, kmp)
	}

	wg3.Wait()
	duration = time.Since(start)

	for i, pattern := range patterns {
		fmt.Printf("Found [%s] %d times\n", pattern, len(foundKMP[i]))
	}
	fmt.Printf("Parallel KMP search time: %v\n", duration)
}

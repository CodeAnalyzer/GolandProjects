package main

import "runtime"

func getThreshold(arraySize int) int {
	numCPU := runtime.NumCPU()
	if numCPU == 0 {
		numCPU = 1
	}
	return arraySize / numCPU
}

func merge(a, b []int) []int {
	result := make([]int, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if a[i] <= b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	// Добавляем оставшиеся элементы
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}

func split_merge(a []int) []int {
	if len(a) <= 1 {
		return a
	}

	mid := len(a) / 2
	left := split_merge(a[:mid])
	right := split_merge(a[mid:])

	return merge(left, right)
}

func parallelSplitMerge(a []int, originalSize int) []int {
	if len(a) <= 1 {
		return a
	}

	// Используем динамический порог
	if len(a) <= getThreshold(originalSize) {
		return split_merge(a)
	}

	mid := len(a) / 2

	var left, right []int

	// Запускаем обе половины параллельно
	leftChan := make(chan []int, 1)
	rightChan := make(chan []int, 1)

	go func() {
		leftChan <- parallelSplitMerge(a[:mid], originalSize)
	}()

	go func() {
		rightChan <- parallelSplitMerge(a[mid:], originalSize)
	}()

	left = <-leftChan
	right = <-rightChan

	return merge(left, right)
}

func mergeSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	sorted := split_merge(arr)
	copy(arr, sorted)
}

func parallelMergeSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	sorted := parallelSplitMerge(arr, len(arr))
	copy(arr, sorted)
}

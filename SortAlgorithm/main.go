package main

import (
	"fmt"
	"math/rand"
	"time"
)

// generateUniqueArray генерирует массив уникальных целых чисел
func generateUniqueArray(size int, min, max int) []int {
	if size > (max - min + 1) {
		panic("Размер массива превышает количество уникальных значений в диапазоне")
	}

	rand.Seed(time.Now().UnixNano())

	// Создаем мапу для отслеживания использованных чисел
	used := make(map[int]bool)
	result := make([]int, 0, size)

	for len(result) < size {
		num := rand.Intn(max-min+1) + min
		if !used[num] {
			used[num] = true
			result = append(result, num)
		}
	}

	return result
}

func main() {
	fmt.Println("Sort Algorithm Project")

	// Генерируем массив на 100000 уникальных элементов от 1 до maxInt32
	arr := generateUniqueArray(100000, 1, 2147483647)
	fmt.Printf("Сгенерирован массив из %d элементов\n\n", len(arr))

	// Копируем массив для сортировки
	arrQuick := make([]int, len(arr))
	copy(arrQuick, arr)

	// Сортируем с помощью быстрой сортировки
	fmt.Println("Начинаем быструю сортировку...")
	start := time.Now()
	quickSort(arrQuick, 0, len(arrQuick)-1)
	elapsed := time.Since(start)

	fmt.Printf("Быстрая сортировка завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrQuick[:10])

	// Копируем массив для radix сортировки
	arrRadix := make([]int, len(arr))
	copy(arrRadix, arr)

	// Сортируем с помощью поразрядной сортировки
	fmt.Println("Начинаем поразрядную сортировку...")
	start = time.Now()
	radixSort(arrRadix)
	elapsed = time.Since(start)

	fmt.Printf("Поразрядная сортировка завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrRadix[:10])

	// Копируем массив для merge сортировки
	arrMerge := make([]int, len(arr))
	copy(arrMerge, arr)

	// Сортируем с помощью merge сортировки
	fmt.Println("Начинаем сортировку слиянием...")
	start = time.Now()
	mergeSort(arrMerge)
	elapsed = time.Since(start)

	fmt.Printf("Сортировка слиянием завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrMerge[:10])

	// Копируем массив для параллельной merge сортировки
	arrParallelMerge := make([]int, len(arr))
	copy(arrParallelMerge, arr)

	// Сортируем с помощью параллельной merge сортировки
	fmt.Println("Начинаем параллельную сортировку слиянием...")
	start = time.Now()
	parallelMergeSort(arrParallelMerge)
	elapsed = time.Since(start)

	fmt.Printf("Параллельная сортировка слиянием завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrParallelMerge[:10])

	// Копируем массив для heap сортировки
	arrHeap := make([]int, len(arr))
	copy(arrHeap, arr)

	// Сортируем с помощью heap сортировки
	fmt.Println("Начинаем пирамидальную сортировку...")
	start = time.Now()
	heapSort(arrHeap)
	elapsed = time.Since(start)

	fmt.Printf("Пирамидальная сортировка завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrHeap[:10])

	// Копируем массив для comb сортировки
	arrComb := make([]int, len(arr))
	copy(arrComb, arr)

	// Сортируем с помощью comb сортировки
	fmt.Println("Начинаем сортировку расческой...")
	start = time.Now()
	combSort(arrComb)
	elapsed = time.Since(start)

	fmt.Printf("Comb сортировка завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrComb[:10])

	// Копируем массив для сортировки вставками
	arrInsertion := make([]int, len(arr))
	copy(arrInsertion, arr)

	// Сортируем с помощью сортировки вставками
	fmt.Println("Начинаем сортировку вставками...")
	start = time.Now()
	insertionSort(arrInsertion)
	elapsed = time.Since(start)

	fmt.Printf("Сортировка вставками завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrInsertion[:10])

	// Копируем массив для shaker сортировки
	arrShaker := make([]int, len(arr))
	copy(arrShaker, arr)

	// Сортируем с помощью shaker сортировки
	fmt.Println("Начинаем сортировку перемешиванием...")
	start = time.Now()
	shakerSort(arrShaker)
	elapsed = time.Since(start)

	fmt.Printf("Сортировка перемешиванием завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrShaker[:10])

	// Копируем массив для пузырьковой сортировки
	arrBubble := make([]int, len(arr))
	copy(arrBubble, arr)

	// Сортируем с помощью пузырьковой сортировки
	fmt.Println("Начинаем пузырьковую сортировку...")
	start = time.Now()
	bubbleSort(arrBubble)
	elapsed = time.Since(start)

	fmt.Printf("Пузырьковая сортировка завершена за %v\n", elapsed)
	fmt.Printf("Первые 10 элементов отсортированного массива: %v\n\n", arrBubble[:10])
}

package main

// countingSortByDigit выполняет сортировку подсчётом для конкретного разряда
func countingSortByDigit(arr []int, exp int) {
	n := len(arr)
	output := make([]int, n)
	count := make([]int, 10) // цифры 0-9 для десятичной системы

	// Подсчет частот цифр в текущем разряде
	for i := 0; i < n; i++ {
		digit := (arr[i] / exp) % 10
		count[digit]++
	}

	// Преобразование счетчиков в кумулятивные суммы
	// для определения позиций элементов
	for i := 1; i < 10; i++ {
		count[i] += count[i-1]
	}

	// Построение выходного массива
	// идем справа налево для сохранения стабильности
	for i := n - 1; i >= 0; i-- {
		digit := (arr[i] / exp) % 10
		output[count[digit]-1] = arr[i]
		count[digit]--
	}

	// Копирование отсортированного массива обратно
	copy(arr, output)
}

// radixSort реализует поразрядную сортировку (LSD - Least Significant Digit)
func radixSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	// Находим максимальное число для определения количества разрядов
	max := arr[0]
	for _, num := range arr[1:] {
		if num > max {
			max = num
		}
	}

	// Сортируем по каждому разряду, начиная с единиц (exp = 1)
	// и двигаясь к старшим разрядам (exp *= 10)
	for exp := 1; max/exp > 0; exp *= 10 {
		countingSortByDigit(arr, exp)
	}
}

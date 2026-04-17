package main

func shakerSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	n := len(arr)
	start := 0
	end := n - 1
	swapped := true

	for swapped {
		// Проход слева направо (большие элементы вверх)
		swapped = false
		for i := start; i < end; i++ {
			if arr[i] > arr[i+1] {
				arr[i], arr[i+1] = arr[i+1], arr[i]
				swapped = true
			}
		}

		// Если не было обменов, массив отсортирован
		if !swapped {
			break
		}

		// Уменьшаем end, так как последний элемент уже на месте
		end--

		// Проход справа налево (маленькие элементы вниз)
		swapped = false
		for i := end; i > start; i-- {
			if arr[i-1] > arr[i] {
				arr[i-1], arr[i] = arr[i], arr[i-1]
				swapped = true
			}
		}

		// Увеличиваем start, так как первый элемент уже на месте
		start++
	}
}
package main

func insertionSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	for i := 1; i < len(arr); i++ {
		current := arr[i]
		j := i
		for j > 0 && arr[j-1] > current {
			arr[j] = arr[j-1]
			j--
		}
		arr[j] = current
	}
}

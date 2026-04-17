package main

func swap(arr []int, a, b int) {
	arr[a], arr[b] = arr[b], arr[a]
}

func heapify(arr []int, root int, size int) {
	largest := root
	left := 2*root + 1
	right := left + 1

	if left < size && arr[left] > arr[largest] {
		largest = left
	}

	if right < size && arr[right] > arr[largest] {
		largest = right
	}

	if largest != root {
		swap(arr, root, largest)
		heapify(arr, largest, size)
	}
}

func heapSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	n := len(arr)
	for root := n/2 - 1; root >= 0; root-- {
		heapify(arr, root, n)
	}

	for i := n - 1; i > 0; i-- {
		swap(arr, 0, i)
		heapify(arr, 0, i)
	}
}

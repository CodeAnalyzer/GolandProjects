package main

func combSort(arr []int) {
	if len(arr) <= 1 {
		return
	}

	n := len(arr)
	gap := n
	shrink := 1.247
	sorted := false

	for !sorted {
		gap = int(float64(gap) / shrink)
		if gap < 1 {
			gap = 1
		}

		sorted = gap == 1

		for i := 0; i+gap < n; i++ {
			if arr[i] > arr[i+gap] {
				arr[i], arr[i+gap] = arr[i+gap], arr[i]
				sorted = false
			}
		}
	}
}

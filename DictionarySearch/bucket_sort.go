package main

import "fmt"

type node struct {
	key   int
	value string
	next  *node
}

func newNode(key int, value string, next *node) *node {
	return &node{key: key, value: value, next: next}
}

type buckets struct {
	n      int
	max    int
	size   int
	bucket []*node
}

func newBuckets(n int, max int) *buckets {
	return &buckets{n: n, max: max, size: 0, bucket: make([]*node, n)}
}

func hash(n int, max int, key int) int {
	return (int)(int64(key) * int64(n) / int64(max))
}

func addItem(b *buckets, key int, value string) {
	var num = hash(b.n, b.max, key)
	if b.bucket[num] == nil {
		b.bucket[num] = newNode(key, value, nil)
		b.size++
	} else {
		var root = b.bucket[num]
		for root != nil {
			if key == root.key {
				root.value = value
				break
			}
			if key < root.key {
				var item = newNode(root.key, root.value, root.next)
				root.key = key
				root.value = value
				root.next = item
				b.size++
				break
			}
			if root.next == nil {
				root.next = newNode(key, value, nil)
				b.size++
				break
			}
			root = root.next
		}
	}
}

func findItem(b *buckets, key int) *node {
	var num = hash(b.n, b.max, key)
	var root = b.bucket[num]
	for root != nil {
		if key == root.key {
			return root
		}
		root = root.next
	}
	return nil
}

func printDictionary(b *buckets) {
	fmt.Println("n=", b.n)
	fmt.Println("max=", b.max)
	fmt.Println("size=", b.size)
	for i := 0; i < len(b.bucket); i++ {
		var item = b.bucket[i]
		for item != nil {
			fmt.Println("key=", item.key, ", value=", item.value)
			item = item.next
		}
	}
}

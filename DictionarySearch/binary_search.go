package main

import "fmt"

type bstNode struct {
	key   int
	value string
	left  *bstNode
	right *bstNode
}

func newBstNode(key int, value string) *bstNode {
	return &bstNode{key: key, value: value, left: nil, right: nil}
}

func addBstNode(root *bstNode, newKey int, newValue string) {
	if root.key == newKey {
		root.value = newValue
		return
	}

	if root.key > newKey {
		if root.left == nil {
			root.left = newBstNode(newKey, newValue)
		} else {
			addBstNode(root.left, newKey, newValue)
		}
	} else {
		if root.right == nil {
			root.right = newBstNode(newKey, newValue)
		} else {
			addBstNode(root.right, newKey, newValue)
		}
	}
}

func finBstNode(root *bstNode, key int) *bstNode {

	if root.key == key {
		return root
	} else {
		if root.key > key {
			if root.left == nil {
				return nil
			} else {
				return finBstNode(root.left, key)
			}
		} else {
			if root.right == nil {
				return nil
			} else {
				return finBstNode(root.right, key)
			}
		}
	}
}

func printBstTree(root *bstNode) {
	if root != nil {
		printBstTree(root.left)
		fmt.Println("Key: ", root.key, ", value: ", root.value)
		printBstTree(root.right)
	}
}

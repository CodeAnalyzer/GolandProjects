package main

import "fmt"

const abc = 26 // Размер алфавита - только английские прописные буквы
const a = 97   // код буквы «a» в таблице ASCII

type treeNode struct {
	child  []*treeNode
	symbol byte
	value  *word
}

type prefixTree struct {
	root *treeNode
}

func newPrefixTree() *prefixTree {
	return &prefixTree{
		root: newTreeNode(0), // корневой узел, символ не важен
	}
}

func newTreeNode(symbol byte) *treeNode {
	return &treeNode{
		child:  make([]*treeNode, abc),
		symbol: symbol,
		value:  nil,
	}
}

func exists(root *treeNode, symbol byte) bool {
	return root.child[symbol-a] != nil
}

func next(root *treeNode, symbol byte) *treeNode {
	if !exists(root, symbol) {
		root.child[symbol-a] = newTreeNode(symbol)
	}
	return root.child[symbol-a]
}

func putTreeNode(tree *prefixTree, key string, value *word) {
	curNode := tree.root
	for _, char := range key {
		curNode = next(curNode, (byte)(char))
	}
	curNode.value = value
}

func putTreeNodeByLine(tree *prefixTree, line string) {
	nWord := newWordByLine(line)
	putTreeNode(tree, nWord.english, nWord)
}

func getTreeNode(tree *prefixTree, key string) *word {
	curNode := tree.root
	for _, char := range key {
		if !exists(curNode, (byte)(char)) {
			return nil // слово не найдено
		}
		curNode = curNode.child[char-a]
	}
	return curNode.value
}

func printPDictionary(tree *prefixTree) {
	printTreeNode(tree.root, "")
}

func printTreeNode(node *treeNode, prefix string) {
	if node == nil {
		return
	}

	// Если в узле есть значение (слово), выводим его
	if node.value != nil {
		fmt.Printf("word: %s, transcription: %s, russian: %s\n",
			node.value.english, node.value.transcription, node.value.russian)
	}

	// Рекурсивно проходим по всем дочерним узлам
	for i := 0; i < abc; i++ {
		if node.child[i] != nil {
			printTreeNode(node.child[i], prefix+string(rune(i+a)))
		}
	}
}

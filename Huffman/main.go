package main

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
)

// Узел дерева Хаффмана
type node struct {
	freq   int
	bit0   *node
	bit1   *node
	symbol byte
}

func newNode(freq int, bit0 *node, bit1 *node) *node {
	return &node{freq: freq, bit0: bit0, bit1: bit1, symbol: 0}
}

func newNodeList(freq int, symbol byte) *node {
	return &node{freq: freq, bit0: nil, bit1: nil, symbol: symbol}
}

// Приоритетная очередь узлов
type priorityNodeQueue struct {
	size      int
	sortOrder []int
	storage   map[int][]*node
}

func newPriorityNodeQueue(size int, storage map[int][]*node) *priorityNodeQueue {
	return &priorityNodeQueue{size: size, storage: storage}
}

func enqueue(q *priorityNodeQueue, priority int, item *node) {
	if q.storage[priority] == nil {
		q.storage[priority] = make([]*node, 0)
	}
	q.storage[priority] = append(q.storage[priority], item)
	q.size++
}

func sortQueue(q *priorityNodeQueue) {
	if q.size > 0 {
		q.sortOrder = make([]int, 0)
		for key, value := range q.storage {
			if len(value) > 0 {
				q.sortOrder = append(q.sortOrder, key)
			}
		}
		sort.Ints(q.sortOrder)
	}
}

func dequeue(q *priorityNodeQueue) *node {
	var result *node
	if q.size > 0 {
		q.size--
		for _, value := range q.sortOrder {
			if len(q.storage[value]) > 0 {
				result = q.storage[value][0]
				if len(q.storage[value]) > 1 {
					q.storage[value] = q.storage[value][1:]
				} else {
					q.storage[value] = make([]*node, 0)
				}
				break
			}
		}
	}
	return result
}

// Сжатие
func compressBits(data []byte, codes []string) []byte {
	var (
		bits      = make([]byte, 0)
		sum  byte = 0
		bit  byte = 1
		line string
		one  = "1"
	)

	for _, symbol := range data {
		line = codes[symbol]
		for i := 0; i < len(line); i++ {
			if line[i] == one[0] {
				sum |= bit
			}
			if bit == 128 {
				bits = append(bits, sum)
				sum = 0
				bit = 1
			} else {
				bit <<= 1
			}
		}
	}

	if bit > 1 {
		bits = append(bits, sum)
	}

	return bits
}

// Построение кодов Хаффмана
func createHuffmanCode(root *node) []string {
	var codes = make([]string, 256)
	nodeNext(root, codes, "")
	return codes
}

// Следующий узел
func nodeNext(item *node, codes []string, code string) {
	if item.bit0 == nil {
		codes[item.symbol] = code
	} else {
		nodeNext(item.bit0, codes, code+"0")
		nodeNext(item.bit1, codes, code+"1")
	}
}

// Построение дерева Хаффмана
func createHuffmanTree(freq []int) *node {
	// Используем структуру priorityNodeQueue для построения дерева Хаффмана
	pq := newPriorityNodeQueue(0, make(map[int][]*node))
	for index, value := range freq {
		if value > 0 {
			var nodeList = newNodeList(value, (byte)(index&255))
			enqueue(pq, value, nodeList)
		}
	}
	sortQueue(pq)

	for pq.size > 1 {
		var bit0 = dequeue(pq)
		var bit1 = dequeue(pq)
		var parent = newNode(bit0.freq+bit1.freq, bit0, bit1)
		enqueue(pq, parent.freq, parent)
		sortQueue(pq)
	}

	return dequeue(pq)
}

// Создание заголовка архива
func createHeader(dataLength int, freq []int) []byte {
	var head = make([]byte, 0)

	// Размер файла (4 байта)
	head = append(head, (byte)(dataLength&255))
	head = append(head, (byte)((dataLength>>8)&255))
	head = append(head, (byte)((dataLength>>16)&255))
	head = append(head, (byte)((dataLength>>24)&255))

	var count = 0
	var maxFreq = 0
	for index, value := range freq {
		if freq[index] > 0 {
			count++
			if value > maxFreq {
				maxFreq = value
			}
		}
	}

	// Размер таблицы частот
	head = append(head, (byte)(count))

	// Нормировка таблицы (если максимальная частота превышает размер байта)
	if maxFreq > 255 {
		for index, value := range freq {
			if freq[index] > 0 {
				if value*255/maxFreq > 0 {
					freq[index] = value * 255 / maxFreq
				} else {
					freq[index] = 1
				}
			}
		}
	}

	for index, value := range freq {
		if value > 0 {
			head = append(head, (byte)(index&255))
			head = append(head, (byte)(value&255))
		}
	}
	return head
}

// Расчет частот для всех символов
func calculateFreq(data []byte) []int {
	var freq = make([]int, 256)
	for _, chr := range data {
		freq[chr]++
	}
	return freq
}

// Сжатие файла
func compressFile(fromFile string, toFile string) {
	var data, arch []byte

	fmt.Println("Читаем файл:", fromFile)
	data, err := os.ReadFile(fromFile)
	if err != nil {
		fmt.Println("Ошибка чтения файла:", err)
		return
	}

	fmt.Println("Архивируем файл")
	arch, err = doCompress(data)
	if err != nil {
		fmt.Println("Ошибка архивации файла:", err)
		return
	}
	fmt.Println("Длина исходного файл (байт):", len(data))
	fmt.Println("Длина архива (байт):", len(arch))
	fmt.Println("Соотношение (%):", (100*len(arch))/len(data))

	fmt.Println("Сохраняем архив:", toFile)
	err = os.WriteFile(toFile, arch, 0777)
	if err != nil {
		fmt.Println("Ошибка сохранения архива:", err)
		return
	}
}

// Архивация
func doCompress(data []byte) ([]byte, error) {
	var (
		freq  []int
		head  []byte
		root  *node
		codes []string
		bits  []byte
	)

	freq = calculateFreq(data)
	head = createHeader(len(data), freq)
	root = createHuffmanTree(freq)
	codes = createHuffmanCode(root)
	bits = compressBits(data, codes)
	return slices.Concat(head, bits), nil
}

// Разархивация файла
func decompressFile(fromFile string, toFile string) {
	var arch, data []byte

	fmt.Println("Читаем архив:", fromFile)
	arch, err := os.ReadFile(fromFile)
	if err != nil {
		fmt.Println("Ошибка чтения архива:", err)
		return
	}

	fmt.Println("Разархивируем файл")
	data, err = doDecompress(arch)
	if err != nil {
		fmt.Println("Ошибка разархивации файла:", err)
		return
	}

	fmt.Println("Сохраняем файл:", toFile)
	err = os.WriteFile(toFile, data, 0777)
	if err != nil {
		fmt.Println("Ошибка сохранения файла:", err)
		return
	}
}

// Разархивация
func doDecompress(arch []byte) ([]byte, error) {
	var (
		dataLength = 0
		startIndex = 0
		freq       []int
		root       *node
		data       []byte
	)

	dataLength, startIndex, freq = parseHeader(arch)
	root = createHuffmanTree(freq)
	data = decompressBytes(arch, dataLength, startIndex, root)

	return data, nil
}

// Разархивация потока байтов
func decompressBytes(arch []byte, dataLength int, startIndex int, root *node) []byte {
	var size = 0
	var current = root
	var data = make([]byte, dataLength)

	for index := startIndex; index < len(arch); index++ {
		for bit := 1; bit <= 128; bit <<= 1 {
			if (arch[index] & (byte)(bit)) == 0 {
				current = current.bit0
			} else {
				current = current.bit1
			}
			if current.bit0 == nil {
				if size < dataLength {
					data[size] = current.symbol
					size++
				}
				current = root
			}
		}
	}

	return data
}

// Разбор заголовка архива
func parseHeader(arch []byte) (dataLength int, startIndex int, freq []int) {
	dataLength = (int)(arch[0]) + ((int)(arch[1]) << 8) + ((int)(arch[2]) << 16) + ((int)(arch[3]) << 24)
	var count = (int)(arch[4])
	if count == 0 {
		count = 256
	}
	var symbol byte
	freq = make([]int, 256)
	for index := 0; index < count; index++ {
		symbol = arch[5+index*2]
		freq[symbol] = (int)(arch[5+index*2+1])
	}
	startIndex = 4 + 1 + 2*count
	return dataLength, startIndex, freq
}

// Имя файла без расширения
func getOnlyName(fullName string) string {
	fileName := filepath.Base(fullName)
	extension := filepath.Ext(fullName)
	result := fileName[:len(fileName)-len(extension)]
	return result
}

// Основная функция
func main() {
	var (
		archMode   byte = 0
		sourceName      = ""
		destName        = ""
	)

	if len(os.Args) < 3 {
		fmt.Println("Параметры командной строки: -ключ имя_файла")
		fmt.Println("Ключи: а - архивировать файл, x - разархивировать файл")
	} else {
		if os.Args[1] == "-a" {
			archMode = 1
		} else {
			archMode = 2
		}

		sourceName = os.Args[2]
		_, err := os.Stat(sourceName)
		if os.IsNotExist(err) {
			fmt.Println("Файл не найден: ", sourceName)
		}
	}

	if archMode == 1 {
		destName = getOnlyName(sourceName) + ".hff"
		compressFile(sourceName, destName)
	} else if archMode == 2 {
		destName = getOnlyName(sourceName) + ".txt"
		decompressFile(sourceName, destName)
	}
}

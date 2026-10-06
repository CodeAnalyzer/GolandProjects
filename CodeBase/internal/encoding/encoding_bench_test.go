package encoding

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

// benchBuffer строит синтетический буфер заданного размера: ASCII-код SQL
// с вкраплениями кириллических комментариев в CP1251 (типичный профиль
// legacy-файла Diasoft).
func benchBuffer(size int) []byte {
	comment, err := charmap.Windows1251.NewEncoder().Bytes([]byte("Процедура начисления процентов по договору"))
	if err != nil {
		panic(err)
	}
	cp866Word, err := charmap.CodePage866.NewEncoder().Bytes([]byte("РАСЧЁТ"))
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 0, size)
	line := []byte("select A.field1, B.field2 from Table1 A join Table2 B on A.id = B.id -- ")
	i := 0
	for len(buf) < size {
		buf = append(buf, line...)
		switch i % 3 {
		case 0:
			buf = append(buf, comment...)
		case 1:
			buf = append(buf, cp866Word...)
		}
		buf = append(buf, '\n')
		i++
	}
	return buf[:size]
}

func benchmarkDetect(b *testing.B, size int) {
	data := benchBuffer(size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DetectFromBytesWithPrior(data, CP866)
	}
}

func BenchmarkDetectFromBytesWithPrior_1KB(b *testing.B)   { benchmarkDetect(b, 1<<10) }
func BenchmarkDetectFromBytesWithPrior_100KB(b *testing.B) { benchmarkDetect(b, 100<<10) }
func BenchmarkDetectFromBytesWithPrior_1MB(b *testing.B)   { benchmarkDetect(b, 1<<20) }

// BenchmarkSHA256 — ориентир: детекция обязана быть не медленнее хэша,
// который индексатор уже платит за каждый файл.
func benchmarkSHA(b *testing.B, size int) {
	data := benchBuffer(size)
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sha256.Sum256(data)
	}
}

func BenchmarkSHA256_1KB(b *testing.B)   { benchmarkSHA(b, 1<<10) }
func BenchmarkSHA256_100KB(b *testing.B) { benchmarkSHA(b, 100<<10) }
func BenchmarkSHA256_1MB(b *testing.B)   { benchmarkSHA(b, 1<<20) }

func BenchmarkDetectASCIIFastPath(b *testing.B) {
	data := make([]byte, 100<<10)
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DetectFromBytesWithPrior(data, CP866)
	}
}

func ExampleDetectFromBytesWithPrior() {
	cp1251Bytes, _ := charmap.Windows1251.NewEncoder().Bytes([]byte("тип тура"))
	fmt.Println(DetectFromBytesWithPrior(cp1251Bytes, CP866))
	// Output: WIN1251
}

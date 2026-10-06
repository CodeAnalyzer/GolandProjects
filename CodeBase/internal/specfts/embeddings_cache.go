package specfts

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

// EmbeddingRow — строка кэша эмбеддингов: метаданные сериализуются в JSON,
// эмбеддинг — плотным блоком float32 (LE) отдельно от метаданных.
// Реализуется типами строк кэша (desc-корпус, spec-корпус).
type EmbeddingRow interface {
	// EmbeddingDim — размерность эмбеддинга строки.
	EmbeddingDim() int
	// EmbeddingFloats — значения эмбеддинга (len == EmbeddingDim).
	EmbeddingFloats() []float32
	// SetEmbedding заполняет эмбеддинг при чтении кэша.
	SetEmbedding(dim int, floats []float32)
}

// WriteEmbeddingsCache сериализует кэш эмбеддингов: magic + generation +
// JSON-метаданные строк + count + dim + плоский блок float32 (LE).
// Запись через temp + rename (атомарность на Windows и Unix).
// magic определяет версию формата/владельца кэша: несовпадение при чтении — промах.
func WriteEmbeddingsCache[R EmbeddingRow](path, magic, generation string, rows []R) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()
	// Буфер обязателен: миллионы одиночных f.Write занимают десятки секунд
	bw := bufio.NewWriterSize(f, 1<<20)
	writeUint32 := func(v uint32) error {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		_, err := bw.Write(b[:])
		return err
	}
	writeString := func(s string) error {
		if err := writeUint32(uint32(len(s))); err != nil {
			return err
		}
		_, err := bw.Write([]byte(s))
		return err
	}

	if err := writeString(magic); err != nil {
		return err
	}
	if err := writeString(generation); err != nil {
		return err
	}
	meta, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if err := writeString(string(meta)); err != nil {
		return err
	}
	dim := 0
	if len(rows) > 0 {
		dim = rows[0].EmbeddingDim()
	}
	if err := writeUint32(uint32(len(rows))); err != nil {
		return err
	}
	if err := writeUint32(uint32(dim)); err != nil {
		return err
	}
	buf := make([]byte, 4)
	for i := range rows {
		for _, v := range rows[i].EmbeddingFloats() {
			binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
			if _, err := bw.Write(buf); err != nil {
				return err
			}
		}
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// ReadEmbeddingsCache читает кэш эмбеддингов. os.ErrNotExist — кэша нет;
// несовпадение magic или generation, усечённость и битый JSON — ошибка
// (промах: вызывающий пересобирает кэш из БД).
func ReadEmbeddingsCache[R EmbeddingRow](path, magic, generation string) ([]R, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	off := 0
	readString := func() (string, error) {
		if len(data)-off < 4 {
			return "", fmt.Errorf("embeddings cache: truncated string header")
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if n < 0 || n > len(data)-off {
			return "", fmt.Errorf("embeddings cache: truncated string")
		}
		s := string(data[off : off+n])
		off += n
		return s, nil
	}

	storedMagic, err := readString()
	if err != nil {
		return nil, err
	}
	if storedMagic != magic {
		return nil, fmt.Errorf("embeddings cache: bad magic")
	}
	storedGeneration, err := readString()
	if err != nil {
		return nil, err
	}
	if storedGeneration != generation {
		return nil, fmt.Errorf("embeddings cache: generation mismatch")
	}
	metaJSON, err := readString()
	if err != nil {
		return nil, err
	}
	var rows []R
	if err := json.Unmarshal([]byte(metaJSON), &rows); err != nil {
		return nil, fmt.Errorf("embeddings cache: meta: %w", err)
	}
	if len(data)-off < 8 {
		return nil, fmt.Errorf("embeddings cache: truncated dims")
	}
	count := int(binary.LittleEndian.Uint32(data[off:]))
	dim := int(binary.LittleEndian.Uint32(data[off+4:]))
	off += 8
	if len(rows) != count || len(data)-off < count*dim*4 {
		return nil, fmt.Errorf("embeddings cache: truncated floats")
	}
	for i := range rows {
		floats := make([]float32, dim)
		for j := 0; j < dim; j++ {
			floats[j] = math.Float32frombits(binary.LittleEndian.Uint32(data[off:]))
			off += 4
		}
		rows[i].SetEmbedding(dim, floats)
	}
	return rows, nil
}

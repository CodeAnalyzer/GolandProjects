package trc

import (
	"math/rand"
	"testing"
)

func TestDecodeUTF16LEBytes_ASCII(t *testing.T) {
	// "Hello" в UTF-16LE
	b := []byte{'H', 0, 'e', 0, 'l', 0, 'l', 0, 'o', 0}
	got := decodeUTF16LEBytes(b)
	want := "Hello"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecodeUTF16LEBytes_Cyrillic(t *testing.T) {
	// "Привет" в UTF-16LE: П=0x041F, р=0x0440, и=0x0438, в=0x0432, е=0x0435, т=0x0442
	b := []byte{0x1F, 0x04, 0x40, 0x04, 0x38, 0x04, 0x32, 0x04, 0x35, 0x04, 0x42, 0x04}
	got := decodeUTF16LEBytes(b)
	want := "Привет"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecodeUTF16LEBytes_SurrogatePair(t *testing.T) {
	// U+1F600 (😀) = D83D DE00 в UTF-16LE
	b := []byte{0x3D, 0xD8, 0x00, 0xDE}
	got := decodeUTF16LEBytes(b)
	want := "😀"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecodeUTF16LEBytes_OddLength(t *testing.T) {
	// 3 байта — последний отбрасывается
	b := []byte{'A', 0, 'B'}
	got := decodeUTF16LEBytes(b)
	want := "A"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecodeUTF16LEBytes_Empty(t *testing.T) {
	got := decodeUTF16LEBytes(nil)
	if got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}

func TestDecodeUTF16LEBytes_NullBytes(t *testing.T) {
	// U+0000 в UTF-16LE
	b := []byte{0x00, 0x00}
	got := decodeUTF16LEBytes(b)
	want := "\x00"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecodeUTF16LEBytes_ParallelWithOld(t *testing.T) {
	// Сравнение decodeUTF16LEBytes со старым decodeUTF16 на случайных данных
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 1000; iter++ {
		n := rng.Intn(200) + 1
		b := make([]byte, n*2)
		for i := range b {
			b[i] = byte(rng.Intn(256))
		}
		gotNew := decodeUTF16LEBytes(b)
		gotOld := decodeUTF16(b)
		if gotNew != gotOld {
			t.Errorf("mismatch on iter %d: new=%q old=%q (bytes=%x)", iter, gotNew, gotOld, b)
		}
	}
}

func TestDecodeUTF16LEBytes_ParallelWithXELDecoder(t *testing.T) {
	// Сравнение с decodeUTF16LE из xel_format.go
	rng := rand.New(rand.NewSource(99))
	for iter := 0; iter < 1000; iter++ {
		n := rng.Intn(200) + 1
		b := make([]byte, n*2)
		for i := range b {
			b[i] = byte(rng.Intn(256))
		}
		gotNew := decodeUTF16LEBytes(b)
		gotOld := decodeUTF16LE(b)
		if gotNew != gotOld {
			t.Errorf("mismatch on iter %d: new=%q old=%q (bytes=%x)", iter, gotNew, gotOld, b)
		}
	}
}

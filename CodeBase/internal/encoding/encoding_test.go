package encoding

import (
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
)

func TestDetectEncoding(t *testing.T) {
	tests := []struct {
		ext  string
		want Encoding
	}{
		{ext: ".sql", want: CP866},
		{ext: ".h", want: CP866},
		{ext: ".tpr", want: CP866},
		{ext: ".pas", want: WIN1251},
		{ext: ".inc", want: WIN1251},
		{ext: ".js", want: WIN1251},
		{ext: ".smf", want: WIN1251},
		{ext: ".dfm", want: WIN1251},
		{ext: ".rpt", want: WIN1251},
		{ext: ".xml", want: UTF8},
	}

	for _, tt := range tests {
		t.Run(tt.ext, func(t *testing.T) {
			if got := DetectEncoding(tt.ext); got != tt.want {
				t.Fatalf("DetectEncoding(%q) = %q, want %q", tt.ext, got, tt.want)
			}
		})
	}
}

func TestReadFileDecodesCP866(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.sql")
	data, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Привет"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := ReadFile(path, CP866)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if got != "Привет" {
		t.Fatalf("decoded content = %q, want %q", got, "Привет")
	}
}

func TestReadFileBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.txt")
	want := []byte{1, 2, 3, 4}
	if err := os.WriteFile(path, want, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	got, err := ReadFileBytes(path)
	if err != nil {
		t.Fatalf("ReadFileBytes returned error: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("bytes = %v, want %v", got, want)
	}
}

func TestGetDecoder(t *testing.T) {
	if GetDecoder(CP866) == nil {
		t.Fatalf("expected CP866 decoder")
	}
	if GetDecoder(WIN1251) == nil {
		t.Fatalf("expected WIN1251 decoder")
	}
	if GetDecoder(UTF8) != nil {
		t.Fatalf("expected nil decoder for UTF8")
	}
}

func TestDetectEncodingFromContent(t *testing.T) {
	cp866Data, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Тест"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}

	if got := DetectEncodingFromContent(cp866Data); got != CP866 {
		t.Fatalf("DetectEncodingFromContent(CP866 data) = %q, want %q", got, CP866)
	}
	if got := DetectEncodingFromContent([]byte("plain ascii")); got != UTF8 {
		t.Fatalf("DetectEncodingFromContent(ascii) = %q, want %q", got, UTF8)
	}
}

func TestConvertToUTF8(t *testing.T) {
	if got, err := ConvertToUTF8("plain", UTF8); err != nil || got != "plain" {
		t.Fatalf("ConvertToUTF8 UTF8 = %q, %v", got, err)
	}

	encoded, err := charmap.Windows1251.NewEncoder().String("Привет")
	if err != nil {
		t.Fatalf("encode WIN1251: %v", err)
	}
	got, err := ConvertToUTF8(encoded, WIN1251)
	if err != nil {
		t.Fatalf("ConvertToUTF8 returned error: %v", err)
	}
	if got != "Привет" {
		t.Fatalf("ConvertToUTF8 = %q, want %q", got, "Привет")
	}
}

func TestHasCyrillic(t *testing.T) {
	if !hasCyrillic([]byte("Привет")) {
		t.Fatalf("hasCyrillic(Привет) = false, want true")
	}
	if !hasCyrillic([]byte{0x80}) {
		t.Fatalf("hasCyrillic(0x80) = false, want true")
	}
	if hasCyrillic([]byte("Hello World")) {
		t.Fatalf("hasCyrillic(ascii) = true, want false")
	}
	if hasCyrillic([]byte("")) {
		t.Fatalf("hasCyrillic(empty) = true, want false")
	}
	if hasCyrillic(nil) {
		t.Fatalf("hasCyrillic(nil) = true, want false")
	}
}

func TestDetectXMLEncoding(t *testing.T) {
	// Test: XML with windows-1251 declaration
	xml1251 := []byte(`<?xml version="1.0" encoding="windows-1251"?><Object><Name>Test</Name></Object>`)
	if got := DetectXMLEncoding(xml1251); got != WIN1251 {
		t.Fatalf("DetectXMLEncoding(windows-1251 declaration) = %q, want %q", got, WIN1251)
	}

	// Test: XML with UTF-8 declaration
	xmlUTF8 := []byte(`<?xml version="1.0" encoding="UTF-8"?><Object><Name>Test</Name></Object>`)
	if got := DetectXMLEncoding(xmlUTF8); got != UTF8 {
		t.Fatalf("DetectXMLEncoding(UTF-8 declaration) = %q, want %q", got, UTF8)
	}

	// Test: XML with cp1251 (lowercase) declaration
	xmlCP1251 := []byte(`<?xml version="1.0" encoding="cp1251"?><Object><Name>Test</Name></Object>`)
	if got := DetectXMLEncoding(xmlCP1251); got != WIN1251 {
		t.Fatalf("DetectXMLEncoding(cp1251 declaration) = %q, want %q", got, WIN1251)
	}

	// Test: XML without declaration - valid UTF-8 content
	xmlNoDeclUTF8 := []byte(`<Object><Name>Проверка UTF-8</Name></Object>`)
	if got := DetectXMLEncoding(xmlNoDeclUTF8); got != UTF8 {
		t.Fatalf("DetectXMLEncoding(no decl, valid UTF-8) = %q, want %q", got, UTF8)
	}

	// Test: XML without declaration - CP1251 content (invalid UTF-8)
	// "Проверка" encoded in WIN1251
	cp1251Data := []byte{0xCF, 0xF0, 0xEE, 0xE2, 0xE5, 0xF0, 0xEA, 0xE0} // "Проверка" in WIN1251
	xmlNoDeclCP1251 := append([]byte(`<Object><RusName>`), append(cp1251Data, []byte(`</RusName></Object>`)...)...)
	if got := DetectXMLEncoding(xmlNoDeclCP1251); got != WIN1251 {
		t.Fatalf("DetectXMLEncoding(no decl, invalid UTF-8) = %q, want %q", got, WIN1251)
	}

	// Test: Empty content defaults to UTF8
	if got := DetectXMLEncoding([]byte("")); got != UTF8 {
		t.Fatalf("DetectXMLEncoding(empty) = %q, want %q", got, UTF8)
	}
}

func TestDecodeBytes(t *testing.T) {
	// Test: UTF8 passthrough
	utf8Data := []byte("Hello UTF-8")
	got, err := DecodeBytes(utf8Data, UTF8)
	if err != nil || got != "Hello UTF-8" {
		t.Fatalf("DecodeBytes(UTF8) = %q, %v, want %q", got, err, "Hello UTF-8")
	}

	// Test: WIN1251 decoding
	cp1251Bytes, err := charmap.Windows1251.NewEncoder().Bytes([]byte("Проверка"))
	if err != nil {
		t.Fatalf("encode WIN1251: %v", err)
	}
	got, err = DecodeBytes(cp1251Bytes, WIN1251)
	if err != nil {
		t.Fatalf("DecodeBytes(WIN1251) returned error: %v", err)
	}
	if got != "Проверка" {
		t.Fatalf("DecodeBytes(WIN1251) = %q, want %q", got, "Проверка")
	}

	// Test: CP866 decoding
	cp866Bytes, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Тест"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	got, err = DecodeBytes(cp866Bytes, CP866)
	if err != nil {
		t.Fatalf("DecodeBytes(CP866) returned error: %v", err)
	}
	if got != "Тест" {
		t.Fatalf("DecodeBytes(CP866) = %q, want %q", got, "Тест")
	}
}

func TestDetectFromBytes_ASCII(t *testing.T) {
	data := []byte("plain ascii text")
	if got := DetectFromBytes(data); got != CP866 {
		t.Fatalf("DetectFromBytes(ascii) = %q, want %q (CP866 as ASCII-compatible)", got, CP866)
	}
}

func TestDetectFromBytes_UTF8(t *testing.T) {
	data := []byte("Привет мир UTF-8")
	if got := DetectFromBytes(data); got != UTF8 {
		t.Fatalf("DetectFromBytes(valid UTF-8) = %q, want %q", got, UTF8)
	}
}

func TestDetectFromBytes_CP1251(t *testing.T) {
	// "Проверка" encoded in WIN1251 — bytes 0xC0-0xDF dominate
	cp1251Data := []byte{0xCF, 0xF0, 0xEE, 0xE2, 0xE5, 0xF0, 0xEA, 0xE0}
	if got := DetectFromBytes(cp1251Data); got != WIN1251 {
		t.Fatalf("DetectFromBytes(CP1251 data) = %q, want %q", got, WIN1251)
	}
}

func TestDetectFromBytes_CP866(t *testing.T) {
	// CP866 data — bytes 0x80-0x9F dominate (А-Я in CP866)
	cp866Data, err := charmap.CodePage866.NewEncoder().Bytes([]byte("АБВГД"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	if got := DetectFromBytes(cp866Data); got != CP866 {
		t.Fatalf("DetectFromBytes(CP866 data) = %q, want %q", got, CP866)
	}
}

func TestDetectFromBytes_CP866LowercaseNotMisdetected(t *testing.T) {
	// "Получение счета" in CP866 — uppercase П (0x8F) + lowercase letters in 0xA0-0xAF/0xE0-0xEF.
	// Must NOT be misdetected as CP1251 (regression for real Diasoft SQL files).
	cp866Data, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Получение счета"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	if got := DetectFromBytes(cp866Data); got != CP866 {
		t.Fatalf("DetectFromBytes(CP866 lowercase) = %q, want %q", got, CP866)
	}
}

func TestDetectFromBytes_Empty(t *testing.T) {
	if got := DetectFromBytes([]byte{}); got != CP866 {
		t.Fatalf("DetectFromBytes(empty) = %q, want %q (CP866 as ASCII-compatible)", got, CP866)
	}
}

func TestDetectFromBytes_MostlyUTF8WithFewInvalidBytes(t *testing.T) {
	// RTI-лог: преимущественно ASCII + UTF-8 кириллица в RetValContext,
	// но с единичными невалидными байтами (0x98 — CP866 Ш, 0xC2 0xE8 — некорректная
	// UTF-8 пара для "ё"). utf8.Valid вернёт false, но isLikelyUTF8 должна вернуть true.
	utf8Text := []byte("Отбор объектов старт")                   // валидный UTF-8
	invalidByte := []byte{0x98}                                   // CP866 Ш, невалидный UTF-8
	invalidPair := []byte{0xC2, 0xE8}                             // C2+non-continuation, невалидный UTF-8
	ascii := []byte("RetVal = 0#Enter proc @@NestLevel = 1\n")    // ASCII структура RTI

	// Строим данные как в реальном RTI-файле: много ASCII, немного UTF-8, единичные артефакты
	var data []byte
	for i := 0; i < 50; i++ {
		data = append(data, ascii...)
		data = append(data, utf8Text...)
	}
	data = append(data, invalidByte...)
	data = append(data, invalidPair...)

	if utf8.Valid(data) {
		t.Fatal("test data must NOT be valid UTF-8 (precondition)")
	}
	if got := DetectFromBytes(data); got != UTF8 {
		t.Fatalf("DetectFromBytes(mostly UTF-8 with few invalid bytes) = %q, want %q", got, UTF8)
	}
}

func TestDetectFromBytes_PureCP866NotMistakenForUTF8(t *testing.T) {
	// Чистый CP866 файл: все высокие байты — одиночные CP866 символы,
	// не образующие валидных UTF-8 последовательностей → должен вернуть CP866.
	cp866Data, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Расчёт приоритетов начисления"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	if got := DetectFromBytes(cp866Data); got == UTF8 {
		t.Fatalf("DetectFromBytes(pure CP866) = UTF8, must not return UTF8")
	}
}

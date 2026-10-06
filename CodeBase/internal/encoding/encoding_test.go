package encoding

import (
	"os"
	"path/filepath"
	"strings"
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

func TestNormalizeMojibake(t *testing.T) {
	if got := NormalizeMojibake("Договор эквайринга"); got != "Договор эквайринга" {
		t.Fatalf("normal UTF-8 text changed: %q", got)
	}

	cp866Mojibake := "╨Ф╨╛╨│╨╛╨▓╨╛╤А"
	if got := NormalizeMojibake(cp866Mojibake); got != "Договор" {
		t.Fatalf("CP866 mojibake = %q, want %q", got, "Договор")
	}

	utf8Bytes := []byte("Договор")
	cp1251MojibakeBytes, err := charmap.Windows1251.NewDecoder().Bytes(utf8Bytes)
	if err != nil {
		t.Fatalf("decode UTF-8 bytes as CP1251: %v", err)
	}
	cp1251Mojibake := string(cp1251MojibakeBytes)
	if got := NormalizeMojibake(cp1251Mojibake); got != "Договор" {
		t.Fatalf("CP1251 mojibake = %q, want %q", got, "Договор")
	}
}

func TestDetectFromBytesWithPrior(t *testing.T) {
	// ASCII → prior (декод identity), для обоих кандидатов prior.
	ascii := []byte("select 1 -- plain ascii comment")
	if got := DetectFromBytesWithPrior(ascii, CP866); got != CP866 {
		t.Fatalf("ASCII with CP866 prior = %q, want CP866", got)
	}
	if got := DetectFromBytesWithPrior(ascii, WIN1251); got != WIN1251 {
		t.Fatalf("ASCII with WIN1251 prior = %q, want WIN1251", got)
	}

	// Валидный UTF-8 с кириллицей → UTF8 независимо от prior.
	utf8Cyr := []byte("Процедура начисления")
	if got := DetectFromBytesWithPrior(utf8Cyr, CP866); got != UTF8 {
		t.Fatalf("UTF-8 Cyrillic = %q, want UTF8", got)
	}

	// Равенство счёта → prior: 2 байта 0x80-0x9F против 2 байт 0xC0-0xDF.
	tie := []byte{0x80, 0x81, 0xC0, 0xC1}
	if got := DetectFromBytesWithPrior(tie, WIN1251); got != WIN1251 {
		t.Fatalf("tie with WIN1251 prior = %q, want WIN1251", got)
	}
	if got := DetectFromBytesWithPrior(tie, CP866); got != CP866 {
		t.Fatalf("tie with CP866 prior = %q, want CP866", got)
	}

	// DetectFromBytes без prior эквивалентен prior CP866 (регрессия RTI/review).
	if got := DetectFromBytes(tie); got != CP866 {
		t.Fatalf("DetectFromBytes(tie) = %q, want CP866", got)
	}
}

func TestDetectFromBytesWithPriorCP1251LowercaseArtifactSet(t *testing.T) {
	// Сточный русский текст в CP1251 без единой заглавной: байты 0xF2-0xFF
	// (строчные р-я в CP1251, украинские буквы/спецруны в CP866) перевешивают.
	// "процедура расчёта процентов" — только строчные.
	cp1251Bytes, err := charmap.Windows1251.NewEncoder().Bytes([]byte("процедура расчёта -- тип тура яч"))
	if err != nil {
		t.Fatalf("encode CP1251: %v", err)
	}
	if got := DetectFromBytesWithPrior(cp1251Bytes, CP866); got != WIN1251 {
		t.Fatalf("CP1251 lowercase-only = %q, want WIN1251", got)
	}
}

func TestDetectFromBytesWithPriorCP866FramesStay(t *testing.T) {
	// Легитимный CP866-файл: рамка комментария (байты 0xC4 ─, 0xB3 │, углы
	// 0xC0/0xDA/0xD9) плюс кириллица в 0x80-0x9F (заглавные) и 0xA0-0xAF.
	// Кириллические заглавные должны перевесить рамку.
	enc := charmap.CodePage866.NewEncoder()
	frame, err := enc.Bytes([]byte("┌────────┐\n│ НАЧИСЛЕНИЕ ПРОЦЕНТОВ по договору │\n└────────┘\n-- строчный комментарий о расчёте"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	if got := DetectFromBytesWithPrior(frame, CP866); got != CP866 {
		t.Fatalf("CP866 with frames = %q, want CP866", got)
	}
}

func TestDetectFromBytesWithPriorInvalidByteFallback(t *testing.T) {
	// Байт 0x98 не мапится ни в UTF-8, ни в CP1251 (в CP866 — з) — детекция
	// обязана вернуть детерминированный результат, а не упасть.
	data := []byte("select 'x' -- \x98\x80\x81 comment")
	if got := DetectFromBytesWithPrior(data, CP866); got != CP866 {
		t.Fatalf("invalid-byte file = %q, want CP866 (2 cp866 votes vs 0 cp1251)", got)
	}
}

func TestDetectFromBytesFixtureGoldenDecode(t *testing.T) {
	// Fixture в стиле fa-administrator: CP1251 SQL с header-описанием процедуры.
	// До детекции такой файл декодировался CP866 в mojibake «╧ЁюЎхфєЁр…».
	header := "Процедура: Начисление процентов по договору\n-- параметры расчёта тип тура\ncreate procedure CalcPercent\nas\nbegin\n  select 1\nend\n"
	cp1251Bytes, err := charmap.Windows1251.NewEncoder().Bytes([]byte(header))
	if err != nil {
		t.Fatalf("encode CP1251: %v", err)
	}

	if got := DetectFromBytesWithPrior(cp1251Bytes, CP866); got != WIN1251 {
		t.Fatalf("fa-administrator fixture detection = %q, want WIN1251", got)
	}

	decoded, err := DecodeBytes(cp1251Bytes, WIN1251)
	if err != nil {
		t.Fatalf("decode WIN1251: %v", err)
	}
	if !strings.Contains(decoded, "Процедура: Начисление процентов") {
		t.Fatalf("decoded header lost Russian text: %q", decoded)
	}
	// Golden: в корректном декоде нет рун U+2500–U+259F (псевдографика mojibake).
	for _, r := range decoded {
		if r >= 0x2500 && r <= 0x259F {
			t.Fatalf("decoded text contains box-drawing rune U+%04X (mojibake artifact)", r)
		}
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
	// UTF-8 пара для "ё"). utf8.Valid вернёт false, но эвристика «почти UTF-8» (scanUTF8AndScore) должна вернуть UTF8.
	utf8Text := []byte("Отбор объектов старт")                 // валидный UTF-8
	invalidByte := []byte{0x98}                                // CP866 Ш, невалидный UTF-8
	invalidPair := []byte{0xC2, 0xE8}                          // C2+non-continuation, невалидный UTF-8
	ascii := []byte("RetVal = 0#Enter proc @@NestLevel = 1\n") // ASCII структура RTI

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

func TestDetectMarkdownEncoding(t *testing.T) {
	// UTF-8 без BOM — типичный openspec-артефакт
	if got := DetectMarkdownEncoding([]byte("# Лимиты по операциям\n")); got != UTF8 {
		t.Fatalf("DetectMarkdownEncoding(UTF-8 no BOM) = %q, want UTF8", got)
	}
	// UTF-8 с BOM — BOM является валидной UTF-8 последовательностью ("# ка" в UTF-8)
	if got := DetectMarkdownEncoding([]byte{0xEF, 0xBB, 0xBF, '#', ' ', 0xD0, 0xBA, 0xD0, 0xB0}); got != UTF8 {
		t.Fatalf("DetectMarkdownEncoding(UTF-8 with BOM) = %q, want UTF8", got)
	}
	// CP1251 legacy-файл
	cp1251Data, err := charmap.Windows1251.NewEncoder().Bytes([]byte("# Привет из legacy README"))
	if err != nil {
		t.Fatalf("encode CP1251: %v", err)
	}
	if got := DetectMarkdownEncoding(cp1251Data); got != WIN1251 {
		t.Fatalf("DetectMarkdownEncoding(CP1251) = %q, want WIN1251", got)
	}
	// Чистый ASCII — валидный UTF-8
	if got := DetectMarkdownEncoding([]byte("# plain markdown")); got != UTF8 {
		t.Fatalf("DetectMarkdownEncoding(ASCII) = %q, want UTF8", got)
	}
}

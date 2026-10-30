package encoding

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// fixturePath возвращает путь к фикстуре Bug-reports относительно пакета.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "Bug-reports", name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture %s not found: %v", path, err)
	}
	return path
}

// encodeUTF16 кодирует строку в UTF-16 без BOM заданного порядка байтов.
func encodeUTF16(t *testing.T, s string, endian unicode.Endianness) []byte {
	t.Helper()
	enc := unicode.UTF16(endian, unicode.IgnoreBOM).NewEncoder()
	out, _, err := transform.Bytes(enc, []byte(s))
	if err != nil {
		t.Fatalf("encode UTF-16: %v", err)
	}
	return out
}

func TestDetectBOMOrUTF16_BOMs(t *testing.T) {
	if enc, ok := DetectBOMOrUTF16([]byte{0xFF, 0xFE, 'a', 0}); !ok || enc != UTF16LE {
		t.Fatalf("BOM FF FE = %q,%v want UTF16LE,true", enc, ok)
	}
	if enc, ok := DetectBOMOrUTF16([]byte{0xFE, 0xFF, 0, 'a'}); !ok || enc != UTF16BE {
		t.Fatalf("BOM FE FF = %q,%v want UTF16BE,true", enc, ok)
	}
	if enc, ok := DetectBOMOrUTF16([]byte{0xEF, 0xBB, 0xBF, 'a'}); !ok || enc != UTF8 {
		t.Fatalf("BOM EF BB BF = %q,%v want UTF8,true", enc, ok)
	}
}

func TestDetectBOMOrUTF16_TC01Fixture(t *testing.T) {
	data, err := os.ReadFile(fixturePath(t, "TC01_TechAddAgree_Status.sql"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if data[0] != 0xFF || data[1] != 0xFE {
		t.Fatalf("fixture must start with FF FE BOM, got % X", data[:2])
	}
	if got := DetectFromBytesWithPrior(data, CP866); got != UTF16LE {
		t.Fatalf("DetectFromBytesWithPrior(TC01, prior CP866) = %q, want UTF16LE", got)
	}
	if got := DetectXMLEncoding(data); got != UTF16LE {
		t.Fatalf("DetectXMLEncoding(TC01) = %q, want UTF16LE", got)
	}

	decoded, err := DecodeBytes(data, UTF16LE)
	if err != nil {
		t.Fatalf("DecodeBytes UTF16LE: %v", err)
	}
	if strings.ContainsRune(decoded, 0) {
		t.Fatal("decoded content contains NUL")
	}
	if !utf8.ValidString(decoded) {
		t.Fatal("decoded content is not valid UTF-8")
	}
	if !strings.Contains(decoded, "pgonline version:") {
		t.Fatal("decoded content lost marker '-- pgonline version:'")
	}
}

func TestDetectBOMOrUTF16_BEWithoutBOM(t *testing.T) {
	// ASCII-доминирующий текст (SQL с ASCII-структурой) в UTF-16BE без BOM:
	// NUL ложатся на чётные смещения.
	src := "create procedure pTest as\nbegin\n  -- plain ascii comment\n  select 1\nend\ngo\n"
	data := encodeUTF16(t, src, unicode.BigEndian)
	if enc, ok := DetectBOMOrUTF16(data); !ok || enc != UTF16BE {
		t.Fatalf("UTF16BE without BOM = %q,%v want UTF16BE,true", enc, ok)
	}
	decoded, err := DecodeBytes(data, UTF16BE)
	if err != nil {
		t.Fatalf("DecodeBytes UTF16BE: %v", err)
	}
	if decoded != src {
		t.Fatalf("roundtrip mismatch: %q", decoded)
	}
}

func TestDetectBOMOrUTF16_LEWithoutBOM(t *testing.T) {
	src := "select Field1, Field2 from Table where X = 1 -- comment line\n"
	data := encodeUTF16(t, src, unicode.LittleEndian)
	if enc, ok := DetectBOMOrUTF16(data); !ok || enc != UTF16LE {
		t.Fatalf("UTF16LE without BOM = %q,%v want UTF16LE,true", enc, ok)
	}
	if got := DetectFromBytesWithPrior(data, CP866); got != UTF16LE {
		t.Fatalf("DetectFromBytesWithPrior = %q, want UTF16LE", got)
	}
}

func TestDetectBOMOrUTF16_CP866WithRareNULs(t *testing.T) {
	// Легитимный однобайтовый файл с единичными NUL обеих чётностей:
	// плотность и чётность не проходят — не UTF-16.
	cp866Bytes, err := charmap.CodePage866.NewEncoder().Bytes([]byte("Процедура расчёта процентов по договору"))
	if err != nil {
		t.Fatalf("encode CP866: %v", err)
	}
	data := append([]byte{}, cp866Bytes...)
	for i := 0; i < 10; i++ { // 10 NUL на чётных и нечётных позициях
		data = append(data, byte(i))
	}
	for i := 0; i < 20; i++ {
		data = append(data, 0)
		if i%2 == 0 {
			data = append(data, 0)
		}
	}
	if enc, ok := DetectBOMOrUTF16(data); ok {
		t.Fatalf("CP866 with rare NULs misdetected as %q", enc)
	}
	if got := DetectFromBytesWithPrior(data, CP866); got != CP866 {
		t.Fatalf("CP866 with rare NULs = %q, want CP866", got)
	}
}

func TestDetectUTF16NULThresholds(t *testing.T) {
	// Плотность: 3000 NUL на 9999 байтах ≈ 30% — LE (все NUL на нечётных).
	n := 9999
	data := make([]byte, n)
	for i := range data {
		data[i] = 'x'
	}
	var count int
	for i := 1; i < n && count < 3000; i += 2 {
		data[i] = 0
		count++
	}
	if count*100/n < utf16NULDensityPct {
		t.Fatalf("precondition: density %d%% < %d%%", count*100/n, utf16NULDensityPct)
	}
	if enc, ok := DetectBOMOrUTF16(data); !ok || enc != UTF16LE {
		t.Fatalf("boundary density = %q,%v want UTF16LE,true", enc, ok)
	}

	// Ниже порога плотности — не UTF-16 (шаг 6: ~17% NUL в окне 4096).
	data2 := make([]byte, n)
	for i := range data2 {
		data2[i] = 'x'
	}
	for i := 1; i < n; i += 6 {
		data2[i] = 0
	}
	if enc, ok := DetectBOMOrUTF16(data2); ok {
		t.Fatalf("below density threshold misdetected as %q", enc)
	}

	// Чётность: 30%+ NUL, но распределены ~50/50 — не UTF-16.
	data3 := make([]byte, n)
	for i := range data3 {
		data3[i] = 'x'
	}
	for i := 0; i < n-1; i += 6 { // пары NUL (чёт+нечёт) с шагом 6
		data3[i] = 0
		data3[i+1] = 0
	}
	// Контроль: доля NUL ≥30%, но чётность ~50/50.
	var nul, oddNUL, evenNUL int
	for i, b := range data3 {
		if b == 0 {
			nul++
			if i%2 == 1 {
				oddNUL++
			} else {
				evenNUL++
			}
		}
	}
	if nul*100/n < utf16NULDensityPct {
		t.Fatalf("precondition: NUL density %d%% < %d%%", nul*100/n, utf16NULDensityPct)
	}
	if oddNUL*100/nul >= utf16NULParityPct || evenNUL*100/nul >= utf16NULParityPct {
		t.Fatalf("precondition: parity split %d/%d must be below %d%%", evenNUL, oddNUL, utf16NULParityPct)
	}
	if enc, ok := DetectBOMOrUTF16(data3); ok {
		t.Fatalf("mixed parity misdetected as %q", enc)
	}
}

func TestDecodeBytes_UTF8BOMStripped(t *testing.T) {
	data := append([]byte{0xEF, 0xBB, 0xBF}, []byte("# Заголовок")...)
	got, err := DecodeBytes(data, UTF8)
	if err != nil {
		t.Fatalf("DecodeBytes UTF8 BOM: %v", err)
	}
	if strings.HasPrefix(got, "\ufeff") {
		t.Fatalf("U+FEFF leaked into decoded content: %q", got)
	}
	if got != "# Заголовок" {
		t.Fatalf("DecodeBytes(UTF8+BOM) = %q", got)
	}
}

func TestDecodeBytes_UTF16Cyrillic(t *testing.T) {
	src := "-- Процедура начисления процентов\ncreate procedure pCalc as begin select 1 end\n"
	le := encodeUTF16(t, src, unicode.LittleEndian)
	withBOM := append([]byte{0xFF, 0xFE}, le...)

	got, err := DecodeBytes(withBOM, UTF16LE)
	if err != nil {
		t.Fatalf("DecodeBytes UTF16LE with BOM: %v", err)
	}
	if got != src {
		t.Fatalf("UTF16LE decode mismatch: %q", got)
	}
	if strings.ContainsRune(got, 0xFEFF) {
		t.Fatal("BOM leaked into decoded content")
	}

	be := encodeUTF16(t, src, unicode.BigEndian)
	gotBE, err := DecodeBytes(be, UTF16BE)
	if err != nil {
		t.Fatalf("DecodeBytes UTF16BE: %v", err)
	}
	if gotBE != src {
		t.Fatalf("UTF16BE decode mismatch: %q", gotBE)
	}
}

func TestReadFile_UTF16(t *testing.T) {
	src := "select 1 -- комментарий\n"
	path := filepath.Join(t.TempDir(), "sample.sql")
	data := append([]byte{0xFF, 0xFE}, encodeUTF16(t, src, unicode.LittleEndian)...)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	got, err := ReadFile(path, UTF16LE)
	if err != nil {
		t.Fatalf("ReadFile UTF16LE: %v", err)
	}
	if got != src {
		t.Fatalf("ReadFile UTF16LE = %q, want %q", got, src)
	}
}

func TestGetDecoder_UTF16(t *testing.T) {
	if GetDecoder(UTF16LE) == nil {
		t.Fatal("expected UTF16LE decoder")
	}
	if GetDecoder(UTF16BE) == nil {
		t.Fatal("expected UTF16BE decoder")
	}
}

func TestDetectXMLEncoding_GSODFixture(t *testing.T) {
	// Декларация encoding="UTF-8" при CP1251-байтах — декларация отвергается.
	data, err := os.ReadFile(fixturePath(t, "API_GSOD_MassCreate.xml"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	if got := DetectXMLEncoding(data); got != WIN1251 {
		t.Fatalf("DetectXMLEncoding(GSOD) = %q, want WIN1251", got)
	}
	decoded, err := DecodeBytes(data, WIN1251)
	if err != nil {
		t.Fatalf("decode WIN1251: %v", err)
	}
	if !strings.Contains(decoded, "<Object") {
		t.Fatal("decoded GSOD lost <Object> root")
	}
}

func TestDetectXMLEncoding_UTF16WithoutBOM(t *testing.T) {
	// ASCII-доминирующий UTF-16LE XML без BOM: NUL на нечётных смещениях.
	src := `<?xml version="1.0" encoding="UTF-16"?><Object><Name>Test ASCII</Name></Object>`
	data := encodeUTF16(t, src, unicode.LittleEndian)
	if got := DetectXMLEncoding(data); got != UTF16LE {
		t.Fatalf("DetectXMLEncoding(UTF16LE xml no BOM) = %q, want UTF16LE", got)
	}
}

func TestDetectXMLEncoding_UTF16BOM(t *testing.T) {
	src := `<?xml version="1.0" encoding="UTF-16"?><Object><Name>Тест</Name></Object>`
	data := append([]byte{0xFF, 0xFE}, encodeUTF16(t, src, unicode.LittleEndian)...)
	if got := DetectXMLEncoding(data); got != UTF16LE {
		t.Fatalf("DetectXMLEncoding(UTF16LE BOM xml) = %q, want UTF16LE", got)
	}
	decoded, err := DecodeBytes(data, UTF16LE)
	if err != nil {
		t.Fatalf("decode UTF16LE: %v", err)
	}
	if !strings.Contains(decoded, "Тест") {
		t.Fatalf("UTF-16 XML decode lost Cyrillic: %q", decoded)
	}
}

func TestDetectXMLEncoding_FalseUTF8DeclarationFallback(t *testing.T) {
	// Ложная декларация UTF-8, содержимое CP1251 — эвристика маркерных
	// диапазонов возвращает WIN1251.
	cp1251Body, err := charmap.Windows1251.NewEncoder().Bytes([]byte("<Object><RusName>Проверка Начисления</RusName></Object>"))
	if err != nil {
		t.Fatalf("encode CP1251: %v", err)
	}
	data := append([]byte(`<?xml version="1.0" encoding="utf-8"?>`), cp1251Body...)
	if got := DetectXMLEncoding(data); got != WIN1251 {
		t.Fatalf("DetectXMLEncoding(false utf-8 decl) = %q, want WIN1251", got)
	}
}

func TestDetectMarkdownEncoding_UTF16(t *testing.T) {
	src := "# Лимиты по операциям\n\n- пункт списка ASCII\n"
	data := append([]byte{0xFF, 0xFE}, encodeUTF16(t, src, unicode.LittleEndian)...)
	if got := DetectMarkdownEncoding(data); got != UTF16LE {
		t.Fatalf("DetectMarkdownEncoding(UTF16LE BOM) = %q, want UTF16LE", got)
	}

	noBOM := encodeUTF16(t, "# Section Heading\n\n- list item one\n- list item two\n\ntext paragraph ascii\n", unicode.LittleEndian)
	if got := DetectMarkdownEncoding(noBOM); got != UTF16LE {
		t.Fatalf("DetectMarkdownEncoding(UTF16LE no BOM) = %q, want UTF16LE", got)
	}
}

func TestReadFile_UTF16OverridesPrior(t *testing.T) {
	// Standalone-точки входа (ParseFile парсеров, review) передают prior
	// CP866/WIN1251 — BOM/NUL-детект в ReadFile перекрывает prior, файл
	// читается как UTF-16 без mojibake.
	src := "/* Начисление процентов: договор №7 */\nselect 1\n"
	data := append([]byte{0xFF, 0xFE}, encodeUTF16(t, src, unicode.LittleEndian)...)
	path := filepath.Join(t.TempDir(), "proc_utf16.sql")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	for _, prior := range []Encoding{CP866, WIN1251, UTF8} {
		got, err := ReadFile(path, prior)
		if err != nil {
			t.Fatalf("ReadFile(prior=%s): %v", prior, err)
		}
		if !strings.Contains(got, "Начисление процентов") {
			t.Fatalf("ReadFile(prior=%s) lost Cyrillic: %q", prior, got)
		}
		if strings.ContainsRune(got, 0) {
			t.Fatalf("ReadFile(prior=%s) contains NUL: %q", prior, got)
		}
	}
}

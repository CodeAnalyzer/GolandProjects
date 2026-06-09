package encoding

import (
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

// Encoding типы кодировок
type Encoding string

const (
	CP866   Encoding = "CP866"
	WIN1251 Encoding = "WIN1251"
	UTF8    Encoding = "UTF8"
)

// DetectEncoding определяет кодировку файла по расширению
func DetectEncoding(ext string) Encoding {
	switch ext {
	case ".sql", ".h", ".tpr":
		return CP866
	case ".pas", ".inc", ".js", ".smf", ".dfm", ".rpt":
		return WIN1251
	default:
		return UTF8
	}
}

// ReadFile читает файл с правильной кодировкой и возвращает UTF-8 строку
func ReadFile(path string, encoding Encoding) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var reader io.Reader = f

	switch encoding {
	case CP866:
		reader = transform.NewReader(f, charmap.CodePage866.NewDecoder())
	case WIN1251:
		reader = transform.NewReader(f, charmap.Windows1251.NewDecoder())
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// ReadFileBytes читает файл и возвращает сырые байты
func ReadFileBytes(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// GetDecoder возвращает декодер для кодировки
func GetDecoder(encoding Encoding) transform.Transformer {
	switch encoding {
	case CP866:
		return charmap.CodePage866.NewDecoder()
	case WIN1251:
		return charmap.Windows1251.NewDecoder()
	default:
		return nil
	}
}

// DetectEncodingFromContent пытается определить кодировку по содержимому
func DetectEncodingFromContent(data []byte) Encoding {
	// Простая эвристика: пробуем декодировать как CP866 и WIN1251
	// Если есть символы в диапазоне 0x80-0xFF, это не ASCII

	cp866Decoder := charmap.CodePage866.NewDecoder()
	win1251Decoder := charmap.Windows1251.NewDecoder()

	// Пробуем CP866
	cp866Result, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), cp866Decoder))
	if err == nil {
		// Проверяем, есть ли русские символы
		if hasCyrillic(cp866Result) {
			return CP866
		}
	}

	// Пробуем WIN1251
	win1251Result, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), win1251Decoder))
	if err == nil {
		if hasCyrillic(win1251Result) {
			return WIN1251
		}
	}

	return UTF8
}

// hasCyrillic проверяет наличие кириллических символов
func hasCyrillic(data []byte) bool {
	for _, b := range data {
		// Символы кириллицы в CP866 и WIN1251 находятся в диапазоне 0x80-0xFF
		if b >= 0x80 {
			return true
		}
	}
	return false
}

// ConvertToUTF8 конвертирует строку из указанной кодировки в UTF8
func ConvertToUTF8(input string, fromEncoding Encoding) (string, error) {
	if fromEncoding == UTF8 {
		return input, nil
	}

	decoder := GetDecoder(fromEncoding)
	if decoder == nil {
		return input, nil
	}

	result, _, err := transform.String(decoder, input)
	if err != nil {
		return "", err
	}

	return result, nil
}

// xmlEncodingRegexp ищет encoding в XML declaration: <?xml version="1.0" encoding="windows-1251"?>
var xmlEncodingRegexp = regexp.MustCompile(`<?xml[^>]*encoding=["']([^"']+)["'][^>]*?>`)

// DetectXMLEncoding определяет кодировку XML по declaration или содержимому
// 1. Ищет encoding в XML declaration
// 2. Если declaration нет или encoding не указан:
//    - Проверяет валидность UTF-8
//    - Если невалидно -> предполагает WIN1251 (Diasoft heuristic)
func DetectXMLEncoding(data []byte) Encoding {
	// Ищем XML declaration в начале файла (первые 200 байт достаточно)
	prefix := data
	if len(prefix) > 200 {
		prefix = prefix[:200]
	}

	match := xmlEncodingRegexp.FindSubmatch(prefix)
	if len(match) > 1 {
		enc := strings.ToLower(strings.TrimSpace(string(match[1])))
		switch enc {
		case "windows-1251", "cp1251", "windows1251":
			return WIN1251
		case "utf-8", "utf8":
			return UTF8
		case "cp866", "ibm866":
			return CP866
		}
	}

	// Нет declaration или encoding не указан - проверяем валидность UTF-8
	if utf8.Valid(data) {
		return UTF8
	}

	// Невалидный UTF-8 - для Diasoft файлов предполагаем WIN1251
	return WIN1251
}

// DecodeBytes декодирует байты из указанной кодировки в UTF-8 строку
func DecodeBytes(data []byte, encoding Encoding) (string, error) {
	if encoding == UTF8 {
		return string(data), nil
	}

	decoder := GetDecoder(encoding)
	if decoder == nil {
		return string(data), nil
	}

	result, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), decoder))
	if err != nil {
		return "", err
	}

	return string(result), nil
}

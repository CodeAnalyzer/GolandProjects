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

// DetectEncodingFromContent удалён (мёртвый код: hasCyrillic считал
// кириллицей любой байт >= 0x80, из-за чего WIN1251-ветка была недостижима).
// Детекция по содержимому — DetectFromBytesWithPrior (см. ниже).

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
//   - Проверяет валидность UTF-8
//   - Если невалидно -> предполагает WIN1251 (Diasoft heuristic)
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

// DetectFromBytes определяет кодировку по содержимому байтов без prior:
// эквивалентно вызову DetectFromBytesWithPrior с prior CP866 (default для
// Diasoft SQL). Существующие потребители (RTI, review) сохраняют поведение.
func DetectFromBytes(data []byte) Encoding {
	return DetectFromBytesWithPrior(data, CP866)
}

// DetectFromBytesWithPrior определяет кодировку по содержимому байтов,
// используя prior как предположение по умолчанию: результат для ASCII-файлов
// и tie-breaker при равенстве счёта. Индексатор передаёт prior из карты
// расширений (см. fswalk.getEncodingAndLanguage).
//
// Алгоритм:
//  1. Нет байт > 0x7F → prior (ASCII, декод — identity).
//  2. Валидный UTF-8 → UTF-8.
//  3. «Почти UTF-8»: ≥80% высоких байт входят в валидные многобайтные UTF-8
//     последовательности (`isLikelyUTF8`) → UTF-8 (для RTI-логов с единичными
//     CP866-артефактами).
//  4. Эвристика по неоднозначным маркерным диапазонам:
//     cp866Score  = байты 0x80–0x9F (заглавные А-Я в CP866, редкие спецсимволы в CP1251)
//     cp1251Score = байты 0xC0–0xDF (заглавные А-Я в CP1251, псевдографика в CP866)
//                  + байты 0xF2–0xFF без 0xFC (строчные р–я в CP1251 против
//                    украинских букв и спец-рун в CP866; № 0xFC легитимен в
//                    CP866 и исключён; 0xFF — «я» в CP1251 — включён)
//     Диапазоны 0xA0–0xBF и 0xE0–0xF1 — амбигуальны (строчные русские в обеих
//     кодировках / частая пунктуация), не голосуют. Побеждает бо́льший счёт;
//     при равенстве — prior. Сигнатура mojibake точна: байты CP1251-кириллицы
//     при CP866-декоде дают руны U+2500–U+259F и украинские буквы, которые
//     не встречаются в легитимном русском тексте.
func DetectFromBytesWithPrior(data []byte, prior Encoding) Encoding {
	hasHigh := false
	for _, b := range data {
		if b > 0x7F {
			hasHigh = true
			break
		}
	}
	if !hasHigh {
		return prior // ASCII compatible
	}

	if utf8.Valid(data) {
		return UTF8
	}

	// Файл не полностью валидный UTF-8, но может быть «почти UTF-8» —
	// например, RTI-логи с единичными байтами CP866 или некорректно закодированными
	// символами ё/Ё среди преимущественно UTF-8 контента. Проверка «почти UTF-8»
	// и маркерные счёты выполняются одним проходом (см. scanUTF8AndScore).
	validHigh, invalidHigh, cp866Score, cp1251Score := scanUTF8AndScore(data)

	totalHigh := validHigh + invalidHigh
	if totalHigh > 0 && validHigh*100/totalHigh >= 80 {
		return UTF8
	}

	switch {
	case cp1251Score > cp866Score:
		return WIN1251
	case cp866Score > cp1251Score:
		return CP866
	default:
		return prior
	}
}

// scanUTF8AndScore выполняет один проход по данным: считает байты, входящие
// в валидные многобайтные UTF-8 последовательности (validHigh) и не входящие
// (invalidHigh), одновременно накапливая маркерные счёты кодировок по тем же
// правилам, что и отдельный проход по сырым байтам (каждый байт ≥0x80
// оценивается switch-ем независимо от UTF-8-структуры — семантика совпадает
// с двумя независимыми проходами, но проход один).
func scanUTF8AndScore(data []byte) (validHigh, invalidHigh, cp866Score, cp1251Score int) {
	for i := 0; i < len(data); {
		b := data[i]
		if b < 0x80 {
			i++
			continue // ASCII — не учитываем
		}
		start := i
		r, size := utf8.DecodeRune(data[i:])
		if r == utf8.RuneError && size == 1 {
			invalidHigh++
			i++
		} else {
			validHigh += size
			i += size
		}
		for _, bb := range data[start:i] {
			switch {
			case bb >= 0x80 && bb <= 0x9F:
				cp866Score++
			case bb >= 0xC0 && bb <= 0xDF:
				cp1251Score++
			case bb >= 0xF2 && bb != 0xFC:
				cp1251Score++
			}
		}
	}
	return validHigh, invalidHigh, cp866Score, cp1251Score
}

// DetectMarkdownEncoding определяет кодировку markdown-файла по содержимому.
// Openspec-артефакты финпродуктов хранятся в UTF-8 (с BOM и без), но в дереве
// встречаются legacy-файлы в CP1251. Приоритет UTF-8: валидный UTF-8 (BOM —
// валидная UTF-8 последовательность) → UTF8; иначе — CP1251 fallback.
func DetectMarkdownEncoding(data []byte) Encoding {
	if utf8.Valid(data) {
		return UTF8
	}
	return WIN1251
}

// NormalizeMojibake восстанавливает UTF-8-текст, который уже был ошибочно
// интерпретирован как CP866 или Windows-1251. Нормальный UTF-8-текст не меняется.
func NormalizeMojibake(input string) string {
	if input == "" {
		return input
	}

	originalScore := mojibakeScore(input)
	if originalScore == 0 {
		return input
	}

	best := input
	bestScore := originalScore
	for _, sourceEncoding := range []Encoding{CP866, WIN1251} {
		bytes, err := charmapEncoder(sourceEncoding, input)
		if err != nil || !utf8.Valid(bytes) {
			continue
		}
		candidate := string(bytes)
		score := mojibakeScore(candidate)
		if score < bestScore && hasCyrillicText(candidate) {
			best = candidate
			bestScore = score
		}
	}
	return best
}

func charmapEncoder(sourceEncoding Encoding, input string) ([]byte, error) {
	var encoder *charmap.Charmap
	switch sourceEncoding {
	case CP866:
		encoder = charmap.CodePage866
	case WIN1251:
		encoder = charmap.Windows1251
	default:
		return []byte(input), nil
	}
	return encoder.NewEncoder().Bytes([]byte(input))
}

func mojibakeScore(input string) int {
	score := 0
	for _, r := range input {
		switch r {
		case '╨', '╤', 'Р', 'С', 'в', '•', '�':
			score++
		}
	}
	return score
}

func hasCyrillicText(input string) bool {
	for _, r := range input {
		if (r >= 'А' && r <= 'я') || r == 'Ё' || r == 'ё' {
			return true
		}
	}
	return false
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

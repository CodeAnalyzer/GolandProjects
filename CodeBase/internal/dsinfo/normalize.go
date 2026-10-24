package dsinfo

import (
	"regexp"
	"strings"
)

// hotfixRegexp — признак хотфикс-пакета в ссылке манифеста
// (например FA-AdminToolKit-11-01-7-HF001).
var hotfixRegexp = regexp.MustCompile(`-HF\d+`)

// isNumericToken сообщает, состоит ли токен только из цифр (ведущие нули —
// тоже числовой токен: «01», «06»).
func isNumericToken(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// NormalizeProduct вычисляет нормализованные поля строки манифеста:
//
//   - ProductName — токены RawName (разделитель '-') до первого чисто
//     числового токена, lowercase. Пример: FA-Contracts-Ext-9-01-553-PKG-X →
//     fa-contracts-ext. Чисто числовых токенов нет → RawName целиком.
//   - BranchKey — ProductName + первые три числовых токена RawName с
//     сохранением ведущих нулей как в файле: fa-contracts-ext-9-01-553.
//     Составные версии вида 7-02-2-7-02-2 коллапсируют в ветку 7-02-2.
//   - HotfixName — PacketRef целиком, если он матчит -HF<цифры>, иначе "".
//
// Правило проверено на всех 62 строках манифеста реального файла, включая
// PKG-, SP6- и составные 7-02-2-7-02-2 случаи.
func NormalizeProduct(rawName, packetRef string) (productName, branchKey, hotfixName string) {
	tokens := strings.Split(rawName, "-")

	nameEnd := len(tokens)
	for i, token := range tokens {
		if isNumericToken(token) {
			nameEnd = i
			break
		}
	}
	productName = strings.ToLower(strings.Join(tokens[:nameEnd], "-"))

	var numeric []string
	for _, token := range tokens {
		if isNumericToken(token) {
			numeric = append(numeric, token)
			if len(numeric) == 3 {
				break
			}
		}
	}
	branchKey = productName
	if len(numeric) > 0 {
		branchKey = productName + "-" + strings.Join(numeric, "-")
	}

	if hotfixRegexp.MatchString(packetRef) {
		hotfixName = packetRef
	}
	return productName, branchKey, hotfixName
}

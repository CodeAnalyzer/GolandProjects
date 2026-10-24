// Package branchkey — единый источник нормализации имён релизных линий
// продуктов Diasoft. Используется двумя сторонами мультиветочной модели:
//
//   - парсер манифеста DiasoftSystemInfo.csv (internal/dsinfo) вычисляет
//     branch_key из строки манифеста;
//   - индексатор вычисляет branch_key из имени git-ветки каталога-источника.
//
// Обе стороны обязаны получать одинаковый ключ для одной линии: ветка
// FA-Contracts-11-01-7-SP и манифест-строка FA-Contracts-11-01-7\FA-Contracts-11-01-7-HF008
// дают fa-contracts-11-01-7.
package branchkey

import (
	"regexp"
	"strings"
)

// hotfixRegexp — признак хотфикс-пакета в ссылке манифеста
// (например FA-AdminToolKit-11-01-7-HF001).
var hotfixRegexp = regexp.MustCompile(`-HF\d+`)

// spSuffixRegexp — суффиксы служебных линий в имени git-ветки (-SP, -HF008),
// снимаемые при вычислении ключа.
var spSuffixRegexp = regexp.MustCompile(`-(sp|hf\d+)$`)

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

// NormalizeBranchKey вычисляет ключ релизной линии из имени git-ветки:
// lowercase, символы '.'/'_' заменяются на '-', повторные суффиксы -SP и
// -HF<цифры> снимаются. Пример: FA-Contracts-11-01-7-SP → fa-contracts-11-01-7.
//
// Служебные ветки без признаков линии (sup/11-01-7, prj/NOVIKOM-PERSFORM)
// нормализуются теми же правилами и остаются без автоматического соответствия
// манифест-строкам — сопоставление делает вызывающая сторона по равенству
// ключей, а не эвристика.
func NormalizeBranchKey(branch string) string {
	s := strings.ReplaceAll(branch, ".", "-")
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ToLower(s)
	for spSuffixRegexp.MatchString(s) {
		s = spSuffixRegexp.ReplaceAllString(s, "")
	}
	return s
}

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
	"unicode"
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

// NumericTokens возвращает последовательность числовых сегментов строки:
// сегменты выделяются по не-алфанумерическим разделителям, и в результат
// попадают только целиком числовые сегменты. Цифры внутри буквенных токенов
// (например, «ftfns440reqprocadapter») игнорируются — иначе версия продукта
// с цифрами в имени ломала бы fallback-сопоставление.
// Пример: sup/11-01-7 → ["11","01","7"];
// fa-ftfns440reqprocadapter-11-01-11 → ["11","01","11"].
func NumericTokens(s string) []string {
	var tokens []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			if isNumericToken(cur.String()) {
				tokens = append(tokens, cur.String())
			}
			cur.Reset()
		}
	}
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur.WriteRune(r)
			continue
		}
		flush()
	}
	flush()
	return tokens
}

// CompareVersion сравнивает версии линий по числовым сегментам: большее
// значение первого различающегося сегмента — старшая версия; при равном
// префиксе более длинная последовательность считается старшей
// (11-01-7 > 11-01). Строки без версии равны между собой и младше любой
// версии. Возвращает -1, 0 или 1.
func CompareVersion(a, b string) int {
	at, bt := NumericTokens(a), NumericTokens(b)
	n := len(at)
	if len(bt) < n {
		n = len(bt)
	}
	for i := 0; i < n; i++ {
		if c := compareNumericSegment(at[i], bt[i]); c != 0 {
			return c
		}
	}
	switch {
	case len(at) < len(bt):
		return -1
	case len(at) > len(bt):
		return 1
	}
	return 0
}

// compareNumericSegment сравнивает числовые сегменты без учёта ведущих нулей.
func compareNumericSegment(a, b string) int {
	at := strings.TrimLeft(a, "0")
	bt := strings.TrimLeft(b, "0")
	if len(at) != len(bt) {
		if len(at) < len(bt) {
			return -1
		}
		return 1
	}
	switch {
	case at < bt:
		return -1
	case at > bt:
		return 1
	}
	return 0
}

// NumericTailMatches сообщает, совпадает ли последовательность числовых
// токенов requested с хвостом последовательности токенов candidate.
// Используется как fallback-матчинг линии манифеста на нестандартное имя
// git-ветки: fa-contracts-11-01-7 ↔ sup/11-01-7.
//
// Пустая последовательность requested (у продукта нет версии) не совпадает
// ни с чем: без информации о версии fallback не применяется.
func NumericTailMatches(requested, candidate string) bool {
	want := NumericTokens(requested)
	got := NumericTokens(candidate)
	if len(want) == 0 || len(got) < len(want) {
		return false
	}
	tail := got[len(got)-len(want):]
	for i := range want {
		if want[i] != tail[i] {
			return false
		}
	}
	return true
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

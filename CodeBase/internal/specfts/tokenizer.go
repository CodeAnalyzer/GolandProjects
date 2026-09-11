package specfts

import (
	"regexp"
	"strings"
	"unicode"
)

// Категории терминов (design D6):
// 1. Русские слова — lowercase + суффиксный стемминг
// 2. Техимена (API_*, FCD_*, t[A-Z]*, p[A-Z]*, CON_*) — единый токен без стемминга
// 3. Гибриды (758П, 275-ФЗ, 1064-1) — как есть

var (
	// reTechName — технические имена: заглавные латинские + _ и цифры, длиной ≥ 3
	reTechName = regexp.MustCompile(`\b[A-Z][A-Za-z0-9_]{2,}\b`)
	// reRussianWord — русские слова (включая ё/Ё), длиной ≥ 2
	reRussianWord = regexp.MustCompile(`[А-Яа-яЁё]{2,}`)
	// reHybrid — гибридные токены: цифры + русские/латинские буквы + дефис
	// \b не работает с русскими буквами, используем lookbehind-free паттерн
	reHybrid = regexp.MustCompile(`(?:^|[\s])(\d+[-]?[А-Яа-яЁёA-Za-z]*)`)
	// reTechPrefix — префиксы техимён: API_, FCD_, t[A-Z], p[A-Z], CON_, SD_
	reTechPrefix = regexp.MustCompile(`^(API_|FCD_|CON_|SD_|t[A-Z]|p[A-Z]|T[A-Z]|P[A-Z])`)
)

// stopWords — стоп-словарь для русского и английского
var stopWords = map[string]bool{
	// русские
	"система": true, "системы": true, "систему": true, "систем": true,
	"должен": true, "должна": true, "должно": true, "должны": true,
	"требование": true, "требования": true,
	"спецификация": true, "спецификации": true,
	"условие": true, "условия": true,
	"данный": true, "данные": true, "данных": true,
	"который": true, "которая": true, "которое": true, "которые": true,
	"этот": true, "эта": true, "это": true, "эти": true,
	"для": true, "при": true, "или": true, "и": true,
	"не": true, "на": true, "в": true, "с": true,
	"по": true, "от": true, "до": true, "из": true,
	"как": true, "что": true, "то": true, "же": true,
	"бы": true, "ли": true,
	"если": true, "так": true, "также": true,
	"после": true, "перед": true, "между": true,
	"может": true, "могут": true,
	// английские
	"shall": true, "then": true, "the": true, "a": true, "an": true,
	"is": true, "are": true, "be": true, "been": true,
	"of": true, "to": true, "in": true, "on": true, "at": true,
	"for": true, "with": true, "by": true, "from": true,
	"and": true, "or": true, "not": true, "no": true,
	"this": true, "that": true, "these": true, "those": true,
	"must": true, "should": true, "may": true, "can": true,
	"requirement": true, "scenario": true, "given": true, "when": true,
	"system": true, "spec": true, "specification": true,
}

// Token — токен с категорией
type Token struct {
	Term      string
	Category  TokenCategory
	Stemmed   string // для русских — стем, для остальных = Term
}

// TokenCategory — категория токена
type TokenCategory int

const (
	CatRussian TokenCategory = iota
	CatTech
	CatHybrid
)

// Tokenize разбивает текст на токены 3 категорий.
func Tokenize(text string) []Token {
	var tokens []Token

	// Нормализация ё→е: «счёт» и «счет» дают один стем (TokenizerVersion 2).
	text = strings.ReplaceAll(text, "ё", "е")
	text = strings.ReplaceAll(text, "Ё", "Е")

	// 2. Техимена — извлекаем первыми, чтобы не разрезать русским стеммером
	techSet := map[string]bool{}
	for _, m := range reTechName.FindAllString(text, -1) {
		if isTechName(m) {
			term := m
			techSet[term] = true
			tokens = append(tokens, Token{
				Term:     term,
				Category: CatTech,
				Stemmed:  term,
			})
		}
	}

	// 3. Гибриды — цифры + русские/латинские буквы + дефис
	for _, m := range reHybrid.FindAllStringSubmatch(text, -1) {
		hybrid := m[1]
		if isHybrid(hybrid) {
			term := hybrid
			if !techSet[term] {
				tokens = append(tokens, Token{
					Term:     term,
					Category: CatHybrid,
					Stemmed:  term,
				})
			}
		}
	}

	// 1. Русские слова — lowercase + стемминг
	for _, m := range reRussianWord.FindAllString(text, -1) {
		// Пропускаем русские аббревиатуры (все заглавные, ≤ 4 символов) — они не стеммятся
		if len(m) <= 4 && m == strings.ToUpper(m) && len(m) >= 2 {
			// Аббревиатура — добавляем как техимя
			lower := strings.ToLower(m)
			if stopWords[lower] {
				continue
			}
			tokens = append(tokens, Token{
				Term:     lower,
				Category: CatTech,
				Stemmed:  lower,
			})
			continue
		}
		lower := strings.ToLower(m)
		stemmed := stemRussian(lower)
		if stopWords[stemmed] || stopWords[lower] {
			continue
		}
		if len(stemmed) < 2 {
			continue
		}
		tokens = append(tokens, Token{
			Term:     lower,
			Category: CatRussian,
			Stemmed:  stemmed,
		})
	}

	return tokens
}

// TokenizeToStems возвращает только стеммы (уникальные термины).
func TokenizeToStems(text string) []string {
	tokens := Tokenize(text)
	seen := map[string]bool{}
	var result []string
	for _, t := range tokens {
		if !seen[t.Stemmed] {
			seen[t.Stemmed] = true
			result = append(result, t.Stemmed)
		}
	}
	return result
}

// TokenizeToStemCounts возвращает стемы с кратностями вхождения —
// основа TF: повторения термина в документе (включая удвоенный title) повышают вес.
func TokenizeToStemCounts(text string) map[string]int {
	tokens := Tokenize(text)
	counts := make(map[string]int, len(tokens))
	for _, t := range tokens {
		counts[t.Stemmed]++
	}
	return counts
}

// isTechName проверяет, является ли строка техименем.
func isTechName(s string) bool {
	if len(s) < 3 {
		return false
	}
	// Содержит _ или начинается с известного префикса
	if strings.Contains(s, "_") {
		return true
	}
	if reTechPrefix.MatchString(s) {
		return true
	}
	// Все заглавные — аббревиатура
	if s == strings.ToUpper(s) && len(s) >= 3 {
		return true
	}
	return false
}

// isHybrid проверяет, является ли строка гибридным токеном.
func isHybrid(s string) bool {
	hasDigit := false
	hasLetter := false
	for _, r := range s {
		if unicode.IsDigit(r) {
			hasDigit = true
		}
		if unicode.IsLetter(r) {
			hasLetter = true
		}
	}
	return hasDigit && hasLetter
}

// stemRussian — простой суффиксный стеммер для русского языка.
// Удаляет окончания: -ый, -ой, -ая, -ое, -ые, -ий, -ий, -ть, -ти, -тся,
// -ется, -аться, -ение, -ация, -ость, -ство, -ние, -ния, -ом, -ам, -ям,
// -ах, -ях, -ов, -ев, -ам, -ям, -ами, -ями, -ых, -их
func stemRussian(word string) string {
	// Окончания упорядочены по убыванию длины — длинные проверяются раньше,
	// чтобы -ется не отрезалось раньше -яется.
	endings := []string{
		"ование", "ывание", "ирование",
		"аться", "яться", "ться", "яется", "ется", "ация", "изация",
		"ание", "ение", "ость", "ство", "ние", "ния",
		"ами", "ями", "ых", "их", "ов", "ев", "ам", "ям", "ах", "ях",
		"ый", "ой", "ая", "ое", "ые", "ий", "ее", "ить", "ть", "ти",
		"ом", "ой", "ей", "ию", "ия", "ий", "тся",
		"на", "но", "ны", "ну",
		"ет", "ут", "ют", "ит", "ат", "ят",
		"ел", "ал", "ил", "ол",
		"го", "му", "ва", "во",
		"а", "я", "ы", "и", "е", "о", "у", "ю",
	}
	for _, ending := range endings {
		if strings.HasSuffix(word, ending) && len(word) > len(ending)+2 {
			return word[:len(word)-len(ending)]
		}
	}
	return word
}

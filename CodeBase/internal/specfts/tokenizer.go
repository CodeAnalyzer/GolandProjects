package specfts

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/kljensen/snowball"
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
	// reHybridCandidate — широкий кандидат гибрида: цифра + буквы/цифры/_/-.
	// Границы проверяются вручную: RE2 не поддерживает lookbehind, а \b не
	// работает с кириллицей, поэтому кандидат ловится без привязки к пробелу.
	reHybridCandidate = regexp.MustCompile(`[0-9][0-9A-Za-zА-Яа-яЁё_-]*`)
	// reTechPrefix — префиксы техимён: API_, FCD_, t[A-Z], p[A-Z], CON_, SD_
	reTechPrefix = regexp.MustCompile(`^(API_|FCD_|CON_|SD_|t[A-Z]|p[A-Z]|T[A-Z]|P[A-Z])`)
	// reHyphenDigitLetter / reHyphenLetterDigit — дефис между цифровой частью и
	// буквенным суффиксом нормализуется (6406-У и 6406U → один термин).
	reHyphenDigitLetter = regexp.MustCompile(`([0-9])-([a-zа-я])`)
	reHyphenLetterDigit = regexp.MustCompile(`([a-zа-я])-([0-9])`)
)

// identHomoglyphs — кириллические символы, совпадающие с латинскими визуально
// или по транслитерации; используются при канонизации идентификаторов (У/U, П/P).
var identHomoglyphs = map[rune]rune{
	'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k', 'м': 'm', 'н': 'h',
	'о': 'o', 'р': 'p', 'с': 'c', 'т': 't', 'у': 'u', 'х': 'x', 'п': 'p',
}

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
	// частотные служебные слова (падежные формы покрываются stopStems)
	"да": true, "еще": true, "уже": true, "ее": true,
	"тот": true, "где": true, "всю": true, "вся": true,
	"был": true, "была": true, "было": true, "были": true,
	"им": true, "их": true, "его": true, "ему": true,
	"нее": true, "него": true, "чем": true, "когда": true,
	"тогда": true, "чтобы": true,
}

// Token — токен с категорией
type Token struct {
	Term     string
	Category TokenCategory
	Stemmed  string // для русских — стем, для остальных = Term
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

	runes := []rune(text)
	consumed := make([]bool, len(runes))

	markConsumed := func(byteStart, byteEnd int) {
		rs := utf8.RuneCountInString(text[:byteStart])
		re := utf8.RuneCountInString(text[:byteEnd])
		for i := rs; i < re && i < len(consumed); i++ {
			consumed[i] = true
		}
	}
	anyConsumed := func(runeStart, runeEnd int) bool {
		for i := runeStart; i < runeEnd && i < len(consumed); i++ {
			if consumed[i] {
				return true
			}
		}
		return false
	}

	// 2. Техимена — извлекаем первыми, чтобы не разрезать русским стеммером.
	for _, loc := range reTechName.FindAllStringIndex(text, -1) {
		m := text[loc[0]:loc[1]]
		if !isTechName(m) {
			continue
		}
		// Префикс семейства без имени (API_, FCD_, ADP_) — не идентификатор.
		if strings.HasSuffix(m, "_") {
			continue
		}
		term := canonicalIdent(m)
		tokens = append(tokens, Token{
			Term:     term,
			Category: CatTech,
			Stemmed:  term,
		})
		markConsumed(loc[0], loc[1])
	}

	// 3. Гибриды — цифры + буквенный суффикс (758П, 275-ФЗ, 1-4212U).
	// Кандидат ловится без привязки к пробелу; границы валидируются вручную.
	for _, loc := range reHybridCandidate.FindAllStringIndex(text, -1) {
		cand := strings.TrimRight(text[loc[0]:loc[1]], "-_")
		if cand == "" || !endsWithLetter(cand) {
			continue
		}
		runeStart := utf8.RuneCountInString(text[:loc[0]])
		runeEnd := runeStart + utf8.RuneCountInString(cand)
		if anyConsumed(runeStart, runeEnd) {
			continue
		}
		if runeStart > 0 && isWordRune(runes[runeStart-1]) {
			continue
		}
		term := canonicalIdent(cand)
		tokens = append(tokens, Token{
			Term:     term,
			Category: CatHybrid,
			Stemmed:  term,
		})
		markConsumed(loc[0], loc[0]+len(cand))
	}

	// Замаскировать потреблённые спаны, чтобы буквенные хвосты гибридов и
	// техимён не порождали отдельных русских токенов (нет «го» из «2-го»).
	masked := make([]rune, len(runes))
	for i, r := range runes {
		if consumed[i] {
			masked[i] = ' '
		} else {
			masked[i] = r
		}
	}

	// 1. Русские слова — lowercase + стемминг
	for _, m := range reRussianWord.FindAllString(string(masked), -1) {
		// Пропускаем русские аббревиатуры (все заглавные, ≤ 4 символов) — они не стеммятся
		// Длина считается в рунах: кириллица в UTF-8 — 2 байта на символ.
		if runes := utf8.RuneCountInString(m); runes <= 4 && m == strings.ToUpper(m) && runes >= 2 {
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
		if stopWords[stemmed] || stopWords[lower] || stopStems[stemmed] {
			continue
		}
		if utf8.RuneCountInString(stemmed) < 2 {
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

// canonicalIdent приводит технический или гибридный токен к канонической форме:
// нижний регистр, складывание кириллических гомоглифов в латиницу, удаление
// дефиса между цифровой частью и буквенным суффиксом. Варианты написания одного
// идентификатора (6406-У/6406U, RPT_F711/Rpt_F711) дают один термин словаря.
func canonicalIdent(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToLower(s) {
		if mapped, ok := identHomoglyphs[r]; ok {
			b.WriteRune(mapped)
			continue
		}
		b.WriteRune(r)
	}
	out := b.String()
	out = reHyphenDigitLetter.ReplaceAllString(out, "$1$2")
	out = reHyphenLetterDigit.ReplaceAllString(out, "$1$2")
	return out
}

// endsWithLetter сообщает, заканчивается ли строка буквой: гибрид — это
// цифры/дефисы, оканчивающиеся буквенным суффиксом (отсекает даты и диапазоны).
func endsWithLetter(s string) bool {
	r, _ := utf8.DecodeLastRuneInString(s)
	return unicode.IsLetter(r)
}

// isWordRune — символ, продолжающий токен (буква/цифра/подчёркивание).
func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// stemRussian — стемминг русского слова алгоритмом Snowball (Russian).
// Единая точка подмены стеммера: нормализация ё→е выполняется в Tokenize
// до вызова (Snowball сам ё не нормализует). При ошибке стемминга слово
// возвращается как есть.
func stemRussian(word string) string {
	stemmed, err := snowball.Stem(word, "russian", true)
	if err != nil || stemmed == "" {
		return word
	}
	return stemmed
}

// stopStems — предвычисленные стемы записей стоп-словаря (Snowball, русские
// записи): ловят падежные формы стоп-слов («которого», «которому») без
// ручного перечисления форм. Английские записи стеммятся только по
// исходной форме — они неизменяемые служебные слова.
var stopStems = buildStopStems()

func buildStopStems() map[string]bool {
	stems := make(map[string]bool, len(stopWords)*2)
	for w := range stopWords {
		stems[w] = true
		if hasCyrillic(w) {
			stems[stemRussian(w)] = true
		}
	}
	return stems
}

// hasCyrillic сообщает, содержит ли строка кириллические буквы.
func hasCyrillic(s string) bool {
	for _, r := range s {
		if unicode.Is(unicode.Cyrillic, r) {
			return true
		}
	}
	return false
}

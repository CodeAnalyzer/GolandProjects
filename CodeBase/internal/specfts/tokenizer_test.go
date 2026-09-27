package specfts

import (
	"testing"
	"unicode/utf8"
)

func TestTokenize_RussianStemming(t *testing.T) {
	// арест/ареста → один стем
	tokens1 := TokenizeToStems("арест счёта")
	tokens2 := TokenizeToStems("ареста счёта")

	found1 := false
	found2 := false
	for _, tok := range tokens1 {
		if tok == "арест" {
			found1 = true
		}
	}
	for _, tok := range tokens2 {
		if tok == "арест" {
			found2 = true
		}
	}
	if !found1 {
		t.Errorf("expected stem 'арест' in арест счёта, got %v", tokens1)
	}
	if !found2 {
		t.Errorf("expected stem 'арест' in ареста счёта, got %v", tokens2)
	}
}

func TestTokenize_TechName(t *testing.T) {
	// CON_STP_MassAccrual → один токен в канонической форме (нижний регистр)
	tokens := TokenizeToStems("Вызов CON_STP_MassAccrual в цикле")
	found := false
	for _, tok := range tokens {
		if tok == "con_stp_massaccrual" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected tech token 'con_stp_massaccrual', got %v", tokens)
	}
}

func TestTokenize_APIPrefix(t *testing.T) {
	tokens := TokenizeToStems("API_DepoAccount_MassInsert выполняется")
	found := false
	for _, tok := range tokens {
		if tok == "api_depoaccount_massinsert" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected tech token 'api_depoaccount_massinsert', got %v", tokens)
	}
}

func TestTokenize_Hybrid(t *testing.T) {
	// 758П и 275-ФЗ — единые токены в канонической форме
	tokens := TokenizeToStems("Согласно 758П и 275-ФЗ")
	found758 := false
	found275 := false
	for _, tok := range tokens {
		if tok == "758p" {
			found758 = true
		}
		if tok == "275фз" {
			found275 = true
		}
	}
	if !found758 {
		t.Errorf("expected hybrid '758p', got %v", tokens)
	}
	if !found275 {
		t.Errorf("expected hybrid '275фз', got %v", tokens)
	}
}

// TestTokenize_HybridNumericTail — цифровой хвост сохраняется целиком
// (1-4212U), а не отбрасывается и не режется на части.
func TestTokenize_HybridNumericTail(t *testing.T) {
	tokens := TokenizeToStems("см. 1-4212U и 2-4637U")
	seen := map[string]bool{}
	for _, tok := range tokens {
		seen[tok] = true
	}
	for _, want := range []string{"1-4212u", "2-4637u"} {
		if !seen[want] {
			t.Errorf("expected hybrid %q, got %v", want, tokens)
		}
	}
}

// TestTokenize_HybridAfterPunctuation — гибрид распознаётся сразу после
// скобки/кавычки/№ без пробела.
func TestTokenize_HybridAfterPunctuation(t *testing.T) {
	tokens := TokenizeToStems("ссылка (6406-У) и «7047-У» и № 542-П")
	seen := map[string]bool{}
	for _, tok := range tokens {
		seen[tok] = true
	}
	for _, want := range []string{"6406u", "7047u", "542p"} {
		if !seen[want] {
			t.Errorf("expected hybrid %q, got %v", want, tokens)
		}
	}
}

// TestTokenize_NoHybridTailLeak — буквенный хвост гибрида не эмитится
// повторно как самостоятельный токен.
func TestTokenize_NoHybridTailLeak(t *testing.T) {
	counts := TokenizeToStemCounts("см. 2-го и 275-ФЗ")
	if counts["го"] != 0 {
		t.Errorf("hybrid tail 'го' leaked as separate token: %v", counts)
	}
	if counts["фз"] != 0 {
		t.Errorf("hybrid tail 'фз' leaked as separate token: %v", counts)
	}
}

// TestTokenize_HomoglyphVariantsMerge — варианты написания одного
// идентификатора (Cyrillic-гомоглиф + дефис vs Latin) дают один термин.
func TestTokenize_HomoglyphVariantsMerge(t *testing.T) {
	for _, pair := range [][2]string{
		{"6406-У", "6406U"},
		{"385-П", "385P"},
		{"1417-У", "1417U"},
	} {
		a := TokenizeToStems(pair[0])
		b := TokenizeToStems(pair[1])
		if len(a) != 1 || len(b) != 1 || a[0] != b[0] {
			t.Errorf("variants %q/%q did not merge: %v vs %v", pair[0], pair[1], a, b)
		}
	}
}

// TestTokenize_TechNameCaseMerge — регистро-варианты техимени дают один термин.
func TestTokenize_TechNameCaseMerge(t *testing.T) {
	a := TokenizeToStems("RPT_F711")
	b := TokenizeToStems("Rpt_F711")
	if len(a) != 1 || len(b) != 1 || a[0] != b[0] {
		t.Errorf("case variants did not merge: %v vs %v", a, b)
	}
}

// TestTokenize_WildcardPrefixDropped — префикс семейства без имени
// (API_, FCD_, ADP_) в словарь не попадает.
func TestTokenize_WildcardPrefixDropped(t *testing.T) {
	counts := TokenizeToStemCounts("семейства API_* и FCD_* и ADP_*")
	for _, bad := range []string{"api_", "fcd_", "adp_"} {
		if counts[bad] != 0 {
			t.Errorf("wildcard prefix %q must be dropped, got %v", bad, counts)
		}
	}
}

// TestTokenize_ExtendedStopWords — частотные служебные слова и их формы
// отфильтрованы, а легитимные короткие аббревиатуры сохранены.
func TestTokenize_ExtendedStopWords(t *testing.T) {
	counts := TokenizeToStemCounts("уже её был где цб фл юл ип")
	for _, bad := range []string{"уж", "ее", "был", "где"} {
		if counts[bad] != 0 {
			t.Errorf("stop word stem %q leaked: %v", bad, counts)
		}
	}
	for _, keep := range []string{"цб", "фл", "юл", "ип"} {
		if counts[keep] == 0 {
			t.Errorf("legitimate abbreviation %q wrongly removed: %v", keep, counts)
		}
	}
}

func TestTokenize_StopWords(t *testing.T) {
	// «система», «shall», «then» не должны появиться
	tokens := TokenizeToStems("система shall then выполнять")
	for _, tok := range tokens {
		if tok == "система" || tok == "shall" || tok == "then" {
			t.Errorf("stop word '%s' should be filtered", tok)
		}
	}
}

func TestTokenize_MixedText(t *testing.T) {
	// Смешанный текст: русские + техимена + гибриды
	tokens := Tokenize("Блокировка счёта по API_CardBlock в 758П")
	hasRussian := false
	hasTech := false
	hasHybrid := false
	for _, t := range tokens {
		switch t.Category {
		case CatRussian:
			hasRussian = true
		case CatTech:
			hasTech = true
		case CatHybrid:
			hasHybrid = true
		}
	}
	if !hasRussian {
		t.Error("expected russian tokens")
	}
	if !hasTech {
		t.Error("expected tech token API_CardBlock")
	}
	if !hasHybrid {
		t.Error("expected hybrid token 758П")
	}
}

func TestStemRussian_SimpleCases(t *testing.T) {
	// stemRussian получает уже нормализованный (ё→е) ввод — нормализация
	// выполняется в Tokenize до вызова.
	tests := []struct {
		input  string
		expect string
	}{
		{"блокировка", "блокировк"},
		{"блокировки", "блокировк"},
		{"счета", "счет"},
		{"счет", "счет"},
		{"выполнять", "выполня"},
		{"выполняется", "выполня"},
	}
	for _, tt := range tests {
		got := stemRussian(tt.input)
		if got != tt.expect {
			t.Errorf("stemRussian(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

// TestStemRussian_AdjectivalParadigm — вся падежная парадигма прилагательного
// схлопывается в один стем; мусорные осколки («автоматическо», «автоматическу»)
// не порождаются (регрессия самописного суффиксного стеммера).
func TestStemRussian_AdjectivalParadigm(t *testing.T) {
	forms := []string{
		"автоматический", "автоматическим", "автоматического", "автоматическому", "автоматически",
	}
	const want = "автоматическ"
	for _, f := range forms {
		if got := stemRussian(f); got != want {
			t.Errorf("stemRussian(%q) = %q, want %q", f, got, want)
		}
	}
	for _, garbage := range []string{"автоматическо", "автоматическу"} {
		for _, f := range forms {
			if got := stemRussian(f); got == garbage {
				t.Errorf("stemRussian(%q) produced garbage stem %q", f, got)
			}
		}
	}
}

// TestTokenize_AdjectivalParadigmSingleVocabTerm — формы одной лексемы дают
// один стем на уровне токенизации: словарь LSA получает одну строку.
func TestTokenize_AdjectivalParadigmSingleVocabTerm(t *testing.T) {
	stems := TokenizeToStemCounts("автоматический автоматическим автоматического автоматическому")
	if len(stems) != 1 {
		t.Fatalf("expected single stem for adjectival paradigm, got %v", stems)
	}
	if _, ok := stems["автоматическ"]; !ok {
		t.Errorf("expected stem 'автоматическ', got %v", stems)
	}
}

// TestTokenize_AbbreviationNotStemmed — аббревиатуры ≤ 4 символов (руны,
// не байты) проходят целиком без стемминга: «США» не усекается до «сш».
func TestTokenize_AbbreviationNotStemmed(t *testing.T) {
	tokens := TokenizeToStems("США НДС ЦБ и БИК")
	seen := map[string]bool{}
	for _, tok := range tokens {
		seen[tok] = true
	}
	for _, abbr := range []string{"сша", "ндс", "цб", "бик"} {
		if !seen[abbr] {
			t.Errorf("expected abbreviation token %q kept whole, got %v", abbr, tokens)
		}
	}
	if seen["сш"] {
		t.Errorf("abbreviation 'США' was stemmed to 'сш': %v", tokens)
	}
}

// TestTokenize_MinStemLength — стемы короче 2 символов отбрасываются
// («аяя» стеммится в одно-буквенный «а»).
func TestTokenize_MinStemLength(t *testing.T) {
	if got := stemRussian("аяя"); utf8.RuneCountInString(got) != 1 {
		t.Fatalf("precondition: stemRussian(аяя) must yield 1-rune stem, got %q", got)
	}
	tokens := TokenizeToStems("аяя")
	for _, tok := range tokens {
		if tok == "а" {
			t.Errorf("1-rune stem must be discarded, got %v", tokens)
		}
	}
}

// TestTokenize_StopWordInflectedForms — падежные формы стоп-слов фильтруются
// по предвычисленным стемам записей стоп-словаря.
func TestTokenize_StopWordInflectedForms(t *testing.T) {
	tokens := TokenizeToStems("которого которому должны должна должным")
	for _, tok := range tokens {
		if tok == "котор" || tok == "должн" {
			t.Errorf("inflected stop word leaked into tokens: %v", tokens)
		}
	}
}

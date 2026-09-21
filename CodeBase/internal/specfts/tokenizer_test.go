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
	// CON_STP_MassAccrual → один токен
	tokens := TokenizeToStems("Вызов CON_STP_MassAccrual в цикле")
	found := false
	for _, tok := range tokens {
		if tok == "CON_STP_MassAccrual" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected tech token 'CON_STP_MassAccrual', got %v", tokens)
	}
}

func TestTokenize_APIPrefix(t *testing.T) {
	tokens := TokenizeToStems("API_DepoAccount_MassInsert выполняется")
	found := false
	for _, tok := range tokens {
		if tok == "API_DepoAccount_MassInsert" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected tech token 'API_DepoAccount_MassInsert', got %v", tokens)
	}
}

func TestTokenize_Hybrid(t *testing.T) {
	// 758П, 275-ФЗ — как есть
	tokens := TokenizeToStems("Согласно 758П и 275-ФЗ")
	found758 := false
	found275 := false
	for _, tok := range tokens {
		if tok == "758П" {
			found758 = true
		}
		if tok == "275-ФЗ" || tok == "275" {
			found275 = true
		}
	}
	if !found758 {
		t.Errorf("expected hybrid '758П', got %v", tokens)
	}
	// 275-ФЗ может быть разбит на 275 и ФЗ — проверяем хотя бы 275
	if !found275 {
		t.Errorf("expected hybrid '275' or '275-ФЗ', got %v", tokens)
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

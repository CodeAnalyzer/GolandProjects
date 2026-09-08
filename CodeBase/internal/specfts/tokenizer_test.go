package specfts

import (
	"testing"
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
	tests := []struct {
		input  string
		expect string
	}{
		{"блокировка", "блокировк"},
		{"блокировки", "блокировк"},
		{"счёта", "счёт"},
		{"счёт", "счёт"},
		{"выполнять", "выполня"},
		{"выполняется", "выполн"},
	}
	for _, tt := range tests {
		got := stemRussian(tt.input)
		if got != tt.expect {
			t.Errorf("stemRussian(%q) = %q, want %q", tt.input, got, tt.expect)
		}
	}
}

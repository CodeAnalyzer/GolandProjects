package query

import "testing"

func TestBuildSymbolLookupCondition_FormTypeSearchesNameAndSignature(t *testing.T) {
	got := buildSymbolLookupCondition("form", false, 1)
	want := "s.symbol_name = $1 OR s.signature = $1"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_EmptyTypeSearchesNameAndFormSignature(t *testing.T) {
	got := buildSymbolLookupCondition("", false, 1)
	want := "s.symbol_name = $1 OR (s.symbol_type = 'form' AND s.signature = $1)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_OtherTypeKeepsFormSignatureGuarded(t *testing.T) {
	got := buildSymbolLookupCondition("class", false, 1)
	want := "LOWER(s.symbol_name) = LOWER($1)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_ProcedureExactUsesLowerNameOnly(t *testing.T) {
	got := buildSymbolLookupCondition("procedure", false, 3)
	want := "LOWER(s.symbol_name) = LOWER($3)"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

func TestBuildSymbolLookupCondition_LikeUsesILike(t *testing.T) {
	got := buildSymbolLookupCondition("form", true, 2)
	want := "s.symbol_name ILIKE $2 OR s.signature ILIKE $2"
	if got != want {
		t.Fatalf("condition: got=%q want=%q", got, want)
	}
}

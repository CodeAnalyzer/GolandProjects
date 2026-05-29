package store

import "testing"

func TestNormalizeDSProductName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "trim and lowercase", input: "  FA-Contracts-EXT  ", want: "fa-contracts-ext"},
		{name: "already normalized", input: "fa-cards", want: "fa-cards"},
		{name: "empty", input: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeDSProductName(tt.input); got != tt.want {
				t.Fatalf("normalizeDSProductName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

package store

import (
	"strings"
	"testing"
)

// TestSanitizeUTF8String_NULStripped — NUL-байт (валидный UTF-8, отвергается
// PostgreSQL) удаляется из значения; строки без NUL не меняются.
func TestSanitizeUTF8String_NULStripped(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"plain ascii", "plain ascii"},
		{"Начисление \x00процентов", "Начисление процентов"},
		{"\x00start", "start"},
		{"end\x00", "end"},
		{"a\x00b\x00c", "abc"},
		{"\x00", ""},
	}
	for _, tc := range cases {
		if got := sanitizeUTF8String(tc.in); got != tc.want {
			t.Fatalf("sanitizeUTF8String(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if strings.IndexByte(sanitizeUTF8String(tc.in), 0) >= 0 {
			t.Fatalf("sanitizeUTF8String(%q) still contains NUL", tc.in)
		}
	}
}

// TestSanitizeUTF8String_NoAllocationsWithoutNUL — предикат NUL не копирует
// строку: для валидного UTF-8 без NUL санитайзер не аллоцирует.
func TestSanitizeUTF8String_NoAllocationsWithoutNUL(t *testing.T) {
	value := strings.Repeat("Начисление процентов по договору №7; select @RetVal = 0\n", 64)
	allocs := testing.AllocsPerRun(100, func() {
		if got := sanitizeUTF8String(value); got != value {
			t.Fatalf("sanitizeUTF8String changed NUL-free value")
		}
	})
	if allocs != 0 {
		t.Fatalf("sanitizeUTF8String allocates %v times for NUL-free valid UTF-8, want 0", allocs)
	}
}

func BenchmarkSanitizeUTF8String_NoNUL(b *testing.B) {
	value := strings.Repeat("Начисление процентов по договору №7; select @RetVal = 0\n", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizeUTF8String(value)
	}
}

func BenchmarkSanitizeUTF8String_WithNUL(b *testing.B) {
	value := strings.Repeat("Начисление\x00 процентов\x00 по договору\n", 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sanitizeUTF8String(value)
	}
}

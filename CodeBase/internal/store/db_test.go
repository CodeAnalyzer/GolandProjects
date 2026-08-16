package store

import (
	"fmt"
	"testing"

	"github.com/lib/pq"
)

func TestLookupKeyBuilders(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "h define", got: BuildHDefineLookupKey("  DEFINE_NAME  ", 12), want: "define_name|12"},
		{name: "js function", got: BuildJSFunctionLookupKey("  OnLoad  ", 3), want: "onload|3"},
		{name: "dfm form", got: BuildDFMFormLookupKey("  MainForm  ", 4), want: "mainform|4"},
		{name: "dfm component", got: BuildDFMComponentLookupKey("  ButtonOK  ", 5), want: "buttonok|5"},
		{name: "pas unit", got: BuildPASUnitLookupKey("  UnitA  ", 6), want: "unita|6"},
		{name: "pas class", got: BuildPASClassLookupKey("  TClass  ", 7), want: "tclass|7"},
		{name: "pas method", got: BuildPASMethodLookupKey(" TClass ", " Execute ", 8), want: "tclass|execute|8"},
		{name: "pas field", got: BuildPASFieldLookupKey(" TClass ", " FieldA ", 9), want: "tclass|fielda|9"},
		{name: "sql table", got: BuildSQLTableLookupKey(" TAccount ", " SELECT ", 10), want: "taccount|select|10"},
		{name: "sql index", got: BuildSQLIndexDefinitionLookupKey(" TAccount ", " XAK1 ", 11), want: "taccount|xak1|11"},
		{name: "query fragment", got: BuildQueryFragmentLookupKey(" hash-value ", " METHOD ", 12), want: "hash-value|method|12"},
		{name: "report field", got: BuildReportFieldLookupKey(" FieldA ", 13), want: "fielda|13"},
		{name: "report param", got: BuildReportParamLookupKey(" ParamA ", 14), want: "parama|14"},
		{name: "vb function", got: BuildVBFunctionLookupKey(" Main ", 15), want: "main|15"},
		{name: "js constant", got: BuildJSConstantLookupKey(" CONST_A ", 16), want: "const_a|16"},
		{name: "sql column definition", got: BuildSQLColumnDefinitionLookupKey(" TAccount ", " AccountID ", 17, 2), want: "taccount|accountid|17|2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("lookup key = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

func TestNullableHelpers(t *testing.T) {
	if got := NullableString(""); got != nil {
		t.Fatalf("NullableString(empty) = %#v, want nil", got)
	}
	if got := NullableString("   "); got != nil {
		t.Fatalf("NullableString(spaces) = %#v, want nil", got)
	}
	if got := NullableString(" value "); got != " value " {
		t.Fatalf("NullableString(value) = %#v, want original value", got)
	}

	if got := NullableInt(0); got != nil {
		t.Fatalf("NullableInt(0) = %#v, want nil", got)
	}
	if got := NullableInt(5); got != 5 {
		t.Fatalf("NullableInt(5) = %#v, want 5", got)
	}

	if got := NullableInt64(0); got != nil {
		t.Fatalf("NullableInt64(0) = %#v, want nil", got)
	}
	if got := NullableInt64(5); got != int64(5) {
		t.Fatalf("NullableInt64(5) = %#v, want int64(5)", got)
	}

	if got := NullableProcID(0); got != nil {
		t.Fatalf("NullableProcID(0) = %#v, want nil", got)
	}
	if got := NullableProcID(7); got != int64(7) {
		t.Fatalf("NullableProcID(7) = %#v, want int64(7)", got)
	}
}

func TestSanitizeHelpers(t *testing.T) {
	if got := sanitizeUTF8String(""); got != "" {
		t.Fatalf("sanitizeUTF8String(empty) = %q, want empty", got)
	}
	invalid := string([]byte{'a', 0xff, 'b'})
	if got := sanitizeUTF8String(invalid); got != "ab" {
		t.Fatalf("sanitizeUTF8String(invalid) = %q, want ab", got)
	}

	if got := sanitizeNullableJSON(nil); got != nil {
		t.Fatalf("sanitizeNullableJSON(nil) = %#v, want nil", got)
	}
	if got := sanitizeNullableJSON("   "); got != nil {
		t.Fatalf("sanitizeNullableJSON(blank string) = %#v, want nil", got)
	}
	if got := sanitizeNullableJSON(invalid); got != "ab" {
		t.Fatalf("sanitizeNullableJSON(invalid string) = %#v, want ab", got)
	}
	if got := sanitizeNullableJSON(10); got != 10 {
		t.Fatalf("sanitizeNullableJSON(non-string) = %#v, want 10", got)
	}
}

func TestSplitPathsByKeepIDs(t *testing.T) {
	keep := map[string]int64{"keep/a.sql": 10, "keep/b.sql": 20}
	withKeep, withoutKeep := splitPathsByKeepIDs(
		[]string{"keep/a.sql", "drop/c.sql", "keep/b.sql", "drop/d.sql"},
		keep,
	)
	if len(withKeep) != 2 || withKeep[0] != "keep/a.sql" || withKeep[1] != "keep/b.sql" {
		t.Fatalf("withKeep = %#v", withKeep)
	}
	if len(withoutKeep) != 2 || withoutKeep[0] != "drop/c.sql" || withoutKeep[1] != "drop/d.sql" {
		t.Fatalf("withoutKeep = %#v", withoutKeep)
	}

	emptyKeep, emptyDrop := splitPathsByKeepIDs(nil, keep)
	if len(emptyKeep) != 0 || len(emptyDrop) != 0 {
		t.Fatalf("empty paths: with=%#v without=%#v", emptyKeep, emptyDrop)
	}
}

func TestChunkStrings(t *testing.T) {
	if got := chunkStrings(nil, 500); got != nil {
		t.Fatalf("nil values = %#v, want nil", got)
	}
	if got := chunkStrings([]string{"a"}, 0); got != nil {
		t.Fatalf("zero chunk size = %#v, want nil", got)
	}

	values := make([]string, 501)
	for i := range values {
		values[i] = string(rune('a' + i%26))
	}
	chunks := chunkStrings(values, 500)
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	if len(chunks[0]) != 500 || len(chunks[1]) != 1 {
		t.Fatalf("chunk sizes = %d, %d", len(chunks[0]), len(chunks[1]))
	}
}

func TestQuoteDSNValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "simple", input: "localhost", want: "localhost"},
		{name: "with space", input: "my secret", want: "'my secret'"},
		{name: "with quote", input: "it's", want: `'it\'s'`},
		{name: "empty", input: "", want: "''"},
		{name: "with backslash", input: `a\b`, want: `'a\\b'`},
		{name: "injection attempt", input: "x sslmode=disable", want: "'x sslmode=disable'"},
		{name: "with tab", input: "a\tb", want: "'a\tb'"},
		{name: "with newline", input: "a\nb", want: "'a\nb'"},
		{name: "numeric", input: "5432", want: "5432"},
		{name: "ipv6", input: "::1", want: "::1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteDSNValue(tt.input)
			if got != tt.want {
				t.Fatalf("quoteDSNValue(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// TestQuoteDSNValue_ParsedByLibPQ проверяет экранирование против реального
// парсера DSN из lib/pq: значение должно дойти до драйвера без искажений.
func TestQuoteDSNValue_ParsedByLibPQ(t *testing.T) {
	secrets := []string{
		"simple",
		"my secret",
		"it's",
		`a\b`,
		`back\\slash`,
		"x sslmode=disable",
		"tab\tsep",
		"",
	}
	for _, secret := range secrets {
		t.Run(secret, func(t *testing.T) {
			dsn := fmt.Sprintf(
				"host=localhost port=5432 user=postgres password=%s dbname=codebase sslmode=disable",
				quoteDSNValue(secret),
			)
			if _, err := pq.NewConnector(dsn); err != nil {
				t.Fatalf("lib/pq rejected DSN %s: %v", dsn, err)
			}
		})
	}
}

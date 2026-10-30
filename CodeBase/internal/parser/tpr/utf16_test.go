package tpr

import (
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

func TestParseBytes_UTF16EncodedTPR(t *testing.T) {
	content := `[Fields]
@AccountID@ = Field{AccountID, 10,0}
@NameRus@ = Field{NameRus, 50,0, Left}
@MainQuery@ = SQL {
  select AccountID, NameRus
    from tAccount
   where ClientID = :ClientID
}
`

	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
	encoded, _, err := transform.Bytes(enc, []byte(content))
	if err != nil {
		t.Fatalf("encode UTF-16LE: %v", err)
	}
	data := append([]byte{0xFF, 0xFE}, encoded...)

	result, err := NewParser().ParseBytes(data, "/reports/AccountUTF16.tpr")
	if err != nil {
		t.Fatalf("ParseBytes UTF-16LE: %v", err)
	}
	if result.Form == nil || result.Form.ReportName != "AccountUTF16" {
		t.Fatalf("form = %+v", result.Form)
	}
	if len(result.Fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(result.Fields))
	}
	if len(result.Fragments) != 1 || !strings.Contains(strings.ToLower(result.Fragments[0].QueryText), "from taccount") {
		t.Fatalf("fragments = %+v", result.Fragments)
	}
}

package smf

import (
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// encodeUTF16LE кодирует строку в UTF-16LE с BOM FF FE.
func encodeUTF16LE(t *testing.T, s string) []byte {
	t.Helper()
	out := []byte{0xFF, 0xFE}
	enc := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()
	encoded, _, err := transform.Bytes(enc, []byte(s))
	if err != nil {
		t.Fatalf("encode UTF-16LE: %v", err)
	}
	return append(out, encoded...)
}

func TestParseBytes_UTF16EncodedSMF(t *testing.T) {
	content := `<job>
<description>Расчёт процентов по договору</description>
<prequery>select * from tDeal</prequery>
<script><![CDATA[
function CreateInstrument() {
    Instrument.Name = "AccrualPercent";
    Instrument.Brief = "Начисление процентов";
}
]]></script>
</job>`

	data := encodeUTF16LE(t, content)
	result, err := NewParser().ParseBytes(data, "/repo/Scenario/Accrual.smf")
	if err != nil {
		t.Fatalf("ParseBytes UTF-16LE: %v", err)
	}
	if result.Description != "Расчёт процентов по договору" {
		t.Fatalf("description = %q (Cyrillic lost)", result.Description)
	}
	if result.Instrument == nil || result.Instrument.InstrumentName != "AccrualPercent" {
		t.Fatalf("instrument = %+v", result.Instrument)
	}
	if result.Instrument.Brief != "Начисление процентов" {
		t.Fatalf("instrument brief = %q (Cyrillic lost)", result.Instrument.Brief)
	}
	if !strings.Contains(result.PrequerySQL, "tDeal") {
		t.Fatalf("prequery = %q", result.PrequerySQL)
	}
}

func TestParseBytes_UTF8BOMSMF(t *testing.T) {
	// UTF-8 BOM: пре-детект отдаёт UTF8, DecodeBytes удаляет U+FEFF.
	content := "<job><description>Plain job</description></job>"
	data := append([]byte{0xEF, 0xBB, 0xBF}, []byte(content)...)
	result, err := NewParser().ParseBytes(data, "/repo/Scenario/Plain.smf")
	if err != nil {
		t.Fatalf("ParseBytes UTF-8 BOM: %v", err)
	}
	if result.Description != "Plain job" {
		t.Fatalf("description = %q", result.Description)
	}
}

package rti

import (
	"strings"
	"testing"
)

func TestDecodeHRTIString_Basic(t *testing.T) {
	got := decodeHRTIString("6D65Pe4PgъPUU6D6", "")
	want := "МасНачЗалл"
	if got != want {
		t.Errorf("decodeHRTIString(\"6D65Pe4PgъPUU6D6\") = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_NameWithSpaces(t *testing.T) {
	// "Массовое начисление задолженностей" + padding
	got := decodeHRTIString("6D65PeeZNZS9aPgXeUSaXS9QPTZURSaaZedSW999999999999999999999999996D6", "")
	want := "Массовое начисление задолженностей                          "
	if got != want {
		t.Errorf("decodeHRTIString(Name1) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_Brief2(t *testing.T) {
	got := decodeHRTIString("6D69э4g8f2fь56D6", "")
	want := " ДНчСрПрЕМ"
	if got != want {
		t.Errorf("decodeHRTIString(Brief2) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_Name2(t *testing.T) {
	got := decodeHRTIString("6D64PgXeUXdr9C9aP9efZgacp9eecTacp9QPTZURSaaZedr9Hь5G9Hяf9PbэG996D6", "")
	want := "Начислить % на срочную ссудную задолженность (ЕМ) (Кр амД)  "
	if got != want {
		t.Errorf("decodeHRTIString(Name2) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_NoMarker(t *testing.T) {
	got := decodeHRTIString("RegularParameterValue", "")
	want := "RegularParameterValue"
	if got != want {
		t.Errorf("decodeHRTIString(no marker) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_NULL(t *testing.T) {
	got := decodeHRTIString("NULL", "")
	want := "NULL"
	if got != want {
		t.Errorf("decodeHRTIString(\"NULL\") = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_Numeric(t *testing.T) {
	got := decodeHRTIString("20000009238", "")
	want := "20000009238"
	if got != want {
		t.Errorf("decodeHRTIString(numeric) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_Empty(t *testing.T) {
	got := decodeHRTIString("", "")
	want := ""
	if got != want {
		t.Errorf("decodeHRTIString(\"\") = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_SysNames(t *testing.T) {
	tests := []struct {
		enc  string
		want string
	}{
		{"6D64Pg8fZg8f2f999999999999999996D6", "НачСрочСрПр                 "},
		{"6D64Pg5Se8f8f2f9999999999999999996D6", "НачМесСрСрПр                  "},
		{"6D64Pg5Se2f8f2f9999999999999999996D6", "НачМесПрСрПр                  "},
		{"6D67SV8f2f8f2f99999999999999999996D6", "ТекСрПрСрПр                   "},
		{"6D67SV2f2fZh8f2f999999999999999996D6", "ТекПрПроцСрПр                 "},
		{"6D6цYY4Pg2fh8f2f999999999999999996D6", "ГппНачПрцСрПр                 "},
		{"6D64Pg5SeohHd3э9999999999999999996D6", "НачМесяц(тОД                  "},
		{"6D64Pg8fZgHd3э99999999999999999996D6", "НачСроч(тОД                   "},
		{"6D64Pg9PaSSHd3э9999999999999999996D6", "Нач анее(тОД                  "},
		{"6D64Pg5SeohHd2f9999999999999999996D6", "НачМесяц(тПр                  "},
		{"6D64Pg8fZgHd2f99999999999999999996D6", "НачСроч(тПр                   "},
		{"6D64Pg9PaSSHd2f9999999999999999996D6", "Нач анее(тПр                  "},
		{"6D64Pg2fef2f2f8e999999999999999996D6", "НачПрсрПрПрСс                 "},
		{"6D64PgцZT2f2f8e9999999999999999996D6", "НачГодПрПрСс                  "},
		{"6D67SV4Pg2f2f2f8e99999999999999996D6", "ТекНачПрПрПрСс                "},
		{"6D6я999999999999999999999999999996D6", "К                             "},
		{"6D64Pg2fef2f2f8eя99999999999999996D6", "НачПрсрПрПрСсК                "},
		{"6D64Pg8fZgHd3эя9999999999999999996D6", "НачСроч(тОДК                  "},
		{"6D64Pg8fZgHd3э99999999999999999996D6", "НачСроч(тОД                   "},
		{"6D64Pg8fZgHd2f99999999999999999996D6", "НачСроч(тПр                   "},
	}
	for i, tt := range tests {
		got := decodeHRTIString(tt.enc, "")
		if got != tt.want {
			t.Errorf("SysName%d: decodeHRTIString(%q) = %q, want %q", i+1, tt.enc, got, tt.want)
		}
	}
}

func TestIsHRTIContent_Positive(t *testing.T) {
	calls := []*RTICall{
		{Procedure: "TestProc", Params: []RTIParam{
			{Name: "Brief", Type: "DSBRIEFNAME", Value: "6D65Pe4PgъPUU6D6"},
		}},
		{Procedure: "TestProc2", Params: []RTIParam{
			{Name: "Name", Type: "DSFULLNAME", Value: "6D65PeeZNZS9aPgXeUSaXS9QPTZURSaaZedSW999999999999999999999999996D6"},
		}},
		{Procedure: "TestProc3", Params: []RTIParam{
			{Name: "SysName", Type: "DSSYSNAME", Value: "6D64Pg8fZg8f2f999999999999999996D6"},
		}},
	}
	if !isHRTIContent(calls) {
		t.Error("isHRTIContent should return true for HRTI-encoded calls")
	}
}

func TestIsHRTIContent_Negative(t *testing.T) {
	calls := []*RTICall{
		{Procedure: "TestProc", Params: []RTIParam{
			{Name: "Brief", Type: "DSBRIEFNAME", Value: "ЗакрКредит"},
		}},
		{Procedure: "TestProc2", Params: []RTIParam{
			{Name: "Amount", Type: "MONEY", Value: "1000000"},
		}},
	}
	if isHRTIContent(calls) {
		t.Error("isHRTIContent should return false for plain-text calls")
	}
}

func TestDecodeHRTIResult(t *testing.T) {
	result := &RTIParseResult{
		Calls: []*RTICall{
			{
				Procedure: "TestProc",
				Params: []RTIParam{
					{Name: "Brief", Type: "DSBRIEFNAME", Value: "6D65Pe4PgъPUU6D6"},
					{Name: "Amount", Type: "MONEY", Value: "1000000"},
				},
				RetValContext: "6D65Pe4PgъPUU6D6",
				BLogBlocks: []RTIBLogBlock{
					{BlockName: "6D65PeeZNZS9aPgXeUSaXS9QPTZURSaaZedSW999999999999999999999999996D6"},
				},
				BLogTables: []RTIBLogTable{
					{
						TableName: "6D64Pg8fZg8f2f999999999999999996D6",
						Columns:   []string{"6D65Pe4PgъPUU6D6", "ID"},
						Rows:      []string{"6D65Pe4PgъPUU6D6_|_42"},
					},
				},
			},
		},
	}

	DecodeHRTIResult(result)

	call := result.Calls[0]

	if call.Params[0].Value != "МасНачЗалл" {
		t.Errorf("Param[0].Value = %q, want %q", call.Params[0].Value, "МасНачЗалл")
	}
	if call.Params[1].Value != "1000000" {
		t.Errorf("Param[1].Value = %q, want %q", call.Params[1].Value, "1000000")
	}
	if call.RetValContext != "МасНачЗалл" {
		t.Errorf("RetValContext = %q, want %q", call.RetValContext, "МасНачЗалл")
	}
	wantBlock := "Массовое начисление задолженностей                          "
	if call.BLogBlocks[0].BlockName != wantBlock {
		t.Errorf("BLogBlock.BlockName = %q, want %q", call.BLogBlocks[0].BlockName, wantBlock)
	}
	if call.BLogTables[0].TableName != "НачСрочСрПр                 " {
		t.Errorf("BLogTable.TableName = %q, want %q", call.BLogTables[0].TableName, "НачСрочСрПр                 ")
	}
	if call.BLogTables[0].Columns[0] != "МасНачЗалл" {
		t.Errorf("BLogTable.Columns[0] = %q, want %q", call.BLogTables[0].Columns[0], "МасНачЗалл")
	}
	if call.BLogTables[0].Columns[1] != "ID" {
		t.Errorf("BLogTable.Columns[1] = %q, want %q", call.BLogTables[0].Columns[1], "ID")
	}
	if call.BLogTables[0].Rows[0] != "МасНачЗалл_|_42" {
		t.Errorf("BLogTable.Rows[0] = %q, want %q", call.BLogTables[0].Rows[0], "МасНачЗалл_|_42")
	}
}

func TestDecodeHRTIString_TableRow(t *testing.T) {
	row := "6D65Pe4PgъPUU6D6_|_42_|_6D64Pg8fZg8f2f999999999999999996D6"
	got := decodeHRTIRow(row)
	want := "МасНачЗалл_|_42_|_НачСрочСрПр                 "
	if got != want {
		t.Errorf("decodeHRTIRow = %q, want %q", got, want)
	}
}

// encodeHRTIString кодирует строку алгоритмом TDsHash для использования в тестах.
// Формула: NewIndex = ((Ord(char) mod 128) XOR M1 + M2) mod 128, CipherChar = cLetters[NewIndex].
func encodeHRTIString(s string) string {
	var b strings.Builder
	b.WriteString(hrtiMarker)
	for _, r := range s {
		v := int(r) % 128
		idx := ((v ^ hrtiM1) + hrtiM2) % 128
		if idx < 0 {
			idx += 128
		}
		b.WriteRune(cLetters[idx])
	}
	b.WriteString(hrtiMarker)
	return b.String()
}

// --- Tests for bug fix: Latin chars A-O, Q and / were misdecoded as Russian ---

func TestDecodeHRTIString_LatinFormField(t *testing.T) {
	// "TContractSetFundCrdLinePmntPF" — DSFIELDNAMEVAR, always Latin
	want := "TContractSetFundCrdLinePmntPF"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
	if got != want {
		t.Errorf("decodeHRTIString(Latin form, DSFIELDNAMEVAR) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_LatinFilePath(t *testing.T) {
	// "Consumer\\RSRPRConsRam.smf" — contains / and Latin A-O, type DSCOMMENT
	want := "Consumer\\RSRPRConsRam.smf"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSCOMMENT")
	if got != want {
		t.Errorf("decodeHRTIString(Latin file path, DSCOMMENT) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_LatinCheckLimitForOver(t *testing.T) {
	want := "CheckLimitForOver"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
	if got != want {
		t.Errorf("decodeHRTIString(CheckLimitForOver) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_LatinCheckLimitForTranche(t *testing.T) {
	want := "CheckLimitForTranche"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
	if got != want {
		t.Errorf("decodeHRTIString(CheckLimitForTranche) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_SlashNotYa(t *testing.T) {
	// v=47 should decode as / not Я
	want := "/"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "")
	if got != want {
		t.Errorf("decodeHRTIString(slash) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_LatinAtoO(t *testing.T) {
	// All Latin uppercase A-O were previously misdecoded as Russian с,т,у,ф,х,ц,ч,ш,щ,ъ,ы,ь,э,ю,я
	want := "ABCDEFGHIJKLMNO"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
	if got != want {
		t.Errorf("decodeHRTIString(A-O) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_LatinQ(t *testing.T) {
	// Q was previously misdecoded as ё
	want := "Q"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
	if got != want {
		t.Errorf("decodeHRTIString(Q) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_HeuristicLatinInComment(t *testing.T) {
	// DSCOMMENT field with mostly Latin text — heuristic should detect Latin
	want := "CheckLimitForOver"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSCOMMENT")
	if got != want {
		t.Errorf("decodeHRTIString(heuristic Latin in DSCOMMENT) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_HeuristicRussianPreferred(t *testing.T) {
	// Russian text should still be decoded as Russian via heuristic
	want := "МасНачЗалл"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSCOMMENT")
	if got != want {
		t.Errorf("decodeHRTIString(heuristic Russian) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIString_HeuristicMixedLatinPath(t *testing.T) {
	// Mixed content: Latin path with dots — heuristic should prefer Latin
	want := "Consumer\\RSRPRConsRam.smf"
	enc := encodeHRTIString(want)
	got := decodeHRTIString(enc, "DSCOMMENT")
	if got != want {
		t.Errorf("decodeHRTIString(heuristic mixed Latin path) = %q, want %q", got, want)
	}
}

func TestDecodeHRTIResult_LatinFieldsNotCorrupted(t *testing.T) {
	latinVal := "TContractSetFundCrdLinePmntPF"
	russianVal := "МасНачЗалл"
	result := &RTIParseResult{
		Calls: []*RTICall{
			{
				Procedure: "TestProc",
				Params: []RTIParam{
					{Name: "FormName", Type: "DSFIELDNAMEVAR", Value: encodeHRTIString(latinVal)},
					{Name: "Brief", Type: "DSBRIEFNAME", Value: encodeHRTIString(russianVal)},
				},
			},
		},
	}

	DecodeHRTIResult(result)

	call := result.Calls[0]
	if call.Params[0].Value != latinVal {
		t.Errorf("Param[0] (DSFIELDNAMEVAR) = %q, want %q", call.Params[0].Value, latinVal)
	}
	if call.Params[1].Value != russianVal {
		t.Errorf("Param[1] (DSBRIEFNAME) = %q, want %q", call.Params[1].Value, russianVal)
	}
}

func TestIsLatinFieldType(t *testing.T) {
	tests := []struct {
		fieldType string
		want      bool
	}{
		{"DSFIELDNAMEVAR", true},
		{"DSFIELDNAME", true},
		{"DSPARAMNAME", true},
		{"DSTYPENAME", true},
		{"DSBRIEFNAME", false},
		{"DSFULLNAME", false},
		{"DSCOMMENT", false},
		{"MONEY", false},
		{"", false},
	}
	for _, tt := range tests {
		got := isLatinFieldType(tt.fieldType)
		if got != tt.want {
			t.Errorf("isLatinFieldType(%q) = %v, want %v", tt.fieldType, got, tt.want)
		}
	}
}

func TestDetectPreferLatin(t *testing.T) {
	// Latin-heavy string — heuristic should return true
	latinEnc := encodeHRTIString("CheckLimitForOver")
	inner := latinEnc[3 : len(latinEnc)-3]
	if !detectPreferLatin(inner) {
		t.Error("detectPreferLatin should return true for Latin-heavy string")
	}

	// Russian-heavy string — heuristic should return false
	russianEnc := encodeHRTIString("МасНачЗалл")
	inner2 := russianEnc[3 : len(russianEnc)-3]
	if detectPreferLatin(inner2) {
		t.Error("detectPreferLatin should return false for Russian-heavy string")
	}
}

func TestEncodeDecodeRoundTrip_Latin(t *testing.T) {
	tests := []string{
		"TContractSetFundCrdLinePmntPF",
		"CheckLimitForOver",
		"CheckLimitForTranche",
		"Consumer\\RSRPRConsRam.smf",
		"ABCDEFGHIJKLMNO",
		"Q",
		"PQR",
		"FormName",
		"@ManualCreationFlag",
		"@TypeID",
	}
	for _, want := range tests {
		enc := encodeHRTIString(want)
		got := decodeHRTIString(enc, "DSFIELDNAMEVAR")
		if got != want {
			t.Errorf("roundtrip(%q) = %q, want %q", want, got, want)
		}
	}
}

func TestEncodeDecodeRoundTrip_Russian(t *testing.T) {
	tests := []string{
		"МасНачЗалл",
		"Массовое начисление задолженностей",
		"Начислить % на срочную ссудную задолженность",
	}
	for _, want := range tests {
		enc := encodeHRTIString(want)
		got := decodeHRTIString(enc, "DSBRIEFNAME")
		if got != want {
			t.Errorf("roundtrip(%q) = %q, want %q", want, got, want)
		}
	}
}

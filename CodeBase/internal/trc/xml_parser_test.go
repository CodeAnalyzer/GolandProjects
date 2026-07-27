package trc

import (
	"path/filepath"
	"testing"
)

func TestDetectFormat_XMLWithBOM(t *testing.T) {
	// UTF-16LE BOM (FF FE) + "<?xml" в UTF-16LE.
	data := []byte{0xFF, 0xFE}
	for _, r := range "<?xml version=\"1.0\" encoding=\"utf-16\"?>" {
		data = append(data, byte(r), 0)
	}
	if got := DetectFormat(data); got != FormatXML {
		t.Fatalf("DetectFormat: got %s, expected xml for UTF-16 XML prologue", got)
	}
}

func TestDetectFormat_XMLPlainUTF8(t *testing.T) {
	data := []byte("<TraceData xmlns=\"http://tempuri.org/TracePersistence.xsd\">")
	if got := DetectFormat(data); got != FormatXML {
		t.Fatalf("DetectFormat: got %s, expected xml for plain UTF-8 <TraceData prologue", got)
	}
}

func TestDetectFormat_Binary(t *testing.T) {
	data := []byte{0x00, 0x00, 0x00, 0x01, 0xF6, 0xFF, 0x06, 0x25, 0x00}
	if got := DetectFormat(data); got != FormatBinary {
		t.Fatalf("DetectFormat: got %s, expected binary", got)
	}
}

func TestDetectFormat_XEL(t *testing.T) {
	data := append([]byte{0x5A, 0x37, 0xAB, 0xEF, 0x0A, 0x00, 0x00, 0x02}, make([]byte, 16)...)
	if got := DetectFormat(data); got != FormatXEL {
		t.Fatalf("DetectFormat: got %s, expected xel", got)
	}
}

func TestDecodeXMLColumnValue_Int32(t *testing.T) {
	got := decodeXMLColumnValue(12, "55") // SPID, TypeInt32
	v, ok := got.(int32)
	if !ok || v != 55 {
		t.Fatalf("got %v (%T), want int32(55)", got, got)
	}
}

func TestDecodeXMLColumnValue_Int64(t *testing.T) {
	got := decodeXMLColumnValue(13, "123310") // Duration, TypeInt64
	v, ok := got.(int64)
	if !ok || v != 123310 {
		t.Fatalf("got %v (%T), want int64(123310)", got, got)
	}
}

func TestDecodeXMLColumnValue_String(t *testing.T) {
	got := decodeXMLColumnValue(1, "while 1 = 1") // TextData, TypeString
	s, ok := got.(string)
	if !ok || s != "while 1 = 1" {
		t.Fatalf("got %v (%T), want %q", got, got, "while 1 = 1")
	}
}

func TestDecodeXMLColumnValue_DateTime(t *testing.T) {
	got := decodeXMLColumnValue(14, "2026-03-19T15:55:32.757+03:00") // StartTime, TypeDateTime
	st, ok := got.(SystemTime)
	if !ok {
		t.Fatalf("got %T, want SystemTime", got)
	}
	if st.Year != 2026 || st.Month != 3 || st.Day != 19 {
		t.Errorf("date = %d-%d-%d, want 2026-3-19", st.Year, st.Month, st.Day)
	}
	if st.Hour != 15 || st.Minute != 55 || st.Second != 32 {
		t.Errorf("time = %d:%d:%d, want 15:55:32", st.Hour, st.Minute, st.Second)
	}
	if st.Milliseconds != 757 {
		t.Errorf("ms = %d, want 757", st.Milliseconds)
	}
}

func TestDecodeXMLColumnValue_Binary(t *testing.T) {
	got := decodeXMLColumnValue(41, "3F28485733559D42A2631F19875EC9B4") // LoginSid, TypeBinary
	b, ok := got.([]byte)
	if !ok || len(b) != 16 {
		t.Fatalf("got %v (%T), want 16-byte []byte", got, got)
	}
}

func TestDecodeXMLColumnValue_InvalidFallsBackToString(t *testing.T) {
	got := decodeXMLColumnValue(12, "not-a-number") // SPID, TypeInt32, некорректное значение
	s, ok := got.(string)
	if !ok || s != "not-a-number" {
		t.Fatalf("got %v (%T), want fallback string %q", got, got, "not-a-number")
	}
}

// TestParseFile_XMLMatchesBinary — golden-тест: ParseFile на XML-экспорте
// того же трейса (DIAPR-391.xml) должен дать результат, эквивалентный
// ParseFile на бинарном .trc, для ключевых полей каждого события
// (EventClass, TextData, SPID, EventSequence, StartTime, DurationMs,
// Procedure). Проверяет весь путь: DetectFormat -> ParseXML -> enrichEvent.
func TestParseFile_XMLMatchesBinary(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	xmlPath := filepath.Join(dir, "DIAPR-391.xml")
	skipIfMissing(t, trcPath)
	skipIfMissing(t, xmlPath)

	binResult, err := ParseFile(trcPath)
	if err != nil {
		t.Fatalf("ParseFile(%s): %v", trcPath, err)
	}
	xmlResult, err := ParseFile(xmlPath)
	if err != nil {
		t.Fatalf("ParseFile(%s): %v", xmlPath, err)
	}

	if len(xmlResult.Events) != len(binResult.Events) {
		t.Fatalf("event count = %d, want %d", len(xmlResult.Events), len(binResult.Events))
	}

	mismatches := 0
	for i := range binResult.Events {
		bin := binResult.Events[i]
		xm := xmlResult.Events[i]
		if xm.EventClass != bin.EventClass {
			t.Errorf("event %d: EventClass = %d, want %d", i, xm.EventClass, bin.EventClass)
			mismatches++
			continue
		}
		if xm.Procedure != bin.Procedure {
			t.Errorf("event %d (class %d): Procedure = %q, want %q", i, bin.EventClass, xm.Procedure, bin.Procedure)
			mismatches++
		}
		if xm.DurationMs != bin.DurationMs {
			t.Errorf("event %d (class %d): DurationMs = %d, want %d", i, bin.EventClass, xm.DurationMs, bin.DurationMs)
			mismatches++
		}
		if binSPID, ok := bin.Columns[12]; ok {
			if xm.Columns[12] != binSPID {
				t.Errorf("event %d (class %d): SPID = %v, want %v", i, bin.EventClass, xm.Columns[12], binSPID)
				mismatches++
			}
		}
		if binSeq, ok := bin.Columns[51]; ok {
			if xm.Columns[51] != binSeq {
				t.Errorf("event %d (class %d): EventSequence = %v, want %v", i, bin.EventClass, xm.Columns[51], binSeq)
				mismatches++
			}
		}
		if binStart, ok := bin.Columns[14].(SystemTime); ok {
			xmStart, ok := xm.Columns[14].(SystemTime)
			if !ok || xmStart != binStart {
				t.Errorf("event %d (class %d): StartTime = %+v, want %+v", i, bin.EventClass, xm.Columns[14], binStart)
				mismatches++
			}
		}
		if mismatches > 20 {
			t.Fatalf("too many mismatches, aborting early")
		}
	}
}

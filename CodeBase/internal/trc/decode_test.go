package trc

import (
	"encoding/binary"
	"testing"
	"time"
)

// TestDecodeColumnValue_String — UTF-16LE строка декодируется корректно.
func TestDecodeColumnValue_String(t *testing.T) {
	// "Hi" в UTF-16LE: 0x48 0x00 0x69 0x00
	raw := []byte{0x48, 0x00, 0x69, 0x00}
	got, err := decodeColumnValue(1, raw) // column 1 = TextData = TypeString
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	s, ok := got.(string)
	if !ok {
		t.Fatalf("expected string, got %T", got)
	}
	if s != "Hi" {
		t.Errorf("got %q, want %q", s, "Hi")
	}
}

// TestDecodeColumnValue_Int32 — 4-байтовое значение декодируется как int32.
func TestDecodeColumnValue_Int32(t *testing.T) {
	// SPID = 55, колонка 12 (TypeInt32)
	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, 55)
	got, err := decodeColumnValue(12, raw)
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	v, ok := got.(int32)
	if !ok {
		t.Fatalf("expected int32, got %T", got)
	}
	if v != 55 {
		t.Errorf("got %d, want 55", v)
	}
}

// TestDecodeColumnValue_Int64 — 8-байтовое значение декодируется как int64.
func TestDecodeColumnValue_Int64(t *testing.T) {
	// Duration = 12345678 (микросекунды), колонка 13 (TypeInt64)
	raw := make([]byte, 8)
	binary.LittleEndian.PutUint64(raw, 12345678)
	got, err := decodeColumnValue(13, raw)
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	v, ok := got.(int64)
	if !ok {
		t.Fatalf("expected int64, got %T", got)
	}
	if v != 12345678 {
		t.Errorf("got %d, want 12345678", v)
	}
}

// TestDecodeColumnValue_SystemTime — 16-байтовое DateTime декодируется как SystemTime.
func TestDecodeColumnValue_SystemTime(t *testing.T) {
	// StartTime, колонка 14 (TypeDateTime), 16 байт
	raw := make([]byte, 16)
	binary.LittleEndian.PutUint16(raw[0:2], 2024)  // Year
	binary.LittleEndian.PutUint16(raw[2:4], 6)     // Month
	binary.LittleEndian.PutUint16(raw[4:6], 4)     // DayOfWeek
	binary.LittleEndian.PutUint16(raw[6:8], 15)   // Day
	binary.LittleEndian.PutUint16(raw[8:10], 10)  // Hour
	binary.LittleEndian.PutUint16(raw[10:12], 30) // Minute
	binary.LittleEndian.PutUint16(raw[12:14], 45) // Second
	binary.LittleEndian.PutUint16(raw[14:16], 500) // Milliseconds

	got, err := decodeColumnValue(14, raw)
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	st, ok := got.(SystemTime)
	if !ok {
		t.Fatalf("expected SystemTime, got %T", got)
	}
	if st.Year != 2024 || st.Month != 6 || st.Day != 15 {
		t.Errorf("date = %d-%d-%d, want 2024-6-15", st.Year, st.Month, st.Day)
	}
	if st.Hour != 10 || st.Minute != 30 || st.Second != 45 {
		t.Errorf("time = %d:%d:%d, want 10:30:45", st.Hour, st.Minute, st.Second)
	}
	if st.Milliseconds != 500 {
		t.Errorf("ms = %d, want 500", st.Milliseconds)
	}

	// Проверяем ToTime
	tm, ok := st.ToTime()
	if !ok {
		t.Fatalf("ToTime returned ok=false")
	}
	want := time.Date(2024, 6, 15, 10, 30, 45, 500_000_000, time.UTC)
	if !tm.Equal(want) {
		t.Errorf("ToTime = %v, want %v", tm, want)
	}
}

// TestDecodeColumnValue_EmptyString — пустая UTF-16LE строка (0 байт).
func TestDecodeColumnValue_EmptyString(t *testing.T) {
	got, err := decodeColumnValue(1, []byte{})
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	s, ok := got.(string)
	if !ok || s != "" {
		t.Fatalf("expected empty string, got %v", got)
	}
}

// TestDecodeColumnValue_Binary — неизвестная длина возвращает []byte.
func TestDecodeColumnValue_Binary(t *testing.T) {
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01}
	got, err := decodeColumnValue(99, raw) // column 99 — unknown type
	if err != nil {
		t.Fatalf("decodeColumnValue: %v", err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("expected []byte, got %T", got)
	}
	if len(b) != len(raw) {
		t.Errorf("len = %d, want %d", len(b), len(raw))
	}
	for i := range raw {
		if b[i] != raw[i] {
			t.Errorf("byte %d: got %02x, want %02x", i, b[i], raw[i])
		}
	}
}

// TestSystemTime_ZeroYear — нулевой год → ok=false.
func TestSystemTime_ZeroYear(t *testing.T) {
	st := SystemTime{}
	_, ok := st.ToTime()
	if ok {
		t.Fatal("expected ok=false for zero SystemTime")
	}
}

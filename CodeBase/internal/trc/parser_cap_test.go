package trc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

// makeEventBytes строит байтовую последовательность одного события:
// маркер(3) + EventClass(2) + Length(4) + fields(Length байт).
func makeEventBytes(eventClass int, fields []byte) []byte {
	var buf bytes.Buffer
	buf.Write(eventHeaderMarker[:])
	buf.WriteByte(0x06)
	var hdr [6]byte
	binary.LittleEndian.PutUint16(hdr[0:2], uint16(eventClass))
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(len(fields)))
	buf.Write(hdr[:])
	buf.Write(fields)
	return buf.Bytes()
}

// makeFieldBytes строит одно поле события: PropID(2) + lenByte(1) + value.
func makeFieldBytes(propID int, value []byte) []byte {
	var buf bytes.Buffer
	var pid [2]byte
	binary.LittleEndian.PutUint16(pid[:], uint16(propID))
	buf.Write(pid[:])
	if len(value) >= 0xFF {
		buf.WriteByte(0xFF)
		var ext [4]byte
		binary.LittleEndian.PutUint32(ext[:], uint32(len(value)))
		buf.Write(ext[:])
	} else {
		buf.WriteByte(byte(len(value)))
	}
	buf.Write(value)
	return buf.Bytes()
}

// TestParseEventsStreaming_LengthCap_SkipsOversizedEvent проверяет, что
// событие с length > maxEventSize пропускается (без make([]byte, length)),
// а последующее валидное событие корректно парсится.
func TestParseEventsStreaming_LengthCap_SkipsOversizedEvent(t *testing.T) {
	// Событие 1: oversize length (maxEventSize + 1). Поля — мусорные байты,
	// за которыми сразу следует маркер следующего события.
	oversizeLen := maxEventSize + 1
	garbageFields := make([]byte, 50) // немного мусора

	var stream bytes.Buffer
	// Событие 1: маркер + class=44 + oversize length + немного мусора
	stream.Write(eventHeaderMarker[:])
	stream.WriteByte(0x06)
	var hdr1 [6]byte
	binary.LittleEndian.PutUint16(hdr1[0:2], 44)
	binary.LittleEndian.PutUint32(hdr1[2:6], uint32(oversizeLen))
	stream.Write(hdr1[:])
	stream.Write(garbageFields)

	// Событие 2: валидное событие class=10 с одним полем TextData.
	// skipToEventMarker найдёт этот маркер внутри мусора после oversize события.
	validFields := makeFieldBytes(1, []byte("exec sp_test"))
	stream.Write(makeEventBytes(10, validFields))

	r := bufio.NewReader(&stream)
	h := &TraceHeader{EventsOffset: 0}
	events, err := ParseEventsStreaming(r, h)
	if err != nil {
		t.Fatalf("ParseEventsStreaming: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event (oversize skipped), got %d", len(events))
	}
	if events[0].EventClass != 10 {
		t.Errorf("event class = %d, want 10", events[0].EventClass)
	}
}

// TestParseEvents_LengthCap_SkipsOversizedEvent — то же для in-memory парсера.
func TestParseEvents_LengthCap_SkipsOversizedEvent(t *testing.T) {
	// Событие 1: oversize length, но fieldsEnd > len(data) — будет skip.
	oversizeLen := maxEventSize + 1
	validFields := makeFieldBytes(1, []byte("exec sp_test"))

	var data bytes.Buffer
	// Событие 1: oversize
	data.Write(eventHeaderMarker[:])
	data.WriteByte(0x06)
	var hdr1 [6]byte
	binary.LittleEndian.PutUint16(hdr1[0:2], 44)
	binary.LittleEndian.PutUint32(hdr1[2:6], uint32(oversizeLen))
	data.Write(hdr1[:])
	// Не пишем oversizeLen байт мусора — просто переходим к следующему событию.
	// Событие 2: валидное
	data.Write(makeEventBytes(10, validFields))

	h := &TraceHeader{EventsOffset: 0}
	events, err := ParseEvents(data.Bytes(), h)
	if err != nil {
		t.Fatalf("ParseEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event (oversize skipped), got %d", len(events))
	}
	if events[0].EventClass != 10 {
		t.Errorf("event class = %d, want 10", events[0].EventClass)
	}
}

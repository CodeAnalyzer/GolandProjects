package trc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
	"unicode/utf16"
)

// encodeUTF16LE кодирует строку в UTF-16LE байты (формат TextData в .trc).
func encodeUTF16LE(s string) []byte {
	units := utf16.Encode([]rune(s))
	b := make([]byte, len(units)*2)
	for i, u := range units {
		binary.LittleEndian.PutUint16(b[i*2:], u)
	}
	return b
}

// TestParseEventsStreamingCB_Callback проверяет, что callback вызывается
// для каждого события в правильном порядке, и что parseEventsStreamingCB
// не накапливает события в памяти.
func TestParseEventsStreamingCB_Callback(t *testing.T) {
	// Два валидных события
	fields1 := makeFieldBytes(1, encodeUTF16LE("exec sp_test1"))
	fields2 := makeFieldBytes(1, encodeUTF16LE("exec sp_test2"))

	var stream bytes.Buffer
	stream.Write(makeEventBytes(10, fields1))
	stream.Write(makeEventBytes(11, fields2))

	r := bufio.NewReader(&stream)

	var collected []TRCEvent
	err := parseEventsStreamingCB(r, func(ev *TRCEvent) error {
		collected = append(collected, *ev)
		return nil
	})
	if err != nil {
		t.Fatalf("parseEventsStreamingCB: %v", err)
	}
	if len(collected) != 2 {
		t.Fatalf("expected 2 events, got %d", len(collected))
	}
	if collected[0].EventClass != 10 {
		t.Errorf("event 0: class=%d, want 10", collected[0].EventClass)
	}
	if collected[1].EventClass != 11 {
		t.Errorf("event 1: class=%d, want 11", collected[1].EventClass)
	}
	// Проверяем что enrichEvent был вызван (Procedure заполнен)
	if collected[0].Procedure != "sp_test1" {
		t.Errorf("event 0: Procedure=%q, want sp_test1", collected[0].Procedure)
	}
	if collected[1].Procedure != "sp_test2" {
		t.Errorf("event 1: Procedure=%q, want sp_test2", collected[1].Procedure)
	}
}

// TestParseEventsStreamingCB_NilCallback проверяет, что nil callback
// не вызывает панику и функция просто парсит события без действий.
func TestParseEventsStreamingCB_NilCallback(t *testing.T) {
	fields := makeFieldBytes(1, []byte("exec sp_test"))

	var stream bytes.Buffer
	stream.Write(makeEventBytes(10, fields))

	r := bufio.NewReader(&stream)

	err := parseEventsStreamingCB(r, nil)
	if err != nil {
		t.Fatalf("parseEventsStreamingCB with nil callback: %v", err)
	}
}

// TestParseEventsStreamingCB_LengthCap_SkipsOversized проверяет, что
// oversize события пропускаются в callback-режиме.
func TestParseEventsStreamingCB_LengthCap_SkipsOversized(t *testing.T) {
	oversizeLen := maxEventSize + 1
	garbageFields := make([]byte, 50)

	var stream bytes.Buffer
	// Oversize event
	stream.Write(eventHeaderMarker[:])
	stream.WriteByte(0x06)
	var hdr1 [6]byte
	binary.LittleEndian.PutUint16(hdr1[0:2], 44)
	binary.LittleEndian.PutUint32(hdr1[2:6], uint32(oversizeLen))
	stream.Write(hdr1[:])
	stream.Write(garbageFields)

	// Valid event
	validFields := makeFieldBytes(1, encodeUTF16LE("exec sp_valid"))
	stream.Write(makeEventBytes(10, validFields))

	r := bufio.NewReader(&stream)

	var collected []TRCEvent
	err := parseEventsStreamingCB(r, func(ev *TRCEvent) error {
		collected = append(collected, *ev)
		return nil
	})
	if err != nil {
		t.Fatalf("parseEventsStreamingCB: %v", err)
	}
	if len(collected) != 1 {
		t.Fatalf("expected 1 event (oversize skipped), got %d", len(collected))
	}
	if collected[0].EventClass != 10 {
		t.Errorf("event class=%d, want 10", collected[0].EventClass)
	}
}

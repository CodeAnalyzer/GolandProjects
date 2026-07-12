package trc

import (
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/unicode"
)

// xmlEvent/xmlColumn — минимальная структура для разбора секции <Events> из
// XML-экспорта SQL Server Profiler (Modifications/*.xml), достаточная для
// сверки TextData/EventClass/SPID/EventSequence с результатом ParseEvents.
type xmlColumn struct {
	ID    int    `xml:"id,attr"`
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

type xmlEvent struct {
	ID      int         `xml:"id,attr"`
	Name    string      `xml:"name,attr"`
	Columns []xmlColumn `xml:"Column"`
}

type xmlEventsDoc struct {
	Events []xmlEvent `xml:"Events>Event"`
}

// readXMLEvents читает весь .xml файл (DIAPR-391.xml — 2.3 МБ, целиком в
// память допустимо) в UTF-16 и разбирает секцию <Events>.
func readXMLEvents(t *testing.T, path string) []xmlEvent {
	t.Helper()
	content := decodeUTF16File(t, path)

	// Отрезаем XML-пролог (<?xml ... encoding="utf-16"?>) — содержимое уже
	// декодировано в UTF-8 строку, но encoding/xml.Unmarshal откажется
	// работать, увидев в прологе "utf-16" без зарегистрированного
	// CharsetReader (тот же приём, что и в readXMLHeaderPrefix, header_test.go).
	startIdx := indexOrLen(content, "<TraceData")
	fragment := content[startIdx:]

	var doc xmlEventsDoc
	if err := xml.Unmarshal([]byte(fragment), &doc); err != nil {
		t.Fatalf("unmarshal XML events %s: %v", path, err)
	}
	return doc.Events
}

// decodeUTF16File читает весь файл и декодирует его из UTF-16LE (с BOM) в
// UTF-8 строку — DIAPR-391.xml (~2.3 МБ) целиком в память допустим; для
// больших файлов (nbki.xml, 754 МБ) нужен потоковый вариант, см.
// readXMLHeaderPrefix в header_test.go.
func decodeUTF16File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	decoder := unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder()
	utf8Bytes, err := decoder.Bytes(raw)
	if err != nil {
		t.Fatalf("decode UTF-16 %s: %v", path, err)
	}
	return string(utf8Bytes)
}

// columnValueByID возвращает значение колонки id из xmlEvent, либо "" если
// колонка отсутствует (события экспортируют только непустые/установленные
// колонки, см. Modifications/trc-trace-parser-ef18d1.md находка Phase 0).
func (e xmlEvent) columnValueByID(id int) (string, bool) {
	for _, c := range e.Columns {
		if c.ID == id {
			return c.Value, true
		}
	}
	return "", false
}

// TestParseEvents_DIAPR391_MatchesXML — golden-тест Phase 1: разбирает
// DIAPR-391.trc через ParseFile и сверяет с XML-экспортом того же трейса
// количество событий, EventClass и значения TextData/SPID/EventSequence
// (устойчивый монотонный идентификатор события) для каждого события по
// порядку. Полная построчная сверка всех колонок всех событий — предмет
// отдельного расширения теста при необходимости.
func TestParseEvents_DIAPR391_MatchesXML(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	xmlPath := filepath.Join(dir, "DIAPR-391.xml")

	result, err := ParseFile(trcPath)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	wantEvents := readXMLEvents(t, xmlPath)

	if len(result.Events) != len(wantEvents) {
		t.Fatalf("event count = %d, want %d", len(result.Events), len(wantEvents))
	}

	mismatches := 0
	for i, got := range result.Events {
		want := wantEvents[i]
		if got.EventClass != want.ID {
			t.Errorf("event %d: EventClass = %d, want %d", i, got.EventClass, want.ID)
			mismatches++
			continue
		}
		if wantText, ok := want.columnValueByID(1); ok {
			gotText, _ := got.Columns[1].(string)
			// XML-парсер по спецификации XML 1.0 нормализует переводы
			// строк \r\n -> \n и одиночный \r -> \n при парсинге chardata;
			// бинарь хранит исходный текст с \r\n или \r как есть —
			// нормализуем перед сверкой.
			gotNormalized := strings.ReplaceAll(gotText, "\r\n", "\n")
			gotNormalized = strings.ReplaceAll(gotNormalized, "\r", "\n")
			if gotNormalized != wantText {
				t.Errorf("event %d (class %d): TextData mismatch:\n got=%q\nwant=%q", i, got.EventClass, gotText, wantText)
				mismatches++
			}
		}
		if wantSPID, ok := want.columnValueByID(12); ok {
			gotSPID := fmt.Sprintf("%d", got.Columns[12])
			if gotSPID != wantSPID {
				t.Errorf("event %d (class %d): SPID mismatch: got=%v want=%v", i, got.EventClass, got.Columns[12], wantSPID)
				mismatches++
			}
		}
		if wantSeq, ok := want.columnValueByID(51); ok {
			gotSeq := fmt.Sprintf("%d", got.Columns[51])
			if gotSeq != wantSeq {
				t.Errorf("event %d (class %d): EventSequence mismatch: got=%v want=%v", i, got.EventClass, got.Columns[51], wantSeq)
				mismatches++
			}
		}
		if mismatches > 20 {
			t.Fatalf("too many mismatches, aborting early")
		}
	}
}

// TestParseEvents_NBKI_MatchesXML — golden-тест на втором файле (nbki.trc,
// SQL2019 build 4335, 212 МБ). Сравнивает EventClass, TextData, SPID и
// EventSequence с XML-экспортом. nbki.xml — 754 МБ в UTF-16, чтение целиком
// в память допустимо для тестовой машины; если файл отсутствует, тест
// пропускается.
func TestParseEvents_NBKI_MatchesXML(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "nbki.trc")
	xmlPath := filepath.Join(dir, "nbki.xml")

	if _, err := os.Stat(trcPath); err != nil {
		t.Skipf("nbki.trc not found: %v", err)
	}
	if _, err := os.Stat(xmlPath); err != nil {
		t.Skipf("nbki.xml not found: %v", err)
	}

	result, err := ParseFile(trcPath)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	wantEvents := readXMLEvents(t, xmlPath)

	if len(result.Events) != len(wantEvents) {
		t.Fatalf("event count = %d, want %d", len(result.Events), len(wantEvents))
	}

	mismatches := 0
	for i, got := range result.Events {
		want := wantEvents[i]
		if got.EventClass != want.ID {
			t.Errorf("event %d: EventClass = %d, want %d", i, got.EventClass, want.ID)
			mismatches++
			continue
		}
		if wantText, ok := want.columnValueByID(1); ok {
			gotText, _ := got.Columns[1].(string)
			// XML нормализует \r\n -> \n и \r -> \n; бинарь хранит как есть.
			gotNormalized := strings.ReplaceAll(gotText, "\r\n", "\n")
			gotNormalized = strings.ReplaceAll(gotNormalized, "\r", "\n")
			if gotNormalized != wantText {
				t.Errorf("event %d (class %d): TextData mismatch:\n got=%q\nwant=%q", i, got.EventClass, gotText, wantText)
				mismatches++
			}
		}
		if wantSPID, ok := want.columnValueByID(12); ok {
			gotSPID := fmt.Sprintf("%d", got.Columns[12])
			if gotSPID != wantSPID {
				t.Errorf("event %d (class %d): SPID mismatch: got=%v want=%v", i, got.EventClass, got.Columns[12], wantSPID)
				mismatches++
			}
		}
		if wantSeq, ok := want.columnValueByID(51); ok {
			gotSeq := fmt.Sprintf("%d", got.Columns[51])
			if gotSeq != wantSeq {
				t.Errorf("event %d (class %d): EventSequence mismatch: got=%v want=%v", i, got.EventClass, got.Columns[51], wantSeq)
				mismatches++
			}
		}
		if mismatches > 20 {
			t.Fatalf("too many mismatches, aborting early")
		}
	}
}

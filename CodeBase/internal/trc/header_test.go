package trc

import (
	"encoding/xml"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"golang.org/x/text/encoding/unicode"
)

// xmlHeader — минимальная структура для разбора <Header> секции XML-экспорта
// SQL Server Profiler (Modifications/*.xml), достаточная для сверки со
// схемой, разобранной ParseHeader из бинарного .trc.
type xmlHeader struct {
	TraceProvider struct {
		Name         string `xml:"name,attr"`
		MajorVersion int    `xml:"MajorVersion,attr"`
		MinorVersion int    `xml:"MinorVersion,attr"`
		BuildNumber  int    `xml:"BuildNumber,attr"`
	} `xml:"Header>TraceProvider"`
	ServerInformation struct {
		Name string `xml:"name,attr"`
	} `xml:"Header>ServerInformation"`
	OrderedColumns struct {
		ID []int `xml:"ID"`
	} `xml:"Header>ProfilerUI>OrderedColumns"`
	TracedEvents struct {
		Event []struct {
			ID          int `xml:"id,attr"`
			EventColumn []struct {
				ID int `xml:"id,attr"`
			} `xml:"EventColumn"`
		} `xml:"Event"`
	} `xml:"Header>ProfilerUI>TracedEvents"`
}

// modificationsDir — папка с тестовыми файлами (.trc/.xml), относительно
// пакета internal/trc.
func modificationsDir(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "Modifications")
}

// readXMLHeaderPrefix читает первые maxBytes байт UTF-16 XML файла (без
// загрузки всего файла — nbki.xml занимает 754 МБ), декодирует в UTF-8 и
// парсит секцию <Header>...</Header> в xmlHeader. Не требует валидного
// завершения всего XML-документа.
func readXMLHeaderPrefix(t *testing.T, path string, maxBytes int64) *xmlHeader {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	n := maxBytes
	if fi.Size() < n {
		n = fi.Size()
	}
	n -= n % 2 // выравнивание по границе UTF-16 code unit

	raw := make([]byte, n)
	if _, err := io.ReadFull(f, raw); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	decoder := unicode.UTF16(unicode.LittleEndian, unicode.ExpectBOM).NewDecoder()
	utf8Bytes, err := decoder.Bytes(raw)
	if err != nil {
		t.Fatalf("decode UTF-16 %s: %v", path, err)
	}
	content := string(utf8Bytes)

	startIdx := indexOrLen(content, "<TraceData")
	closeIdx := indexOrLen(content, "</Header>") + len("</Header>")
	fragment := content[startIdx:closeIdx] + "</TraceData>"

	var h xmlHeader
	if err := xml.Unmarshal([]byte(fragment), &h); err != nil {
		t.Fatalf("unmarshal XML header %s: %v", path, err)
	}
	return &h
}

func indexOrLen(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return len(s)
}

func TestParseHeader_DIAPR391_MatchesXML(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	xmlPath := filepath.Join(dir, "DIAPR-391.xml")
	assertHeaderMatchesXML(t, trcPath, xmlPath, 2<<20)
}

func TestParseHeader_NBKI_MatchesXML(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "nbki.trc")
	xmlPath := filepath.Join(dir, "nbki.xml")
	if _, err := os.Stat(trcPath); err != nil {
		t.Skipf("nbki.trc not available: %v", err)
	}
	assertHeaderMatchesXML(t, trcPath, xmlPath, 2<<20)
}

func assertHeaderMatchesXML(t *testing.T, trcPath, xmlPath string, xmlPrefixBytes int64) {
	t.Helper()

	trcData, err := os.ReadFile(trcPath)
	if err != nil {
		t.Fatalf("read %s: %v", trcPath, err)
	}
	got, err := ParseHeader(trcData)
	if err != nil {
		t.Fatalf("ParseHeader(%s): %v", trcPath, err)
	}

	want := readXMLHeaderPrefix(t, xmlPath, xmlPrefixBytes)

	if got.ProviderName != want.TraceProvider.Name {
		t.Errorf("ProviderName = %q, want %q", got.ProviderName, want.TraceProvider.Name)
	}
	if got.ServerName != want.ServerInformation.Name {
		t.Errorf("ServerName = %q, want %q", got.ServerName, want.ServerInformation.Name)
	}

	wantOrdered := want.OrderedColumns.ID
	if !reflect.DeepEqual(got.OrderedColumns, wantOrdered) {
		t.Errorf("OrderedColumns = %v, want %v", got.OrderedColumns, wantOrdered)
	}

	if len(got.EventClasses) != len(want.TracedEvents.Event) {
		t.Fatalf("EventClasses count = %d, want %d", len(got.EventClasses), len(want.TracedEvents.Event))
	}

	for _, ev := range want.TracedEvents.Event {
		schema, ok := got.EventClasses[ev.ID]
		if !ok {
			t.Errorf("missing EventClassSchema for EventClass %d (%s)", ev.ID, EventClassName(ev.ID))
			continue
		}
		wantCols := make([]int, 0, len(ev.EventColumn))
		for _, c := range ev.EventColumn {
			wantCols = append(wantCols, c.ID)
		}
		gotCols := append([]int{}, schema.Columns...)
		sort.Ints(gotCols)
		sortedWant := append([]int{}, wantCols...)
		sort.Ints(sortedWant)
		if !reflect.DeepEqual(gotCols, sortedWant) {
			t.Errorf("EventClass %d (%s): Columns = %v, want %v", ev.ID, EventClassName(ev.ID), schema.Columns, wantCols)
		}
	}
}

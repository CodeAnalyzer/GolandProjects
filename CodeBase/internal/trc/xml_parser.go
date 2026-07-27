package trc

import (
	"bytes"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"strconv"
	"time"

	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Format — распознанный формат файла трейса (см. DetectFormat).
type Format int

const (
	// FormatBinary — бинарный .trc (SQL Server Profiler), разбирается
	// ParseHeader/ParseEventsStreaming.
	FormatBinary Format = iota
	// FormatXML — XML-экспорт трейса (<TraceData>...), разбирается ParseXMLReader.
	FormatXML
	// FormatXEL — Extended Events (.xel), разбирается ParseXELReader.
	FormatXEL
)

func (f Format) String() string {
	switch f {
	case FormatXML:
		return "xml"
	case FormatXEL:
		return "xel"
	default:
		return "binary"
	}
}

// DetectFormat определяет формат содержимого файла трейса по сигнатуре, а
// не по расширению: XML-экспорт (<TraceData>...</TraceData>, обычно в
// UTF-16 с BOM), бинарный Extended Events (.xel, см. looksLikeXEL) или
// бинарный .trc (SQL Server Profiler) как формат по умолчанию.
func DetectFormat(data []byte) Format {
	if hasBOM(data) {
		prefixLen := 4096
		if prefixLen > len(data) {
			prefixLen = len(data)
		}
		decoded, _ := decodeToUTF8(data[:prefixLen])
		if looksLikeTraceXML(decoded) {
			return FormatXML
		}
	} else if looksLikeTraceXML(data) {
		// Нет BOM — защитная проверка на случай экспорта без BOM (чистый UTF-8/ASCII XML).
		return FormatXML
	}
	if looksLikeXEL(data) {
		return FormatXEL
	}
	return FormatBinary
}

// xelFileSignature — первые 4 байта заголовка .xel, подтверждённые
// побайтово в Phase 0 на STP3_1.xel ("5A 37 AB EF"). Оставшиеся 4 байта
// заголовка ("0A 00 00 02") предположительно являются версией формата и
// намеренно не проверяются здесь как менее надёжные для разных версий
// SQL Server/файлов.
var xelFileSignature = []byte{0x5A, 0x37, 0xAB, 0xEF}

// looksLikeXEL проверяет 4-байтовую сигнатуру заголовка .xel (см.
// xelFileSignature). Известное ограничение: сигнатура подтверждена только
// на одном тестовом файле (STP3_1.xel) — для файлов другой версии SQL
// Server сигнатура может отличаться (открытая задача, см. Phase 0).
func looksLikeXEL(data []byte) bool {
	return bytes.HasPrefix(data, xelFileSignature)
}

func hasBOM(data []byte) bool {
	return bytes.HasPrefix(data, []byte{0xFF, 0xFE}) ||
		bytes.HasPrefix(data, []byte{0xFE, 0xFF}) ||
		bytes.HasPrefix(data, []byte{0xEF, 0xBB, 0xBF})
}

func looksLikeTraceXML(data []byte) bool {
	trimmed := bytes.TrimLeft(data, "\x00 \t\r\n\ufeff")
	if len(trimmed) > 128 {
		trimmed = trimmed[:128]
	}
	return bytes.HasPrefix(trimmed, []byte("<?xml")) || bytes.HasPrefix(trimmed, []byte("<TraceData"))
}

// decodeToUTF8 декодирует данные в UTF-8, автоматически определяя исходную
// кодировку по BOM (UTF-16LE/UTF-16BE/UTF-8); при отсутствии BOM данные
// пропускаются как есть (предполагается UTF-8/ASCII).
func decodeToUTF8(data []byte) ([]byte, error) {
	dec := unicode.BOMOverride(unicode.UTF8.NewDecoder())
	out, err := io.ReadAll(transform.NewReader(bytes.NewReader(data), dec))
	if err != nil && len(out) == 0 {
		return nil, err
	}
	return out, nil
}

// xmlTraceData — корневой элемент XML-экспорта трейса (см. пример в
// Modifications/DIAPR-391.xml). Пространство имён (xmlns) игнорируется —
// encoding/xml сопоставляет по локальному имени, если namespace не указан
// в теге явно.
//
// Имена типов ниже намеренно отличаются от одноимённых вспомогательных
// структур в *_test.go (xmlHeader/xmlEvent/xmlColumn, используемых
// golden-тестами для независимой сверки бинарного и XML-парсера) — тестовые
// файлы находятся в том же пакете trc, и совпадение имён вызвало бы
// конфликт объявлений.
type xmlTraceData struct {
	Header traceXMLHeader `xml:"Header"`
	Events xmlEventsList  `xml:"Events"`
}

type traceXMLHeader struct {
	TraceProvider struct {
		Name         string `xml:"name,attr"`
		MajorVersion int    `xml:"MajorVersion,attr"`
		MinorVersion int    `xml:"MinorVersion,attr"`
		BuildNumber  int    `xml:"BuildNumber,attr"`
	} `xml:"TraceProvider"`
	ServerInformation struct {
		Name string `xml:"name,attr"`
	} `xml:"ServerInformation"`
	ProfilerUI struct {
		OrderedColumns struct {
			ID []int `xml:"ID"`
		} `xml:"OrderedColumns"`
		TracedEvents struct {
			Event []xmlTracedEvent `xml:"Event"`
		} `xml:"TracedEvents"`
	} `xml:"ProfilerUI"`
}

// xmlTracedEvent — запись схемы <TracedEvents><Event id="X"><EventColumn
// id="Y"/>...</Event> — аналог блока FCFF бинарного заголовка.
type xmlTracedEvent struct {
	ID          int `xml:"id,attr"`
	EventColumn []struct {
		ID int `xml:"id,attr"`
	} `xml:"EventColumn"`
}

type xmlEventsList struct {
	Event []traceXMLEvent `xml:"Event"`
}

// traceXMLEvent — одно событие потока <Events><Event id="EventClassID"
// name="EventName"><Column id="ColumnID" name="ColumnName">value</Column>...
type traceXMLEvent struct {
	ID     int              `xml:"id,attr"`
	Name   string           `xml:"name,attr"`
	Column []traceXMLColumn `xml:"Column"`
}

type traceXMLColumn struct {
	ID    int    `xml:"id,attr"`
	Value string `xml:",chardata"`
}

// ParseXMLReader разбираёт XML-экспорт трейса из io.Reader, не загружая
// весь файл в память. Использует BOM-aware transform.NewReader поверх
// io.Reader для перекодирования и xml.NewDecoder для streaming-разбора.
func ParseXMLReader(r io.Reader) (*TRCParseResult, error) {
	// BOM-aware декодер: автоматически определяет UTF-16LE/BE/UTF-8 по BOM,
	// при отсутствии BOM пропускает как UTF-8.
	dec := unicode.BOMOverride(unicode.UTF8.NewDecoder())
	utf8Reader := transform.NewReader(r, dec)

	xmlDec := xml.NewDecoder(utf8Reader)
	// Пролог исходного XML (<?xml version="1.0" encoding="utf-16"?>)
	// остаётся в тексте как есть после перекодирования в UTF-8 — decoder
	// откажется работать, увидев некорректно заявленную кодировку без
	// зарегистрированного CharsetReader, хотя данные уже валидный UTF-8.
	// CharsetReader-заглушка возвращает вход без изменений для любой метки.
	xmlDec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	var doc xmlTraceData
	if err := xmlDec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("unmarshal xml: %w", err)
	}

	h := &TraceHeader{
		ProviderName:   doc.Header.TraceProvider.Name,
		MajorVersion:   doc.Header.TraceProvider.MajorVersion,
		MinorVersion:   doc.Header.TraceProvider.MinorVersion,
		BuildNumber:    doc.Header.TraceProvider.BuildNumber,
		ServerName:     doc.Header.ServerInformation.Name,
		OrderedColumns: doc.Header.ProfilerUI.OrderedColumns.ID,
		EventClasses:   map[int]*EventClassSchema{},
	}
	for _, te := range doc.Header.ProfilerUI.TracedEvents.Event {
		cols := make([]int, 0, len(te.EventColumn))
		for _, c := range te.EventColumn {
			cols = append(cols, c.ID)
		}
		h.EventClasses[te.ID] = &EventClassSchema{
			EventClass: te.ID,
			EventName:  EventClassName(te.ID),
			Columns:    cols,
		}
	}

	events := make([]TRCEvent, 0, len(doc.Events.Event))
	for _, xe := range doc.Events.Event {
		ev := TRCEvent{
			EventClass: xe.ID,
			EventName:  xe.Name,
			Columns:    make(map[int]any, len(xe.Column)),
		}
		for _, col := range xe.Column {
			ev.Columns[col.ID] = decodeXMLColumnValue(col.ID, col.Value)
		}
		events = append(events, ev)
	}

	enrichEventsParallel(events)
	ComputeParentIDs(events)
	return &TRCParseResult{Header: h, Events: events, SourceFormat: "trc_xml"}, nil
}

// parseXMLReaderCB — streaming-версия ParseXMLReader: обходит XML токены
// через xml.Decoder.Token и для каждого <Event> вызывает cb. Заголовок
// (<Header>) декодируется одним DecodeElement, события (<Event> внутри
// <Events>) — по одному через DecodeElement, без накопления всего массива
// в памяти. enrichEvent вызывается одиночно (как в parseEventsStreamingCB
// для бинарного формата).
//
// Возвращает разобранный заголовок TraceHeader.
func parseXMLReaderCB(r io.Reader, cb func(*TRCEvent) error) (*TraceHeader, error) {
	dec := unicode.BOMOverride(unicode.UTF8.NewDecoder())
	utf8Reader := transform.NewReader(r, dec)

	xmlDec := xml.NewDecoder(utf8Reader)
	xmlDec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil
	}

	var h *TraceHeader

	for {
		tok, err := xmlDec.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("xml token: %w", err)
		}

		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		switch se.Name.Local {
		case "Header":
			var hdr traceXMLHeader
			if err := xmlDec.DecodeElement(&hdr, &se); err != nil {
				return nil, fmt.Errorf("decode Header: %w", err)
			}
			h = &TraceHeader{
				ProviderName:   hdr.TraceProvider.Name,
				MajorVersion:   hdr.TraceProvider.MajorVersion,
				MinorVersion:   hdr.TraceProvider.MinorVersion,
				BuildNumber:    hdr.TraceProvider.BuildNumber,
				ServerName:     hdr.ServerInformation.Name,
				OrderedColumns: hdr.ProfilerUI.OrderedColumns.ID,
				EventClasses:   map[int]*EventClassSchema{},
			}
			for _, te := range hdr.ProfilerUI.TracedEvents.Event {
				cols := make([]int, 0, len(te.EventColumn))
				for _, c := range te.EventColumn {
					cols = append(cols, c.ID)
				}
				h.EventClasses[te.ID] = &EventClassSchema{
					EventClass: te.ID,
					EventName:  EventClassName(te.ID),
					Columns:    cols,
				}
			}

		case "Event":
			// <Event> внутри <Events> — декодируем одно событие
			var xe traceXMLEvent
			if err := xmlDec.DecodeElement(&xe, &se); err != nil {
				return nil, fmt.Errorf("decode Event: %w", err)
			}
			ev := TRCEvent{
				EventClass: xe.ID,
				EventName:  xe.Name,
				Columns:    make(map[int]any, len(xe.Column)),
			}
			for _, col := range xe.Column {
				ev.Columns[col.ID] = decodeXMLColumnValue(col.ID, col.Value)
			}
			enrichEvent(&ev)
			if err := cb(&ev); err != nil {
				return h, err
			}
		}
	}

	if h == nil {
		h = &TraceHeader{EventClasses: map[int]*EventClassSchema{}}
	}
	return h, nil
}

// ParseXML разбирает XML-экспорт трейса из среза байтов (in-memory режим).
// Сохранён для обратной совместимости с тестами. Продуктивный путь использует
// ParseXMLReader через io.Reader.
func ParseXML(data []byte) (*TRCParseResult, error) {
	utf8Data, err := decodeToUTF8(data)
	if err != nil {
		return nil, fmt.Errorf("decode xml encoding: %w", err)
	}
	return parseXMLFromBytes(utf8Data)
}

func parseXMLFromBytes(utf8Data []byte) (*TRCParseResult, error) {
	dec := xml.NewDecoder(bytes.NewReader(utf8Data))
	dec.CharsetReader = func(_ string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	var doc xmlTraceData
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("unmarshal xml: %w", err)
	}

	h := &TraceHeader{
		ProviderName:   doc.Header.TraceProvider.Name,
		MajorVersion:   doc.Header.TraceProvider.MajorVersion,
		MinorVersion:   doc.Header.TraceProvider.MinorVersion,
		BuildNumber:    doc.Header.TraceProvider.BuildNumber,
		ServerName:     doc.Header.ServerInformation.Name,
		OrderedColumns: doc.Header.ProfilerUI.OrderedColumns.ID,
		EventClasses:   map[int]*EventClassSchema{},
	}
	for _, te := range doc.Header.ProfilerUI.TracedEvents.Event {
		cols := make([]int, 0, len(te.EventColumn))
		for _, c := range te.EventColumn {
			cols = append(cols, c.ID)
		}
		h.EventClasses[te.ID] = &EventClassSchema{
			EventClass: te.ID,
			EventName:  EventClassName(te.ID),
			Columns:    cols,
		}
	}

	events := make([]TRCEvent, 0, len(doc.Events.Event))
	for _, xe := range doc.Events.Event {
		ev := TRCEvent{
			EventClass: xe.ID,
			EventName:  xe.Name,
			Columns:    make(map[int]any, len(xe.Column)),
		}
		for _, col := range xe.Column {
			ev.Columns[col.ID] = decodeXMLColumnValue(col.ID, col.Value)
		}
		events = append(events, ev)
	}

	enrichEventsParallel(events)
	ComputeParentIDs(events)
	return &TRCParseResult{Header: h, Events: events, SourceFormat: "trc_xml"}, nil
}

// decodeXMLColumnValue декодирует текстовое значение колонки согласно её
// ColumnType (та же таблица типов, что используется бинарным декодером в
// decodeColumnValue). При ошибке разбора числового/бинарного значения
// возвращает исходную строку как есть, не теряя данные.
func decodeXMLColumnValue(id int, value string) any {
	switch ColumnType(id) {
	case TypeInt32:
		if n, err := strconv.ParseInt(value, 10, 32); err == nil {
			return int32(n)
		}
		return value
	case TypeInt64:
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			return n
		}
		return value
	case TypeDateTime:
		if t, err := time.Parse(time.RFC3339, value); err == nil {
			return SystemTimeFromLocalParts(t)
		}
		return value
	case TypeGUID, TypeBinary:
		if b, err := hex.DecodeString(value); err == nil {
			return b
		}
		return value
	default:
		// TypeString и TypeUnknown — как есть.
		return value
	}
}

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

// DetectFormat определяет, является ли содержимое XML-экспортом трейса
// (<TraceData>...</TraceData>), а не бинарным .trc. Экспорт из SQL Server
// Profiler обычно в UTF-16 с BOM (<?xml version="1.0" encoding="utf-16"?>),
// но детект не полагается на расширение файла: декодирует префикс через
// BOM-aware декодер и ищет "<?xml" / "<TraceData" в начале.
func DetectFormat(data []byte) bool {
	if hasBOM(data) {
		prefixLen := 4096
		if prefixLen > len(data) {
			prefixLen = len(data)
		}
		decoded, _ := decodeToUTF8(data[:prefixLen])
		return looksLikeTraceXML(decoded)
	}
	// Нет BOM — защитная проверка на случай экспорта без BOM (чистый UTF-8/ASCII XML).
	return looksLikeTraceXML(data)
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

// ParseXML разбирает XML-экспорт трейса (TraceData) в общий TRCParseResult
// — ту же модель (TraceHeader/TRCEvent), которую строит бинарный парсер
// (ParseHeader+ParseEvents), чтобы всё нижестоящее (store.go, tree.go,
// aggregate.go, enrich.go, CLI/MCP) не зависело от исходного формата файла.
func ParseXML(data []byte) (*TRCParseResult, error) {
	utf8Data, err := decodeToUTF8(data)
	if err != nil {
		return nil, fmt.Errorf("decode xml encoding: %w", err)
	}

	// Пролог исходного XML (<?xml version="1.0" encoding="utf-16"?>)
	// остаётся в тексте как есть после перекодирования в UTF-8 — decoder
	// откажется работать, увидев некорректно заявленную кодировку без
	// зарегистрированного CharsetReader, хотя данные уже валидный UTF-8.
	// CharsetReader-заглушка возвращает вход без изменений для любой метки.
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
		enrichEvent(&ev)
		events = append(events, ev)
	}

	return &TRCParseResult{Header: h, Events: events}, nil
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

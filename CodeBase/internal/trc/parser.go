package trc

import (
	"encoding/binary"
	"fmt"
	"os"
)

// eventHeaderMarker — маркер начала записи события в потоке данных .trc:
// 2 байта F6 FF + фиксированный байт 0x06 (назначение байта не подтверждено,
// во всех встреченных записях он равен 0x06), затем EventClassID (2 байта
// LE) и Length (4 байта LE) — суммарный размер идущей далее последовательности
// полей в байтах. Найдено и проверено побайтово в
// Modifications/trc-trace-parser-ef18d1.md (Phase 0, TestTLVScan_FirstRealEvent).
var eventHeaderMarker = [2]byte{0xF6, 0xFF}

// extendedLengthSentinel — если однобайтовая длина поля равна этому
// значению (255), настоящая длина следует сразу за ним как uint32 LE (4
// байта) — расширенное кодирование для значений длиннее 254 байт
// (например, длинный TextData). Подтверждено на TextData длиной 1050 байт.
const extendedLengthSentinel = 0xFF

// ParseFile читает файл трейса и разбирает его в TRCParseResult. Формат
// файла определяется по сигнатуре содержимого (DetectFormat), а не по
// расширению: XML-экспорт трейса (<TraceData>...) разбирается ParseXML,
// иначе файл считается бинарным .trc и разбирается ParseHeader+ParseEvents.
func ParseFile(path string) (*TRCParseResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("trc: read %s: %w", path, err)
	}
	if DetectFormat(data) {
		result, err := ParseXML(data)
		if err != nil {
			return nil, fmt.Errorf("trc: parse xml %s: %w", path, err)
		}
		return result, nil
	}
	h, err := ParseHeader(data)
	if err != nil {
		return nil, fmt.Errorf("trc: parse header %s: %w", path, err)
	}
	events, err := ParseEvents(data, h)
	if err != nil {
		return nil, fmt.Errorf("trc: parse events %s: %w", path, err)
	}
	return &TRCParseResult{Header: h, Events: events}, nil
}

// ParseEvents разбирает поток событий, начиная с TraceHeader.EventsOffset.
// Между концом таблицы схемы и первой записью события может находиться
// нерасшифрованный до конца преамбул ("connection info", см. README Phase 0)
// — он пропускается поиском первого валидного eventHeaderMarker.
func ParseEvents(data []byte, h *TraceHeader) ([]TRCEvent, error) {
	pos := findEventHeader(data, h.EventsOffset)
	if pos < 0 {
		// Не нашли ни одной записи события — это не ошибка сама по себе
		// (файл может быть пуст после заголовка), возвращаем пустой список.
		return nil, nil
	}

	var events []TRCEvent
	for pos+9 <= len(data) {
		if data[pos] != eventHeaderMarker[0] || data[pos+1] != eventHeaderMarker[1] || data[pos+2] != 0x06 {
			// Неожиданный разрыв потока — пытаемся ресинхронизироваться,
			// найдя следующий валидный маркер, вместо того чтобы считать
			// файл полностью повреждённым.
			next := findEventHeader(data, pos+1)
			if next < 0 {
				break
			}
			pos = next
			continue
		}
		eventClass := int(binary.LittleEndian.Uint16(data[pos+3 : pos+5]))
		length := int(binary.LittleEndian.Uint32(data[pos+5 : pos+9]))
		fieldsStart := pos + 9
		fieldsEnd := fieldsStart + length
		if length < 0 || fieldsEnd > len(data) {
			return events, fmt.Errorf("trc: event at offset %d: invalid length %d", pos, length)
		}

		ev := TRCEvent{
			EventClass: eventClass,
			EventName:  EventClassName(eventClass),
			Columns:    map[int]any{},
		}
		if err := decodeEventFields(data[fieldsStart:fieldsEnd], ev.Columns); err != nil {
			return events, fmt.Errorf("trc: event at offset %d (class %d): %w", pos, eventClass, err)
		}
		enrichEvent(&ev)
		events = append(events, ev)
		pos = fieldsEnd
	}
	return events, nil
}

// enrichEvent заполняет производные поля TRCEvent (Procedure, Params,
// DurationMs) на основе уже декодированных Columns. TextData(1) и
// Duration(13) — источники; отсутствие любой из колонок не является
// ошибкой (типично для событий, где эти колонки не входят в схему класса).
func enrichEvent(ev *TRCEvent) {
	if textData, ok := ev.Columns[1].(string); ok {
		ev.Procedure, ev.Params = ExtractProcedureAndParams(textData)
	}
	if durationUs, ok := ev.Columns[13].(int64); ok {
		ev.DurationMs = durationUs / 1000
	}
}

// findEventHeader ищет следующее вхождение eventHeaderMarker + 0x06 начиная
// с offset from.
func findEventHeader(data []byte, from int) int {
	for i := from; i+3 <= len(data); i++ {
		if data[i] == eventHeaderMarker[0] && data[i+1] == eventHeaderMarker[1] && data[i+2] == 0x06 {
			return i
		}
	}
	return -1
}

// decodeEventFields разбирает последовательность полей вида
// PropID(uint16 LE) + Length(1 байт, либо 0xFF + 4-байтовая расширенная
// длина) + Value(Length байт), заполняя columns[PropID] декодированным
// значением согласно ColumnType(PropID).
func decodeEventFields(buf []byte, columns map[int]any) error {
	pos := 0
	for pos+3 <= len(buf) {
		propID := int(binary.LittleEndian.Uint16(buf[pos : pos+2]))
		lenByte := buf[pos+2]
		headerSize := 3
		length := int(lenByte)
		if lenByte == extendedLengthSentinel {
			if pos+3+4 > len(buf) {
				return fmt.Errorf("truncated extended length at %d", pos)
			}
			length = int(binary.LittleEndian.Uint32(buf[pos+3 : pos+7]))
			headerSize = 7
		}
		valueStart := pos + headerSize
		valueEnd := valueStart + length
		if length < 0 || valueEnd > len(buf) {
			return fmt.Errorf("field propID=%d at %d: invalid length %d", propID, pos, length)
		}
		value := buf[valueStart:valueEnd]
		decoded, err := decodeColumnValue(propID, value)
		if err != nil {
			return fmt.Errorf("field propID=%d at %d: %w", propID, pos, err)
		}
		columns[propID] = decoded
		pos = valueEnd
	}
	return nil
}

// decodeColumnValue декодирует сырые байты значения колонки согласно её
// ColumnDataType. Строки — UTF-16LE без завершающего null (в отличие от
// строк заголовка/преамбула, которые null-terminated) . Числа — по
// фактической длине value (4 -> int32, 8 -> int64), независимо от
// заявленной в columnDefinitions ширины, чтобы не терять данные при
// неучтённых вариациях. SystemTime — ровно 16 байт (8×uint16).
func decodeColumnValue(propID int, value []byte) (any, error) {
	typ := ColumnType(propID)
	switch {
	case typ == TypeDateTime && len(value) == 16:
		return SystemTime{
			Year:         binary.LittleEndian.Uint16(value[0:2]),
			Month:        binary.LittleEndian.Uint16(value[2:4]),
			DayOfWeek:    binary.LittleEndian.Uint16(value[4:6]),
			Day:          binary.LittleEndian.Uint16(value[6:8]),
			Hour:         binary.LittleEndian.Uint16(value[8:10]),
			Minute:       binary.LittleEndian.Uint16(value[10:12]),
			Second:       binary.LittleEndian.Uint16(value[12:14]),
			Milliseconds: binary.LittleEndian.Uint16(value[14:16]),
		}, nil
	case typ == TypeString:
		return decodeUTF16(value), nil
	case len(value) == 4:
		return int32(binary.LittleEndian.Uint32(value)), nil
	case len(value) == 8:
		return int64(binary.LittleEndian.Uint64(value)), nil
	default:
		// GUID/Binary либо колонка с неожиданной длиной — возвращаем как
		// есть, без потери данных.
		raw := make([]byte, len(value))
		copy(raw, value)
		return raw, nil
	}
}

// decodeUTF16 декодирует UTF-16LE байты (без завершающего null) в string.
// Переиспользует utf16ToString из header.go для собственно декодирования;
// здесь код units не ограничен ASCII-печатным диапазоном, как в
// findNextUTF16String, поэтому декодируется отдельно.
func decodeUTF16(b []byte) string {
	units := make([]uint16, 0, len(b)/2)
	for k := 0; k+2 <= len(b); k += 2 {
		units = append(units, binary.LittleEndian.Uint16(b[k:k+2]))
	}
	return utf16ToString(units)
}

package trc

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
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
// расширению: XML-экспорт трейса (<TraceData>...) разбирается streaming-парсером
// ParseXMLReader, иначе файл считается бинарным .trc и разбирается через
// ParseHeader (из буфера) + ParseEventsStreaming (из bufio.Reader).
// Использует streaming для всего файла — не загружает его целиком в память.
func ParseFile(path string) (*TRCParseResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("trc: open %s: %w", path, err)
	}
	defer f.Close()

	// Читаем первый буфер для детекции формата и парсинга заголовка.
	// Заголовок .trc (provider name, server name, schema table) обычно
	// занимает < 4KB, 64KB — с большим запасом.
	const headerBufSize = 65536
	headerBuf := make([]byte, 0, headerBufSize)
	chunk := make([]byte, headerBufSize)
	n, err := io.ReadFull(f, chunk)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("trc: read %s: %w", path, err)
	}
	headerBuf = append(headerBuf, chunk[:n]...)

	if DetectFormat(headerBuf) {
		// XML: перематываем файл в начало и используем xml.NewDecoder
		// (encoding/xml — streaming-парсер, не загружает весь файл в RAM).
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("trc: seek %s: %w", path, err)
		}
		result, err := ParseXMLReader(f)
		if err != nil {
			return nil, fmt.Errorf("trc: parse xml %s: %w", path, err)
		}
		return result, nil
	}

	// Бинарный .trc: парсим заголовок из буфера.
	h, err := ParseHeader(headerBuf)
	if err != nil {
		// Заголовок может выходить за пределы 64KB (очень маловероятно,
		// но обрабатываем: дочитываем ещё данных).
		if len(headerBuf) < headerBufSize && n == headerBufSize {
			// Уже прочитали всё — заголовок действительно повреждён.
			return nil, fmt.Errorf("trc: parse header %s: %w", path, err)
		}
		// Дочитываем до 1MB и пробуем снова.
		for len(headerBuf) < 1<<20 {
			nn, rerr := io.ReadFull(f, chunk)
			headerBuf = append(headerBuf, chunk[:nn]...)
			if rerr != nil && rerr != io.EOF && rerr != io.ErrUnexpectedEOF {
				return nil, fmt.Errorf("trc: read %s: %w", path, rerr)
			}
			h, err = ParseHeader(headerBuf)
			if err == nil {
				break
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
		}
		if err != nil {
			return nil, fmt.Errorf("trc: parse header %s: %w", path, err)
		}
	}

	// Позиционируемся на EventsOffset и стримим события.
	if _, err := f.Seek(int64(h.EventsOffset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("trc: seek %s: %w", path, err)
	}
	r := bufio.NewReaderSize(f, 1<<20) // 1MB buffer
	events, err := ParseEventsStreaming(r, h)
	if err != nil {
		return nil, fmt.Errorf("trc: parse events %s: %w", path, err)
	}
	return &TRCParseResult{Header: h, Events: events}, nil
}

// ParseEvents разбирает поток событий из среза байтов (in-memory режим).
// Сохранён для обратной совместимости с тестами. Продуктивный путь использует
// ParseEventsStreaming через bufio.Reader.
func ParseEvents(data []byte, h *TraceHeader) ([]TRCEvent, error) {
	pos := findEventHeader(data, h.EventsOffset)
	if pos < 0 {
		return nil, nil
	}

	var events []TRCEvent
	for pos+9 <= len(data) {
		if data[pos] != eventHeaderMarker[0] || data[pos+1] != eventHeaderMarker[1] || data[pos+2] != 0x06 {
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
		events = append(events, ev)
		pos = fieldsEnd
	}
	enrichEventsParallel(events)
	ComputeParentIDs(events)
	return events, nil
}

// ParseEventsStreaming разбирает поток событий из bufio.Reader, не загружая
// весь файл в память. Читает события по одному: 9-байтовый заголовок
// (маркер(2) + 0x06(1) + class(2) + length(4)), затем Length байт полей.
// При несовпадении маркера — ресинхронизация через skipToEventMarker.
func ParseEventsStreaming(r *bufio.Reader, h *TraceHeader) ([]TRCEvent, error) {
	var events []TRCEvent

	// Пропуск преамбулы: ищем первый валидный eventHeaderMarker.
	// skipToEventMarker позиционирует reader сразу после 3-байтового маркера.
	if err := skipToEventMarker(r); err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	for {
		// После skipToEventMarker прочитаны 3 байта маркера (F6 FF 06).
		// Читаем оставшиеся 6 байт заголовка: EventClass(2) + Length(4).
		var hdr6 [6]byte
		if _, err := io.ReadFull(r, hdr6[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return events, fmt.Errorf("trc: read event header: %w", err)
		}

		eventClass := int(binary.LittleEndian.Uint16(hdr6[0:2]))
		length := int(binary.LittleEndian.Uint32(hdr6[2:6]))
		if length < 0 {
			return events, fmt.Errorf("trc: event (class %d): invalid length %d", eventClass, length)
		}

		// Читаем Length байт полей события.
		fields := make([]byte, length)
		if _, err := io.ReadFull(r, fields); err != nil {
			return events, fmt.Errorf("trc: event (class %d): truncated fields: %w", eventClass, err)
		}

		ev := TRCEvent{
			EventClass: eventClass,
			EventName:  EventClassName(eventClass),
			Columns:    map[int]any{},
		}
		if err := decodeEventFields(fields, ev.Columns); err != nil {
			return events, fmt.Errorf("trc: event (class %d): %w", eventClass, err)
		}
		events = append(events, ev)

		// Проверяем, что следующий байт — начало нового события (маркер).
		// Если нет — ресинхронизируемся.
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return events, fmt.Errorf("trc: read after event: %w", err)
		}
		if b == eventHeaderMarker[0] {
			b2, err := r.ReadByte()
			if err != nil {
				if err == io.EOF {
					break
				}
				return events, fmt.Errorf("trc: read marker byte 2: %w", err)
			}
			if b2 == eventHeaderMarker[1] {
				b3, err := r.ReadByte()
				if err != nil {
					if err == io.EOF {
						break
					}
					return events, fmt.Errorf("trc: read marker byte 3: %w", err)
				}
				if b3 == 0x06 {
					// Маркер найден — продолжаем чтение следующего события.
					continue
				}
				// F6 FF но не 06 — unread b3 и b2, сканируем заново.
				r.UnreadByte()
				r.UnreadByte()
			} else {
				// F6 но не FF — unread b2, сканируем заново.
				r.UnreadByte()
			}
		}
		// Не маркер — unread и ресинхронизация.
		r.UnreadByte()
		if err := skipToEventMarker(r); err != nil {
			if err == io.EOF {
				break
			}
			return events, err
		}
	}
	enrichEventsParallel(events)
	ComputeParentIDs(events)
	return events, nil
}

// skipToEventMarker читает по одному байту из r, пока не найдёт
// eventHeaderMarker + 0x06. Возвращает nil если найдено, io.EOF если поток
// закончился.
func skipToEventMarker(r *bufio.Reader) error {
	for {
		b, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b != eventHeaderMarker[0] {
			continue
		}
		b2, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b2 != eventHeaderMarker[1] {
			if b2 == eventHeaderMarker[0] {
				// Первый байт маркера — переиспользуем как начало нового кандидата.
				r.UnreadByte()
			}
			continue
		}
		b3, err := r.ReadByte()
		if err != nil {
			return err
		}
		if b3 == 0x06 {
			return nil
		}
		if b3 == eventHeaderMarker[0] {
			r.UnreadByte()
		}
	}
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

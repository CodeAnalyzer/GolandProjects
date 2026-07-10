package trc

import (
	"encoding/binary"
	"fmt"
)

// markerSchema — маркер блока схемы одного класса события в таблице
// TracedEvents: 2 байта FC FF, за которыми следует байт длины, EventClassID
// (2 байта LE) и список ColumnID (2 байта LE каждый).
var markerSchema = [2]byte{0xFC, 0xFF}

// markerOrderedColumns — маркер блока общего списка колонок OrderedColumns:
// 2 байта FC FB, байт длины, список ColumnID (2 байта LE каждый), без
// EventClassID.
var markerOrderedColumns = [2]byte{0xFC, 0xFB}

// ParseHeader разбирает заголовок .trc файла: строки ProviderName/ServerName
// и таблицы схемы OrderedColumns/TracedEvents. Кодирование значений самих
// событий (тело потока после заголовка) не разбирается — см. TraceHeader.EventsOffset
// и README по Phase 0.
func ParseHeader(data []byte) (*TraceHeader, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("trc: file too small (%d bytes)", len(data))
	}

	h := &TraceHeader{EventClasses: map[int]*EventClassSchema{}}

	// ProviderName — первая читаемая ASCII-строка в UTF-16LE, встречающаяся в файле.
	providerName, _, end, err := findNextUTF16String(data, 0)
	if err != nil {
		return nil, fmt.Errorf("trc: provider name not found: %w", err)
	}
	h.ProviderName = providerName

	// ServerName — следующая читаемая строка после ProviderName (после
	// нуль-паддинга и произвольных служебных байт).
	serverName, _, end2, err := findNextUTF16String(data, end)
	if err != nil {
		return nil, fmt.Errorf("trc: server name not found: %w", err)
	}
	h.ServerName = serverName

	// Таблица схемы: серия блоков FCFF (по одному на класс события),
	// завершающаяся одним блоком FCFB (OrderedColumns). Ищем первое
	// вхождение любого из маркеров после ServerName и разбираем блоки
	// подряд, пока маркер совпадает.
	pos := findMarker(data, end2, markerSchema, markerOrderedColumns)
	if pos < 0 {
		return nil, fmt.Errorf("trc: schema table not found after header strings")
	}

	for pos+3 <= len(data) {
		switch {
		case matchMarker(data, pos, markerSchema):
			block, size, err := parseSchemaBlock(data, pos)
			if err != nil {
				return nil, fmt.Errorf("trc: schema block at %d: %w", pos, err)
			}
			h.EventClasses[block.EventClass] = block
			pos += size
		case matchMarker(data, pos, markerOrderedColumns):
			cols, size, err := parseOrderedColumnsBlock(data, pos)
			if err != nil {
				return nil, fmt.Errorf("trc: ordered columns block at %d: %w", pos, err)
			}
			h.OrderedColumns = cols
			pos += size
		default:
			// Маркер больше не совпадает — таблица схемы закончилась.
			h.EventsOffset = pos
			return h, nil
		}
	}

	h.EventsOffset = pos
	return h, nil
}

// matchMarker проверяет, что данные в позиции pos начинаются с marker.
func matchMarker(data []byte, pos int, marker [2]byte) bool {
	return pos+2 <= len(data) && data[pos] == marker[0] && data[pos+1] == marker[1]
}

// findMarker ищет первое вхождение любого из marker'ов начиная с offset from.
func findMarker(data []byte, from int, markers ...[2]byte) int {
	for i := from; i+2 <= len(data); i++ {
		for _, m := range markers {
			if data[i] == m[0] && data[i+1] == m[1] {
				return i
			}
		}
	}
	return -1
}

// parseSchemaBlock разбирает один блок FCFF: маркер(2) + lenByte(1) +
// EventClassID(2, LE) + список ColumnID (2 байта LE каждый, lenByte-2 байт).
// Возвращает разобранный блок и суммарный размер блока в байтах.
func parseSchemaBlock(data []byte, pos int) (*EventClassSchema, int, error) {
	if pos+3 > len(data) {
		return nil, 0, fmt.Errorf("truncated block header")
	}
	lenByte := int(data[pos+2])
	if lenByte < 2 {
		return nil, 0, fmt.Errorf("invalid length byte %d", lenByte)
	}
	blockSize := 3 + lenByte
	if pos+blockSize > len(data) {
		return nil, 0, fmt.Errorf("block extends beyond file (size=%d)", blockSize)
	}

	eventClass := int(binary.LittleEndian.Uint16(data[pos+3 : pos+5]))
	colBytes := data[pos+5 : pos+3+lenByte]
	if len(colBytes)%2 != 0 {
		return nil, 0, fmt.Errorf("odd column bytes length %d", len(colBytes))
	}
	cols := make([]int, 0, len(colBytes)/2)
	for i := 0; i < len(colBytes); i += 2 {
		cols = append(cols, int(binary.LittleEndian.Uint16(colBytes[i:i+2])))
	}

	return &EventClassSchema{
		EventClass: eventClass,
		EventName:  EventClassName(eventClass),
		Columns:    cols,
	}, blockSize, nil
}

// parseOrderedColumnsBlock разбирает блок FCFB: маркер(2) + lenByte(1) +
// список ColumnID (2 байта LE каждый, lenByte байт всего).
func parseOrderedColumnsBlock(data []byte, pos int) ([]int, int, error) {
	if pos+3 > len(data) {
		return nil, 0, fmt.Errorf("truncated block header")
	}
	lenByte := int(data[pos+2])
	blockSize := 3 + lenByte
	if pos+blockSize > len(data) {
		return nil, 0, fmt.Errorf("block extends beyond file (size=%d)", blockSize)
	}
	colBytes := data[pos+3 : pos+3+lenByte]
	if len(colBytes)%2 != 0 {
		return nil, 0, fmt.Errorf("odd column bytes length %d", len(colBytes))
	}
	cols := make([]int, 0, len(colBytes)/2)
	for i := 0; i < len(colBytes); i += 2 {
		cols = append(cols, int(binary.LittleEndian.Uint16(colBytes[i:i+2])))
	}
	return cols, blockSize, nil
}

// findNextUTF16String ищет следующую "читаемую" null-terminated UTF-16LE
// строку (минимум 3 печатных ASCII-символа) начиная с байтового смещения from.
// Возвращает декодированную строку, смещение начала и смещение сразу после
// завершающего нуль-символа (0x0000).
func findNextUTF16String(data []byte, from int) (string, int, int, error) {
	i := from
	for i+4 <= len(data) {
		if isPrintableUTF16Char(data, i) {
			start := i
			runLen := 0
			j := i
			for j+2 <= len(data) && isPrintableUTF16Char(data, j) {
				runLen++
				j += 2
			}
			if runLen >= 3 {
				// Нашли достаточно длинную последовательность печатных символов —
				// читаем до терминирующего 0x0000 (или конца печатной последовательности).
				end := j
				if end+2 <= len(data) && data[end] == 0 && data[end+1] == 0 {
					end += 2
				}
				runes := make([]uint16, 0, runLen)
				for k := start; k < j; k += 2 {
					runes = append(runes, binary.LittleEndian.Uint16(data[k:k+2]))
				}
				return utf16ToString(runes), start, end, nil
			}
			i = j + 2
			continue
		}
		i += 2
	}
	return "", 0, 0, fmt.Errorf("no printable UTF-16 string found from offset %d", from)
}

// isPrintableUTF16Char проверяет, что 2 байта в позиции i образуют печатный
// ASCII-символ (0x20-0x7E) в UTF-16LE (старший байт = 0).
func isPrintableUTF16Char(data []byte, i int) bool {
	if i+2 > len(data) {
		return false
	}
	lo, hi := data[i], data[i+1]
	return hi == 0 && lo >= 0x20 && lo <= 0x7E
}

// utf16ToString декодирует срез UTF-16 code units (без суррогатных пар,
// т.к. здесь используется только для ASCII-печатных строк) в string.
func utf16ToString(units []uint16) string {
	runes := make([]rune, len(units))
	for i, u := range units {
		runes[i] = rune(u)
	}
	return string(runes)
}

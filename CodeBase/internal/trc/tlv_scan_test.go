package trc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// Гипотеза Phase 0 (см. Modifications/trc-trace-parser-ef18d1.md), выведенная
// из TestReverseEngineerEventBody: тело события кодируется НЕ позиционно
// (по порядку колонок схемы), а как последовательность самоописывающих
// TLV-полей:
//
//	PropID  uint16 LE  — совпадает с ColumnID из columnDefinitions
//	Type    byte       — код типа значения (0x00 int32, 0x04 int64,
//	                     0x06 UTF-16LE null-terminated string, 0x10 8×uint16
//	                     SYSTEMTIME-подобная структура; другие коды пока не
//	                     подтверждены)
//	Length  uint32 LE  — длина Value в байтах (для строк включает
//	                     завершающий null-символ)
//	Value   []byte     — Length байт
//
// Поток TLV-полей прерывается специальными 2-байтовыми маркерами
// (FB FB / FC FF / FC FB / FB FF), встреченными также в заголовке —
// вероятно, повторные блоки схемы/"connection info" перед событиями новых
// SPID. tlvScan программно проходит по потоку и распознаёт оба вида
// элементов, не полагаясь на визуальный hex-разбор.
type tlvField struct {
	Offset int
	PropID int
	Type   byte
	Length int
}

type markerBlock struct {
	Offset int
	Marker [2]byte
}

var knownMarkers = [][2]byte{{0xFC, 0xFF}, {0xFC, 0xFB}, {0xFB, 0xFB}, {0xFB, 0xFF}}

func isKnownMarker(data []byte, pos int) ([2]byte, bool) {
	if pos+2 > len(data) {
		return [2]byte{}, false
	}
	for _, m := range knownMarkers {
		if data[pos] == m[0] && data[pos+1] == m[1] {
			return m, true
		}
	}
	return [2]byte{}, false
}

// skipInfoBlock пытается разобрать блок вида marker(2) + u16 + u16 +
// strLen(uint32 LE) + UTF-16LE строка (strLen байт, включая завершающий
// null) — структура, эмпирически найденная для маркеров FB FB / FB FF
// перед строкой "SQL Server Profiler - <guid>" (см. TestReverseEngineerEventBody).
// Возвращает размер блока в байтах (включая маркер) либо ok=false, если
// структура не подтверждается (strLen неразумный).
func skipInfoBlock(data []byte, pos, maxEnd int) (blockSize int, ok bool) {
	if pos+10 > maxEnd {
		return 0, false
	}
	strLen := int(binary.LittleEndian.Uint32(data[pos+6 : pos+10]))
	if strLen <= 0 || strLen > 1<<16 || pos+10+strLen > maxEnd {
		return 0, false
	}
	return 10 + strLen, true
}

// skipSchemaBlock разбирает блок FC FF (per-EventClass схема) или FC FB
// (OrderedColumns) переиспользуя parseSchemaBlock/parseOrderedColumnsBlock
// из header.go — те же блоки, что встречаются в заголовке, здесь
// встречаются повторно перед частью событий.
func skipSchemaBlock(data []byte, pos, maxEnd int, marker [2]byte) (blockSize int, ok bool) {
	switch marker {
	case markerSchema:
		_, size, err := parseSchemaBlock(data[:maxEnd], pos)
		if err != nil {
			return 0, false
		}
		return size, true
	case markerOrderedColumns:
		_, size, err := parseOrderedColumnsBlock(data[:maxEnd], pos)
		if err != nil {
			return 0, false
		}
		return size, true
	}
	return 0, false
}

// tlvScan проходит данные начиная с start, декодируя TLV-поля. При
// встрече известного маркера пытается разобрать и ПРОПУСТИТЬ
// соответствующий блок (schema/info), продолжая сканирование TLV-полей
// сразу после него — маркерные блоки в самих markers всё равно
// фиксируются для статистики. Останавливается на структурно невалидной
// записи либо при исчерпании данных.
func tlvScan(data []byte, start, maxEnd int) (fields []tlvField, markers []markerBlock, stopPos int, stopReason string) {
	pos := start
	for pos+7 <= maxEnd {
		if m, ok := isKnownMarker(data, pos); ok {
			markers = append(markers, markerBlock{pos, m})
			var size int
			var skipped bool
			switch m {
			case markerSchema, markerOrderedColumns:
				size, skipped = skipSchemaBlock(data, pos, maxEnd, m)
			default:
				size, skipped = skipInfoBlock(data, pos, maxEnd)
			}
			if !skipped {
				return fields, markers, pos, "marker-unskippable"
			}
			pos += size
			continue
		}
		propID := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		typ := data[pos+2]
		length := int(binary.LittleEndian.Uint32(data[pos+3 : pos+7]))
		if propID < 0 || propID > 4096 || length < 0 || length > 1<<20 || pos+7+length > maxEnd {
			return fields, markers, pos, fmt.Sprintf("invalid field propID=%d type=%d length=%d", propID, typ, length)
		}
		fields = append(fields, tlvField{Offset: pos, PropID: propID, Type: typ, Length: length})
		pos += 7 + length
	}
	return fields, markers, pos, "eof-or-window"
}

// TestTLVScan_TypeConsistency сканирует TLV-поля по всему файлу DIAPR-391.trc
// начиная с TraceHeader.EventsOffset, перезапускаясь после каждого
// маркерного блока (пропуская сам маркер эвристически: ищем следующий
// валидный TLV после маркера в пределах разумного окна), и агрегирует по
// PropID встреченные комбинации (Type, Length) — чтобы подтвердить, что
// каждый PropID кодируется одним и тем же Type стабильно, и зафиксировать
// типы для ВСЕХ встретившихся колонок, а не только для одного примера.
func TestTLVScan_TypeConsistency(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	data, err := os.ReadFile(trcPath)
	if err != nil {
		t.Fatalf("read %s: %v", trcPath, err)
	}
	h, err := ParseHeader(data)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}

	type typeLen struct {
		Type   byte
		Length int
	}
	seen := map[int]map[typeLen]int{} // propID -> (type,length combo) -> count
	var allMarkers []markerBlock
	var stopReasons []string

	pos := h.EventsOffset
	end := len(data)
	totalFields := 0
	for iter := 0; iter < 2_000_000 && pos < end; iter++ {
		fields, markers, stopPos, reason := tlvScan(data, pos, end)
		for _, f := range fields {
			totalFields++
			tl := typeLen{f.Type, f.Length}
			if seen[f.PropID] == nil {
				seen[f.PropID] = map[typeLen]int{}
			}
			seen[f.PropID][tl]++
		}
		allMarkers = append(allMarkers, markers...)
		// tlvScan сам пропускает известные и структурно валидные маркерные
		// блоки; остановка означает либо конец данных, либо непонятную
		// структуру (marker-unskippable/invalid field) — фиксируем и
		// пробуем продолжить с следующего байта, чтобы собрать максимум
		// статистики по остальному файлу.
		stopReasons = append(stopReasons, fmt.Sprintf("pos=%d(abs) reason=%s", stopPos, reason))
		if reason == "eof-or-window" {
			break
		}
		// Ресинхронизация: пытаемся найти в пределах следующих 64 байт
		// позицию, где PropID известен (есть в columnDefinitions) и Type
		// похож на один из подтверждённых кодов (0/4/6/0x10) — тогда
		// продолжаем сканирование оттуда. Это позволяет пережить редкие
		// (единичные) необъяснённые вставки типа 9-байтового эпилога
		// после блока "connection info" и продолжить проверку основной
		// массы событий файла.
		resynced := false
		for try := stopPos + 1; try < stopPos+64 && try+7 <= end; try++ {
			pid := int(binary.LittleEndian.Uint16(data[try : try+2]))
			typ := data[try+2]
			ln := int(binary.LittleEndian.Uint32(data[try+3 : try+7]))
			if _, known := columnDefinitions[pid]; !known {
				continue
			}
			if typ != 0x00 && typ != 0x04 && typ != 0x06 && typ != 0x10 {
				continue
			}
			if ln < 0 || ln > 1<<16 || try+7+ln > end {
				continue
			}
			pos = try
			resynced = true
			break
		}
		if !resynced {
			if len(stopReasons) > 30 {
				break
			}
			pos = stopPos + 1
		}
	}

	t.Logf("totalFields=%d distinctPropIDs=%d markers=%d stopEvents=%d",
		totalFields, len(seen), len(allMarkers), len(stopReasons))

	propIDs := make([]int, 0, len(seen))
	for p := range seen {
		propIDs = append(propIDs, p)
	}
	sort.Ints(propIDs)
	for _, p := range propIDs {
		combos := seen[p]
		name := ColumnName(p)
		t.Logf("PropID=%3d (%s): combos=%v", p, name, combos)
	}

	t.Logf("first stop reasons: %v", stopReasons[:min(10, len(stopReasons))])

	markerCounts := map[[2]byte]int{}
	for _, m := range allMarkers {
		markerCounts[m.Marker]++
	}
	t.Logf("marker counts: %v", markerCounts)
}

// TestTLVScan_FirstRealEvent сканирует TLV-поля начиная прямо с заголовка
// поля TextData("while 1 = 1") первого события SP:Recompile (EventClass 37,
// см. Modifications/DIAPR-391.utf8full.txt:394-420) — то есть минуя
// нерасшифрованный до конца "connection info" преамбл в начале файла — и
// печатает КАЖДОЕ поле с именем колонки и декодированным значением.
// Цель: получить один полностью декодированный, вручную сверяемый с XML
// пример события в чистой (не преамбульной) части потока.
func TestTLVScan_FirstRealEvent(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	data, err := os.ReadFile(trcPath)
	if err != nil {
		t.Fatalf("read %s: %v", trcPath, err)
	}
	h, err := ParseHeader(data)
	if err != nil {
		t.Fatalf("ParseHeader: %v", err)
	}

	// body offset 557 (найдено TestReverseEngineerEventBody) — начало
	// значения TextData; заголовок TLV-поля (7 байт) начинается на 7 байт
	// раньше.
	start := h.EventsOffset + 557 - 7
	end := len(data)
	dumpEnd := start + 512
	if dumpEnd > end {
		dumpEnd = end
	}
	t.Logf("=== Hex-дамп компактного per-event формата, offset 0 = %d (abs) ===", start)
	for off := start; off < dumpEnd; off += 16 {
		lineEnd := off + 16
		if lineEnd > dumpEnd {
			lineEnd = dumpEnd
		}
		t.Logf("%6d: % X", off-start, data[off:lineEnd])
	}
}

// TestTLVScan_LongTextDataLength проверяет, как кодируется Length для
// TextData длиннее 255 байт (компактный формат per-event поля, судя по
// TestTLVScan_FirstRealEvent, использует однобайтовый Length — нужно
// подтвердить наличие расширенного кодирования для длинных строк).
func TestTLVScan_LongTextDataLength(t *testing.T) {
	dir := modificationsDir(t)
	trcPath := filepath.Join(dir, "DIAPR-391.trc")
	data, err := os.ReadFile(trcPath)
	if err != nil {
		t.Fatalf("read %s: %v", trcPath, err)
	}

	needle := []byte{}
	for _, r := range "select @Exists = 1" {
		needle = append(needle, byte(r), 0)
	}
	idx := bytes.Index(data, needle)
	if idx < 0 {
		t.Fatal("needle not found")
	}
	// PropID(2)+Length(?) должны быть непосредственно перед строкой.
	pre := data[idx-8 : idx]
	t.Logf("bytes before long TextData at abs=%d: % X", idx, pre)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

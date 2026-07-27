package trc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// parseXELHeader читает заголовок .xel: первые 8 байт сигнатуры/версии,
// следующие 16 байт файлового GUID, и калибровочные константы timestamp
// (см. decodeXELTimeCalibration). Возвращает ошибку, если файл короче
// минимально необходимого для чтения калибровочных констант.
func parseXELHeader(data []byte) (*XELHeader, error) {
	if len(data) < xelFreqOffset+8 {
		return nil, fmt.Errorf("xel: файл слишком короткий для заголовка (%d байт)", len(data))
	}
	h := &XELHeader{
		Magic:    append([]byte{}, data[0:8]...),
		FileGUID: append([]byte{}, data[8:24]...),
	}
	h.BaseFileTime100ns, h.Freq = decodeXELTimeCalibration(data)
	if h.Freq == 0 {
		return nil, fmt.Errorf("xel: некорректная калибровочная константа FREQ=0 (offset %d)", xelFreqOffset)
	}
	return h, nil
}

// xelRawTimestampToUTC конвертирует "сырой" timestamp DATA-секции события в
// абсолютное время UTC по калибровочным константам заголовка. Считаем
// смещение относительно эпохи Unix (не 1601), иначе итоговое количество
// 100ns-тиков (~423 года) переполняет диапазон time.Duration.
func xelRawTimestampToUTC(rawTS uint64, h *XELHeader) time.Time {
	ft100ns := h.BaseFileTime100ns + (rawTS*10_000_000)/h.Freq
	unixNanos := int64(ft100ns-filetimeToUnixOffset100ns) * 100
	return time.Unix(0, unixNanos).UTC()
}

// ---------------------------------------------------------------------------
// DATA-секции известных типов событий (см. подробный комментарий формата в
// xel_reverse_test.go). Функции ниже принимают p — абсолютное смещение
// НАЧАЛА timestamp-поля (8 байт) DATA-секции.
// ---------------------------------------------------------------------------

type xelWaitCompletedData struct {
	Timestamp      uint64
	TotalLen       uint32
	WaitType       uint32
	WaitResult     uint32
	Duration       uint64
	SignalDuration uint64
	TypeTag        uint32
	WaitResource   string
	ActionsStart   int
	DataEnd        int
}

// decodeXELWaitCompletedData декодирует DATA-секцию wait_completed, начиная
// с markerOffset — начала 12-байтового маркера xelWaitCompletedMarker.
func decodeXELWaitCompletedData(data []byte, markerOffset int) (xelWaitCompletedData, error) {
	p := markerOffset + len(xelWaitCompletedMarker)
	if p+8+4 > len(data) {
		return xelWaitCompletedData{}, fmt.Errorf("xel: truncated wait_completed timestamp/totalLen at offset %d", p)
	}
	var d xelWaitCompletedData
	d.Timestamp = binary.LittleEndian.Uint64(data[p : p+8])
	p += 8
	tlp := p
	d.TotalLen = binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	fixedEnd := p + 32
	if fixedEnd+4 > len(data) {
		return xelWaitCompletedData{}, fmt.Errorf("xel: truncated wait_completed fixed part at offset %d", p)
	}
	d.WaitType = binary.LittleEndian.Uint32(data[p : p+4])
	d.WaitResult = binary.LittleEndian.Uint32(data[p+8 : p+12])
	d.Duration = binary.LittleEndian.Uint64(data[p+12 : p+20])
	d.SignalDuration = binary.LittleEndian.Uint64(data[p+20 : p+28])
	d.TypeTag = binary.LittleEndian.Uint32(data[p+28 : p+32])
	p = fixedEnd
	resLen := binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	if resLen > xelMaxVariableLen || p+int(resLen) > len(data) {
		return xelWaitCompletedData{}, fmt.Errorf("xel: truncated/oversized wait_resource at offset %d (len=%d)", p, resLen)
	}
	d.WaitResource = decodeUTF16LE(data[p : p+int(resLen)])
	p += int(resLen)
	d.ActionsStart = p
	d.DataEnd = tlp + 4 + int(d.TotalLen)
	return d, nil
}

type xelSQLBatchCompletedData struct {
	Timestamp     uint64
	TotalLen      uint32
	CPUTime       uint64
	Duration      uint64
	PhysicalReads uint64
	LogicalReads  uint64
	Writes        uint64
	Spills        uint64
	RowCount      uint64
	Result        uint8
	TypeTag       uint32
	BatchText     string
	DataEnd       int
}

// decodeXELSQLBatchCompletedData декодирует DATA-секцию sql_batch_completed,
// начиная с p — абсолютным смещением начала timestamp-поля.
func decodeXELSQLBatchCompletedData(data []byte, p int) (xelSQLBatchCompletedData, error) {
	if p+8+4 > len(data) {
		return xelSQLBatchCompletedData{}, fmt.Errorf("xel: truncated sql_batch_completed timestamp/totalLen at offset %d", p)
	}
	var d xelSQLBatchCompletedData
	d.Timestamp = binary.LittleEndian.Uint64(data[p : p+8])
	p += 8
	tlp := p
	d.TotalLen = binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	fixedEnd := p + 56
	if fixedEnd+9 > len(data) {
		return xelSQLBatchCompletedData{}, fmt.Errorf("xel: truncated sql_batch_completed fixed part at offset %d", p)
	}
	d.CPUTime = binary.LittleEndian.Uint64(data[p : p+8])
	d.Duration = binary.LittleEndian.Uint64(data[p+8 : p+16])
	d.PhysicalReads = binary.LittleEndian.Uint64(data[p+16 : p+24])
	d.LogicalReads = binary.LittleEndian.Uint64(data[p+24 : p+32])
	d.Writes = binary.LittleEndian.Uint64(data[p+32 : p+40])
	d.Spills = binary.LittleEndian.Uint64(data[p+40 : p+48])
	d.RowCount = binary.LittleEndian.Uint64(data[p+48 : p+56])
	p = fixedEnd
	d.Result = data[p]
	p++
	d.TypeTag = binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	// Проверяем тег до дорогого UTF-16 декодирования: при вызове на
	// несинхронизированной позиции (см. tryDecodeActionThenDataEvent) мисматч
// тега позволяет отбросить кандидата, не тратя O(len) на декодирование
// потенциально огромной "длины" из гарбажа.
	if d.TypeTag != xelBatchTypeTag {
		return xelSQLBatchCompletedData{}, fmt.Errorf("xel: sql_batch_completed TypeTag=0x%X, ожидалось 0x%X", d.TypeTag, xelBatchTypeTag)
	}
	textLen := binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	if textLen > xelMaxVariableLen || p+int(textLen) > len(data) {
		return xelSQLBatchCompletedData{}, fmt.Errorf("xel: truncated/oversized batch_text at offset %d (len=%d)", p, textLen)
	}
	d.BatchText = decodeUTF16LE(data[p : p+int(textLen)])
	d.DataEnd = tlp + 4 + int(d.TotalLen)
	return d, nil
}

type xelSQLStatementCompletedData struct {
	Timestamp      uint64
	TotalLen       uint32
	Duration       uint64
	CPUTime        uint64
	PhysicalReads  uint64
	LogicalReads   uint64
	Writes         uint64
	Spills         uint64
	RowCount       uint64
	LastRowCount   uint64
	LineNumber     uint32
	Offset         uint32
	OffsetEnd      int32
	TypeTag        uint32
	StatementLen   uint32
	TotalLenRepeat uint32
	Statement      string
	DataEnd        int
}

// decodeXELSQLStatementCompletedData декодирует DATA-секцию
// sql_statement_completed, начиная с p — абсолютным смещением начала
// timestamp-поля.
func decodeXELSQLStatementCompletedData(data []byte, p int) (xelSQLStatementCompletedData, error) {
	if p+8+4 > len(data) {
		return xelSQLStatementCompletedData{}, fmt.Errorf("xel: truncated sql_statement_completed timestamp/totalLen at offset %d", p)
	}
	var d xelSQLStatementCompletedData
	d.Timestamp = binary.LittleEndian.Uint64(data[p : p+8])
	p += 8
	tlp := p
	d.TotalLen = binary.LittleEndian.Uint32(data[p : p+4])
	p += 4
	if p+76+16 > len(data) {
		return xelSQLStatementCompletedData{}, fmt.Errorf("xel: truncated sql_statement_completed body at offset %d", p)
	}
	d.Duration = binary.LittleEndian.Uint64(data[p : p+8])
	d.CPUTime = binary.LittleEndian.Uint64(data[p+8 : p+16])
	d.PhysicalReads = binary.LittleEndian.Uint64(data[p+16 : p+24])
	d.LogicalReads = binary.LittleEndian.Uint64(data[p+24 : p+32])
	d.Writes = binary.LittleEndian.Uint64(data[p+32 : p+40])
	d.Spills = binary.LittleEndian.Uint64(data[p+40 : p+48])
	d.RowCount = binary.LittleEndian.Uint64(data[p+48 : p+56])
	d.LastRowCount = binary.LittleEndian.Uint64(data[p+56 : p+64])
	d.LineNumber = binary.LittleEndian.Uint32(data[p+64 : p+68])
	d.Offset = binary.LittleEndian.Uint32(data[p+68 : p+72])
	d.OffsetEnd = int32(binary.LittleEndian.Uint32(data[p+72 : p+76]))
	p += 76
	d.TypeTag = binary.LittleEndian.Uint32(data[p : p+4])
	d.StatementLen = binary.LittleEndian.Uint32(data[p+4 : p+8])
	d.TotalLenRepeat = binary.LittleEndian.Uint32(data[p+8 : p+12])
	p += 16
	// Проверяем тег до UTF-16 декодирования (см. аналогичный комментарий в
	// decodeXELSQLBatchCompletedData).
	if d.TypeTag != xelStatementTypeTag {
		return xelSQLStatementCompletedData{}, fmt.Errorf("xel: sql_statement_completed TypeTag=0x%X, ожидалось 0x%X", d.TypeTag, xelStatementTypeTag)
	}
	if d.StatementLen > xelMaxVariableLen || p+int(d.StatementLen) > len(data) {
		return xelSQLStatementCompletedData{}, fmt.Errorf("xel: truncated/oversized statement at offset %d (len=%d)", p, d.StatementLen)
	}
	d.Statement = decodeUTF16LE(data[p : p+int(d.StatementLen)])
	d.DataEnd = tlp + 4 + int(d.TotalLen)
	return d, nil
}

type xelSPStatementCompletedData struct {
	Timestamp        uint64
	TotalLen         uint32
	SourceDatabaseID uint32
	ObjectID         uint32
	ObjectType       uint16
	Duration         uint64
	CPUTime          uint64
	PhysicalReads    uint64
	LogicalReads     uint64
	Writes           uint64
	Spills           uint64
	RowCount         uint64
	LastRowCount     uint64
	NestLevel        uint16
	LineNumber       uint32
	Offset           uint32
	OffsetEnd        int32
	ObjectNameLen    uint32
	StatementLen     uint32
	ObjectName       string
	Statement        string
	DataEnd          int
}

// decodeXELSPStatementCompletedData декодирует DATA-секцию
// sp_statement_completed, начиная с p — абсолютным смещением начала
// timestamp-поля. Раскладка полей подробно описана в xel_reverse_test.go.
func decodeXELSPStatementCompletedData(data []byte, p int) (xelSPStatementCompletedData, error) {
	if p+12 > len(data) {
		return xelSPStatementCompletedData{}, fmt.Errorf("xel: truncated sp_statement_completed timestamp/totalLen at offset %d", p)
	}
	var d xelSPStatementCompletedData
	d.Timestamp = binary.LittleEndian.Uint64(data[p : p+8])
	tlp := p + 8
	d.TotalLen = binary.LittleEndian.Uint32(data[tlp : tlp+4])
	rel := p + 12
	if rel+104 > len(data) {
		return xelSPStatementCompletedData{}, fmt.Errorf("xel: truncated sp_statement_completed fixed part at offset %d", rel)
	}
	d.SourceDatabaseID = binary.LittleEndian.Uint32(data[rel+0 : rel+4])
	d.ObjectID = binary.LittleEndian.Uint32(data[rel+4 : rel+8])
	d.ObjectType = binary.LittleEndian.Uint16(data[rel+8 : rel+10])
	d.Duration = binary.LittleEndian.Uint64(data[rel+10 : rel+18])
	d.CPUTime = binary.LittleEndian.Uint64(data[rel+18 : rel+26])
	d.PhysicalReads = binary.LittleEndian.Uint64(data[rel+26 : rel+34])
	d.LogicalReads = binary.LittleEndian.Uint64(data[rel+34 : rel+42])
	d.Writes = binary.LittleEndian.Uint64(data[rel+42 : rel+50])
	d.Spills = binary.LittleEndian.Uint64(data[rel+50 : rel+58])
	d.RowCount = binary.LittleEndian.Uint64(data[rel+58 : rel+66])
	d.LastRowCount = binary.LittleEndian.Uint64(data[rel+66 : rel+74])
	d.NestLevel = binary.LittleEndian.Uint16(data[rel+74 : rel+76])
	d.LineNumber = binary.LittleEndian.Uint32(data[rel+76 : rel+80])
	d.Offset = binary.LittleEndian.Uint32(data[rel+80 : rel+84])
	d.OffsetEnd = int32(binary.LittleEndian.Uint32(data[rel+84 : rel+88]))
	objectNameTag := binary.LittleEndian.Uint32(data[rel+88 : rel+92])
	if objectNameTag != xelSPStatementObjectNameTag {
		return xelSPStatementCompletedData{}, fmt.Errorf("xel: sp_statement_completed objectNameTag=0x%X, ожидалось 0x%X at offset %d", objectNameTag, xelSPStatementObjectNameTag, rel+88)
	}
	d.ObjectNameLen = binary.LittleEndian.Uint32(data[rel+92 : rel+96])
	d.StatementLen = binary.LittleEndian.Uint32(data[rel+100 : rel+104])
	p2 := rel + 104
	if d.ObjectNameLen > xelMaxVariableLen || d.StatementLen > xelMaxVariableLen || p2+int(d.ObjectNameLen)+int(d.StatementLen) > len(data) {
		return xelSPStatementCompletedData{}, fmt.Errorf("xel: truncated/oversized sp_statement_completed object_name/statement at offset %d", p2)
	}
	d.ObjectName = decodeUTF16LE(data[p2 : p2+int(d.ObjectNameLen)])
	p2 += int(d.ObjectNameLen)
	d.Statement = decodeUTF16LE(data[p2 : p2+int(d.StatementLen)])
	d.DataEnd = tlp + 4 + int(d.TotalLen)
	return d, nil
}

// ---------------------------------------------------------------------------
// Диспетчер и основной цикл ParseXEL.
// ---------------------------------------------------------------------------

// applyActionColumns заполняет ev.Columns значениями actions, которые
// удалось сопоставить с TRC ColumnID через xelKnownActionNames +
// xeActionNameToColumn (см. xel_event_map_generated.go). Actions без
// известного имени или без TRC-эквивалента колонки пропускаются (данные не
// теряются полностью — они остаются доступны через errors/логи вызывающего
// кода при необходимости, здесь опускаются как несущественные для MVP).
func applyActionColumns(ev *TRCEvent, fields []xelActionField) {
	for _, f := range fields {
		name, ok := xelKnownActionNames[xelActionKey{f.Package, f.ActionID}]
		if !ok {
			continue
		}
		colID, ok := xeActionNameToColumn[name]
		if !ok {
			continue
		}
		ev.Columns[colID] = decodeXELActionValue(colID, f.Value)
	}
}

// decodeXELActionValue декодирует сырое значение action-поля согласно типу
// целевой TRC-колонки (см. ColumnType/columnDefinitions в format.go).
func decodeXELActionValue(colID int, raw []byte) any {
	switch ColumnType(colID) {
	case TypeInt32:
		switch len(raw) {
		case 2:
			return int32(binary.LittleEndian.Uint16(raw))
		case 4:
			return int32(binary.LittleEndian.Uint32(raw))
		case 8:
			return int32(binary.LittleEndian.Uint64(raw))
		}
	case TypeInt64:
		switch len(raw) {
		case 2:
			return int64(binary.LittleEndian.Uint16(raw))
		case 4:
			return int64(binary.LittleEndian.Uint32(raw))
		case 8:
			return int64(binary.LittleEndian.Uint64(raw))
		}
	case TypeString:
		return decodeUTF16LE(raw)
	}
	// Неизвестный/несовпадающий размер — не теряем данные, возвращаем как есть.
	return append([]byte{}, raw...)
}

// tryDecodeActionThenDataEvent пытается декодировать DATA-секцию,
// начинающуюся через xelCPUIDGap байт после dataGapStart (конец списка
// actions), как один из известных типов "action-then-data" событий
// (sql_batch_completed, sql_statement_completed, sp_statement_completed),
// перебирая их в порядке проверки типового тега/константы. Возвращает
// собранное TRCEvent, XE-имя события и абсолютное смещение конца события
// (для продолжения основного цикла), либо ok=false, если ни один из
// известных типов не подошёл (тогда вызывающий код должен пересинхронизироваться).
func tryDecodeActionThenDataEvent(data []byte, dataGapStart int, fields []xelActionField, header *XELHeader) (xeName string, ev TRCEvent, dataEnd int, ok bool) {
	p := dataGapStart + xelCPUIDGap
	if p < 0 || p+12 > len(data) {
		return "", TRCEvent{}, 0, false
	}

	if bd, err := decodeXELSQLBatchCompletedData(data, p); err == nil && bd.TypeTag == xelBatchTypeTag {
		e := TRCEvent{
			EventClass: xeEventNameToClass["sql_batch_completed"],
			EventName:  EventClassName(xeEventNameToClass["sql_batch_completed"]),
			Columns:    map[int]any{},
		}
		e.Columns[1] = bd.BatchText   // TextData
		e.Columns[13] = int64(bd.Duration)
		e.Columns[18] = int32(bd.CPUTime)
		e.Columns[16] = int64(bd.LogicalReads)
		e.Columns[17] = int64(bd.Writes)
		e.Columns[48] = int64(bd.RowCount)
		e.Columns[15] = SystemTimeFromLocalParts(xelRawTimestampToUTC(bd.Timestamp, header))
		applyActionColumns(&e, fields)
		return "sql_batch_completed", e, bd.DataEnd, true
	}

	if sd, err := decodeXELSQLStatementCompletedData(data, p); err == nil && sd.TypeTag == xelStatementTypeTag {
		e := TRCEvent{
			EventClass: xeEventNameToClass["sql_statement_completed"],
			EventName:  EventClassName(xeEventNameToClass["sql_statement_completed"]),
			Columns:    map[int]any{},
		}
		e.Columns[1] = sd.Statement
		e.Columns[13] = int64(sd.Duration)
		e.Columns[18] = int32(sd.CPUTime)
		e.Columns[16] = int64(sd.LogicalReads)
		e.Columns[17] = int64(sd.Writes)
		e.Columns[48] = int64(sd.RowCount)
		e.Columns[15] = SystemTimeFromLocalParts(xelRawTimestampToUTC(sd.Timestamp, header))
		applyActionColumns(&e, fields)
		return "sql_statement_completed", e, sd.DataEnd, true
	}

	if spd, err := decodeXELSPStatementCompletedData(data, p); err == nil {
		e := TRCEvent{
			EventClass: xeEventNameToClass["sp_statement_completed"],
			EventName:  EventClassName(xeEventNameToClass["sp_statement_completed"]),
			Columns:    map[int]any{},
		}
		e.Columns[1] = spd.Statement
		e.Columns[34] = spd.ObjectName // ObjectName
		e.Columns[22] = int32(spd.ObjectID)
		e.Columns[13] = int64(spd.Duration)
		e.Columns[18] = int32(spd.CPUTime)
		e.Columns[16] = int64(spd.LogicalReads)
		e.Columns[17] = int64(spd.Writes)
		e.Columns[48] = int64(spd.RowCount)
		e.Columns[15] = SystemTimeFromLocalParts(xelRawTimestampToUTC(spd.Timestamp, header))
		applyActionColumns(&e, fields)
		return "sp_statement_completed", e, spd.DataEnd, true
	}

	return "", TRCEvent{}, 0, false
}

// ParseXEL разбирает .xel-файл (Extended Events, бинарный формат) из среза
// байтов. Реализует последовательный (не needle-based) проход по потоку
// событий:
//   - на каждой позиции сначала проверяется 12-байтовый маркер
//     wait_completed (события с порядком "data-then-actions");
//   - иначе жадно разбираются TLV-записи actions (consumeActionTLVs) и, если
//     хотя бы одна найдена, делается попытка декодировать последующую
//     DATA-секцию как один из известных типов "action-then-data" событий
//     (tryDecodeActionThenDataEvent);
//   - если ни один вариант не подошёл, позиция считается нераспознанной и
//     курсор продвигается на 1 байт для пересинхронизации (счётчик таких
//     байт не отслеживается отдельно — см. известные ограничения формата в
//     комментарии xel_reverse_test.go).
//
// ИЗВЕСТНОЕ ОГРАНИЧЕНИЕ: разрешены только события wait_completed,
// sql_batch_completed, sql_statement_completed и sp_statement_completed
// (покрывают ~100% событий тестового файла STP3_1.xel, см. Phase 0). Прочие
// типы событий (module_start/end, rpc_completed и т.п.) в текущей версии
// парсера не декодируются и пропускаются молча — это открытая задача
// следующей итерации, а не ошибка данного Phase.
//
// Также ограничение: имена actions резолвятся через хардкод-таблицу
// xelKnownActionNames, подтверждённую только для STP3_1.xel (см. её
// комментарий) — для файлов с другим dictionary-блоком имена/колонки actions
// не будут распознаны (но событие всё равно будет создано с доступными
// DATA-полями).
// parseXELCB — callback-версия ParseXEL: обходит поток событий .xel и для
// каждого декодированного события вызывает cb. enrichEvent вызывается
// одиночно (как в parseEventsStreamingCB для бинарного формата). Не
// накапливает []TRCEvent — вызывающая сторона решает, что делать с событием
// (например, стримить в БД через ParseFileToDB).
//
// header должен быть предварительно разобран через parseXELHeader.
func parseXELCB(data []byte, header *XELHeader, cb func(*TRCEvent) error) error {
	startPos := bytes.Index(data, xelWaitCompletedMarker)
	if startPos < 0 {
		startPos = xelFreqOffset + 8
	}

	pos := startPos
	for pos+len(xelWaitCompletedMarker) <= len(data) {
		if bytes.Equal(data[pos:pos+len(xelWaitCompletedMarker)], xelWaitCompletedMarker) {
			wd, werr := decodeXELWaitCompletedData(data, pos)
			if werr == nil && wd.DataEnd > pos {
				fields, actionsEnd := consumeActionTLVs(data, wd.ActionsStart)
				className := xeEventNameToClass["wait_completed"]
				e := TRCEvent{
					EventClass: className,
					EventName:  "wait_completed",
					Columns:    map[int]any{},
				}
				e.Columns[13] = int64(wd.Duration)
				e.Columns[15] = SystemTimeFromLocalParts(xelRawTimestampToUTC(wd.Timestamp, header))
				applyActionColumns(&e, fields)
				enrichEvent(&e)
				if err := cb(&e); err != nil {
					return err
				}
				pos = actionsEnd
				continue
			}
		}

		fields, afterActions := consumeActionTLVs(data, pos)
		if len(fields) > 0 {
			if _, ev, dataEnd, ok := tryDecodeActionThenDataEvent(data, afterActions, fields, header); ok && dataEnd > pos {
				enrichEvent(&ev)
				if err := cb(&ev); err != nil {
					return err
				}
				pos = dataEnd
				continue
			}
		}

		pos++
	}
	return nil
}

func ParseXEL(data []byte) (*TRCParseResult, error) {
	header, err := parseXELHeader(data)
	if err != nil {
		return nil, err
	}

	var events []TRCEvent
	err = parseXELCB(data, header, func(ev *TRCEvent) error {
		events = append(events, *ev)
		return nil
	})
	if err != nil {
		return nil, err
	}

	enrichEventsParallel(events)
	ComputeParentIDs(events)
	return &TRCParseResult{
		Header: &TraceHeader{
			EventClasses: map[int]*EventClassSchema{},
		},
		Events:       events,
		SourceFormat: "xel",
	}, nil
}

// ParseXELReader разбирает .xel-файл из io.Reader. Формат требует
// произвольного доступа/повторного сканирования байтов (dictionary-блок,
// маркеры, TLV) — потоковый (single-pass streaming) разбор без полной
// загрузки в память не поддерживается текущей реализацией, поэтому весь
// вход читается в память перед разбором (аналогично поведению ParseXML для
// небольших/средних файлов; для очень больших .xel это известное
// ограничение, задокументированное как открытая задача).
func ParseXELReader(r io.Reader) (*TRCParseResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("xel: read all: %w", err)
	}
	return ParseXEL(data)
}

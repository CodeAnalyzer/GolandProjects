package trc

import "encoding/binary"

//go:generate go run gen_xel_mapping.go

// ---------------------------------------------------------------------------
// Phase 2 — структуры и константы формата .xel, подтверждённые побайтово в
// Phase 0 (см. подробный комментарий формата в xel_reverse_test.go). Здесь —
// только "производственные" (не тестовые) определения, используемые
// xel_parser.go.
// ---------------------------------------------------------------------------

// xelBaseFileTimeOffset/xelFreqOffset — фиксированные абсолютные смещения в
// заголовке .xel, где хранится пара калибровочных констант для конвертации
// "сырого" timestamp DATA-секции в абсолютное время UTC (см.
// decodeXELTimeCalibration). Подтверждено только на STP3_1.xel — для
// произвольного .xel-файла эти смещения являются эвристикой (см. Phase 0
// вывод в плане xel-parser-impl-2640ba.md).
const (
	xelBaseFileTimeOffset = 620
	xelFreqOffset         = 628
)

// filetimeToUnixOffset100ns — разница между эпохой FILETIME (1601-01-01) и
// эпохой Unix (1970-01-01) в единицах 100ns. Стандартная константа Windows.
const filetimeToUnixOffset100ns = 116444736000000000

// xelWaitCompletedMarker — стабильный 12-байтовый маркер перед DATA-секцией
// КАЖДОГО события wait_completed (см. Phase 0). Используется как надёжный
// generic-якорь: встречается идентично для всех wait_completed-событий
// файла независимо от их значений.
var xelWaitCompletedMarker = []byte{0x01, 0xC0, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00}

// xelCPUIDGap — фиксированное количество байт между концом TLV-поля cpu_id
// (package=1, actionID=28 — всегда последнее в списке actions для событий с
// порядком "action-then-data") и началом DATA-секции. Подтверждено на трёх
// типах событий: sql_batch_completed, sql_statement_completed,
// sp_statement_completed.
const xelCPUIDGap = 20

// xelMaxVariableLen — верхняя граница разумной длины переменного поля
// (wait_resource/batch_text/statement/object_name). Используется для
// быстрого отбрасывания заведомо мусорных значений длины, полученных при
// попытке декодирования на несинхронизированной позиции потока (см.
// tryDecodeActionThenDataEvent) — без этой проверки декодер мог бы
// попытаться UTF-16-декодировать сотни МБ "текста", распознанного из
// случайных байт, что на большом файле приводит к неприемлемо долгому
// разбору (см. регрессию производительности на полном STP3_1.xel).
const xelMaxVariableLen = 1 << 20 // 1 МБ

// xelSPStatementObjectNameTag — константный тег (4 байта), предшествующий
// длине object_name в DATA-секции sp_statement_completed.
const xelSPStatementObjectNameTag = 0x68

// xelBatchTypeTag/xelStatementTypeTag — константные теги, различающие
// DATA-секции sql_batch_completed (0x41) и sql_statement_completed (0x5C)
// после общей фиксированной части.
const (
	xelBatchTypeTag     = 0x41
	xelStatementTypeTag = 0x5C
)

// XELHeader — заголовок .xel файла: сырые байты сигнатуры/версии и файлового
// GUID (семантика байт не декодирована в Phase 0 — не требуется для
// парсинга событий), плюс калибровочные константы timestamp.
type XELHeader struct {
	Magic             []byte // первые 8 байт (сигнатура+версия, точная семантика не выяснена)
	FileGUID          []byte // 16-байтовый GUID файла
	BaseFileTime100ns uint64
	Freq              uint64
}

// decodeXELTimeCalibration читает калибровочные константы из фиксированных
// смещений в начале файла (см. xelBaseFileTimeOffset/xelFreqOffset).
func decodeXELTimeCalibration(data []byte) (baseFileTime100ns uint64, freq uint64) {
	baseFileTime100ns = binary.LittleEndian.Uint64(data[xelBaseFileTimeOffset : xelBaseFileTimeOffset+8])
	freq = binary.LittleEndian.Uint64(data[xelFreqOffset : xelFreqOffset+8])
	return baseFileTime100ns, freq
}

// xelActionField — одно декодированное TLV-поле action из потока событий.
// Формат TLV: [package uint8][actionID uint16 LE][0x10][length uint32 LE]
// [length байт]. Package — номер package, которому принадлежит action
// (1=sqlos, 2=sqlserver — подтверждённые значения).
type xelActionField struct {
	Package  byte
	ActionID uint16
	Value    []byte
	Offset   int // абсолютное смещение начала TLV-записи в файле
}

// xelActionKey — ключ для сопоставления (package,actionID) -> имя action.
type xelActionKey struct {
	Package byte
	ID      uint16
}

// xelKnownActionNames — эмпирически подтверждённая (Phase 0,
// TestXEL_DecodeFirstEventActions) локальная таблица (package,actionID) ->
// xe_action_name для STP3_1.xel.
//
// ВАЖНО (известное ограничение): actionID являются позиционными ссылками на
// нерасшифрованный на уровне TLV dictionary-блок конкретного файла и НЕ
// гарантированно совпадают между разными .xel файлами/версиями SQL Server.
// Полностью универсальное решение требует декодирования TLV-структуры
// dictionary-блока (открытая задача, оставленная Phase 0) — генератор
// сопоставления имён по этому файлу здесь используется как прагматичный
// MVP, покрывающий тестовый образец STP3_1.xel.
var xelKnownActionNames = map[xelActionKey]string{
	{2, 80}: "transaction_sequence",
	{2, 4}:  "transaction_id",
	{2, 8}:  "session_id",
	{2, 76}: "database_name",
	{2, 44}: "client_hostname",
	{2, 36}: "client_app_name",
}

// consumeActionTLVs жадно разбирает подряд идущие TLV-записи actions
// начиная с start, останавливаясь на первом байте, не соответствующем
// формату TLV (что естественным образом совпадает с концом списка actions —
// см. Phase 0 вывод "события пакуются впритык"). Возвращает разобранные
// поля и абсолютное смещение конца списка (первый непрочитанный байт).
func consumeActionTLVs(data []byte, start int) ([]xelActionField, int) {
	var fields []xelActionField
	p := start
	for p+8 <= len(data) {
		pkg := data[p]
		if (pkg != 0x01 && pkg != 0x02) || data[p+3] != 0x10 {
			break
		}
		actionID := binary.LittleEndian.Uint16(data[p+1 : p+3])
		length := binary.LittleEndian.Uint32(data[p+4 : p+8])
		valStart := p + 8
		valEnd := valStart + int(length)
		if length == 0 || length > 1<<20 || valEnd > len(data) {
			break
		}
		fields = append(fields, xelActionField{
			Package:  pkg,
			ActionID: actionID,
			Value:    data[valStart:valEnd],
			Offset:   p,
		})
		p = valEnd
	}
	return fields, p
}

// decodeUTF16LE декодирует байтовый срез как UTF-16LE строку (без BOM). Не
// обрабатывает суррогатные пары самостоятельно (см. utf16Decode) —
// достаточно для значений трейса.
func decodeUTF16LE(b []byte) string {
	if len(b)%2 != 0 {
		b = b[:len(b)-1]
	}
	runes := make([]uint16, len(b)/2)
	for i := range runes {
		runes[i] = binary.LittleEndian.Uint16(b[i*2 : i*2+2])
	}
	return string(utf16Decode(runes))
}

func utf16Decode(s []uint16) []rune {
	out := make([]rune, 0, len(s))
	for i := 0; i < len(s); i++ {
		r := s[i]
		if r >= 0xD800 && r <= 0xDBFF && i+1 < len(s) {
			r2 := s[i+1]
			if r2 >= 0xDC00 && r2 <= 0xDFFF {
				out = append(out, ((rune(r)-0xD800)<<10)|(rune(r2)-0xDC00)+0x10000)
				i++
				continue
			}
		}
		out = append(out, rune(r))
	}
	return out
}

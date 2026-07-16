package trc

import "time"

// TraceHeader — заголовок .trc файла: строковые метаданные провайдера/сервера
// и таблицы схемы (OrderedColumns, TracedEvents), извлечённые из блоков
// FCFB/FCFF, предшествующих потоку событий.
type TraceHeader struct {
	ProviderName string
	MajorVersion int
	MinorVersion int
	BuildNumber  int
	ServerName   string

	// OrderedColumns — общий список ColumnID в порядке, заданном пользователем
	// в Profiler UI (соответствует <OrderedColumns><ID>.../ID> в XML-экспорте).
	OrderedColumns []int

	// EventClasses — схема колонок для каждого трассируемого класса событий
	// (соответствует <TracedEvents><Event id="X"><EventColumn id="Y"/></Event>).
	// Ключ — EventClassID.
	EventClasses map[int]*EventClassSchema

	// EventsOffset — байтовое смещение в файле, с которого (предположительно)
	// начинается поток фактических событий (после заголовка и таблиц схемы).
	// Точное декодирование тела события — открытый вопрос Phase 0, см. README.
	EventsOffset int
}

// EventClassSchema — набор и порядок колонок, объявленных для конкретного
// EventClassID в таблице TracedEvents.
type EventClassSchema struct {
	EventClass int
	EventName  string
	Columns    []int
}

// TRCEvent — одно разобранное событие потока данных .trc: класс события и
// декодированные значения его колонок (ключ — ColumnID). Тип значения в
// карте Columns зависит от ColumnDataType колонки:
//   - TypeString -> string
//   - TypeInt32  -> int32
//   - TypeInt64  -> int64
//   - TypeDateTime -> SystemTime
//   - TypeGUID/TypeBinary -> []byte
//
// Procedure/Params извлекаются из TextData (колонка 1) простым regex-разбором
// (см. extract.go) — эвристика, покрывающая типовые вызовы `exec Proc @p=v`
// и `exec @Ret = Proc @p=v`, актуальные для Diasoft 5NT трейсов.
// DurationMs — Duration(13) в миллисекундах (исходная колонка в Columns[13]
// хранится в микросекундах, как задокументировано в Data Columns.txt).
type TRCEvent struct {
	EventClass int
	EventName  string
	Columns    map[int]any
	Procedure  string
	Params     []TRCParam
	DurationMs int64

	// ParentID — индекс родительского события в срезе Events (для tree building).
	// -1 (или 0) означает корень. Заполняется в ComputeParentIDs.
	ParentID int

	// Depth — глубина в дереве вызовов (0 = корень).
	Depth int

	// EventIndex — порядковый индекс события в срезе Events.
	// Используется для вычисления parent_id при insert в БД.
	EventIndex int
}

// TRCParam — параметр вызова процедуры, извлечённый из TextData.
type TRCParam struct {
	Name  string
	Value string
}

// SystemTime — декодированное значение колонки типа TypeDateTime: 8
// 16-битных полей в порядке год/месяц/день_недели/день/час/минута/секунда/
// миллисекунда (см. Modifications/trc-trace-parser-ef18d1.md, находка
// Phase 0 про кодирование StartTime/EndTime).
type SystemTime struct {
	Year         uint16
	Month        uint16
	DayOfWeek    uint16
	Day          uint16
	Hour         uint16
	Minute       uint16
	Second       uint16
	Milliseconds uint16
}

// TRCParseResult — результат разбора всего .trc файла.
type TRCParseResult struct {
	Header *TraceHeader
	Events []TRCEvent
}

// TRCSession — запись о сессии (разобранном .trc файле) в БД.
type TRCSession struct {
	ID           int64     `json:"id"`
	FilePath     string    `json:"file_path"`
	FileSize     int64     `json:"file_size"`
	ParsedAt     time.Time `json:"parsed_at"`
	TotalEvents  int       `json:"total_events"`
	ProviderName string    `json:"provider_name,omitempty"`
	ServerName   string    `json:"server_name,omitempty"`
	MajorVersion int       `json:"major_version,omitempty"`
	MinorVersion int       `json:"minor_version,omitempty"`
	BuildNumber  int       `json:"build_number,omitempty"`
}

// ToTime конвертирует SystemTime в time.UTC. Часовой пояс исходного значения
// не кодируется в бинаре (см. README Phase 0) — используется UTC как
// приближение; ok=false для нулевого/невалидного значения (Year == 0).
func (s SystemTime) ToTime() (time.Time, bool) {
	if s.Year == 0 {
		return time.Time{}, false
	}
	return time.Date(
		int(s.Year), time.Month(s.Month), int(s.Day),
		int(s.Hour), int(s.Minute), int(s.Second),
		int(s.Milliseconds)*1_000_000, time.UTC,
	), true
}

// SystemTimeFromLocalParts конвертирует time.Time (обычно результат парсинга
// ISO-8601 значения с офсетом из XML-экспорта трейса) в SystemTime, беря
// только календарные поля "как есть", без учёта часового пояса/офсета —
// в отличие от ToTime, который трактует SystemTime как UTC. Используется
// XML-парсером (xml_parser.go), чтобы дата/время совпадали с тем, что видит
// аналитик в исходном XML.
//
// DayOfWeek кодируется в конвенции SQL Server DATEPART(weekday) (1=Sunday..
// 7=Saturday, т.е. time.Weekday()+1), а не в 0-based конвенции Go —
// подтверждено сверкой с бинарным декодером (decodeColumnValue) на golden
// файле DIAPR-391 (см. TestParseFile_XMLMatchesBinary).
func SystemTimeFromLocalParts(t time.Time) SystemTime {
	return SystemTime{
		Year:         uint16(t.Year()),
		Month:        uint16(t.Month()),
		DayOfWeek:    uint16(t.Weekday()) + 1,
		Day:          uint16(t.Day()),
		Hour:         uint16(t.Hour()),
		Minute:       uint16(t.Minute()),
		Second:       uint16(t.Second()),
		Milliseconds: uint16(t.Nanosecond() / 1_000_000),
	}
}

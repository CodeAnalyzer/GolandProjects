package trc

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

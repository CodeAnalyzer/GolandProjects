package rti

import "time"

// RTICall — один вызов процедуры из RTI-лога
type RTICall struct {
	ID            int64           `json:"id"`
	Procedure     string          `json:"procedure"`
	EnterLine     int             `json:"enter_line"`
	ExitLine      int             `json:"exit_line,omitempty"`
	EnterTime     time.Time       `json:"enter_time"`
	ExitTime      *time.Time      `json:"exit_time,omitempty"`
	ElapsedMs     int             `json:"elapsed_ms,omitempty"`
	NestLevel     int             `json:"nest_level"`
	ModuleID      int             `json:"module_id"`
	TranCount     int             `json:"tran_count"`
	BeginCnt      int             `json:"begin_cnt,omitempty"`
	RetVal        *int            `json:"ret_val,omitempty"`
	RetValContext string          `json:"ret_val_context,omitempty"`
	Params        []RTIParam      `json:"params,omitempty"`
	Checkpoints   []RTICheckpoint `json:"checkpoints,omitempty"`
	BLogBlocks    []RTIBLogBlock  `json:"blog_blocks,omitempty"`
	BLogTables    []RTIBLogTable  `json:"blog_tables,omitempty"`
	Children      []int64         `json:"children,omitempty"`
	ParentID      *int64          `json:"parent_id,omitempty"`
	SPID          int             `json:"spid"`
	// Enriched fields (заполняются из CodeBase)
	SourceFile    string `json:"source_file,omitempty"`
	ModuleName    string `json:"module_name,omitempty"`
	RetValMeaning string `json:"ret_val_meaning,omitempty"`
	ErrorConstant string `json:"error_constant,omitempty"`
}

// RTIParam — параметр вызова процедуры
type RTIParam struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// RTICheckpoint — контрольная точка в логе
type RTICheckpoint struct {
	Label     string    `json:"label"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	ElapsedMs int       `json:"elapsed_ms"`
	LineNo    int       `json:"line_no"`
}

// RTIBLogBlock — блок бизнес-логирования (M_BUSINESSLOG_BLOCK_BEGIN/END)
type RTIBLogBlock struct {
	BlockName string    `json:"block_name"`
	EnterTime time.Time `json:"enter_time,omitempty"`
	ExitTime  time.Time `json:"exit_time,omitempty"`
	ElapsedMs int       `json:"elapsed_ms,omitempty"`
	EnterLine int       `json:"enter_line"`
	ExitLine  int       `json:"exit_line,omitempty"`
}

// RTIBLogTable — дамп таблицы из M_LOG_TABLE / M_LOG_TABLE_LISTID
type RTIBLogTable struct {
	TableName string   `json:"table_name"`
	Columns   []string `json:"columns,omitempty"`
	Rows      []string `json:"rows,omitempty"`
	RowCount  int      `json:"row_count"`
	EnterLine int      `json:"enter_line"`
}

// RTISummary — сводка по RTI-логу
type RTISummary struct {
	FilePath           string           `json:"file_path"`
	FileSize           int64            `json:"file_size"`
	TotalCalls         int              `json:"total_calls"`
	ErrorsCount        int              `json:"errors_count"`
	SlowCallsCount     int              `json:"slow_calls_count"`
	MaxNestLevel       int              `json:"max_nest_level"`
	UnparsedLines      int              `json:"unparsed_lines"`
	TopSlow            []RTICall        `json:"top_slow,omitempty"`
	ClientEventsCount  int              `json:"client_events_count,omitempty"`
	ClientErrorsCount  int              `json:"client_errors_count,omitempty"`
	ClientSlowSQLCount int              `json:"client_slow_sql_count,omitempty"`
	TopSlowClientSQL   []RTIClientEvent `json:"top_slow_client_sql,omitempty"`
}

// RTIParseResult — результат парсинга RTI-лога
type RTIParseResult struct {
	Calls         []*RTICall
	ClientEvents  []*RTIClientEvent
	Summary       RTISummary
	UnparsedLines int
}

// RTIClientEvent — одно событие из клиентского (толстый клиент d5nt) трейс-лога.
// В отличие от серверных RTICall, клиентские события гетерогенны: BPL-листинг,
// открытие/закрытие ADO recordset, информация о подключении, сырые SQL/exec-блоки,
// trancount, дампы памяти, ошибки уровня SEVERE и т.д. Тип конкретного тела события
// определяется полем Kind, соответствующее typed-поле заполняется, остальные — zero value.
type RTIClientEvent struct {
	ID         int64     `json:"id"`
	Timestamp  time.Time `json:"timestamp"`
	Level      string    `json:"level"`
	Category   string    `json:"category"`
	ClassName  string    `json:"class_name"`
	MethodName string    `json:"method_name"`
	PID        int       `json:"pid"`
	SeqNo      int       `json:"seq_no"`
	Line       int       `json:"line"`
	Kind       string    `json:"kind"`

	BPL          []RTIBPLModule     `json:"bpl,omitempty"`
	Connection   *RTIConnectionInfo `json:"connection,omitempty"`
	SQL          *RTISQLBlock       `json:"sql,omitempty"`
	TranCount    *int               `json:"tran_count,omitempty"`
	Memory       *RTIMemoryUsage    `json:"memory,omitempty"`
	ErrorText    string             `json:"error_text,omitempty"`
	RawBody      string             `json:"raw_body,omitempty"`
	ElapsedMs    int                `json:"elapsed_ms,omitempty"`
	ParentID     *int64             `json:"parent_id,omitempty"`
	Children     []int64            `json:"children,omitempty"`
	ServerCallID *int64             `json:"server_call_id,omitempty"`

	// Enriched fields (заполняются из CodeBase, см. enrich_client.go)
	SourceFile  string `json:"source_file,omitempty"`
	UnitName    string `json:"unit_name,omitempty"`
	DFMFormName string `json:"dfm_form_name,omitempty"`
	DFMCaption  string `json:"dfm_caption,omitempty"`
}

// RTIBPLModule — одна строка из листинга загруженных BPL-модулей клиента.
type RTIBPLModule struct {
	File    string `json:"file"`
	Version string `json:"version"`
	Title   string `json:"title"`
	Comment string `json:"comment"`
}

// RTIConnectionInfo — информация о новом соединении клиента с сервером БД.
type RTIConnectionInfo struct {
	SPID     int    `json:"spid"`
	Server   string `json:"server"`
	Database string `json:"database"`
	User     string `json:"user"`
	AppName  string `json:"app_name"`
}

// RTISQLBlock — SQL/exec-блок из клиентского лога (заголовок SPID/SERVER/DATABASE + текст запроса).
type RTISQLBlock struct {
	SPID          int        `json:"spid"`
	Server        string     `json:"server"`
	Database      string     `json:"database"`
	Text          string     `json:"text"`
	ExecProcedure string     `json:"exec_procedure,omitempty"`
	ExecParams    []RTIParam `json:"exec_params,omitempty"`
	DurationSec   float64    `json:"duration_sec,omitempty"`
	State         string     `json:"state,omitempty"`
}

// RTIMemoryUsage — дамп потребления памяти клиентского процесса.
type RTIMemoryUsage struct {
	DelphiKB    int `json:"delphi_kb"`
	WinAPIKB    int `json:"winapi_kb"`
	Descriptors int `json:"descriptors"`
	ObjectsUser int `json:"objects_user"`
	ObjectsGDI  int `json:"objects_gdi"`
}

// RTISession — запись о сессии в БД
type RTISession struct {
	ID                int64     `json:"id"`
	FilePath          string    `json:"file_path"`
	ParsedAt          time.Time `json:"parsed_at"`
	TotalCalls        int       `json:"total_calls"`
	ErrorsCount       int       `json:"errors_count"`
	FileSize          int64     `json:"file_size"`
	ClientEventsCount int       `json:"client_events_count,omitempty"`
}

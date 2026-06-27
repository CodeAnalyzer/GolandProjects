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
	FilePath       string    `json:"file_path"`
	FileSize       int64     `json:"file_size"`
	TotalCalls     int       `json:"total_calls"`
	ErrorsCount    int       `json:"errors_count"`
	SlowCallsCount int       `json:"slow_calls_count"`
	MaxNestLevel   int       `json:"max_nest_level"`
	UnparsedLines  int       `json:"unparsed_lines"`
	TopSlow        []RTICall `json:"top_slow,omitempty"`
}

// RTIParseResult — результат парсинга RTI-лога
type RTIParseResult struct {
	Calls         []*RTICall
	Summary       RTISummary
	UnparsedLines int
}

// RTISession — запись о сессии в БД
type RTISession struct {
	ID          int64     `json:"id"`
	FilePath    string    `json:"file_path"`
	ParsedAt    time.Time `json:"parsed_at"`
	TotalCalls  int       `json:"total_calls"`
	ErrorsCount int       `json:"errors_count"`
	FileSize    int64     `json:"file_size"`
}

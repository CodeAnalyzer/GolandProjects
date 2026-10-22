package rtisvc

import (
	"time"

	"github.com/codebase/internal/rti"
)

// SessionSource — источник данных: saved session или file parse.
type SessionSource struct {
	SessionID int64
	FilePath  string
}

// ParseOptions — параметры парсинга; Name — явно заданное имя сессии.
type ParseOptions struct {
	Name string
}

// DeleteParams — параметры удаления: ровно один из SessionID/Name.
type DeleteParams struct {
	SessionID int64
	Name      string
}

// PruneParams — параметры очистки: KeepLast и KeepHours взаимоисключающие.
// nil означает, что аргумент не передан.
type PruneParams struct {
	KeepLast  *int
	KeepHours *int
}

// TimelineFilter — alias for rti.TimelineFilter.
type TimelineFilter = rti.TimelineFilter

// ParseResult — результат парсинга RTI-файла.
type ParseResult struct {
	SessionID  int64          `json:"session_id"`
	TotalCalls int            `json:"total_calls"`
	Summary    rti.RTISummary `json:"summary"`
	Warning    string         `json:"warning,omitempty"`
}

// SummaryResult — статистика по сессии.
type SummaryResult struct {
	Summary rti.RTISummary  `json:"summary"`
	Session *rti.RTISession `json:"session,omitempty"`
}

// TreeResult — дерево вызовов.
type TreeResult struct {
	Tree       *rti.RTITreeNode                    `json:"tree"`
	Enrichment map[string]*rti.ProcedureEnrichment `json:"enrichment,omitempty"`
}

// RTICallSlim — RTICall без тяжёлых BLog-полей в JSON.
type RTICallSlim struct {
	*rti.RTICall
	BLogTables interface{} `json:"blog_tables,omitempty"`
	BLogBlocks interface{} `json:"blog_blocks,omitempty"`
}

// ErrorsResult — ошибки с enrich.
type ErrorsResult struct {
	ServerErrors     []RTICallSlim                       `json:"server_errors"`
	ServerErrorCount int                                 `json:"server_error_count"`
	ServerEnrichment map[string]*rti.ProcedureEnrichment `json:"server_enrichment,omitempty"`
	ClientErrors     []*rti.RTIClientEvent               `json:"client_errors"`
	ClientErrorCount int                                 `json:"client_error_count"`
	ClientEnrichment map[string]*rti.ClientEnrichment    `json:"client_enrichment,omitempty"`
	Limit            int                                 `json:"limit"`
}

// SlowResult — медленные вызовы.
type SlowResult struct {
	ServerCalls      []RTICallSlim                       `json:"server_calls"`
	ServerCallCount  int                                 `json:"server_call_count"`
	ServerEnrichment map[string]*rti.ProcedureEnrichment `json:"server_enrichment,omitempty"`
	ClientSQLBlocks  []*rti.RTIClientEvent               `json:"client_sql_blocks"`
	ClientSQLCount   int                                 `json:"client_sql_count"`
	ClientEnrichment map[string]*rti.ClientEnrichment    `json:"client_enrichment,omitempty"`
	Threshold        int                                 `json:"threshold"`
	Limit            int                                 `json:"limit"`
}

// DetailsResult — детали процедуры.
type DetailsResult struct {
	Procedure  string                   `json:"procedure"`
	Calls      []*rti.RTICall           `json:"calls"`
	Count      int                      `json:"count"`
	Enrichment *rti.ProcedureEnrichment `json:"enrichment,omitempty"`
}

// BlogCallItem — выжимка BLog-данных из вызова.
type BlogCallItem struct {
	EnterLine   int                 `json:"enter_line"`
	ElapsedMs   int                 `json:"elapsed_ms,omitempty"`
	BLogBlocks  []rti.RTIBLogBlock  `json:"blog_blocks,omitempty"`
	Checkpoints []rti.RTICheckpoint `json:"checkpoints,omitempty"`
	BLogTables  []rti.RTIBLogTable  `json:"blog_tables,omitempty"`
}

// BlogResult — business log для процедуры.
type BlogResult struct {
	Procedure string         `json:"procedure"`
	Count     int            `json:"count"`
	Calls     []BlogCallItem `json:"calls"`
}

// ClientTreeResult — дерево клиентских событий.
type ClientTreeResult struct {
	Nodes               interface{}                      `json:"nodes"`
	Enrichment          map[string]*rti.ClientEnrichment `json:"enrichment,omitempty"`
	FilteredEventsCount int                              `json:"filtered_events_count"`
	Limit               int                              `json:"limit"`
}

// TimelineResult — единый timeline.
type TimelineResult struct {
	Calls               interface{}                      `json:"calls"`
	ClientEvents        interface{}                      `json:"client_events"`
	Enrichment          map[string]*rti.ClientEnrichment `json:"enrichment,omitempty"`
	FilteredCallsCount  int                              `json:"filtered_calls_count"`
	FilteredEventsCount int                              `json:"filtered_events_count"`
	Limit               int                              `json:"limit"`
}

// ListResult — список сессий.
type ListResult struct {
	Sessions []rti.RTISession `json:"sessions"`
}

// DeleteResult — результат удаления.
type DeleteResult struct {
	Deleted      bool    `json:"deleted"`
	SessionID    int64   `json:"session_id"`
	FilePath     string  `json:"file_path,omitempty"`
	DeletedCount int     `json:"deleted_count"`
	SessionIDs   []int64 `json:"session_ids,omitempty"`
}

// PruneResult — результат очистки.
type PruneResult struct {
	DeletedCount int        `json:"deleted_count"`
	KeptLast     int        `json:"kept_last"`
	KeptHours    *int       `json:"kept_hours"`
	Cutoff       *time.Time `json:"cutoff"`
}

// --- Параметры функций ---

// ErrorsParams — параметры для ExecuteErrors.
type ErrorsParams struct {
	Source SessionSource
	Limit  int
}

// SlowParams — параметры для ExecuteSlow.
type SlowParams struct {
	Source      SessionSource
	ThresholdMs int
	Limit       int
}

// TreeParams — параметры для ExecuteTree.
type TreeParams struct {
	Source    SessionSource
	Procedure string
	MaxDepth  int
}

// DetailsParams — параметры для ExecuteDetails.
type DetailsParams struct {
	Source    SessionSource
	Procedure string
	Limit     int
}

// BlogParams — параметры для ExecuteBlog.
type BlogParams struct {
	Source    SessionSource
	Procedure string
	Limit     int
}

// ClientTreeParams — параметры для ExecuteClientTree.
type ClientTreeParams struct {
	Source SessionSource
	Filter rti.TimelineFilter
	Limit  int
}

// TimelineParams — параметры для ExecuteTimeline.
type TimelineParams struct {
	Source SessionSource
	Filter rti.TimelineFilter
	Limit  int
}

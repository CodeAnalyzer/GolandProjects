package trcsvc

import (
	"github.com/codebase/internal/trc"
)

// SessionSource — источник данных: saved session или file parse.
type SessionSource struct {
	SessionID int64
	FilePath  string
}

// ParseResult — результат парсинга TRC-файла.
type ParseResult struct {
	SessionID   int64  `json:"session_id"`
	TotalEvents int    `json:"total_events"`
	Warning     string `json:"warning,omitempty"`
}

// SummaryResult — статистика по сессии.
type SummaryResult struct {
	TotalEvents int             `json:"total_events"`
	Header      *trc.TraceHeader `json:"header"`
	Session     *trc.TRCSession  `json:"session,omitempty"`
}

// EventsResult — список событий с фильтрацией.
type EventsResult struct {
	Events        []trc.TRCEvent `json:"events"`
	TotalCount    int            `json:"total_count"`
	FilteredCount int            `json:"filtered_count"`
	Limit         int            `json:"limit"`
}

// ProceduresResult — агрегация по процедурам.
type ProceduresResult struct {
	Procedures []trc.TRCProcAgg `json:"procedures"`
	Count      int              `json:"count"`
}

// TreeResult — деревья вызовов по SPID.
type TreeResult struct {
	Trees      map[int][]*trc.TRCTreeNode `json:"trees"`
	EventCount int                        `json:"event_count,omitempty"`
	SPID       int                        `json:"spid,omitempty"`
}

// ErrorsResult — события с ошибками.
type ErrorsResult struct {
	Events []trc.TRCEvent `json:"events"`
	Count  int            `json:"count"`
	Limit  int            `json:"limit"`
}

// SlowResult — медленные события.
type SlowResult struct {
	Events    []trc.TRCEvent `json:"events"`
	Count     int            `json:"count"`
	Threshold int            `json:"threshold"`
	Limit     int            `json:"limit"`
}

// ListResult — список сессий.
type ListResult struct {
	Sessions []trc.TRCSession `json:"sessions"`
}

// DeleteResult — результат удаления.
type DeleteResult struct {
	Deleted   bool   `json:"deleted"`
	SessionID int64  `json:"session_id"`
	FilePath  string `json:"file_path,omitempty"`
}

// PruneResult — результат очистки.
type PruneResult struct {
	DeletedCount int `json:"deleted_count"`
	KeptLast     int `json:"kept_last"`
}

// --- Параметры функций ---

// EventsParams — параметры для ExecuteEvents.
type EventsParams struct {
	Source    SessionSource
	SPID      int
	Procedure string
	EventName string
	Limit     int
}

// TreeParams — параметры для ExecuteTree.
type TreeParams struct {
	Source    SessionSource
	SPID      int
	MaxDepth  int
	Limit     int
	Procedure string
}

// SlowParams — параметры для ExecuteSlow.
type SlowParams struct {
	Source      SessionSource
	ThresholdMs int
	Limit       int
}

// ErrorsParams — параметры для ExecuteErrors.
type ErrorsParams struct {
	Source SessionSource
	Limit  int
}

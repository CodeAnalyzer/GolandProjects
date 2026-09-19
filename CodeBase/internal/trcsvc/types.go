package trcsvc

import (
	"encoding/json"
	"time"

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
	TotalEvents int              `json:"total_events"`
	Header      *trc.TraceHeader `json:"header"`
	Session     *trc.TRCSession  `json:"session,omitempty"`
}

// TRCEventView — компактное представление события в ответе (format=short):
// без params и columns. ID совпадает с id события в format=full.
type TRCEventView struct {
	ID         int64      `json:"id"`
	EventClass int        `json:"event_class"`
	EventName  string     `json:"event_name"`
	SPID       int        `json:"spid,omitempty"`
	Procedure  string     `json:"procedure,omitempty"`
	StartTime  *time.Time `json:"start_time,omitempty"`
	EndTime    *time.Time `json:"end_time,omitempty"`
	DurationMs int64      `json:"duration_ms"`
}

// EventsResult — результат ExecuteEvents. Full-формат возвращает доменные
// trc.TRCEvent (включая params/columns) в Events; short-формат — TRCEventView
// в Views. TotalCount устанавливается только на первой странице (AfterID nil);
// на страницах продолжения поле отсутствует в JSON.
type EventsResult struct {
	Events        []trc.TRCEvent
	Views         []TRCEventView
	Short         bool
	TotalCount    *int
	FilteredCount int
	ReturnedCount int
	Limit         int
	HasMore       bool
	NextAfterID   int64
}

// MarshalJSON сериализует результат: ключ events содержит доменные события
// (full) или компактные представления (short); next_after_id отсутствует при
// HasMore=false или пустой странице; total_count — только на первой странице.
func (r EventsResult) MarshalJSON() ([]byte, error) {
	out := struct {
		Events        interface{} `json:"events"`
		TotalCount    *int        `json:"total_count,omitempty"`
		FilteredCount int         `json:"filtered_count"`
		ReturnedCount int         `json:"returned_count"`
		Limit         int         `json:"limit"`
		HasMore       bool        `json:"has_more"`
		NextAfterID   int64       `json:"next_after_id,omitempty"`
	}{
		TotalCount:    r.TotalCount,
		FilteredCount: r.FilteredCount,
		ReturnedCount: r.ReturnedCount,
		Limit:         r.Limit,
		HasMore:       r.HasMore,
		NextAfterID:   r.NextAfterID,
	}
	if r.Short {
		if r.Views == nil {
			out.Events = []TRCEventView{}
		} else {
			out.Events = r.Views
		}
	} else {
		if r.Events == nil {
			out.Events = []trc.TRCEvent{}
		} else {
			out.Events = r.Events
		}
	}
	return json.Marshal(out)
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

// EventsParams — параметры для ExecuteEvents. SPIDs/EventNames — канонические
// массивы (legacy scalar-алиасы нормализуются на границе CLI/MCP); Format —
// "" или "full" (default) / "short"; AfterID — keyset-курсор предыдущей
// страницы (nil = первая страница).
type EventsParams struct {
	Source        SessionSource
	SPIDs         []int
	Procedure     string
	EventNames    []string
	TimeFrom      *time.Time
	TimeTo        *time.Time
	MinDurationMs *int64
	AfterID       *int64
	Format        string
	Limit         int
}

// ProceduresParams — параметры для ExecuteProcedures. Отсутствие EventNames
// означает агрегацию только SP:Completed (текущее поведение); Top 0 = все
// процедуры; SortBy: total_ms (default), avg_ms, max_ms, count.
type ProceduresParams struct {
	Source      SessionSource
	SPIDs       []int
	EventNames  []string
	Top         int
	SortBy      string
	GroupBySPID bool
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

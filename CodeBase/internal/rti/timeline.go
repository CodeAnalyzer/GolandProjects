package rti

import (
	"strings"
	"time"
)

// TimelineFilter — опциональные фильтры для codebase_rti_timeline.
// Нулевые значения означают «без фильтра».
type TimelineFilter struct {
	TimeFrom   *time.Time
	TimeTo     *time.Time
	PID        *int
	Procedure  string
	ClassName  string
	MethodName string
	Format     string // "full" (по умолчанию) или "short"
}

// ApplyTimelineFilter фильтрует серверные вызовы и клиентские события
// по заданным критериям. Фильтрация по времени применяется независимо:
// calls — по EnterTime, events — по Timestamp (часы клиента и сервера
// могут расходиться).
func ApplyTimelineFilter(calls []*RTICall, events []*RTIClientEvent, f TimelineFilter) ([]*RTICall, []*RTIClientEvent) {
	var outCalls []*RTICall
	for _, c := range calls {
		if f.TimeFrom != nil && c.EnterTime.Before(*f.TimeFrom) {
			continue
		}
		if f.TimeTo != nil && c.EnterTime.After(*f.TimeTo) {
			continue
		}
		if f.Procedure != "" && !strings.EqualFold(c.Procedure, f.Procedure) {
			continue
		}
		outCalls = append(outCalls, c)
	}

	var outEvents []*RTIClientEvent
	for _, e := range events {
		if f.TimeFrom != nil && e.Timestamp.Before(*f.TimeFrom) {
			continue
		}
		if f.TimeTo != nil && e.Timestamp.After(*f.TimeTo) {
			continue
		}
		if f.PID != nil && e.PID != *f.PID {
			continue
		}
		if f.ClassName != "" && !strings.EqualFold(e.ClassName, f.ClassName) {
			continue
		}
		if f.MethodName != "" && !strings.EqualFold(e.MethodName, f.MethodName) {
			continue
		}
		outEvents = append(outEvents, e)
	}

	return outCalls, outEvents
}

// RTICallShort — краткий формат серверного вызова без тяжёлых полей
// (Params, Checkpoints, BLogBlocks, BLogTables, RetValContext).
type RTICallShort struct {
	ID         int64      `json:"id"`
	Procedure  string     `json:"procedure"`
	EnterTime  time.Time  `json:"enter_time"`
	ExitTime   *time.Time `json:"exit_time,omitempty"`
	ElapsedMs  int        `json:"elapsed_ms,omitempty"`
	NestLevel  int        `json:"nest_level"`
	SPID       int        `json:"spid"`
	ModuleName string     `json:"module_name,omitempty"`
	ModuleID   int        `json:"module_id,omitempty"`
	RetVal     *int       `json:"ret_val,omitempty"`
	EnterLine  int        `json:"enter_line"`
	ExitLine   int        `json:"exit_line,omitempty"`
	ParentID   *int64     `json:"parent_id,omitempty"`
}

// RTIClientEventShort — краткий формат клиентского события без тяжёлых
// полей (BPL, Connection, SQL, Memory, ErrorText, RawBody).
type RTIClientEventShort struct {
	ID           int64   `json:"id"`
	Timestamp    time.Time `json:"timestamp"`
	Level        string  `json:"level"`
	Category     string  `json:"category"`
	ClassName    string  `json:"class_name"`
	MethodName   string  `json:"method_name"`
	PID          int     `json:"pid"`
	SeqNo        int     `json:"seq_no"`
	Line         int     `json:"line"`
	Kind         string  `json:"kind"`
	ElapsedMs    int     `json:"elapsed_ms,omitempty"`
	ServerCallID *int64  `json:"server_call_id,omitempty"`
}

// ToShortCall преобразует RTICall в краткий формат.
func ToShortCall(c *RTICall) RTICallShort {
	return RTICallShort{
		ID:         c.ID,
		Procedure:  c.Procedure,
		EnterTime:  c.EnterTime,
		ExitTime:   c.ExitTime,
		ElapsedMs:  c.ElapsedMs,
		NestLevel:  c.NestLevel,
		SPID:       c.SPID,
		ModuleName: c.ModuleName,
		ModuleID:   c.ModuleID,
		RetVal:     c.RetVal,
		EnterLine:  c.EnterLine,
		ExitLine:   c.ExitLine,
		ParentID:   c.ParentID,
	}
}

// ToShortEvent преобразует RTIClientEvent в краткий формат.
func ToShortEvent(ev *RTIClientEvent) RTIClientEventShort {
	return RTIClientEventShort{
		ID:           ev.ID,
		Timestamp:    ev.Timestamp,
		Level:        ev.Level,
		Category:     ev.Category,
		ClassName:    ev.ClassName,
		MethodName:   ev.MethodName,
		PID:          ev.PID,
		SeqNo:        ev.SeqNo,
		Line:         ev.Line,
		Kind:         ev.Kind,
		ElapsedMs:    ev.ElapsedMs,
		ServerCallID: ev.ServerCallID,
	}
}

// FilterClientEvents фильтрует клиентские события через TimelineFilter.
// Переиспользует ApplyTimelineFilter (calls=nil).
func FilterClientEvents(events []*RTIClientEvent, f TimelineFilter) []*RTIClientEvent {
	_, out := ApplyTimelineFilter(nil, events, f)
	return out
}

// RTIClientTreeNodeShort — краткий формат узла дерева клиентских событий.
type RTIClientTreeNodeShort struct {
	PID    int                   `json:"pid"`
	Events []RTIClientEventShort `json:"events"`
}

// ToShortClientTreeNode преобразует узел дерева в краткий формат.
func ToShortClientTreeNode(node *RTIClientTreeNode) RTIClientTreeNodeShort {
	events := make([]RTIClientEventShort, 0, len(node.Events))
	for _, ev := range node.Events {
		events = append(events, ToShortEvent(ev))
	}
	return RTIClientTreeNodeShort{PID: node.PID, Events: events}
}

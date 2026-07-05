package rti

import (
	"testing"
	"time"
)

func TestLinkClientServerCalls_MatchBySPIDAndTime(t *testing.T) {
	base := time.Date(2026, 6, 10, 15, 21, 5, 0, time.UTC)
	calls := []*RTICall{
		{ID: 1, Procedure: "Cons_Get_ProtocolNumber", EnterTime: base.Add(1 * time.Second), SPID: 58},
		{ID: 2, Procedure: "Cons_Get_ProtocolNumber", EnterTime: base.Add(1 * time.Second), SPID: 99},
	}
	events := []*RTIClientEvent{
		{
			ID:        1,
			Kind:      "sql_block",
			Timestamp: base,
			SQL:       &RTISQLBlock{SPID: 58, ExecProcedure: "Cons_Get_ProtocolNumber"},
		},
	}

	LinkClientServerCalls(calls, events)

	if events[0].ServerCallID == nil {
		t.Fatalf("expected ServerCallID to be set")
	}
	if *events[0].ServerCallID != 1 {
		t.Fatalf("expected match with call ID 1 (SPID match), got %d", *events[0].ServerCallID)
	}
}

func TestLinkClientServerCalls_NoMatchOutsideWindow(t *testing.T) {
	base := time.Date(2026, 6, 10, 15, 21, 5, 0, time.UTC)
	calls := []*RTICall{
		{ID: 1, Procedure: "SomeProc", EnterTime: base.Add(10 * time.Second), SPID: 58},
	}
	events := []*RTIClientEvent{
		{
			ID:        1,
			Kind:      "sql_block",
			Timestamp: base,
			SQL:       &RTISQLBlock{SPID: 58, ExecProcedure: "SomeProc"},
		},
	}

	LinkClientServerCalls(calls, events)

	if events[0].ServerCallID != nil {
		t.Fatalf("expected no match (outside time window), got %d", *events[0].ServerCallID)
	}
}

func TestLinkClientServerCalls_NoMatchDifferentProcedure(t *testing.T) {
	base := time.Date(2026, 6, 10, 15, 21, 5, 0, time.UTC)
	calls := []*RTICall{
		{ID: 1, Procedure: "OtherProc", EnterTime: base, SPID: 58},
	}
	events := []*RTIClientEvent{
		{
			ID:        1,
			Kind:      "sql_block",
			Timestamp: base,
			SQL:       &RTISQLBlock{SPID: 58, ExecProcedure: "SomeProc"},
		},
	}

	LinkClientServerCalls(calls, events)

	if events[0].ServerCallID != nil {
		t.Fatalf("expected no match (different procedure name)")
	}
}

func TestLinkClientServerCalls_IgnoresNonSQLBlockEvents(t *testing.T) {
	base := time.Date(2026, 6, 10, 15, 21, 5, 0, time.UTC)
	calls := []*RTICall{
		{ID: 1, Procedure: "SomeProc", EnterTime: base, SPID: 58},
	}
	events := []*RTIClientEvent{
		{ID: 1, Kind: "connection", Timestamp: base},
		{ID: 2, Kind: "sql_block", Timestamp: base, SQL: &RTISQLBlock{}}, // empty ExecProcedure
	}

	LinkClientServerCalls(calls, events)

	if events[0].ServerCallID != nil || events[1].ServerCallID != nil {
		t.Fatalf("expected no matches for non-exec events")
	}
}

package rti

import (
	"testing"
	"time"
)

func TestApplyTimelineFilter_ByTime(t *testing.T) {
	t0 := time.Date(2026, 2, 20, 12, 40, 0, 0, time.UTC)
	t2 := t0.Add(60 * time.Second)

	calls := []*RTICall{
		{ID: 1, Procedure: "proc_a", EnterTime: t0},
		{ID: 2, Procedure: "proc_b", EnterTime: t2},
	}
	events := []*RTIClientEvent{
		{ID: 10, Timestamp: t0, PID: 100},
		{ID: 11, Timestamp: t2, PID: 200},
	}

	from := t0
	to := t0.Add(40 * time.Second)
	f := TimelineFilter{TimeFrom: &from, TimeTo: &to}

	outCalls, outEvents := ApplyTimelineFilter(calls, events, f)

	if len(outCalls) != 1 || outCalls[0].ID != 1 {
		t.Fatalf("expected call ID=1, got %d calls", len(outCalls))
	}
	if len(outEvents) != 1 || outEvents[0].ID != 10 {
		t.Fatalf("expected event ID=10, got %d events", len(outEvents))
	}
}

func TestApplyTimelineFilter_ByProcedure(t *testing.T) {
	t0 := time.Now()
	calls := []*RTICall{
		{ID: 1, Procedure: "MyProc", EnterTime: t0},
		{ID: 2, Procedure: "OtherProc", EnterTime: t0},
	}
	events := []*RTIClientEvent{}

	f := TimelineFilter{Procedure: "myproc"} // case-insensitive
	outCalls, _ := ApplyTimelineFilter(calls, events, f)

	if len(outCalls) != 1 || outCalls[0].Procedure != "MyProc" {
		t.Fatalf("expected 1 call 'MyProc', got %d", len(outCalls))
	}
}

func TestApplyTimelineFilter_ByPID(t *testing.T) {
	t0 := time.Now()
	events := []*RTIClientEvent{
		{ID: 1, Timestamp: t0, PID: 13672},
		{ID: 2, Timestamp: t0, PID: 9999},
	}
	calls := []*RTICall{}

	pid := 13672
	f := TimelineFilter{PID: &pid}
	_, outEvents := ApplyTimelineFilter(calls, events, f)

	if len(outEvents) != 1 || outEvents[0].PID != 13672 {
		t.Fatalf("expected 1 event PID=13672, got %d", len(outEvents))
	}
}

func TestApplyTimelineFilter_ByClassAndMethod(t *testing.T) {
	t0 := time.Now()
	events := []*RTIClientEvent{
		{ID: 1, Timestamp: t0, ClassName: "TFormMain", MethodName: "Button1Click"},
		{ID: 2, Timestamp: t0, ClassName: "TFormMain", MethodName: "Button2Click"},
		{ID: 3, Timestamp: t0, ClassName: "TOtherForm", MethodName: "Button1Click"},
	}
	calls := []*RTICall{}

	f := TimelineFilter{ClassName: "tformmain", MethodName: "button1click"}
	_, outEvents := ApplyTimelineFilter(calls, events, f)

	if len(outEvents) != 1 || outEvents[0].ID != 1 {
		t.Fatalf("expected 1 event ID=1, got %d", len(outEvents))
	}
}

func TestApplyTimelineFilter_NoFilters_ReturnsAll(t *testing.T) {
	t0 := time.Now()
	calls := []*RTICall{
		{ID: 1, Procedure: "proc_a", EnterTime: t0},
		{ID: 2, Procedure: "proc_b", EnterTime: t0},
	}
	events := []*RTIClientEvent{
		{ID: 10, Timestamp: t0, PID: 100},
	}

	f := TimelineFilter{}
	outCalls, outEvents := ApplyTimelineFilter(calls, events, f)

	if len(outCalls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(outCalls))
	}
	if len(outEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(outEvents))
	}
}

func TestToShortCall_DropsHeavyFields(t *testing.T) {
	exitTime := time.Now()
	retVal := 42
	parentID := int64(5)

	c := &RTICall{
		ID:            1,
		Procedure:     "proc_a",
		EnterLine:     10,
		ExitLine:      20,
		EnterTime:     time.Now(),
		ExitTime:      &exitTime,
		ElapsedMs:     100,
		NestLevel:     2,
		ModuleID:      7,
		TranCount:     1,
		BeginCnt:      2,
		RetVal:        &retVal,
		RetValContext: "some context",
		Params:        []RTIParam{{Name: "@p", Type: "int", Value: "1"}},
		Checkpoints:   []RTICheckpoint{{Label: "cp1"}},
		BLogBlocks:    []RTIBLogBlock{{BlockName: "blk1"}},
		BLogTables:    []RTIBLogTable{{TableName: "tbl1"}},
		Children:      []int64{2, 3},
		ParentID:      &parentID,
		SPID:          55,
		SourceFile:    "src.sql",
		ModuleName:    "Mod",
		RetValMeaning: "OK",
		ErrorConstant: "ERR_001",
	}

	short := ToShortCall(c)

	if short.ID != c.ID || short.Procedure != c.Procedure {
		t.Error("ID/Procedure mismatch")
	}
	if short.ElapsedMs != c.ElapsedMs || short.NestLevel != c.NestLevel {
		t.Error("ElapsedMs/NestLevel mismatch")
	}
	if short.SPID != c.SPID || short.ModuleID != c.ModuleID {
		t.Error("SPID/ModuleID mismatch")
	}
	if short.ModuleName != c.ModuleName {
		t.Error("ModuleName mismatch")
	}
	if short.RetVal == nil || *short.RetVal != retVal {
		t.Error("RetVal mismatch")
	}
	if short.ParentID == nil || *short.ParentID != parentID {
		t.Error("ParentID mismatch")
	}
	if short.ExitTime == nil || !short.ExitTime.Equal(exitTime) {
		t.Error("ExitTime mismatch")
	}
	// Verify heavy fields are not in the short struct (compile-time check by design)
	// Just ensure the struct is value type, not pointer
	_ = short
}

func TestToShortEvent_DropsHeavyFields(t *testing.T) {
	serverCallID := int64(99)

	ev := &RTIClientEvent{
		ID:           1,
		Timestamp:    time.Now(),
		Level:        "INFO",
		Category:     "sql",
		ClassName:    "TFormMain",
		MethodName:   "Button1Click",
		PID:          13672,
		SeqNo:        5,
		Line:         100,
		Kind:         "sql_block",
		ElapsedMs:    50,
		ServerCallID: &serverCallID,
		BPL:          []RTIBPLModule{{File: "test.bpl"}},
		Connection:   &RTIConnectionInfo{SPID: 55},
		SQL:          &RTISQLBlock{Text: "SELECT 1"},
		RawBody:      "raw text here",
		ErrorText:    "some error",
	}

	short := ToShortEvent(ev)

	if short.ID != ev.ID || short.ClassName != ev.ClassName {
		t.Error("ID/ClassName mismatch")
	}
	if short.PID != ev.PID || short.Kind != ev.Kind {
		t.Error("PID/Kind mismatch")
	}
	if short.ElapsedMs != ev.ElapsedMs {
		t.Error("ElapsedMs mismatch")
	}
	if short.ServerCallID == nil || *short.ServerCallID != serverCallID {
		t.Error("ServerCallID mismatch")
	}
	_ = short
}

func TestFilterClientEvents_ByTimeAndClass(t *testing.T) {
	t0 := time.Date(2026, 2, 20, 12, 40, 0, 0, time.UTC)
	t1 := t0.Add(30 * time.Second)

	events := []*RTIClientEvent{
		{ID: 1, Timestamp: t0, PID: 100, ClassName: "DSConnectorADO", MethodName: "Connect"},
		{ID: 2, Timestamp: t1, PID: 200, ClassName: "TFormMain", MethodName: "Button1Click"},
		{ID: 3, Timestamp: t0, PID: 100, ClassName: "DSConnectorADO", MethodName: "Disconnect"},
	}

	from := t0
	to := t0.Add(5 * time.Second)
	f := TimelineFilter{TimeFrom: &from, TimeTo: &to, ClassName: "dsconnectorado"}

	out := FilterClientEvents(events, f)

	if len(out) != 2 {
		t.Fatalf("expected 2 events (DSConnectorADO within 5s), got %d", len(out))
	}
	for _, ev := range out {
		if ev.ClassName != "DSConnectorADO" {
			t.Errorf("unexpected ClassName: %s", ev.ClassName)
		}
	}
}

func TestFilterClientEvents_NoFilters_ReturnsAll(t *testing.T) {
	events := []*RTIClientEvent{
		{ID: 1, Timestamp: time.Now(), PID: 100},
		{ID: 2, Timestamp: time.Now(), PID: 200},
	}

	f := TimelineFilter{}
	out := FilterClientEvents(events, f)

	if len(out) != 2 {
		t.Fatalf("expected 2 events, got %d", len(out))
	}
}

func TestToShortClientTreeNode_DropsHeavyFields(t *testing.T) {
	serverCallID := int64(42)

	node := &RTIClientTreeNode{
		PID: 13672,
		Events: []*RTIClientEvent{
			{
				ID:           1,
				Timestamp:    time.Now(),
				Level:        "INFO",
				Category:     "sql",
				ClassName:    "TFormMain",
				MethodName:   "Button1Click",
				PID:          13672,
				SeqNo:        1,
				Line:         50,
				Kind:         "sql_block",
				ElapsedMs:    30,
				ServerCallID: &serverCallID,
				BPL:          []RTIBPLModule{{File: "test.bpl"}},
				SQL:          &RTISQLBlock{Text: "SELECT 1"},
				RawBody:      "raw body text",
			},
		},
	}

	short := ToShortClientTreeNode(node)

	if short.PID != 13672 {
		t.Fatalf("expected PID=13672, got %d", short.PID)
	}
	if len(short.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(short.Events))
	}
	ev := short.Events[0]
	if ev.ID != 1 || ev.ClassName != "TFormMain" {
		t.Error("ID/ClassName mismatch")
	}
	if ev.ServerCallID == nil || *ev.ServerCallID != serverCallID {
		t.Error("ServerCallID mismatch")
	}
	_ = short
}

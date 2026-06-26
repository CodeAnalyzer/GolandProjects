package rti

import (
	"testing"
)

func TestParseContent_EnterExit(t *testing.T) {
	content := "04.03.2026 16:59:03.173\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t349\t357325\t\t0\t245\n" +
		"Enter Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 17\n" +
		"@InterfaceObjectID             : DSIDENTIFIER                   = 161\n" +
		"04.03.2026 16:59:03.203\tINFO\tTrace.Server.Proc\t\tCons_Get_ProtocolNumber\t349\t357327\t\t0\t196\n" +
		"Exit Cons_Get_ProtocolNumber @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 34\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if c.Procedure != "Cons_Get_ProtocolNumber" {
		t.Errorf("Procedure = %q, want %q", c.Procedure, "Cons_Get_ProtocolNumber")
	}
	if c.NestLevel != 1 {
		t.Errorf("NestLevel = %d, want 1", c.NestLevel)
	}
	if c.ModuleID != 39 {
		t.Errorf("ModuleID = %d, want 39", c.ModuleID)
	}
	if c.ModuleName != "Consumer" {
		t.Errorf("ModuleName = %q, want %q", c.ModuleName, "Consumer")
	}
	if c.ElapsedMs != 34 {
		t.Errorf("ElapsedMs = %d, want 34", c.ElapsedMs)
	}
	if c.RetVal == nil || *c.RetVal != 0 {
		t.Errorf("RetVal = %v, want 0", c.RetVal)
	}
	if len(c.Params) != 1 {
		t.Errorf("Params len = %d, want 1", len(c.Params))
	} else {
		if c.Params[0].Name != "InterfaceObjectID" {
			t.Errorf("Param[0].Name = %q, want %q", c.Params[0].Name, "InterfaceObjectID")
		}
		if c.Params[0].Value != "161" {
			t.Errorf("Param[0].Value = %q, want %q", c.Params[0].Value, "161")
		}
	}
}

func TestParseContent_NestedCalls(t *testing.T) {
	content := "04.03.2026 16:59:03.237\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357338\t\t0\t1264\n" +
		"Enter DealProtocol_Select @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:03.250\tINFO\tTrace.Server.Proc\t\tActionPlan_CheckAction\t349\t357346\t\t0\t705\n" +
		"Enter ActionPlan_CheckAction @@TranCount = 0 @@NestLevel = 2 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:03.267\tINFO\tTrace.Server.Proc\t\tActionPlan_CheckAction\t349\t357349\t\t0\t268\n" +
		"Exit ActionPlan_CheckAction @@TranCount = 0 @@NestLevel = 2@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 16\n" +
		"Return 0\n" +
		"04.03.2026 16:59:03.267\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357351\t\t0\t119\n" +
		"Exit DealProtocol_Select @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 30\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(result.Calls))
	}
	parent := result.Calls[0]
	child := result.Calls[1]
	if parent.Procedure != "DealProtocol_Select" {
		t.Errorf("parent Procedure = %q", parent.Procedure)
	}
	if child.Procedure != "ActionPlan_CheckAction" {
		t.Errorf("child Procedure = %q", child.Procedure)
	}
	if child.NestLevel != 2 {
		t.Errorf("child NestLevel = %d, want 2", child.NestLevel)
	}
	if child.ParentID == nil || *child.ParentID != parent.ID {
		t.Errorf("child ParentID = %v, want %d", child.ParentID, parent.ID)
	}
	if len(parent.Children) != 1 || parent.Children[0] != child.ID {
		t.Errorf("parent Children = %v, want [%d]", parent.Children, child.ID)
	}
}

func TestParseContent_RetVal(t *testing.T) {
	content := "04.03.2026 16:59:03.173\tINFO\tTrace.Server.Proc\t\tSomeProc\t349\t100\t\t0\t100\n" +
		"Enter SomeProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 5\n" +
		"RetVal = 0#Отбор действий\n" +
		"04.03.2026 16:59:03.180\tINFO\tTrace.Server.Proc\t\tSomeProc\t349\t101\t\t0\t50\n" +
		"Exit SomeProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 7\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if c.RetVal == nil || *c.RetVal != 0 {
		t.Errorf("RetVal = %v, want 0", c.RetVal)
	}
	if c.RetValContext != "Отбор действий" {
		t.Errorf("RetValContext = %q, want %q", c.RetValContext, "Отбор действий")
	}
}

func TestParseContent_Checkpoint(t *testing.T) {
	content := "04.03.2026 16:59:03.237\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357338\t\t0\t1264\n" +
		"Enter DealProtocol_Select @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"DealProtocol_Select_Begin_1\n" +
		"04.03.2026 16:59:03.250\tINFO\tTrace.Server.Proc\t\tDealProtocol_Select\t349\t357351\t\t0\t119\n" +
		"Exit DealProtocol_Select @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 30\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if len(c.Checkpoints) != 1 {
		t.Fatalf("expected 1 checkpoint, got %d", len(c.Checkpoints))
	}
	if c.Checkpoints[0].Label != "DealProtocol_Select_Begin_1" {
		t.Errorf("Checkpoint Label = %q", c.Checkpoints[0].Label)
	}
}

func TestParseContent_ErrorCount(t *testing.T) {
	content := "04.03.2026 16:59:03.173\tINFO\tTrace.Server.Proc\t\tProc1\t349\t100\t\t0\t100\n" +
		"Enter Proc1 @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 5\n" +
		"04.03.2026 16:59:03.180\tINFO\tTrace.Server.Proc\t\tProc1\t349\t101\t\t0\t50\n" +
		"Exit Proc1 @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 7\n" +
		"Return 1\n" +
		"04.03.2026 16:59:03.190\tINFO\tTrace.Server.Proc\t\tProc2\t349\t102\t\t0\t50\n" +
		"Enter Proc2 @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 3\n" +
		"04.03.2026 16:59:03.193\tINFO\tTrace.Server.Proc\t\tProc2\t349\t103\t\t0\t50\n" +
		"Exit Proc2 @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 3\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if result.Summary.ErrorsCount != 1 {
		t.Errorf("ErrorsCount = %d, want 1", result.Summary.ErrorsCount)
	}
}

func TestModuleNameByID(t *testing.T) {
	if ModuleNameByID(10) != "Core" {
		t.Errorf("ModuleNameByID(10) = %q, want %q", ModuleNameByID(10), "Core")
	}
	if ModuleNameByID(39) != "Consumer" {
		t.Errorf("ModuleNameByID(39) = %q, want %q", ModuleNameByID(39), "Consumer")
	}
	if ModuleNameByID(999) != "Unknown" {
		t.Errorf("ModuleNameByID(999) = %q, want %q", ModuleNameByID(999), "Unknown")
	}
}

func TestBuildTree(t *testing.T) {
	content := "04.03.2026 16:59:03.237\tINFO\tTrace.Server.Proc\t\tParent\t349\t1\t\t0\t100\n" +
		"Enter Parent @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:03.240\tINFO\tTrace.Server.Proc\t\tChild1\t349\t2\t\t0\t50\n" +
		"Enter Child1 @@TranCount = 0 @@NestLevel = 2 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:03.242\tINFO\tTrace.Server.Proc\t\tChild1\t349\t3\t\t0\t50\n" +
		"Exit Child1 @@TranCount = 0 @@NestLevel = 2@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 2\n" +
		"Return 0\n" +
		"04.03.2026 16:59:03.245\tINFO\tTrace.Server.Proc\t\tParent\t349\t4\t\t0\t100\n" +
		"Exit Parent @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 8\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}

	tree := BuildTree(result.Calls, "Parent", 0)
	if tree == nil {
		t.Fatal("BuildTree returned nil")
	}
	if tree.Call.Procedure != "Parent" {
		t.Errorf("root Procedure = %q, want %q", tree.Call.Procedure, "Parent")
	}
	if len(tree.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(tree.Children))
	}
	if tree.Children[0].Call.Procedure != "Child1" {
		t.Errorf("child Procedure = %q, want %q", tree.Children[0].Call.Procedure, "Child1")
	}

	formatted := FormatTree(tree)
	if formatted == "" {
		t.Error("FormatTree returned empty string")
	}
}

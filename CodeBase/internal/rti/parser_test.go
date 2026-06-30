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

func TestParseContent_BLogBlock(t *testing.T) {
	content := "04.03.2026 16:59:08.940\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t100\t\t0\t10\n" +
		"Enter MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:08.957\tINFO\tTrace.Server.BusinessLog\t\t\t349\t101\t\t0\t117\n" +
		"Enter @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"\n" +
		"RetVal = 0#Основной блок\n" +
		"04.03.2026 16:59:09.100\tINFO\tTrace.Server.BusinessLog\t\t\t349\t200\t\t0\t200\n" +
		"Exit @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"\n" +
		"RetVal = 0#Основной блок\n" +
		"04.03.2026 16:59:09.110\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t201\t\t0\t50\n" +
		"Exit MyProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 170\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if len(c.BLogBlocks) != 1 {
		t.Fatalf("expected 1 BLogBlock, got %d", len(c.BLogBlocks))
	}
	b := c.BLogBlocks[0]
	if b.BlockName != "Основной блок" {
		t.Errorf("BlockName = %q, want %q", b.BlockName, "Основной блок")
	}
	if b.EnterTime.IsZero() {
		t.Error("EnterTime should not be zero")
	}
	if b.ExitTime.IsZero() {
		t.Error("ExitTime should not be zero")
	}
	if b.ElapsedMs <= 0 {
		t.Errorf("ElapsedMs = %d, should be > 0", b.ElapsedMs)
	}
	// Return 0 в конце процедуры легитимно ставит RetVal=0, но это не должен быть BLog RetVal
	if c.RetValContext != "" {
		t.Errorf("RetValContext should be empty (not set from BLog RetVal), got %q", c.RetValContext)
	}
}

func TestParseContent_BLogBlock_NoInterferenceWithCallRetVal(t *testing.T) {
	content := "04.03.2026 16:59:08.940\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t100\t\t0\t10\n" +
		"Enter MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:08.957\tINFO\tTrace.Server.BusinessLog\t\t\t349\t101\t\t0\t117\n" +
		"Enter @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"\n" +
		"RetVal = 0#Мой блок\n" +
		"04.03.2026 16:59:09.000\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t200\t\t0\t50\n" +
		"Exit MyProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 60\n" +
		"Return 42\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if len(c.BLogBlocks) != 1 {
		t.Fatalf("expected 1 BLogBlock, got %d", len(c.BLogBlocks))
	}
	if c.BLogBlocks[0].BlockName != "Мой блок" {
		t.Errorf("BlockName = %q", c.BLogBlocks[0].BlockName)
	}
	if c.RetVal == nil || *c.RetVal != 42 {
		t.Errorf("RetVal = %v, want 42", c.RetVal)
	}
}

func TestParseContent_CheckpointTimestamp(t *testing.T) {
	content := "04.03.2026 16:59:03.237\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t1\t\t0\t10\n" +
		"Enter MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"04.03.2026 16:59:03.500\tINFO\tTrace.Server.Trace\t\tMyProc\t349\t2\t\t0\t100\n" +
		"MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 263\n" +
		"MyProc_Begin_1\n" +
		"04.03.2026 16:59:03.600\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t3\t\t0\t50\n" +
		"Exit MyProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 363\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
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
	cp := c.Checkpoints[0]
	if cp.Label != "MyProc_Begin_1" {
		t.Errorf("Label = %q", cp.Label)
	}
	if cp.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero after Trace.Server.Trace header")
	}
	if cp.ElapsedMs != 263 {
		t.Errorf("ElapsedMs = %d, want 263", cp.ElapsedMs)
	}
}

func TestParseContent_BLogTable(t *testing.T) {
	content := "04.03.2026 16:59:08.940\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t100\t\t0\t10\n" +
		"Enter MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"BusinessLog: Data from tMyTable begin\n" +
		"Table header ColA:int_|_ColB:varchar\n" +
		"_|_1_|_hello\n" +
		"_|_2_|_world\n" +
		"BusinessLog: Data from tMyTable end\n" +
		"04.03.2026 16:59:09.000\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t200\t\t0\t50\n" +
		"Exit MyProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 60\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if len(c.BLogTables) != 1 {
		t.Fatalf("expected 1 BLogTable, got %d", len(c.BLogTables))
	}
	tbl := c.BLogTables[0]
	if tbl.TableName != "tMyTable" {
		t.Errorf("TableName = %q, want %q", tbl.TableName, "tMyTable")
	}
	if len(tbl.Columns) != 2 {
		t.Errorf("Columns len = %d, want 2", len(tbl.Columns))
	} else {
		if tbl.Columns[0] != "ColA:int" {
			t.Errorf("Columns[0] = %q", tbl.Columns[0])
		}
		if tbl.Columns[1] != "ColB:varchar" {
			t.Errorf("Columns[1] = %q", tbl.Columns[1])
		}
	}
	if tbl.RowCount != 2 {
		t.Errorf("RowCount = %d, want 2", tbl.RowCount)
	}
	if len(tbl.Rows) != 2 {
		t.Errorf("Rows len = %d, want 2", len(tbl.Rows))
	}
}

func TestParseContent_BLogTable_MultipleTables(t *testing.T) {
	content := "04.03.2026 16:59:08.940\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t100\t\t0\t10\n" +
		"Enter MyProc @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 0\n" +
		"BusinessLog: Data from tAlpha begin\n" +
		"Table header ID:int\n" +
		"_|_1\n" +
		"BusinessLog: Data from tAlpha end\n" +
		"BusinessLog: Data from tBeta begin\n" +
		"Table header Name:varchar\n" +
		"_|_foo\n" +
		"_|_bar\n" +
		"BusinessLog: Data from tBeta end\n" +
		"04.03.2026 16:59:09.000\tINFO\tTrace.Server.Proc\t\tMyProc\t349\t200\t\t0\t50\n" +
		"Exit MyProc @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 10\n" +
		"Elapsed, ms: 60\n" +
		"Return 0\n"

	result, err := parseContent(content, "test.rti", 200)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]
	if len(c.BLogTables) != 2 {
		t.Fatalf("expected 2 BLogTables, got %d", len(c.BLogTables))
	}
	if c.BLogTables[0].TableName != "tAlpha" {
		t.Errorf("BLogTables[0].TableName = %q", c.BLogTables[0].TableName)
	}
	if c.BLogTables[0].RowCount != 1 {
		t.Errorf("tAlpha RowCount = %d, want 1", c.BLogTables[0].RowCount)
	}
	if c.BLogTables[1].TableName != "tBeta" {
		t.Errorf("BLogTables[1].TableName = %q", c.BLogTables[1].TableName)
	}
	if c.BLogTables[1].RowCount != 2 {
		t.Errorf("tBeta RowCount = %d, want 2", c.BLogTables[1].RowCount)
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

func TestParseContent_MLogWithoutEnterExit_DoesNotSetCallRetVal(t *testing.T) {
	// M_LOG record: Trace.Server.BusinessLog header followed by @@TranCount (no Enter/Exit prefix),
	// then RetVal = 0#Вернули флаги договора — this should NOT set RetValContext on the procedure.
	content := "10.06.2026 15:21:05.730\tINFO\tTrace.Server.Proc\t\tUndoConsSale_PurchPortfolio\t58\t692548\t\t0\t177\n" +
		"Enter UndoConsSale_PurchPortfolio @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 0\n" +
		"\n" +
		"@ConsSalePortfolioID : DSIDENTIFIER                   = 20000000165\n" +
		"\n" +
		"10.06.2026 15:21:05.730\tINFO\tTrace.Server.BusinessLog\t\t\t58\t692550\t\t0\t183\n" +
		"Enter @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"\n" +
		"RetVal = 0#UndoConsSale_PurchPortfolio\n" +
		"BLogParam:@ConsSalePortfolioID : DSIDENTIFIER                   = 20000000165\n" +
		"\n" +
		"10.06.2026 15:21:05.803\tINFO\tTrace.Server.BusinessLog\t\t\t58\t692575\t\t0\t164\n" +
		"@@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"\n" +
		"RetVal = 0#Вернули флаги договора\n" +
		"BLogParam:@ContractFlag : DSINT_KEY                      = 1073741824\n" +
		"\n" +
		"10.06.2026 15:21:05.820\tINFO\tTrace.Server.BusinessLog\t\t\t58\t692592\t\t0\t105\n" +
		"Exit @@TranCount = 0 @@NestLevel = 1 @@DsSysModuleID = 39\n" +
		"\n" +
		"RetVal = 28545#UndoConsSale_PurchPortfolio\n" +
		"10.06.2026 15:21:05.820\tINFO\tTrace.Server.Proc\t\tUndoConsSale_PurchPortfolio\t58\t692593\t\t0\t131\n" +
		"Exit UndoConsSale_PurchPortfolio @@TranCount = 0 @@NestLevel = 1@BeginCnt = 0 @@DsSysModuleID = 39\n" +
		"Elapsed, ms: 90\n" +
		"Return 28545\n"

	result, err := parseContent(content, "test.rti", 100)
	if err != nil {
		t.Fatalf("parseContent error: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(result.Calls))
	}
	c := result.Calls[0]

	// RetVal should come from Return 28545, not from M_LOG RetVal = 0
	if c.RetVal == nil || *c.RetVal != 28545 {
		t.Errorf("RetVal = %v, want 28545 (from Return, not from M_LOG)", c.RetVal)
	}

	// RetValContext should be empty — M_LOG "Вернули флаги договора" must not leak
	if c.RetValContext != "" {
		t.Errorf("RetValContext = %q, want empty (M_LOG must not set RetValContext)", c.RetValContext)
	}

	// Should have 2 BLogBlocks: "UndoConsSale_PurchPortfolio" (Enter) and "UndoConsSale_PurchPortfolio" (Exit)
	if len(c.BLogBlocks) != 1 {
		t.Errorf("expected 1 BLogBlock (Enter/Exit pair), got %d", len(c.BLogBlocks))
	} else {
		b := c.BLogBlocks[0]
		if b.BlockName != "UndoConsSale_PurchPortfolio" {
			t.Errorf("BLogBlock name = %q, want %q", b.BlockName, "UndoConsSale_PurchPortfolio")
		}
		if b.ExitTime.IsZero() {
			t.Error("BLogBlock ExitTime should not be zero")
		}
	}

	// Should have 1 BLogParam: ContractFlag from M_LOG section
	if len(c.Params) < 2 {
		t.Errorf("expected at least 2 params (1 input + 1 BLogParam), got %d", len(c.Params))
	}
}

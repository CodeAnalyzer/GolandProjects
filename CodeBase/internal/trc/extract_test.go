package trc

import "testing"

func TestExtractProcedureAndParams_SimpleExec(t *testing.T) {
	text := "exec @RetVal =   RKO_Audit_InsertHistory\r\n" +
		"                   @AuditID = @AuditID,\r\n" +
		"                   @ObjectID = @ObjectID,\r\n" +
		"                   @ObjectName = @ObjectName,\r\n" +
		"                   @Action = @Action,\r\n" +
		"                   @ProcessID = @GlobalProcessID"
	proc, params := ExtractProcedureAndParams(text)
	if proc != "RKO_Audit_InsertHistory" {
		t.Fatalf("procedure = %q, want RKO_Audit_InsertHistory", proc)
	}
	want := map[string]string{
		"AuditID":    "@AuditID",
		"ObjectID":   "@ObjectID",
		"ObjectName": "@ObjectName",
		"Action":     "@Action",
		"ProcessID":  "@GlobalProcessID",
	}
	if len(params) != len(want) {
		t.Fatalf("params count = %d, want %d (%+v)", len(params), len(want), params)
	}
	for _, p := range params {
		if want[p.Name] != p.Value {
			t.Errorf("param %s = %q, want %q", p.Name, p.Value, want[p.Name])
		}
	}
}

func TestExtractProcedureAndParams_WithoutRetVal(t *testing.T) {
	text := "exec Audit_Insert @AuditID = @AuditID output, @ObjectID = @RObjectID, @TranAction = 1"
	proc, params := ExtractProcedureAndParams(text)
	if proc != "Audit_Insert" {
		t.Fatalf("procedure = %q, want Audit_Insert", proc)
	}
	if len(params) != 3 {
		t.Fatalf("params count = %d, want 3 (%+v)", len(params), params)
	}
	if params[0].Name != "AuditID" || params[0].Value != "@AuditID output" {
		t.Errorf("param0 = %+v", params[0])
	}
	if params[2].Name != "TranAction" || params[2].Value != "1" {
		t.Errorf("param2 = %+v", params[2])
	}
}

func TestExtractProcedureAndParams_NonExecStatement(t *testing.T) {
	proc, params := ExtractProcedureAndParams("select @cnt = 1 from pDispatchStack")
	if proc != "" || params != nil {
		t.Fatalf("expected no match, got proc=%q params=%+v", proc, params)
	}
}

func TestExtractProcedureAndParams_NoParams(t *testing.T) {
	proc, params := ExtractProcedureAndParams("while 1 = 1")
	if proc != "" || params != nil {
		t.Fatalf("expected no match, got proc=%q params=%+v", proc, params)
	}
}

func TestExtractProcedureAndParams_DiasoftComments(t *testing.T) {
	text := "exec @RetVal =    \r\n" +
		"/*0*/\r\n" +
		"Cons_MassConsMultipleRepSum\r\n" +
		"/*0*/\r\n" +
		"/*1*/\r\n" +
		"  @AccrualDate = '20260116' /* :AccrualDate */,\r\n" +
		"  @AccrualID = 10000000863 /* :AccrualID */,\r\n" +
		"  @ParentProtocolID = 10085976175 /* :ParentProtocolID */,\r\n" +
		"  @StateType = 0 /* :StateType */,\r\n" +
		"  @SchedPayDate = 1 /* :SchedPayDate */,\r\n" +
		"  @OverduePayment = 0 /* :OverduePayment */\r\n" +
		"/*1*/\r\n" +
		"select @RetVal\r\n"
	proc, params := ExtractProcedureAndParams(text)
	if proc != "Cons_MassConsMultipleRepSum" {
		t.Fatalf("procedure = %q, want Cons_MassConsMultipleRepSum", proc)
	}
	if len(params) != 6 {
		t.Fatalf("params count = %d, want 6 (%+v)", len(params), params)
	}
	if params[0].Name != "AccrualDate" {
		t.Errorf("param0 name = %q, want AccrualDate", params[0].Name)
	}
	if params[0].Value != "'20260116' /* :AccrualDate */" {
		t.Errorf("param0 value = %q, want '20260116' /* :AccrualDate */", params[0].Value)
	}
}

func TestExtractProcedureAndParams_MultipleCommentsBeforeProc(t *testing.T) {
	text := "exec /*1*/ /*2*/ MyProc @p1 = 1"
	proc, params := ExtractProcedureAndParams(text)
	if proc != "MyProc" {
		t.Fatalf("procedure = %q, want MyProc", proc)
	}
	if len(params) != 1 || params[0].Name != "p1" || params[0].Value != "1" {
		t.Fatalf("params = %+v, want [{p1 1}]", params)
	}
}

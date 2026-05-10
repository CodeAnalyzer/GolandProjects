package js

import "testing"

func TestParseContent_ExtractsFunctionsObjectsCallsAndConstants(t *testing.T) {
	content := `var GlobalObj = Sys.CreateObject("ConsCredit");
var MAX_COUNT = 10;

function CreateInstrument(id, name) {
    GlobalObj.ExecProc("DoWork", id);
    GlobalObj.ExecQuery("select * from tAccount where ID = :ID");
}

function Utility() {
    var LocalObj = Sys.CreateObject("LocalType");
    LocalObj.ExecProc("LocalProc");
}
`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Functions) != 2 {
		t.Fatalf("functions count = %d, want 2", len(result.Functions))
	}
	if result.Functions[0].FunctionName != "CreateInstrument" {
		t.Fatalf("first function = %q, want CreateInstrument", result.Functions[0].FunctionName)
	}
	if result.Functions[0].Signature != "function CreateInstrument(id, name)" {
		t.Fatalf("signature = %q", result.Functions[0].Signature)
	}
	if result.Functions[0].ScenarioType != "instrument_model" {
		t.Fatalf("scenario type = %q, want instrument_model", result.Functions[0].ScenarioType)
	}
	if result.Functions[0].ParentObject != "ConsCredit" {
		t.Fatalf("parent object = %q, want ConsCredit", result.Functions[0].ParentObject)
	}

	if len(result.ScriptObjects) != 2 {
		t.Fatalf("script objects count = %d, want 2", len(result.ScriptObjects))
	}
	if result.ScriptObjects[0].Name != "GlobalObj" || result.ScriptObjects[0].Type != "ConsCredit" {
		t.Fatalf("unexpected first object: %+v", result.ScriptObjects[0])
	}

	if len(result.ProcedureCalls) != 2 {
		t.Fatalf("procedure calls count = %d, want 2", len(result.ProcedureCalls))
	}
	if result.ProcedureCalls[0].ObjectName != "GlobalObj" || result.ProcedureCalls[0].ProcName != "DoWork" {
		t.Fatalf("unexpected procedure call: %+v", result.ProcedureCalls[0])
	}

	if len(result.QueryCalls) != 1 {
		t.Fatalf("query calls count = %d, want 1", len(result.QueryCalls))
	}
	if result.QueryCalls[0].ObjectName != "GlobalObj" || result.QueryCalls[0].QueryText != "select * from tAccount where ID = :ID" {
		t.Fatalf("unexpected query call: %+v", result.QueryCalls[0])
	}

	if len(result.Constants) != 1 {
		t.Fatalf("constants count = %d, want 1", len(result.Constants))
	}
	if result.Constants[0].Name != "MAX_COUNT" || result.Constants[0].Value != "10" {
		t.Fatalf("unexpected constant: %+v", result.Constants[0])
	}
}

func TestParseContent_IgnoresLineAndBlockComments(t *testing.T) {
	content := `// function IgnoredLine() {}
/*
function IgnoredBlock() {
    Obj.ExecProc("IgnoredProc");
}
*/
function Real() {
}
`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.Functions) != 1 {
		t.Fatalf("functions count = %d, want 1", len(result.Functions))
	}
	if result.Functions[0].FunctionName != "Real" {
		t.Fatalf("function = %q, want Real", result.Functions[0].FunctionName)
	}
	if len(result.ProcedureCalls) != 0 {
		t.Fatalf("procedure calls count = %d, want 0", len(result.ProcedureCalls))
	}
}

func TestBuildSignature(t *testing.T) {
	if got := buildSignature("Run", ""); got != "function Run()" {
		t.Fatalf("empty signature = %q", got)
	}
	if got := buildSignature("Run", "a, b"); got != "function Run(a, b)" {
		t.Fatalf("signature = %q", got)
	}
}

func TestDetectScenarioType(t *testing.T) {
	if got := detectScenarioType("Any", "function StepForward() {} MassAccrualStarter();"); got != "mass_operation" {
		t.Fatalf("scenario = %q, want mass_operation", got)
	}
	if got := detectScenarioType("Any", "function CreateInstrument() {}"); got != "instrument_model" {
		t.Fatalf("scenario = %q, want instrument_model", got)
	}
	if got := detectScenarioType("BeforeQuery", ""); got != "mass_operation" {
		t.Fatalf("scenario = %q, want mass_operation", got)
	}
	if got := detectScenarioType("Unknown", ""); got != "utility" {
		t.Fatalf("scenario = %q, want utility", got)
	}
}

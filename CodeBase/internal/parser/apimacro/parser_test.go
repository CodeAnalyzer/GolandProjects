package apimacro

import (
	"testing"
)

func TestParseContent_APICreateProcAndInitEvent(t *testing.T) {
	content := `create procedure MainProc
as
begin
  API_CREATE_PROC(MyContract)
  API_INIT_EVENT(MyEvent)
end`

	parser := NewParser()
	result, err := parser.ParseContent("C:\\scripts\\MainProc.sql", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.ProcedureName != "MainProc" {
		t.Fatalf("procedure name = %q, want MainProc", result.ProcedureName)
	}
	if len(result.Invocations) != 2 {
		t.Fatalf("invocations count = %d, want 2: %+v", len(result.Invocations), result.Invocations)
	}
	if result.Invocations[0].MacroType != "create_proc" || result.Invocations[0].TargetName != "MyContract" || result.Invocations[0].TargetKind != "contract" {
		t.Fatalf("unexpected create_proc invocation: %+v", result.Invocations[0])
	}
	if result.Invocations[1].MacroType != "init_event" || result.Invocations[1].TargetName != "MyEvent" {
		t.Fatalf("unexpected init_event invocation: %+v", result.Invocations[1])
	}
}

func TestParseContent_APIExecAndDispatchesTo(t *testing.T) {
	content := `__BEGIN_PROCEDURE__(DispatchProc)
  API_EXEC(OtherContract)
  exec AnotherProc @ProcessID = @GlobalProcessID
  exec GetAPIProcessID @ProcessID = @GlobalProcessID
END_PROCEDURE`

	parser := NewParser()
	result, err := parser.ParseContent("/scripts/DispatchProc.t01", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.ProcedureName != "DispatchProc" {
		t.Fatalf("procedure name = %q, want DispatchProc", result.ProcedureName)
	}
	if len(result.Invocations) != 2 {
		t.Fatalf("invocations count = %d, want 2: %+v", len(result.Invocations), result.Invocations)
	}
	if result.Invocations[0].MacroType != "exec_contract" || result.Invocations[0].TargetName != "OtherContract" {
		t.Fatalf("unexpected exec_contract invocation: %+v", result.Invocations[0])
	}
	if result.Invocations[1].MacroType != "dispatches_to" || result.Invocations[1].TargetName != "AnotherProc" || result.Invocations[1].TargetKind != "procedure" {
		t.Fatalf("unexpected dispatches_to invocation: %+v", result.Invocations[1])
	}
}

func TestParseContent_DetectProcedureNameFallback(t *testing.T) {
	content := `API_CREATE_PROC(ContractA)
API_EXEC(ContractB)`

	parser := NewParser()
	result, err := parser.ParseContent("C:\\scripts\\FallbackProc.sql", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.ProcedureName != "FallbackProc" {
		t.Fatalf("fallback procedure name = %q, want FallbackProc", result.ProcedureName)
	}
	if len(result.Invocations) != 2 {
		t.Fatalf("invocations count = %d, want 2", len(result.Invocations))
	}
}

func TestParseContent_NonT01IgnoresDispatchesTo(t *testing.T) {
	content := `create proc MainProc
as
begin
  exec SomeProc @ProcessID = @GlobalProcessID
end`

	parser := NewParser()
	result, err := parser.ParseContent("/scripts/MainProc.sql", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.Invocations) != 0 {
		t.Fatalf("non-.t01 should ignore dispatches_to: %+v", result.Invocations)
	}
}

func TestDetectProcedureName(t *testing.T) {
	parser := NewParser()
	lines := []string{"create procedure DetectProc", "as begin", "end"}
	if got := detectProcedureName(parser.procNameRe, lines, "fallback.txt"); got != "DetectProc" {
		t.Fatalf("detectProcedureName = %q, want DetectProc", got)
	}
	lines = []string{"no proc here"}
	if got := detectProcedureName(parser.procNameRe, lines, "FileWithoutExtension"); got != "FileWithoutExtension" {
		t.Fatalf("fallback detectProcedureName = %q", got)
	}
	if got := detectProcedureName(parser.procNameRe, []string{}, ""); got != "." {
		t.Fatalf("empty fallback detectProcedureName = %q, want '.'", got)
	}
}

func TestMustParseContent(t *testing.T) {
	parser := NewParser()
	content := "API_CREATE_PROC(ContractC)"
	result := parser.MustParseContent("/scripts/MustProc.sql", content)
	if result.ProcedureName != "MustProc" || len(result.Invocations) != 1 {
		t.Fatalf("MustParseContent failed: %+v", result)
	}
}

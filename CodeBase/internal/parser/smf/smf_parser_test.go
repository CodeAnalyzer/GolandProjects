package smf

import "testing"

func TestParseContent_ParsesXMLJobInstrumentAndScript(t *testing.T) {
	content := `<job>
<description>Test job</description>
<include>
  <inc-file>Common.inc</inc-file>
</include>
<prequery>select * from tClient</prequery>
<script><![CDATA[
const DEAL_OBJECT_ID = 101;
var MODULE_ID = 202;

function CreateInstrument() {
    Instrument.Name = "Credit";
    Instrument.Brief = "Credit Brief";
    Instrument.InterfaceObjectID = DEAL_OBJECT_ID;
    Instrument.DsModuleID = MODULE_ID;
    Instrument.StartState = "START";
    State = Instrument.CreateStateWithSysName("Start", "START");
    State.StateType = PROP_STATETYPE_BEGIN;
    State.CreateConsumerTransitionWithSysName("Approve", "APPROVE", "from", "to");
    Tran.PropVal = CONSUMER_ACTION_APPROVE;
    Tran.Priority = 10;
    TypeAccLinkCreate(RESDEP_MAIN, __ACCMASKN__);
    Obj.ExecProc("DoWork");
}
]]></script>
</job>`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if result.Description != "Test job" {
		t.Fatalf("description = %q, want Test job", result.Description)
	}
	if result.PrequerySQL != "select * from tClient" {
		t.Fatalf("prequery = %q", result.PrequerySQL)
	}
	if len(result.Includes) != 1 || result.Includes[0] != "Common.inc" {
		t.Fatalf("includes = %v, want [Common.inc]", result.Includes)
	}
	if result.Instrument == nil {
		t.Fatalf("expected instrument")
	}
	instr := result.Instrument
	if instr.InstrumentName != "Credit" || instr.Brief != "Credit Brief" || instr.DealObjectID != 101 || instr.DsModuleID != 202 || instr.StartState != "START" {
		t.Fatalf("unexpected instrument: %+v", instr)
	}
	if instr.ScenarioType != "instrument_model" {
		t.Fatalf("scenario type = %q, want instrument_model", instr.ScenarioType)
	}
	if len(instr.States) != 1 || instr.States[0]["name"] != "Start" || instr.States[0]["sys_name"] != "START" || instr.States[0]["state_type"] != "PROP_STATETYPE_BEGIN" {
		t.Fatalf("unexpected states: %+v", instr.States)
	}
	action := findActionByName(instr.Actions, "Approve")
	if action == nil || action["sys_name"] != "APPROVE" || action["prop_val"] != "CONSUMER_ACTION_APPROVE" || action["priority"] != "10" {
		t.Fatalf("unexpected actions: %+v", instr.Actions)
	}
	if len(instr.Accounts) != 1 || instr.Accounts[0]["type"] != "RESDEP_MAIN" || instr.Accounts[0]["mask"] != "N" {
		t.Fatalf("unexpected accounts: %+v", instr.Accounts)
	}
	if result.JSResult == nil || len(result.JSResult.Functions) != 1 || len(result.JSResult.ProcedureCalls) != 1 {
		t.Fatalf("unexpected JS result: %+v", result.JSResult)
	}
}

func TestParseContent_FallbackNonXML(t *testing.T) {
	content := `<description>Fallback job</description>
<include><inc-file>dir/Common.inc</inc-file><inc-file>dir/Common.inc</inc-file></include>
<prequery>
select 1
</prequery>
function CreateInstrument() {
    Instrument.Name = "Fallback";
    Instrument.Brief = "Brief";
    Instrument.DealObjectID = 303;
    Instrument.ModuleID = 404;
}
`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.Description != "Fallback job" {
		t.Fatalf("description = %q", result.Description)
	}
	if result.PrequerySQL != "select 1" {
		t.Fatalf("prequery = %q", result.PrequerySQL)
	}
	if len(result.Includes) != 1 || result.Includes[0] != "Common.inc" {
		t.Fatalf("includes = %v, want [Common.inc]", result.Includes)
	}
	if result.Instrument == nil || result.Instrument.InstrumentName != "Fallback" || result.Instrument.DealObjectID != 303 || result.Instrument.DsModuleID != 404 {
		t.Fatalf("unexpected instrument: %+v", result.Instrument)
	}
}

func TestExtractSMFVarsAndResolveValue(t *testing.T) {
	parser := NewParser()
	vars := parser.extractSMFVars(`
const CONST_VALUE = 10;
var VAR_VALUE = 20;
PLAIN_VALUE = 30;
const ALIAS_VALUE = CONST_VALUE;
`)

	if vars["CONST_VALUE"] != 10 || vars["VAR_VALUE"] != 20 || vars["PLAIN_VALUE"] != 30 {
		t.Fatalf("unexpected vars: %+v", vars)
	}
	if got := parser.resolveSMFValue("42", vars); got != 42 {
		t.Fatalf("resolve numeric = %d, want 42", got)
	}
	if got := parser.resolveSMFValue("VAR_VALUE", vars); got != 20 {
		t.Fatalf("resolve var = %d, want 20", got)
	}
	if got := parser.resolveSMFValue("UNKNOWN", vars); got != 0 {
		t.Fatalf("resolve unknown = %d, want 0", got)
	}
}

func TestParseJSScriptEmptyReturnsInitializedResult(t *testing.T) {
	parser := NewParser()
	result := parser.parseJSScript("")
	if result == nil {
		t.Fatalf("expected result")
	}
	if result.Functions == nil || result.ScriptObjects == nil || result.ProcedureCalls == nil || result.QueryCalls == nil || result.Constants == nil || result.Errors == nil {
		t.Fatalf("expected initialized slices")
	}
}

func findActionByName(actions []map[string]interface{}, name string) map[string]interface{} {
	for _, action := range actions {
		if action["name"] == name {
			return action
		}
	}
	return nil
}

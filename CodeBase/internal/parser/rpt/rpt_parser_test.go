package rpt

import (
	"strings"
	"testing"
)

func TestParseContent_FormParamsFragmentsAndFunctions(t *testing.T) {
	content := `object ReportMain: TReportForm
  object DateFrom: DsDateTimePicker
    DataType = 'DateTime'
    Required = True
    Mask = '01.01.2026'
  end
  object LookupClient: DsFormLookup
    LookupForm = 'ClientForm'
    FieldName = 'ClientID'
    PortName.DataType = 'Integer'
  end
  object QueryBox: TDsHugeBox
    Lines.Strings = (
      'select AccountID'
      '  from tAccount'
      ' where ClientID = :ClientID')
  end
  object TextBox: TTextBox
    SQL.Strings = (
      'update tAccount set Name = ''John''')
  end
  Script.Strings = (
    'Sub BeforePrint'
    '  Call QueryBox.Open'
    'End Sub'
    'Function CalcValue'
    '  CalcValue = 1'
    'End Function')
end`

	parser := NewParser()
	result, err := parser.ParseContent(content, "C:/Reports/AccountReport.rpt")
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if result.Form == nil {
		t.Fatalf("expected report form")
	}
	if result.Form.ReportName != "AccountReport" || result.Form.ReportType != "rpt" {
		t.Fatalf("unexpected report metadata: %+v", result.Form)
	}
	if result.Form.FormName != "ReportMain" || result.Form.FormClass != "TReportForm" {
		t.Fatalf("unexpected form metadata: %+v", result.Form)
	}
	if result.Form.LineStart != 1 || result.Form.LineEnd == 0 {
		t.Fatalf("unexpected form lines: %+v", result.Form)
	}

	paramsByName := map[string]struct {
		component string
		dataType  string
		required  bool
	}{}
	for _, param := range result.Params {
		paramsByName[param.ParamName] = struct {
			component string
			dataType  string
			required  bool
		}{component: param.ComponentType, dataType: param.DataType, required: param.Required}
	}
	if paramsByName["DateFrom"].component != "DsDateTimePicker" || paramsByName["DateFrom"].dataType != "DateTime" || !paramsByName["DateFrom"].required {
		t.Fatalf("unexpected DateFrom param: %+v", paramsByName["DateFrom"])
	}

	var lookupParamFound bool
	for _, param := range result.Params {
		if param.ParamName == "LookupClient" {
			lookupParamFound = param.LookupForm == "ClientForm" && param.LookupColumn == "ClientID" && param.DataType == "Integer"
		}
	}
	if !lookupParamFound {
		t.Fatalf("LookupClient param metadata not parsed: %+v", result.Params)
	}

	if len(result.Fragments) != 2 {
		t.Fatalf("fragments count = %d, want 2: %+v", len(result.Fragments), result.Fragments)
	}
	fragmentsByName := map[string]string{}
	contextsByName := map[string]string{}
	for _, fragment := range result.Fragments {
		fragmentsByName[fragment.ComponentName] = fragment.QueryText
		contextsByName[fragment.ComponentName] = fragment.Context
	}
	if contextsByName["QueryBox"] != "rpt_named_sql" || !strings.Contains(strings.ToLower(fragmentsByName["QueryBox"]), "from taccount") {
		t.Fatalf("unexpected QueryBox fragment: context=%q text=%q", contextsByName["QueryBox"], fragmentsByName["QueryBox"])
	}
	if contextsByName["TextBox"] != "rpt_textbox_sql" || !strings.Contains(strings.ToLower(fragmentsByName["TextBox"]), "update taccount") {
		t.Fatalf("unexpected TextBox fragment: context=%q text=%q", contextsByName["TextBox"], fragmentsByName["TextBox"])
	}

	functions := map[string]string{}
	for _, fn := range result.Functions {
		functions[fn.FunctionName] = fn.FunctionType
		if fn.Signature == "" || fn.BodyText == "" || fn.LineStart == 0 || fn.LineEnd == 0 {
			t.Fatalf("incomplete function metadata: %+v", fn)
		}
	}
	if functions["BeforePrint"] != "sub" || functions["CalcValue"] != "function" {
		t.Fatalf("unexpected functions: %#v", functions)
	}
}

func TestParseContent_FlushesUnclosedFragmentAndScriptAtEOF(t *testing.T) {
	content := `object ReportMain: TReportForm
  object QueryBox: TDsHugeBox
    Lines.Strings = (
      'delete from tAccount'
  Script.Strings = (
    'Sub OnOpen'
    'End Sub'`

	parser := NewParser()
	result, err := parser.ParseContent(content, `/repo/reports/UnclosedReport.rpt`)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if result.Form.ReportName != "UnclosedReport" {
		t.Fatalf("report name = %q", result.Form.ReportName)
	}
	if len(result.Fragments) == 0 {
		t.Fatalf("expected EOF fragment flush")
	}
}

func TestRPTHelpers(t *testing.T) {
	if !isRPTParamType("DsDateTimePicker") || !isRPTParamType("dsformlookup") || !isRPTParamType("TComboBox") {
		t.Fatalf("expected RPT param types")
	}
	if isRPTParamType("TLabel") || isRPTParamType("") {
		t.Fatalf("unexpected RPT param type")
	}
	if got := trimQuoted(" 'John''s value' "); got != "John's value" {
		t.Fatalf("trimQuoted = %q", got)
	}
	if got := reportNameFromPath("C:/Reports/Nested/ReportA.rpt"); got != "ReportA" {
		t.Fatalf("windows reportNameFromPath = %q", got)
	}
	if got := reportNameFromPath("/repo/reports/ReportB"); got != "ReportB" {
		t.Fatalf("unix reportNameFromPath without extension = %q", got)
	}
}

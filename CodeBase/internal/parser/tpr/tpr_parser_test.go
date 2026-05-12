package tpr

import (
	"strings"
	"testing"
)

func TestParseContent_SQLBlocksFieldsAndIncludes(t *testing.T) {
	content := `[Fields]
@AccountID@ = Field{AccountID, 10,0}
@Name@ = Field{Name, 50,0, Left}
@MainQuery@ = SQL {
  select AccountID, Name
    from tAccount
   where ClientID = :ClientID
  #include <common.sql>
}
`

	parser := NewParser()
	result, err := parser.ParseContent(content, "C:/reports/Account.tpr")
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if result.Form == nil || result.Form.ReportName != "Account" || result.Form.ReportType != "tpr" {
		t.Fatalf("unexpected form metadata: %+v", result.Form)
	}
	if result.Form.LineStart != 1 {
		t.Fatalf("unexpected form LineStart: %d", result.Form.LineStart)
	}

	if len(result.Fields) != 2 {
		t.Fatalf("fields count = %d, want 2: %+v", len(result.Fields), result.Fields)
	}
	fieldsByName := map[string]string{}
	for _, field := range result.Fields {
		fieldsByName[field.FieldName] = field.SourceName
	}
	if fieldsByName["AccountID"] != "AccountID" || fieldsByName["Name"] != "Name" {
		t.Fatalf("unexpected fields: %+v", result.Fields)
	}

	if len(result.Fragments) != 1 {
		t.Fatalf("fragments count = %d, want 1: %+v", len(result.Fragments), result.Fragments)
	}
	fragment := result.Fragments[0]
	if fragment.ComponentName != "MainQuery" || fragment.ComponentType != "TPR_SQL_BLOCK" || fragment.Context != "tpr_sql_block" {
		t.Fatalf("unexpected fragment: %+v", fragment)
	}
	if !strings.Contains(strings.ToLower(fragment.QueryText), "from taccount") {
		t.Fatalf("fragment query text missing tAccount: %q", fragment.QueryText)
	}

	if len(result.Includes) != 1 || result.Includes[0].IncludePath != "common.sql" {
		t.Fatalf("unexpected includes: %+v", result.Includes)
	}
}

func TestParseContent_UnclosedSQLBlockFlushedAtEOF(t *testing.T) {
	content := `@Unclosed@ = SQL {
  delete from tAccount`

	parser := NewParser()
	result, err := parser.ParseContent(content, "/reports/Unclosed.tpr")
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.Fragments) != 1 || result.Fragments[0].ComponentName != "Unclosed" {
		t.Fatalf("expected EOF flush for unclosed SQL block: %+v", result.Fragments)
	}
}

func TestTPRHelpers(t *testing.T) {
	if got := splitCSVLike("A, B, , C"); len(got) != 3 || got[0] != "A" || got[1] != "B" || got[2] != "C" {
		t.Fatalf("splitCSVLike = %#v", got)
	}
	if got := normalizeReportToken("@Name@"); got != "Name" {
		t.Fatalf("normalizeReportToken = %q", got)
	}
	if got := reportNameFromPath("C:/reports/ReportA.tpr"); got != "ReportA" {
		t.Fatalf("reportNameFromPath = %q", got)
	}
	if got := reportNameFromPath("/repo/ReportB"); got != "ReportB" {
		t.Fatalf("reportNameFromPath without extension = %q", got)
	}

	parser := NewParser()
	if !parser.isValidEncoding("select ID from tAccount") {
		t.Fatalf("expected valid encoding for plain ASCII")
	}
	if parser.isValidEncoding("select ID from tAccount╚╩╘╟╤╥") {
		t.Fatalf("expected invalid encoding with garbled characters")
	}
}

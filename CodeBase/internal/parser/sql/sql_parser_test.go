package sql

import "testing"

func TestParseContent_SQLDefines(t *testing.T) {
	parser := NewParser()
	content := `#include <macros.h>
#define M_MAIN_RISK_GROUPID_BRIEF 'ГрРискаОсн'
#define SQL_CONST 1
#define SQL_CONST_COMMENT 2 -- comment
#define SQL_MACRO(a,b) select a, b
#define SQL_EMPTY
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	if len(result.Defines) != 5 {
		t.Fatalf("unexpected define count: got=%d want=5", len(result.Defines))
	}

	tests := map[string]struct {
		value      string
		defineType string
		lineNumber int
	}{
		"M_MAIN_RISK_GROUPID_BRIEF": {value: "'ГрРискаОсн'", defineType: "macro", lineNumber: 2},
		"SQL_CONST":                 {value: "1", defineType: "const", lineNumber: 3},
		"SQL_CONST_COMMENT":         {value: "2", defineType: "const", lineNumber: 4},
		"SQL_MACRO":                 {value: "select a, b", defineType: "macro", lineNumber: 5},
		"SQL_EMPTY":                 {value: "", defineType: "const", lineNumber: 6},
	}

	for _, define := range result.Defines {
		if define == nil {
			t.Fatal("unexpected nil define")
		}
		expected, ok := tests[define.DefineName]
		if !ok {
			t.Fatalf("unexpected define: %s", define.DefineName)
		}
		if define.DefineValue != expected.value {
			t.Fatalf("%s value: got=%q want=%q", define.DefineName, define.DefineValue, expected.value)
		}
		if define.DefineType != expected.defineType {
			t.Fatalf("%s type: got=%q want=%q", define.DefineName, define.DefineType, expected.defineType)
		}
		if define.LineNumber != expected.lineNumber {
			t.Fatalf("%s line: got=%d want=%d", define.DefineName, define.LineNumber, expected.lineNumber)
		}
		delete(tests, define.DefineName)
	}
	if len(tests) > 0 {
		t.Fatalf("missing defines: %v", tests)
	}
}

func TestParseContent_SelectIntoStandaloneIntoLine(t *testing.T) {
	parser := NewParser()
	content := `select i.InstitutionID,
       i.Brief,
       case
         when exists(select 1
                       from tUser u
                      where u.InstUserID = i.InstitutionID
                    ) then 3
         else 4
       end as PropDealPart,
       i.Name,
       isnull(trim(i.Name1), "") as Name1,
       isnull(trim(i.Name2), "") as Name2,
       i.MainMember as Resident,
       i.INN,
       i.BranchID,
       i.PORTAL,
       i.ExternalID,
       ia.InDateTime
  into tConsInstitutionSync
  from tInstAttr ia
 inner join tInstitution i
    on i.InstitutionID = ia.InstitutionID
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	expected := []string{
		"InstitutionID",
		"Brief",
		"PropDealPart",
		"Name",
		"Name1",
		"Name2",
		"Resident",
		"INN",
		"BranchID",
		"PORTAL",
		"ExternalID",
		"InDateTime",
	}

	actualByName := make(map[string]struct{}, len(expected))
	for _, item := range result.ColumnDefinitions {
		if item == nil {
			continue
		}
		if item.DefinitionKind != "select_into" {
			continue
		}
		if item.TableName != "tConsInstitutionSync" {
			continue
		}
		actualByName[item.ColumnName] = struct{}{}
	}

	if len(actualByName) != len(expected) {
		t.Fatalf("unexpected select_into column count: got=%d want=%d", len(actualByName), len(expected))
	}

	for _, name := range expected {
		if _, ok := actualByName[name]; !ok {
			t.Fatalf("missing column from select_into: %s", name)
		}
	}
}

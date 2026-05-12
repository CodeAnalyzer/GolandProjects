package sql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
)

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

func TestParseContent_ProcedureParamsCallsAndTables(t *testing.T) {
	parser := NewParser()
	content := `create procedure TestProc
	@AccountID int,
	@Name varchar(100) output
as
begin
	select a.AccountID, a.Name
	  from tAccount a
	  join tClient c on c.ClientID = a.ClientID
	 where a.AccountID = @AccountID
	insert into tLog(AccountID) values(@AccountID)
	update tAccount set Name = @Name where AccountID = @AccountID
	delete from tTemp where AccountID = @AccountID
end
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedure count = %d, want 1", len(result.Procedures))
	}
	proc := result.Procedures[0]
	if proc.ProcName != "TestProc" {
		t.Fatalf("procedure name = %q, want TestProc", proc.ProcName)
	}
	if proc.BodyHash == "" {
		t.Fatalf("expected procedure body hash")
	}
	contextsByTable := map[string]map[string]bool{}
	for _, table := range result.Tables {
		if table == nil {
			continue
		}
		if contextsByTable[table.TableName] == nil {
			contextsByTable[table.TableName] = map[string]bool{}
		}
		contextsByTable[table.TableName][table.Context] = true
	}
	for tableName, context := range map[string]string{"tAccount": "select", "tClient": "select", "tLog": "insert", "tTemp": "delete"} {
		if !contextsByTable[tableName][context] {
			t.Fatalf("missing %s context for table %s: %+v", context, tableName, contextsByTable)
		}
	}
}

func TestParseContent_TopLevelExecCall(t *testing.T) {
	parser := NewParser()
	result, err := parser.ParseContent("exec OtherProc @AccountID\n")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Calls) != 1 {
		t.Fatalf("calls count = %d, want 1", len(result.Calls))
	}
	if result.Calls[0].CallerName != "" || result.Calls[0].CalleeName != "OtherProc" || result.Calls[0].LineNumber != 1 {
		t.Fatalf("unexpected call: %+v", result.Calls[0])
	}
}

func TestParseContent_CreateTableAndIndexes(t *testing.T) {
	parser := NewParser()
	content := `create table tAccount (
	AccountID int not null,
	ClientID int null,
	Name varchar(100) -- comment
)
create unique index XAK_tAccount_Client on tAccount ([ClientID] ASC, Name DESC)
create index XIE_tAccount_Name
on tAccount
(
	Name,
	ClientID
)
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	columns := map[string]string{}
	for _, column := range result.ColumnDefinitions {
		if column != nil && column.TableName == "tAccount" && column.DefinitionKind == "create_table" {
			columns[column.ColumnName] = column.DataType
		}
	}
	for name, dataType := range map[string]string{"AccountID": "int", "ClientID": "int", "Name": "varchar(100)"} {
		if columns[name] != dataType {
			t.Fatalf("column %s type = %q, want %q; all columns: %+v", name, columns[name], dataType, columns)
		}
	}

	indexes := map[string]string{}
	unique := map[string]bool{}
	for _, index := range result.IndexDefinitions {
		if index == nil {
			continue
		}
		indexes[index.IndexName] = index.IndexFields
		unique[index.IndexName] = index.IsUnique
	}
	if indexes["XAK_tAccount_Client"] != "ClientID] ASC,Name DESC" || !unique["XAK_tAccount_Client"] {
		t.Fatalf("unexpected inline unique index: fields=%q unique=%v", indexes["XAK_tAccount_Client"], unique["XAK_tAccount_Client"])
	}
	if indexes["XIE_tAccount_Name"] != "Name,ClientID" || unique["XIE_tAccount_Name"] {
		t.Fatalf("unexpected multiline index: fields=%q unique=%v", indexes["XIE_tAccount_Name"], unique["XIE_tAccount_Name"])
	}
}

func TestParseContent_ParsesRealTableWithStringAndCommentNoise(t *testing.T) {
	parser := NewParser()
	content := `select 'from tString' as TextValue
-- select * from tInlineComment
/* select * from tBlockComment */
select a.ID from tReal a where a.Name = 'join tIgnored'
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	found := map[string]bool{}
	for _, table := range result.Tables {
		if table != nil {
			found[table.TableName] = true
		}
	}
	if !found["tReal"] {
		t.Fatalf("expected tReal table, got %+v", found)
	}
}

func TestSQLParserHelpers(t *testing.T) {
	parser := NewParser()
	if got := topLevelParenDepth("select (1 + (2)) + '('"); got != 0 {
		t.Fatalf("topLevelParenDepth balanced = %d, want 0", got)
	}
	if got := topLevelParenDepth("select (1 + (2)"); got != 1 {
		t.Fatalf("topLevelParenDepth unbalanced = %d, want 1", got)
	}

	parts := splitSQLByTopLevelComma("a, isnull(b, 'x,y') as B, c")
	wantParts := []string{"a", "isnull(b, 'x,y') as B", "c"}
	if !reflect.DeepEqual(parts, wantParts) {
		t.Fatalf("splitSQLByTopLevelComma = %#v, want %#v", parts, wantParts)
	}
	for expr, want := range map[string]string{"u.UserID": "UserID", "isnull(u.Name, '') nm": "nm", "case when 1=1 then 1 end as Flag": "Flag", "select": ""} {
		if got := inferSelectColumnName(expr); got != want {
			t.Fatalf("inferSelectColumnName(%q) = %q, want %q", expr, got, want)
		}
	}

	lines := []string{"", "create proc Test", "as", "select 1"}
	hash := computeBodyHash(lines, 2, 4)
	if hash == "" || len(hash) != 64 {
		t.Fatalf("computeBodyHash returned invalid hash %q", hash)
	}
	if got := computeBodyHash(lines, 0, 4); got != "" {
		t.Fatalf("computeBodyHash invalid range = %q, want empty", got)
	}

	line := "select 't.String''Value' as Name -- from tComment"
	if !parser.isInsideSingleQuotedString(line, strings.Index(line, "String")) {
		t.Fatalf("expected position inside single quoted string")
	}
	if parser.isInsideSingleQuotedString(line, strings.Index(line, "Name")) {
		t.Fatalf("expected position outside single quoted string")
	}
	if !parser.isInsideInlineComment(line, strings.Index(line, "tComment")) {
		t.Fatalf("expected position inside inline comment")
	}
	if parser.isInsideInlineComment(line, strings.Index(line, "Name")) {
		t.Fatalf("expected position outside inline comment")
	}

	blockContent := "select 1\n/* from tBlock */\nselect * from tReal"
	if !parser.isInsideBlockComment(blockContent, 2, strings.Index("/* from tBlock */", "tBlock")) {
		t.Fatalf("expected position inside block comment")
	}
	if parser.isInsideBlockComment(blockContent, 3, strings.Index("select * from tReal", "tReal")) {
		t.Fatalf("expected position outside block comment")
	}
	if got := normalizeIndexFields("[AccountID] ASC, `Name` DESC, \"ClientID\""); got != "AccountID] ASC,`Name` DESC,\"ClientID" {
		t.Fatalf("normalizeIndexFields = %q", got)
	}
	if got := splitIndexFields(" A, , [B] "); !reflect.DeepEqual(got, []string{"A", "[B]"}) {
		t.Fatalf("splitIndexFields = %#v", got)
	}
	if name, dataType, ok := parser.parseColumnDefinition("[AccountID] int not null,"); !ok || name != "AccountID" || dataType != "int" {
		t.Fatalf("parseColumnDefinition = %q, %q, %v", name, dataType, ok)
	}
	if parser.hasProcedureParam(&model.SQLProcedure{Params: []model.SQLParam{{Name: "@ID"}}}, "@missing") {
		t.Fatalf("unexpected missing procedure param match")
	}
	if !parser.hasProcedureParam(&model.SQLProcedure{Params: []model.SQLParam{{Name: "@ID"}}}, "@id") {
		t.Fatalf("expected case-insensitive procedure param match")
	}
}

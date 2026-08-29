package sql

import (
	"reflect"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
)

// Дополнительные юнит-тесты для чистых функций SQL parser

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

	if len(proc.Params) != 2 {
		t.Fatalf("procedure params count = %d, want 2; got %+v", len(proc.Params), proc.Params)
	}
	if proc.Params[0].Name != "AccountID" || proc.Params[0].Type != "int" {
		t.Fatalf("unexpected param[0]: %+v", proc.Params[0])
	}
	if proc.Params[1].Name != "Name" || proc.Params[1].Type != "varchar(100)" || proc.Params[1].Direction != "out" {
		t.Fatalf("unexpected param[1]: %+v", proc.Params[1])
	}
}

func TestParseContent_DCLProcBegin_MixedParamTypes(t *testing.T) {
	parser := NewParser()
	content := `DCL_PROC_BEGIN(TestProcMixed)
	@RetCode DSIDENTIFIER = null,
	@Message varchar(250) = null output,
	@Amount numeric(18,2) = 0
as
/* body */
__BEGIN_PROCEDURE__(TestProcMixed)
__END_PROCEDURE__(TestProcMixed)
go
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Procedures) != 1 {
		t.Fatalf("procedure count = %d, want 1", len(result.Procedures))
	}
	proc := result.Procedures[0]
	if proc.ProcName != "TestProcMixed" {
		t.Fatalf("procedure name = %q, want TestProcMixed", proc.ProcName)
	}
	if len(proc.Params) != 3 {
		t.Fatalf("procedure params count = %d, want 3; got %+v", len(proc.Params), proc.Params)
	}

	tests := []struct {
		name         string
		paramType    string
		direction    string
		defaultValue string
	}{
		{"RetCode", "DSIDENTIFIER", "in", "null"},
		{"Message", "varchar(250)", "out", "null"},
		{"Amount", "numeric(18,2)", "in", "0"},
	}
	for i, tt := range tests {
		p := proc.Params[i]
		if p.Name != tt.name {
			t.Fatalf("param[%d].Name = %q, want %q", i, p.Name, tt.name)
		}
		if p.Type != tt.paramType {
			t.Fatalf("param[%d].Type = %q, want %q", i, p.Type, tt.paramType)
		}
		if p.Direction != tt.direction {
			t.Fatalf("param[%d].Direction = %q, want %q", i, p.Direction, tt.direction)
		}
		if p.DefaultValue != tt.defaultValue {
			t.Fatalf("param[%d].DefaultValue = %q, want %q", i, p.DefaultValue, tt.defaultValue)
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

func TestParseContent_ExecInsideStringLiteral_Ignored(t *testing.T) {
	parser := NewParser()
	content := "PROFILE_TIME_EX('exec ConsAccrualDetail_MassInsert')\n"
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Calls) != 0 {
		t.Fatalf("calls count = %d, want 0; calls = %+v", len(result.Calls), result.Calls)
	}
}

func TestParseContent_ExecInsideIdentifier_Ignored(t *testing.T) {
	parser := NewParser()
	content := "insert into pCON_DocFile_MassExecute M_WITH_ROWLOCK (SPID)\n"
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Calls) != 0 {
		t.Fatalf("calls count = %d, want 0; calls = %+v", len(result.Calls), result.Calls)
	}
}

func TestParseContent_ExecInsideBlockComment_Ignored(t *testing.T) {
	parser := NewParser()
	content := "/*\n  exec FCD_Cons_FindListProtocolByID\n*/\n"
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(result.Calls) != 0 {
		t.Fatalf("calls count = %d, want 0; calls = %+v", len(result.Calls), result.Calls)
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

func TestTopLevelParenDepth(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"select (1 + (2)) + '('", 0},
		{"select (1 + (2)", 1},
		{"select ((a + b) * c)", 0},
		{"select (a + (b + c", 2},
		{"select 'string with (parens)'", 0},
		{"select ''', not (balanced", 0},
		{"", 0},
		{"select 1", 0},
	}
	for _, tt := range tests {
		if got := topLevelParenDepth(tt.input); got != tt.want {
			t.Fatalf("topLevelParenDepth(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestSplitSQLByTopLevelComma(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"a, isnull(b, 'x,y') as B, c", []string{"a", "isnull(b, 'x,y') as B", "c"}},
		{"col1, col2, col3", []string{"col1", "col2", "col3"}},
		{"func(a, b, c)", []string{"func(a, b, c)"}},
		{"'', ''", []string{"''", "''"}},
		{"single", []string{"single"}},
		{"", []string{}},
		{"a, '', c", []string{"a", "''", "c"}},
	}
	for _, tt := range tests {
		got := splitSQLByTopLevelComma(tt.input)
		if !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("splitSQLByTopLevelComma(%q) = %#v, want %#v", tt.input, got, tt.want)
		}
	}
}

func TestInferSelectColumnName(t *testing.T) {
	tests := map[string]string{
		"u.UserID":                         "UserID",
		"isnull(u.Name, '') nm":            "nm",
		"case when 1=1 then 1 end as Flag": "Flag",
		"select":                           "",
		"column_name":                      "column_name",
		"table.column as alias":            "alias",
		"func(a, b) -- comment":            "b",
		"[Bracketed]":                      "Bracketed",
		"'literal'":                        "'literal'",
		"count(*)":                         "count(*",
	}
	for expr, want := range tests {
		if got := inferSelectColumnName(expr); got != want {
			t.Fatalf("inferSelectColumnName(%q) = %q, want %q", expr, got, want)
		}
	}
}

func TestStartsWithAnyCI(t *testing.T) {
	tests := []struct {
		value    string
		prefixes []string
		want     bool
	}{
		{"SELECT * FROM t", []string{"select", "insert", "update"}, true},
		{"  insert into t", []string{"select", "insert"}, true},
		{"UPDATE t SET", []string{"select", "delete"}, false},
		{"from t", []string{"FROM", "JOIN"}, true},
		{"", []string{"select"}, false},
		{"select", nil, false},
	}
	for _, tt := range tests {
		if got := startsWithAnyCI(tt.value, tt.prefixes...); got != tt.want {
			t.Fatalf("startsWithAnyCI(%q, %v) = %v, want %v", tt.value, tt.prefixes, got, tt.want)
		}
	}
}

func TestIsSQLKeyword(t *testing.T) {
	keywords := []string{"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "AS", "INTO", "CASE", "WHEN", "END"}
	nonKeywords := []string{"users", "table_name", "column", "mySelect", "fromage"}

	for _, kw := range keywords {
		if !isSQLKeyword(kw) {
			t.Fatalf("isSQLKeyword(%q) = false, want true", kw)
		}
		if !isSQLKeyword(strings.ToLower(kw)) {
			t.Fatalf("isSQLKeyword(%q) = false, want true (case-insensitive)", strings.ToLower(kw))
		}
	}
	for _, word := range nonKeywords {
		if isSQLKeyword(word) {
			t.Fatalf("isSQLKeyword(%q) = true, want false", word)
		}
	}
}

func TestExtractSelectIntoColumnNames(t *testing.T) {
	tests := []struct {
		projection string
		want       []string
	}{
		{"a, b, c", []string{"a", "b", "c"}},
		{"u.UserID, u.Name as UserName, isnull(u.Email, '') Email", []string{"UserID", "UserName", "Email"}},
		{"case when 1=1 then 1 end as Flag, 2 as Num", []string{"Flag", "Num"}},
		{"", []string{}},
		{"select", []string{}},
	}
	for _, tt := range tests {
		got := extractSelectIntoColumnNames(tt.projection)
		if !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("extractSelectIntoColumnNames(%q) = %v, want %v", tt.projection, got, tt.want)
		}
	}
}

func TestFindTopLevelIntoClause(t *testing.T) {
	tests := []struct {
		text           string
		wantProjection string
		wantTable      string
		wantFound      bool
	}{
		{"a, b into #Temp from t", "a, b", "#Temp", true},
		{"* into tTarget", "*", "tTarget", true},
		{"a, b from t", "", "", false},
		{"into #OnlyTable", "", "", false},
		{"", "", "", false},
	}
	for _, tt := range tests {
		gotProj, gotTable, gotFound := findTopLevelIntoClause(tt.text)
		if gotFound != tt.wantFound {
			t.Fatalf("findTopLevelIntoClause(%q) found = %v, want %v", tt.text, gotFound, tt.wantFound)
		}
		if gotFound {
			if gotProj != tt.wantProjection {
				t.Fatalf("findTopLevelIntoClause(%q) projection = %q, want %q", tt.text, gotProj, tt.wantProjection)
			}
			if gotTable != tt.wantTable {
				t.Fatalf("findTopLevelIntoClause(%q) table = %q, want %q", tt.text, gotTable, tt.wantTable)
			}
		}
	}
}

func TestFindStandaloneIntoTableName(t *testing.T) {
	tests := []struct {
		text      string
		wantTable string
		wantFound bool
	}{
		{"into #TempTable", "#TempTable", true},
		{"into tTarget  ", "tTarget", true},
		{"INTO pAPI_MyTable", "pAPI_MyTable", true},
		{"not into", "", false},
		{"into", "", false},
		{"into123", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		gotTable, gotFound := findStandaloneIntoTableName(tt.text)
		if gotFound != tt.wantFound {
			t.Fatalf("findStandaloneIntoTableName(%q) found = %v, want %v", tt.text, gotFound, tt.wantFound)
		}
		if gotFound && gotTable != tt.wantTable {
			t.Fatalf("findStandaloneIntoTableName(%q) = %q, want %q", tt.text, gotTable, tt.wantTable)
		}
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

func TestParseContent_SelectInsideIfBeginEnd_IsSeparateFragment(t *testing.T) {
	parser := NewParser()
	content := `DCL_PROC_BEGIN(TestProc)
as
  select @Var1 = t.ID
    from tTable t
   where t.SysName = 'TEST1'

  if @ActionType = 1
  begin
      select @Var2 = t.ID
        from tTable t
       where t.SysName = 'TEST2'
      M_ISOLAT
  end
__END_PROCEDURE__(TestProc)
go
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	selectAssignCount := 0
	for _, frag := range result.Fragments {
		if frag == nil {
			continue
		}
		lower := strings.ToLower(strings.TrimSpace(frag.QueryText))
		if strings.HasPrefix(lower, "select @") {
			selectAssignCount++
		}
	}
	if selectAssignCount != 2 {
		t.Fatalf("expected 2 select-assign fragments, got %d (fragments: %+v)", selectAssignCount, result.Fragments)
	}
}

func TestParseContent_HFileLikeContent_DCLProcBegin(t *testing.T) {
	parser := NewParser()
	content := `#define DCL_PROC_BEGIN( NAME ) \
	create procedure NAME as \
	__BEGIN_PROCEDURE__(NAME) \
	__END_PROCEDURE__(NAME)

#define ARC_PROC_BEGIN(proc_name) \
	DCL_PROC_BEGIN(proc_name)

DCL_PROC_BEGIN(Ins_Check_ExistsLinkObject)

as
  __BEGIN_PROCEDURE__(Ins_Check_ExistsLinkObject)
  select 1
__END_PROCEDURE__(Ins_Check_ExistsLinkObject)

go
X_ANYMODE(Ins_Check_ExistsLinkObject)
`

	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	// SQL-парсер увидит все DCL_PROC_BEGIN-совпадения, включая макросы.
	// Индексатор отфильтрует ложные на основе строки #define.
	var realProc *model.SQLProcedure
	for _, proc := range result.Procedures {
		if proc.ProcName == "Ins_Check_ExistsLinkObject" {
			realProc = proc
		}
	}
	if realProc == nil {
		t.Fatalf("expected real procedure Ins_Check_ExistsLinkObject, got procedures: %+v", result.Procedures)
	}
	if realProc.LineStart != 9 {
		t.Fatalf("real procedure line start = %d, want 9", realProc.LineStart)
	}
	if realProc.LineEnd == 0 {
		t.Fatalf("real procedure line end should be set, got 0")
	}
}

func TestBlockCommentCreateTable(t *testing.T) {
	parser := NewParser()
	content := `/*  create table tFoo
    (
       FooID DSIDENTIFIER,
       Brief DSBRIEFNAME
    )  */
select tal.FooID as FooID,
       tal.Brief as Brief
  into tFoo
  from tAnother tal
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	for _, col := range result.ColumnDefinitions {
		if col == nil || col.TableName != "tFoo" {
			continue
		}
		if col.DefinitionKind == "create_table" {
			t.Fatalf("unexpected create_table column %q (dataType=%q) — block comment should be skipped", col.ColumnName, col.DataType)
		}
		if col.DefinitionKind != "select_into" {
			t.Fatalf("unexpected definition_kind %q for column %q, want select_into", col.DefinitionKind, col.ColumnName)
		}
	}

	hasFooID := false
	hasBrief := false
	for _, col := range result.ColumnDefinitions {
		if col == nil || col.TableName != "tFoo" || col.DefinitionKind != "select_into" {
			continue
		}
		if col.ColumnName == "FooID" {
			hasFooID = true
		}
		if col.ColumnName == "Brief" {
			hasBrief = true
		}
	}
	if !hasFooID {
		t.Fatal("missing select_into column FooID")
	}
	if !hasBrief {
		t.Fatal("missing select_into column Brief")
	}
}

func TestCaseElseKeyword(t *testing.T) {
	parser := NewParser()
	if !parser.isKeyword("else") {
		t.Fatal("isKeyword(\"else\") = false, want true")
	}
	if !parser.isKeyword("ELSE") {
		t.Fatal("isKeyword(\"ELSE\") = false, want true")
	}

	content := `select case rn.ResourceType
         when 0 then 1
         when 1 then 2
         else ""
       end as NodeNumber,
       tal.FundID as CurrencyID
  into tTestElse
  from tAnother tal
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	for _, col := range result.ColumnDefinitions {
		if col == nil {
			continue
		}
		if strings.EqualFold(col.ColumnName, "else") {
			t.Fatalf("found column named 'else' — should be filtered as keyword: %+v", col)
		}
	}
}

func TestSplitSQLByTopLevelComma_DashComment(t *testing.T) {
	input := "tal.NodeID, --- Идентификатор Узла или Идентификатор счёта, общего для ФО"
	got := splitSQLByTopLevelComma(input)
	want := []string{"tal.NodeID"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitSQLByTopLevelComma(%q) = %#v, want %#v", input, got, want)
	}

	input2 := "a, b, --- comment with, comma"
	got2 := splitSQLByTopLevelComma(input2)
	want2 := []string{"a", "b"}
	if !reflect.DeepEqual(got2, want2) {
		t.Fatalf("splitSQLByTopLevelComma(%q) = %#v, want %#v", input2, got2, want2)
	}

	input3 := "a, b -- trailing comment, with comma"
	got3 := splitSQLByTopLevelComma(input3)
	want3 := []string{"a", "b"}
	if !reflect.DeepEqual(got3, want3) {
		t.Fatalf("splitSQLByTopLevelComma(%q) = %#v, want %#v", input3, got3, want3)
	}
}

func TestSelectIntoPatch7_2_1056(t *testing.T) {
	parser := NewParser()
	content := `/*  create table tConsRuleAccSync
    (
       RuleID DSIDENTIFIER,
       Brief DSACCNUMBER,
       TypeRule DSBRIEFNAME,
       Name DSNAME,
       NodeNumber DSIDENTIFIER,
       CurrencyID DSIDENTIFIER,
       FindExist DSIDENTIFIER,
       MaskAccountPicture DSMASK
    )  */
select tal.TypeAccLinkID as RuleID,    --- Идентификатор правила
       tal.Brief as Brief,             --- Бrief
       lat.Brief as TypeRule,
       tal.Name as Name,
       case rn.ResourceType
         when 0 then rn.NodeID
         when 1 then isnull(convert(varchar(40),trim(rn.ParentMask)+
         replicate("-",sign(len(trim(rn.ParentMask))))+
         trim(rn.Brief)), "")
         else ""
       end as NodeNumber,
       tal.FundID as CurrencyID,
       (tal.Flags&TAL_FINDEXISTENT)/TAL_FINDEXISTENT as FindExist,
       tal.MaskAccountPicture as MaskAccountPicture
  into tConsRuleAccSync
  from tAnother tal
`
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	for _, col := range result.ColumnDefinitions {
		if col == nil || col.TableName != "tConsRuleAccSync" {
			continue
		}
		if col.DefinitionKind == "create_table" {
			t.Fatalf("unexpected create_table column %q (dataType=%q) — block comment should be skipped", col.ColumnName, col.DataType)
		}
		if strings.EqualFold(col.ColumnName, "else") {
			t.Fatalf("found column named 'else' — should be filtered as keyword")
		}
		if strings.EqualFold(col.ColumnName, "lat.Brief") {
			t.Fatalf("found column 'lat.Brief' — should be 'TypeRule' (alias)")
		}
		if strings.Contains(strings.ToLower(col.ColumnName), "trim(rn.brief)") {
			t.Fatalf("found expression fragment as column name: %q", col.ColumnName)
		}
	}

	hasTypeRule := false
	for _, col := range result.ColumnDefinitions {
		if col == nil || col.TableName != "tConsRuleAccSync" || col.DefinitionKind != "select_into" {
			continue
		}
		if col.ColumnName == "TypeRule" {
			hasTypeRule = true
		}
	}
	if !hasTypeRule {
		t.Fatal("missing select_into column TypeRule (should be extracted from 'lat.Brief as TypeRule')")
	}
}

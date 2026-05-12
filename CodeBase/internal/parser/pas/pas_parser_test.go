package pas

import (
	"reflect"
	"strings"
	"testing"

	"github.com/codebase/internal/model"
)

func TestParseContent_ParsesMultipleClassesWithInlineEndComments(t *testing.T) {
	content := `unit AdmCmd;

interface

type
  TAdmCmd = class
  public
    procedure Execute;
  end; { TAdmCmd }

  TChangePasswordAdmCmd = class(TAdmCmd)
  public
    procedure GetParamValues;
  end; { TChangePasswordAdmCmd }

implementation

procedure TAdmCmd.Execute;
begin
end;

procedure TChangePasswordAdmCmd.GetParamValues;
begin
end;

end.`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Classes) != 2 {
		t.Fatalf("expected 2 classes, got %d", len(result.Classes))
	}

	classes := map[string]bool{}
	for _, class := range result.Classes {
		classes[class.ClassName] = true
	}

	if !classes["TAdmCmd"] {
		t.Fatalf("TAdmCmd not parsed")
	}
	if !classes["TChangePasswordAdmCmd"] {
		t.Fatalf("TChangePasswordAdmCmd not parsed")
	}

	foundMethod := false
	for _, method := range result.Methods {
		if method.ClassName == "TChangePasswordAdmCmd" && method.MethodName == "GetParamValues" {
			foundMethod = true
			break
		}
	}
	if !foundMethod {
		t.Fatalf("GetParamValues for TChangePasswordAdmCmd not parsed")
	}
}

func TestParseContent_ResolvesConstSQLAssignedToSQLText(t *testing.T) {
	content := `unit AdmCmd;

interface

type
  TChangePasswordAdmCmd = class
  public
    procedure GetParamValues;
  end;

const
  QR_DSA_RAIGHTS =
      '#M_FORCEPLAN '
    + 'select ID '
    + '  from tConfigParam #M_NOLOCK_INDEX(XAK0tConfigParam) '
    + ' where SysName = :SysName '
    + '#M_ISOLAT';

implementation

procedure TChangePasswordAdmCmd.GetParamValues;
var
  Qr: TObject;
begin
  Qr.SQL.Text := QR_DSA_RAIGHTS;
end;

end.`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.SQLFragments) == 0 {
		t.Fatalf("expected at least 1 SQL fragment")
	}

	found := false
	for _, fragment := range result.SQLFragments {
		if fragment.ClassName == "TChangePasswordAdmCmd" && fragment.MethodName == "GetParamValues" && strings.Contains(strings.ToLower(fragment.QueryText), "from tconfigparam") {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected resolved SQL fragment for const assignment to Qr.SQL.Text")
	}

	foundTable := false
	for _, table := range result.Tables {
		if strings.EqualFold(table.TableName, "tConfigParam") {
			foundTable = true
			break
		}
	}
	if !foundTable {
		t.Fatalf("expected table tConfigParam to be extracted from SQL fragment")
	}
}

func TestPASParserHelpers(t *testing.T) {
	if got := normalizeMethodName("TAccountForm.Execute"); got != "Execute" {
		t.Fatalf("normalizeMethodName = %q, want Execute", got)
	}
	if got := normalizeTypeDeclarationLine("TValue = classrecord {$M+}"); got != "TValue = class " {
		t.Fatalf("normalizeTypeDeclarationLine = %q", got)
	}

	classes := []*model.PASClass{
		{ClassName: "TBase"},
		{ClassName: "TBaseChild"},
	}
	className, methodName, ok := resolveQualifiedOwnerFallback("TBaseChildTBase.Execute", classes)
	if !ok || className != "TBaseChild" || methodName != "Execute" {
		t.Fatalf("resolveQualifiedOwnerFallback = %q, %q, %v", className, methodName, ok)
	}
	if _, _, ok := resolveQualifiedOwnerFallback("Execute", classes); ok {
		t.Fatalf("resolveQualifiedOwnerFallback must reject unqualified name")
	}

	method := &model.PASMethod{MethodName: "Run", LineNumber: 42}
	if currentMethodName(method) != "Run" || currentMethodLine(method) != 42 {
		t.Fatalf("unexpected current method helpers result")
	}
	if currentMethodName(nil) != "" || currentMethodLine(nil) != 0 {
		t.Fatalf("nil current method helpers must return zero values")
	}

	modules := parseUsesList("SysUtils, Classes,\n  UnitA;")
	if !reflect.DeepEqual(modules, []string{"SysUtils", "Classes", "UnitA"}) {
		t.Fatalf("parseUsesList = %#v", modules)
	}

	if !isLikelySQL("select ID from tAccount") || !isLikelySQL("exec DoWork") {
		t.Fatalf("expected SQL-like strings")
	}
	if isLikelySQL("plain pascal string") {
		t.Fatalf("unexpected SQL detection for plain string")
	}
	if !isKeyword("begin") || !isIgnoredTableName("tmp") {
		t.Fatalf("expected keyword/ignored table detection")
	}
}

func TestParseContent_UsesFieldsPropertiesAndQualifiedMethods(t *testing.T) {
	content := `unit AccountUnit;

interface

uses
  SysUtils,
  Classes;

type
  TAccountForm = class(TBaseForm)
  private
    FQuery: DsQuery;
    FName: string;
  public
    property AccountSQL: string read FName;
    procedure LoadAccount;
    function GetName: string;
  end;

implementation

uses
  DataModule,
  QueryUtils;

procedure TAccountForm.LoadAccount;
begin
  FQuery.SQL.Text := 'select AccountID from tAccount where AccountID = :AccountID';
  FQuery.Open;
end;

function TAccountForm.GetName: string;
begin
  Result := FName;
end;

end.`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Units) != 1 {
		t.Fatalf("unit count = %d, want 1", len(result.Units))
	}
	unit := result.Units[0]
	if unit.UnitName != "AccountUnit" {
		t.Fatalf("unit name = %q", unit.UnitName)
	}
	if !reflect.DeepEqual(unit.InterfaceUses, []string{"SysUtils", "Classes"}) {
		t.Fatalf("interface uses = %#v", unit.InterfaceUses)
	}
	if !reflect.DeepEqual(unit.ImplementationUses, []string{"DataModule", "QueryUtils"}) {
		t.Fatalf("implementation uses = %#v", unit.ImplementationUses)
	}

	var class *model.PASClass
	for _, item := range result.Classes {
		if item.ClassName == "TAccountForm" {
			class = item
			break
		}
	}
	if class == nil || class.ParentClass != "TBaseForm" {
		t.Fatalf("class not parsed with parent: %+v", class)
	}

	fields := map[string]string{}
	for _, field := range result.Fields {
		fields[field.FieldName] = field.FieldType
	}
	if fields["FQuery"] != "DsQuery" || fields["FName"] != "string" {
		t.Fatalf("fields = %#v", fields)
	}
	if len(result.DFMQueries) == 0 || result.DFMQueries[0].ComponentName != "FQuery" {
		t.Fatalf("expected DFM query reference for DsQuery field, got %+v", result.DFMQueries)
	}

	methods := map[string]string{}
	for _, method := range result.Methods {
		if method.ClassName == "TAccountForm" {
			methods[method.MethodName] = method.Signature
		}
	}
	if !strings.Contains(methods["LoadAccount"], "procedure") {
		t.Fatalf("LoadAccount method not parsed: %#v", methods)
	}
	if !strings.Contains(methods["GetName"], "function") {
		t.Fatalf("GetName method not parsed: %#v", methods)
	}

	foundSQL := false
	for _, fragment := range result.SQLFragments {
		if fragment.ClassName == "TAccountForm" && fragment.MethodName == "LoadAccount" && strings.Contains(strings.ToLower(fragment.QueryText), "from taccount") {
			foundSQL = true
		}
	}
	if !foundSQL {
		t.Fatalf("expected SQL fragment inside LoadAccount")
	}
	foundTable := false
	for _, table := range result.Tables {
		if strings.EqualFold(table.TableName, "tAccount") {
			foundTable = true
		}
	}
	if !foundTable {
		t.Fatalf("expected tAccount table from embedded SQL")
	}
}

func TestExtractSQLFromLine(t *testing.T) {
	parser := NewParser()
	result := &ParseResult{}
	parser.extractSQLFromLine("property SQLText: string read 'select ID from tAccount';", 7, result)
	if len(result.SQLFragments) != 1 {
		t.Fatalf("SQL fragments count = %d, want 1", len(result.SQLFragments))
	}
	if result.SQLFragments[0].Context != "property" || result.SQLFragments[0].LineNumber != 7 {
		t.Fatalf("unexpected SQL fragment: %+v", result.SQLFragments[0])
	}
	if len(result.Tables) != 1 || result.Tables[0].TableName != "tAccount" {
		t.Fatalf("unexpected extracted tables: %+v", result.Tables)
	}
}

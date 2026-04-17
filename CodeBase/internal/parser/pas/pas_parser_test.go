package pas

import (
	"strings"
	"testing"
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

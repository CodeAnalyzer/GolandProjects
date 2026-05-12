package dfm

import (
	"strings"
	"testing"
)

func TestParseContent_FormComponentsAndQueries(t *testing.T) {
	content := `object MainForm: TMainForm
  Name = 'RenamedForm'
  Caption = 'Main '#1060#1086#1088#1084#1072
  object pnlMain: TPanel
    Caption = 'Panel'
    object qryAccount: TDsQuery
      SQL.Strings = (
        'select a.AccountID, a.Name'
        '  from tAccount a'
        '  join tClient c on c.ClientID = a.ClientID')
    end
  end
  object tbInline: TDsTextBox
    Text = 'update tAccount set Name = ''A'' where AccountID = :ID'
  end
end`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Forms) != 1 {
		t.Fatalf("forms count = %d, want 1", len(result.Forms))
	}
	form := result.Forms[0]
	if form.FormName != "RenamedForm" || form.FormClass != "TMainForm" {
		t.Fatalf("unexpected form: %+v", form)
	}
	if !strings.Contains(form.Caption, "Форма") {
		t.Fatalf("caption was not decoded: %q", form.Caption)
	}
	if form.LineStart != 1 || form.LineEnd == 0 {
		t.Fatalf("unexpected form lines: %+v", form)
	}

	components := map[string]string{}
	parents := map[string]string{}
	for _, component := range result.Components {
		components[component.ComponentName] = component.ComponentType
		parents[component.ComponentName] = component.ParentName
	}
	if components["pnlMain"] != "TPanel" || components["qryAccount"] != "TDsQuery" || components["tbInline"] != "TDsTextBox" {
		t.Fatalf("unexpected components: %#v", components)
	}
	if parents["qryAccount"] != "pnlMain" || parents["tbInline"] != "RenamedForm" {
		t.Fatalf("unexpected component parents: %#v", parents)
	}

	if len(result.Queries) != 2 {
		t.Fatalf("queries count = %d, want 2: %+v", len(result.Queries), result.Queries)
	}
	queriesByComponent := map[string]string{}
	for _, query := range result.Queries {
		queriesByComponent[query.ComponentName] = query.QueryText
	}
	if !strings.Contains(strings.ToLower(queriesByComponent["qryAccount"]), "from taccount") {
		t.Fatalf("expected SQL.Strings query for qryAccount, got %q", queriesByComponent["qryAccount"])
	}
	if !strings.Contains(strings.ToLower(queriesByComponent["tbInline"]), "update taccount") {
		t.Fatalf("expected inline SQL query for tbInline, got %q", queriesByComponent["tbInline"])
	}

	foundTables := map[string]bool{}
	for _, table := range result.Tables {
		foundTables[table.TableName] = true
		if table.Context != "dfm_embedded" {
			t.Fatalf("unexpected table context: %+v", table)
		}
	}
	for _, tableName := range []string{"tAccount", "tClient"} {
		if !foundTables[tableName] {
			t.Fatalf("missing table %s in %+v", tableName, foundTables)
		}
	}
}

func TestParseContent_InheritedFormOldLinesAndCollectionItemName(t *testing.T) {
	content := `inherited ChildForm: TChildForm
  object hbText: TDsHugeBox
    StrArray = <
      item
        Name = 'NamedSQL'
        Lines = (
          'delete from tOldTable where ID = :ID')
      end
    >
  end
end`

	parser := NewParser()
	result, err := parser.ParseContent(content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.Forms) != 1 || result.Forms[0].FormName != "ChildForm" || result.Forms[0].FormClass != "TChildForm" {
		t.Fatalf("unexpected inherited form: %+v", result.Forms)
	}
	if len(result.Queries) != 1 {
		t.Fatalf("queries count = %d, want 1", len(result.Queries))
	}
	query := result.Queries[0]
	if query.ComponentName != "NamedSQL" || query.ComponentType != "StrArrayItem" {
		t.Fatalf("unexpected collection query metadata: %+v", query)
	}
	if len(result.Tables) != 1 || result.Tables[0].TableName != "tOldTable" {
		t.Fatalf("unexpected tables: %+v", result.Tables)
	}
}

func TestDFMParserHelpers(t *testing.T) {
	if !isLikelySQLText("select ID from tAccount") || !isLikelySQLText("exec DoWork") || !isLikelySQLText("delete from tAccount") {
		t.Fatalf("expected SQL-like text")
	}
	if isLikelySQLText("plain caption") || isLikelySQLText("   ") {
		t.Fatalf("unexpected SQL-like detection")
	}
	if !isKeyword("select") || !isKeyword("JOIN") {
		t.Fatalf("expected keyword detection")
	}
	if !isIgnoredTableName("tmp") || !isIgnoredTableName("A") {
		t.Fatalf("expected ignored table detection")
	}
	if isKeyword("tAccount") || isIgnoredTableName("tAccount") {
		t.Fatalf("unexpected keyword/ignored table detection for table name")
	}

	parser := NewParser()
	result := &ParseResult{}
	parser.extractTablesFromSQL("select * from tAccount join tClient on tClient.ID = tAccount.ClientID insert into #Tmp(ID) values(1)", 12, result)
	found := map[string]bool{}
	for _, table := range result.Tables {
		found[table.TableName] = table.LineNumber == 12 && table.Context == "dfm_embedded"
	}
	if !found["tAccount"] || !found["tClient"] || !found["#Tmp"] {
		t.Fatalf("unexpected extracted tables: %+v", result.Tables)
	}
}

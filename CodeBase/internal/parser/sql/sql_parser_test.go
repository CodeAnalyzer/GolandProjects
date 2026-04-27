package sql

import "testing"

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

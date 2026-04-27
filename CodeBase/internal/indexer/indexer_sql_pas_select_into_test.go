package indexer

import "testing"

func TestParseSelectIntoFragmentInfo_BasicAliasesAndSegments(t *testing.T) {
	query := `select i.InstitutionID,
       i.Name,
       isnull(trim(i.Name1), "") as Name1,
       case when exists(select 1 from tUser u where u.InstUserID = i.InstitutionID) then 3 else 4 end as PropDealPart
  into tConsInstitutionSync
  from tInstAttr ia
  inner join tInstitution i on i.InstitutionID = ia.InstitutionID`

	info, ok := parseSelectIntoFragmentInfo(query, "tConsInstitutionSync")
	if !ok {
		t.Fatalf("expected parseSelectIntoFragmentInfo to parse fragment")
	}
	if len(info.ProjectionSegments) != 4 {
		t.Fatalf("unexpected projection segments count: got=%d want=%d", len(info.ProjectionSegments), 4)
	}
	if info.AliasToTable["i"] != "tInstitution" {
		t.Fatalf("unexpected alias mapping for i: %q", info.AliasToTable["i"])
	}
	if info.AliasToTable["ia"] != "tInstAttr" {
		t.Fatalf("unexpected alias mapping for ia: %q", info.AliasToTable["ia"])
	}
}

func TestExtractSimpleSourceColumn_SupportedAndUnsupported(t *testing.T) {
	qualifier, column, ok := extractSimpleSourceColumn("i.InstitutionID")
	if !ok || qualifier != "i" || column != "InstitutionID" {
		t.Fatalf("direct reference parse failed: ok=%v qualifier=%q column=%q", ok, qualifier, column)
	}

	qualifier, column, ok = extractSimpleSourceColumn("isnull(trim(i.Name1), \"\") as Name1")
	if !ok || qualifier != "i" || column != "Name1" {
		t.Fatalf("isnull reference parse failed: ok=%v qualifier=%q column=%q", ok, qualifier, column)
	}

	_, _, ok = extractSimpleSourceColumn("case when i.Flag = 1 then i.Name else i.Name1 end as Result")
	if ok {
		t.Fatalf("case expression must stay unresolved in first iteration")
	}
}

func TestInferSelectIntoOutputName_QualifiedColumn(t *testing.T) {
	name := inferSelectIntoOutputName("i.Name      ,")
	if name != "Name" {
		t.Fatalf("unexpected output name: got=%q want=%q", name, "Name")
	}
}

package lst

import (
	"reflect"
	"testing"

	"github.com/codebase/internal/model"
)

func TestParse_CanonicalFile(t *testing.T) {
	content := "[FA-Administrator]\r\n" +
		"pAPI_AttrValue;API_Administrator\r\n" +
		"pAPI_Property_Value;API_Administrator\r\n" +
		"\r\n" +
		"[FA-GAAP]\r\n" +
		"pRuleDoc_QtyStack;GAAP\r\n"

	res := Parse(content)
	want := []model.MemoryTableRecord{
		{ProductName: "fa-administrator", ModuleName: "API_Administrator", TableName: "pAPI_AttrValue", LineNumber: 2},
		{ProductName: "fa-administrator", ModuleName: "API_Administrator", TableName: "pAPI_Property_Value", LineNumber: 3},
		{ProductName: "fa-gaap", ModuleName: "GAAP", TableName: "pRuleDoc_QtyStack", LineNumber: 6},
	}
	if !reflect.DeepEqual(res.Records, want) {
		t.Fatalf("records = %+v, want %+v", res.Records, want)
	}
	if len(res.MalformedLines) != 0 {
		t.Fatalf("malformed = %v, want none", res.MalformedLines)
	}
}

func TestParse_NormalizesProduct(t *testing.T) {
	for _, tc := range []struct{ section, want string }{
		{"FA-Administrator", "fa-administrator"},
		{"FA-GAAP", "fa-gaap"},
		{"FA-GeneralLedger", "fa-generalledger"},
		{"FA-Payment", "fa-payment"}, // механическая нормализация; fa-payments не гарантируется
	} {
		res := Parse("[" + tc.section + "]\ntSomeTable;Module\n")
		if len(res.Records) != 1 || res.Records[0].ProductName != tc.want {
			t.Fatalf("section %q: records = %+v, want product %q", tc.section, res.Records, tc.want)
		}
	}
}

func TestParse_EmptySectionNoRecords(t *testing.T) {
	res := Parse("[FA-Contracts]\n[FA-GAAP]\npRuleDoc_QtyStack;GAAP\n")
	if len(res.Records) != 1 || res.Records[0].ProductName != "fa-gaap" {
		t.Fatalf("records = %+v, want single fa-gaap record", res.Records)
	}
}

func TestParse_MalformedDoesNotAbort(t *testing.T) {
	content := "[FA-GAAP]\n" +
		"pRuleDoc_QtyStack;GAAP\n" +
		"мусорная строка без разделителя\n" +
		"  \n" +
		"pOtherTable;OtherModule\n"

	res := Parse(content)
	if len(res.Records) != 2 {
		t.Fatalf("records = %+v, want 2", res.Records)
	}
	if !reflect.DeepEqual(res.MalformedLines, []int{3}) {
		t.Fatalf("malformed = %v, want [3]", res.MalformedLines)
	}
}

func TestParse_EntryBeforeFirstSection(t *testing.T) {
	res := Parse("pSomeTable;SomeModule\n[FA-Administrator]\npAPI_AttrValue;API_Administrator\n")
	if len(res.Records) != 1 || res.Records[0].TableName != "pAPI_AttrValue" {
		t.Fatalf("records = %+v, want only section record", res.Records)
	}
	if !reflect.DeepEqual(res.MalformedLines, []int{1}) {
		t.Fatalf("malformed = %v, want [1]", res.MalformedLines)
	}
}

func TestParse_SeveralSemicolonsAndEmptyFields(t *testing.T) {
	res := Parse("[FA-GAAP]\npTable;Mod;Extra\n;NoTable\npTable2;   \n")
	if len(res.Records) != 2 {
		t.Fatalf("records = %+v, want 2", res.Records)
	}
	// деление по первой «;»: модуль сохраняет остаток как есть
	if res.Records[0].ModuleName != "Mod;Extra" {
		t.Fatalf("module = %q, want %q", res.Records[0].ModuleName, "Mod;Extra")
	}
	// пустой модуль допустим
	if res.Records[1].TableName != "pTable2" || res.Records[1].ModuleName != "" {
		t.Fatalf("record = %+v, want pTable2 with empty module", res.Records[1])
	}
	// «;NoTable» — пустая таблица → мусор
	if !reflect.DeepEqual(res.MalformedLines, []int{3}) {
		t.Fatalf("malformed = %v, want [3]", res.MalformedLines)
	}
}

func TestParse_EmptySectionHeaderIsMalformed(t *testing.T) {
	res := Parse("[]\npTable;Mod\n")
	if len(res.Records) != 0 {
		t.Fatalf("records = %+v, want none", res.Records)
	}
	// «[]» — мусорный заголовок; pTable;Mod осталась до любой валидной секции
	if !reflect.DeepEqual(res.MalformedLines, []int{1, 2}) {
		t.Fatalf("malformed = %v, want [1 2]", res.MalformedLines)
	}
}

func TestParse_TrimsFieldsAndBOM(t *testing.T) {
	res := Parse("\ufeff[FA-GAAP]\n  pRuleDoc_QtyStack ;  GAAP  \n")
	if len(res.Records) != 1 {
		t.Fatalf("records = %+v, want 1", res.Records)
	}
	if res.Records[0].TableName != "pRuleDoc_QtyStack" || res.Records[0].ModuleName != "GAAP" {
		t.Fatalf("record = %+v, want trimmed fields", res.Records[0])
	}
}

func TestParse_EmptyContent(t *testing.T) {
	res := Parse("")
	if len(res.Records) != 0 || len(res.MalformedLines) != 0 {
		t.Fatalf("result = %+v, want empty", res)
	}
}

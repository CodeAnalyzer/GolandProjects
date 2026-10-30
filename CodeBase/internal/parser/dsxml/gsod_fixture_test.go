package dsxml

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestParseFile_GSODFixture_TruncatedUpstream — фикстура
// Bug-reports/API_GSOD_MassCreate.xml (ложная UTF-8 декларация поверх CP1251,
// 37882 байта) воспроизводит upstream-дефект исходника fa-stockmarket: файл
// обрезан (последний <ReturnValue> не закрыт, </ReturnValues>/</Object>
// отсутствуют). Декодирование WIN1251 проходит, xml-декодер падает
// unexpected EOF — ошибка корректна (дефект источника, не парсера).
func TestParseFile_GSODFixture_TruncatedUpstream(t *testing.T) {
	parser := NewParser()
	path := filepath.Join("..", "..", "..", "Bug-reports", "API_GSOD_MassCreate.xml")
	_, err := parser.ParseFile(path)
	if err == nil {
		t.Fatal("truncated fixture unexpectedly parsed; upstream may have fixed the file")
	}
	if !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("error = %v, want unexpected EOF (truncated XML)", err)
	}
}

// TestParseContent_GSODServicePath — валидный Object-контент по пути
// Service/-ветки: контракт получает kind=service и business object из пути.
func TestParseContent_GSODServicePath(t *testing.T) {
	parser := NewParser()
	path := "/repo/fa-stockmarket/API_Security/DSArchitectData/BObject/GlobalSecuritiesOperationsDeal/Service/API_GSOD_MassCreate.xml"
	content := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
		"<Object><ObjectTypeID>1</ObjectTypeID><ObjectName>API_GSOD_MassCreate</ObjectName>" +
		"<Implemented>1</Implemented><ShortDescription>Mass create</ShortDescription>" +
		"<InputTables/><OutputTables/><ReturnValues/></Object>"
	result, err := parser.ParseContent(path, content)
	if err != nil {
		t.Fatalf("ParseContent: %v", err)
	}
	if result.Skipped {
		t.Fatalf("unexpectedly skipped: %s", result.SkipReason)
	}
	if len(result.Contracts) != 1 {
		t.Fatalf("contracts = %d, want 1", len(result.Contracts))
	}
	c := result.Contracts[0]
	if c.ContractKind != "service" {
		t.Fatalf("ContractKind = %q, want service", c.ContractKind)
	}
	if c.BusinessObject != "GlobalSecuritiesOperationsDeal" {
		t.Fatalf("BusinessObject = %q, want GlobalSecuritiesOperationsDeal", c.BusinessObject)
	}
	if !c.Implemented {
		t.Fatal("Implemented = false, want true")
	}
}

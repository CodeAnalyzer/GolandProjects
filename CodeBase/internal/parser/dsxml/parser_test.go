package dsxml

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/text/encoding/charmap"
)

func TestParseContent_ObjectXMLWithParamsTablesAndContexts(t *testing.T) {
	content := `<?xml version="1.0" encoding="windows-1251"?>
<Object>
  <ObjectTypeID>1</ObjectTypeID>
  <ObjectName>TestContract</ObjectName>
  <ObjectNameID>12345</ObjectNameID>
  <APIVersion>2</APIVersion>
  <Implemented>1</Implemented>
  <InternalUse>0</InternalUse>
  <Deprecated>0</Deprecated>
  <IsExternal>0</IsExternal>
  <ShortDescription>Test contract</ShortDescription>
  <FullDescription>Full description</FullDescription>
  <UsedObjectName>UsedContract</UsedObjectName>
  <UsedModuleSysName>UsedModule</UsedModuleSysName>
  <InputParams>
    <Param>
      <ParamOrder>1</ParamOrder>
      <ParamName>AccountID</ParamName>
      <TypeName>Integer</TypeName>
      <Required>1</Required>
      <RusName>ID счета</RusName>
      <Description>Account identifier</Description>
      <IsVirtualLink>0</IsVirtualLink>
      <WsParamName>accountId</WsParamName>
    </Param>
  </InputParams>
  <OutputParams>
    <Param>
      <ParamOrder>1</ParamOrder>
      <ParamName>Result</ParamName>
      <TypeName>String</TypeName>
      <Required>0</Required>
    </Param>
  </OutputParams>
  <InputTables>
    <Table>
      <ParamOrder>1</ParamOrder>
      <ParamName>tAccount</ParamName>
      <Required>1</Required>
      <RusName>Счета</RusName>
      <WsParamName>account</WsParamName>
      <Fields>
        <Param>
          <ParamOrder>1</ParamOrder>
          <ParamName>AccountID</ParamName>
          <TypeName>Integer</TypeName>
          <Required>1</Required>
        </Param>
      </Fields>
    </Table>
  </InputTables>
  <ReturnValues>
    <ReturnValue>
      <Value>0</Value>
      <ReturnType>1</ReturnType>
      <Description>Success</Description>
    </ReturnValue>
  </ReturnValues>
  <Contexts>
    <Context>
      <ContextName>Main</ContextName>
      <TypeName>String</TypeName>
      <ContextOrder>1</ContextOrder>
      <ContextValue>default</ContextValue>
      <IsVirtualLink>0</IsVirtualLink>
    </Context>
  </Contexts>
</Object>`

	parser := NewParser()
	result, err := parser.ParseContent("/repo/DSArchitectData/Module/UsedService/TestContract.xml", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.Contracts) != 1 {
		t.Fatalf("contracts count = %d, want 1: %+v", len(result.Contracts), result.Contracts)
	}
	contract := result.Contracts[0]
	if contract.ContractName != "TestContract" || contract.ContractKind != "used_service" || contract.ObjectTypeID != 1 || contract.ObjectNameID != 12345 || !contract.Implemented {
		t.Fatalf("unexpected contract: %+v", contract)
	}
	if contract.UsedObjectName != "UsedContract" {
		t.Fatalf("unexpected contract metadata: %+v", contract)
	}

	if len(result.Params) != 2 {
		t.Fatalf("params count = %d, want 2", len(result.Params))
	}
	inputParam := result.Params[0]
	if inputParam.Direction != "input" || inputParam.ParamName != "AccountID" || inputParam.TypeName != "Integer" || !inputParam.Required {
		t.Fatalf("unexpected input param: %+v", inputParam)
	}

	if len(result.Tables) != 1 {
		t.Fatalf("tables count = %d, want 1", len(result.Tables))
	}
	table := result.Tables[0]
	if table.Direction != "input" || table.TableName != "tAccount" || !table.Required {
		t.Fatalf("unexpected table: %+v", table)
	}
	if len(result.TableFields) != 1 || result.TableFields[0].FieldName != "AccountID" {
		t.Fatalf("unexpected table fields: %+v", result.TableFields)
	}

	if len(result.ReturnValues) != 1 || result.ReturnValues[0].Value != "0" {
		t.Fatalf("unexpected return values: %+v", result.ReturnValues)
	}
	if len(result.Contexts) != 1 || result.Contexts[0].ContextName != "Main" {
		t.Fatalf("unexpected contexts: %+v", result.Contexts)
	}
}

func TestParseContent_StandaloneTableWithIndexes(t *testing.T) {
	content := `<?xml version="1.0" encoding="windows-1251"?>
<Table>
  <ParamName>tBusinessTable</ParamName>
  <TypeName>Table</TypeName>
  <WsParamName>businessTable</WsParamName>
  <RusName>Бизнес таблица</RusName>
  <Description>Business table description</Description>
  <Fields>
    <Field>
      <ParamOrder>1</ParamOrder>
      <ParamName>Field1</ParamName>
      <TypeName>String</TypeName>
      <WsParamName>field1</WsParamName>
      <RusName>Поле 1</RusName>
      <Description>Field description</Description>
    </Field>
  </Fields>
  <Indexses>
    <Index>
      <IndexName>IX_Field1</IndexName>
      <IndexFields>Field1</IndexFields>
      <IndexType>1</IndexType>
      <IsClustered>1</IsClustered>
      <FieldList>
        <Field>
          <FieldName>Field1</FieldName>
        </Field>
      </FieldList>
    </Index>
  </Indexses>
</Table>`

	parser := NewParser()
	result, err := parser.ParseContent("/repo/API_Module/DSArchitectData/Module/BObject/BusinessObject/Table/tBusinessTable.xml", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.BusinessObjectTables) != 1 {
		t.Fatalf("business tables count = %d, want 1", len(result.BusinessObjectTables))
	}
	table := result.BusinessObjectTables[0]
	if table.TableName != "tBusinessTable" || table.BusinessObject != "BusinessObject" {
		t.Fatalf("unexpected business table: %+v", table)
	}
	if len(result.BusinessTableFields) != 1 || result.BusinessTableFields[0].FieldName != "Field1" {
		t.Fatalf("unexpected business table fields: %+v", result.BusinessTableFields)
	}
	if len(result.BusinessTableIndexes) != 1 || result.BusinessTableIndexes[0].IndexName != "IX_Field1" {
		t.Fatalf("unexpected business table indexes: %+v", result.BusinessTableIndexes)
	}
	if len(result.BusinessIndexFields) != 1 || result.BusinessIndexFields[0].FieldName != "Field1" {
		t.Fatalf("unexpected business index fields: %+v", result.BusinessIndexFields)
	}
}

func TestParseContent_StandaloneParam(t *testing.T) {
	content := `<?xml version="1.0" encoding="windows-1251"?>
<Param>
  <ParamName>BusinessParam</ParamName>
  <PrmSubObject>SubObject</PrmSubObject>
  <TypeName>String</TypeName>
  <WsParamName>businessParam</WsParamName>
  <RusName>Бизнес параметр</RusName>
  <Description>Business param description</Description>
</Param>`

	parser := NewParser()
	result, err := parser.ParseContent("/repo/DSArchitectData/Module/BObject/BusinessObject/Param/BusinessParam.xml", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}

	if len(result.BusinessObjectParams) != 1 {
		t.Fatalf("business params count = %d, want 1", len(result.BusinessObjectParams))
	}
	param := result.BusinessObjectParams[0]
	if param.ParamName != "BusinessParam" || param.BusinessObject != "BusinessObject" || param.PrmSubObject != "SubObject" {
		t.Fatalf("unexpected business param: %+v", param)
	}
}

func TestParseContent_EmptyAndIgnoredRoots(t *testing.T) {
	parser := NewParser()

	result, err := parser.ParseContent("/repo/test.xml", "")
	if err != nil || result == nil {
		t.Fatalf("empty content should return non-nil result: err=%v", err)
	}

	messageXML := `<?xml version="1.0"?><Message><Text>Ignore</Text></Message>`
	result, err = parser.ParseContent("/repo/message.xml", messageXML)
	if err != nil || result == nil {
		t.Fatalf("message root should return non-nil result: err=%v", err)
	}

	fasDocumentXML := `<?xml version="1.0"?><FasDocument><Data>Ignore</Data></FasDocument>`
	result, err = parser.ParseContent("/repo/fasdoc.xml", fasDocumentXML)
	if err != nil || result == nil {
		t.Fatalf("fasdocument root should return non-nil result: err=%v", err)
	}
}

func TestDSXMLHelpers(t *testing.T) {
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/BObject/BO/Service/Test.xml"); kind != "service" || bo != "BO" {
		t.Fatalf("classifyPath service = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/BObject/BO/Event/Test.xml"); kind != "event" || bo != "BO" {
		t.Fatalf("classifyPath event = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/BObject/BO/Table/Test.xml"); kind != "api_table" || bo != "BO" {
		t.Fatalf("classifyPath table = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/BObject/BO/Param/Test.xml"); kind != "api_param" || bo != "BO" {
		t.Fatalf("classifyPath param = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/UsedService/Test.xml"); kind != "used_service" || bo != "" {
		t.Fatalf("classifyPath used_service = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/DSArchitectData/Module/CallbackEvent/Test.xml"); kind != "callback_event" || bo != "" {
		t.Fatalf("classifyPath callback_event = %q, %q", kind, bo)
	}
	if kind, bo := classifyPath("/repo/other.xml"); kind != "xml" || bo != "" {
		t.Fatalf("classifyPath default = %q, %q", kind, bo)
	}

	if got := ownerModuleFromPath("/repo/DSArchitectData/Module/BObject/BO/Test.xml"); got != "repo" {
		t.Fatalf("ownerModuleFromPath = %q", got)
	}
	if got := ownerModuleFromPath("/repo/other.xml"); got != "" {
		t.Fatalf("ownerModuleFromPath no match = %q", got)
	}

	rootName, err := xmlRootName(`<?xml version="1.0"?><Object><Name>Test</Name></Object>`)
	if err != nil || rootName != "object" {
		t.Fatalf("xmlRootName = %q, err=%v", rootName, err)
	}
}

func TestParseFile_CP1251WithoutDeclaration(t *testing.T) {
	// Create a temporary XML file in CP1251 encoding without XML declaration
	// This simulates real Diasoft files that don't have encoding declaration
	tempDir := t.TempDir()
	xmlPath := filepath.Join(tempDir, "API_Test_CP1251.xml")

	// Create XML content with Russian text encoded in WIN1251 (no declaration)
	// <Object><ObjectName>Тест</ObjectName><ShortDescription>Проверка</ShortDescription></Object>
	xmlContent := `<Object>
  <ObjectTypeID>1</ObjectTypeID>
  <ObjectName>TestContract</ObjectName>
  <ShortDescription>Проверка кодировки</ShortDescription>
  <InputParams>
    <Param>
      <ParamName>Param1</ParamName>
      <RusName>Параметр</RusName>
    </Param>
  </InputParams>
</Object>`

	// Encode to WIN1251
	encodedBytes, err := charmap.Windows1251.NewEncoder().Bytes([]byte(xmlContent))
	if err != nil {
		t.Fatalf("failed to encode to WIN1251: %v", err)
	}

	if err := os.WriteFile(xmlPath, encodedBytes, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Parse the file - should auto-detect CP1251
	parser := NewParser()
	result, err := parser.ParseFile(xmlPath)
	if err != nil {
		t.Fatalf("ParseFile returned error: %v", err)
	}

	if len(result.Contracts) != 1 {
		t.Fatalf("contracts count = %d, want 1", len(result.Contracts))
	}

	contract := result.Contracts[0]
	if contract.ContractName != "TestContract" {
		t.Fatalf("contract name = %q, want %q", contract.ContractName, "TestContract")
	}

	// Verify Russian text was decoded correctly
	if contract.ShortDescription != "Проверка кодировки" {
		t.Fatalf("short description = %q, want %q", contract.ShortDescription, "Проверка кодировки")
	}

	if len(result.Params) != 1 {
		t.Fatalf("params count = %d, want 1", len(result.Params))
	}

	if result.Params[0].RusName != "Параметр" {
		t.Fatalf("param rus name = %q, want %q", result.Params[0].RusName, "Параметр")
	}
}

func TestIsAPIModulePath(t *testing.T) {
	cases := []struct {
		path  string
		want  bool
	}{
		{"fa-administrator/API_AccrualCore/DSArchitectData/BObject/AccrualCore_PTbl/Table/pAccrual_Detail.xml", true},
		{"fa-administrator/Administrator/DSArchitectData/BObject/Administrator_PTbl/Table/pPortObject.xml", false},
		{"repo/API_Credit/BObject/Credit/Table/tCredit.xml", true},
		{"repo/Consumer/BObject/Consumer/Table/tConsumer.xml", false},
	}
	for _, tc := range cases {
		parts := strings.Split(tc.path, "/")
		got := isAPIModulePath(parts)
		if got != tc.want {
			t.Fatalf("isAPIModulePath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestParseContent_APITableGoesToBusinessObjectTables(t *testing.T) {
	content := `<?xml version="1.0" encoding="windows-1251"?>
<Table>
  <ParamName>pAPITest</ParamName>
  <TypeName>TestType</TypeName>
  <WsParamName>pApiTest</WsParamName>
  <RusName>API тест</RusName>
  <Description>API test table</Description>
  <Fields>
    <Field>
      <ParamOrder>1</ParamOrder>
      <ParamName>Field1</ParamName>
      <TypeName>Integer</TypeName>
    </Field>
  </Fields>
</Table>`

	parser := NewParser()
	result, err := parser.ParseContent("fa-administrator/API_AccrualCore/DSArchitectData/BObject/AccrualCore_PTbl/Table/pAPITest.xml", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.BusinessObjectTables) != 1 {
		t.Fatalf("BusinessObjectTables count = %d, want 1", len(result.BusinessObjectTables))
	}
	if result.BusinessObjectTables[0].TableName != "pAPITest" {
		t.Fatalf("BusinessObjectTables[0].TableName = %q, want %q", result.BusinessObjectTables[0].TableName, "pAPITest")
	}
	if len(result.InternalPTables) != 0 {
		t.Fatalf("InternalPTables count = %d, want 0", len(result.InternalPTables))
	}
}

func TestParseContent_InternalTableGoesToInternalPTables(t *testing.T) {
	content := `<?xml version="1.0" encoding="windows-1251"?>
<Table>
  <ParamName>pPortObject</ParamName>
  <TypeName>TestType</TypeName>
  <WsParamName>pPortObject</WsParamName>
  <RusName>Внутренняя p-таблица</RusName>
  <Description>Internal p-table</Description>
  <Fields>
    <Field>
      <ParamOrder>1</ParamOrder>
      <ParamName>Field1</ParamName>
      <TypeName>Integer</TypeName>
    </Field>
  </Fields>
</Table>`

	parser := NewParser()
	result, err := parser.ParseContent("fa-administrator/Administrator/DSArchitectData/BObject/Administrator_PTbl/Table/pPortObject.xml", content)
	if err != nil {
		t.Fatalf("ParseContent returned error: %v", err)
	}
	if len(result.InternalPTables) != 1 {
		t.Fatalf("InternalPTables count = %d, want 1", len(result.InternalPTables))
	}
	ptable := result.InternalPTables[0]
	if ptable.TableName != "pPortObject" {
		t.Fatalf("InternalPTables[0].TableName = %q, want %q", ptable.TableName, "pPortObject")
	}
	if ptable.Context != "create" {
		t.Fatalf("InternalPTables[0].Context = %q, want %q", ptable.Context, "create")
	}
	if !ptable.IsTemporary {
		t.Fatalf("InternalPTables[0].IsTemporary = false, want true")
	}
	if len(result.BusinessObjectTables) != 0 {
		t.Fatalf("BusinessObjectTables count = %d, want 0", len(result.BusinessObjectTables))
	}
	hasInternalSymbol := false
	for _, sym := range result.Symbols {
		if sym.SymbolType == "table" && sym.SymbolName == "pPortObject" && sym.EntityType == "sql" {
			hasInternalSymbol = true
		}
	}
	if !hasInternalSymbol {
		t.Fatalf("Symbols should contain table symbol with EntityType=sql for pPortObject")
	}
	if len(result.InternalPTableColumns) != 1 {
		t.Fatalf("InternalPTableColumns count = %d, want 1", len(result.InternalPTableColumns))
	}
	col := result.InternalPTableColumns[0]
	if col.TableName != "pPortObject" || col.ColumnName != "Field1" || col.DataType != "Integer" {
		t.Fatalf("InternalPTableColumns[0] = %+v, want {pPortObject, Field1, Integer}", col)
	}
	if col.DefinitionKind != "create" {
		t.Fatalf("InternalPTableColumns[0].DefinitionKind = %q, want %q", col.DefinitionKind, "create")
	}
}

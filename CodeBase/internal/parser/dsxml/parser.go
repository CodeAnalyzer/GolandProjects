package dsxml

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	cbencoding "github.com/codebase/internal/encoding"
	"github.com/codebase/internal/model"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

type Parser struct{}

type ParseResult struct {
	BusinessObjects      []*model.APIBusinessObject
	Contracts            []*model.APIContract
	Params               []*model.APIContractParam
	Tables               []*model.APIContractTable
	TableFields          []*model.APIContractTableField
	BusinessObjectParams []*model.APIBusinessObjectParam
	BusinessObjectTables []*model.APIBusinessObjectTable
	BusinessTableFields  []*model.APIBusinessObjectTableField
	BusinessTableIndexes []*model.APIBusinessObjectTableIndex
	BusinessIndexFields  []*model.APIBusinessObjectTableIndexField
	ReturnValues         []*model.APIContractReturnValue
	Contexts             []*model.APIContractContext
	Symbols              []*model.Symbol
}

type objectXML struct {
	XMLName           xml.Name         `xml:"Object"`
	ObjectTypeID      int              `xml:"ObjectTypeID"`
	ObjectName        string           `xml:"ObjectName"`
	ObjectNameID      int64            `xml:"ObjectNameID"`
	APIVersion        int              `xml:"APIVersion"`
	ArchApproval      int              `xml:"ArchApproval"`
	Implemented       int              `xml:"Implemented"`
	InternalUse       int              `xml:"InternalUse"`
	Deprecated        int              `xml:"Deprecated"`
	ShortDescription  string           `xml:"ShortDescription"`
	FullDescription   string           `xml:"FullDescription"`
	IsExternal        int              `xml:"IsExternal"`
	UsedObjectName    string           `xml:"UsedObjectName"`
	UsedModuleSysName string           `xml:"UsedModuleSysName"`
	InputParams       []paramXML       `xml:"InputParams>Param"`
	OutputParams      []paramXML       `xml:"OutputParams>Param"`
	InputTables       []tableParamXML  `xml:"InputTables>Table"`
	OutputTables      []tableParamXML  `xml:"OutputTables>Table"`
	ReturnValues      []returnValueXML `xml:"ReturnValues>ReturnValue"`
	Contexts          []contextXML     `xml:"Contexts>Context"`
}

type paramXML struct {
	ParamOrder    int    `xml:"ParamOrder"`
	ParamName     string `xml:"ParamName"`
	PrmSubObject  string `xml:"PrmSubObject"`
	TypeName      string `xml:"TypeName"`
	Required      int    `xml:"Required"`
	RusName       string `xml:"RusName"`
	Description   string `xml:"Description"`
	IsVirtualLink int    `xml:"IsVirtualLink"`
	WsParamName   string `xml:"WsParamName"`
}

type tableParamXML struct {
	ParamOrder    int        `xml:"ParamOrder"`
	ParamName     string     `xml:"ParamName"`
	Required      int        `xml:"Required"`
	RusName       string     `xml:"RusName"`
	Description   string     `xml:"Description"`
	IsVirtualLink int        `xml:"IsVirtualLink"`
	WsParamName   string     `xml:"WsParamName"`
	Fields        []paramXML `xml:"Fields>Param"`
}

type standaloneTableXML struct {
	XMLName     xml.Name             `xml:"Table"`
	ParamName   string               `xml:"ParamName"`
	TypeName    string               `xml:"TypeName"`
	WsParamName string               `xml:"WsParamName"`
	RusName     string               `xml:"RusName"`
	Description string               `xml:"Description"`
	Fields      []standaloneFieldXML `xml:"Fields>Field"`
	Indexes     []standaloneIndexXML `xml:"Indexses>Index"`
}

type standaloneFieldXML struct {
	ParamOrder  int    `xml:"ParamOrder"`
	ParamName   string `xml:"ParamName"`
	TypeName    string `xml:"TypeName"`
	WsParamName string `xml:"WsParamName"`
	RusName     string `xml:"RusName"`
	Description string `xml:"Description"`
}

type standaloneParamXML struct {
	XMLName      xml.Name `xml:"Param"`
	ParamName    string   `xml:"ParamName"`
	PrmSubObject string   `xml:"PrmSubObject"`
	TypeName     string   `xml:"TypeName"`
	WsParamName  string   `xml:"WsParamName"`
	RusName      string   `xml:"RusName"`
	Description  string   `xml:"Description"`
}

type returnValueXML struct {
	Value       string `xml:"Value"`
	ReturnType  int    `xml:"ReturnType"`
	Description string `xml:"Description"`
}

type contextXML struct {
	ContextName   string `xml:"ContextName"`
	TypeName      string `xml:"TypeName"`
	RusName       string `xml:"RusName"`
	Description   string `xml:"Description"`
	ContextOrder  int    `xml:"ContextOrder"`
	ContextValue  string `xml:"ContextValue"`
	IsVirtualLink int    `xml:"IsVirtualLink"`
}

type standaloneIndexXML struct {
	IndexName   string                    `xml:"IndexName"`
	IndexFields string                    `xml:"IndexFields"`
	IndexType   int                       `xml:"IndexType"`
	IsClustered int                       `xml:"IsClustered"`
	Fields      []standaloneIndexFieldXML `xml:"FieldList>Field"`
}

type standaloneIndexFieldXML struct {
	FieldName string `xml:"FieldName"`
}

func NewParser() *Parser { return &Parser{} }

func xmlCharsetReader(charset string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(charset)) {
	case "windows-1251", "cp1251", "windows1251":
		return transform.NewReader(input, charmap.Windows1251.NewDecoder()), nil
	case "utf-8", "utf8", "":
		return input, nil
	default:
		return nil, fmt.Errorf("unsupported xml charset: %s", charset)
	}
}

func unmarshalXML(content string, target interface{}) error {
	decoder := xml.NewDecoder(bytes.NewReader([]byte(content)))
	decoder.CharsetReader = xmlCharsetReader
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func xmlRootName(content string) (string, error) {
	var root struct {
		XMLName xml.Name
	}
	if err := unmarshalXML(content, &root); err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(root.XMLName.Local)), nil
}

func (p *Parser) ParseFile(path string) (*ParseResult, error) {
	content, err := cbencoding.ReadFile(path, cbencoding.UTF8)
	if err != nil {
		return nil, err
	}
	return p.ParseContent(path, content)
}

func (p *Parser) ParseContent(path string, content string) (*ParseResult, error) {
	res := &ParseResult{
		BusinessObjects:      make([]*model.APIBusinessObject, 0),
		Contracts:            make([]*model.APIContract, 0),
		Params:               make([]*model.APIContractParam, 0),
		Tables:               make([]*model.APIContractTable, 0),
		TableFields:          make([]*model.APIContractTableField, 0),
		BusinessObjectParams: make([]*model.APIBusinessObjectParam, 0),
		BusinessObjectTables: make([]*model.APIBusinessObjectTable, 0),
		BusinessTableFields:  make([]*model.APIBusinessObjectTableField, 0),
		BusinessTableIndexes: make([]*model.APIBusinessObjectTableIndex, 0),
		BusinessIndexFields:  make([]*model.APIBusinessObjectTableIndexField, 0),
		ReturnValues:         make([]*model.APIContractReturnValue, 0),
		Contexts:             make([]*model.APIContractContext, 0),
		Symbols:              make([]*model.Symbol, 0),
	}
	if strings.TrimSpace(content) == "" {
		return res, nil
	}
	rootName, err := xmlRootName(content)
	if err != nil {
		return nil, err
	}
	if rootName == "message" {
		return res, nil
	}
	if rootName == "fasdocument" {
		return res, nil
	}
	cleanPath := filepath.ToSlash(path)
	kind, businessObject := classifyPath(cleanPath)
	switch {
	case strings.Contains(cleanPath, "/BObject/") && strings.Contains(cleanPath, "/Table/"):
		var table standaloneTableXML
		if err := unmarshalXML(content, &table); err != nil {
			return nil, fmt.Errorf("failed to parse standalone table xml: %w", err)
		}
		res.BusinessObjectTables = append(res.BusinessObjectTables, &model.APIBusinessObjectTable{
			BusinessObject: businessObject,
			TableName:      strings.TrimSpace(table.ParamName),
			TypeName:       strings.TrimSpace(table.TypeName),
			WsParamName:    strings.TrimSpace(table.WsParamName),
			RusName:        strings.TrimSpace(table.RusName),
			Description:    strings.TrimSpace(table.Description),
			LineNumber:     1,
		})
		res.Symbols = append(res.Symbols, &model.Symbol{SymbolName: strings.TrimSpace(table.ParamName), SymbolType: "api_table", EntityType: "xml", LineNumber: 1, Signature: strings.TrimSpace(table.ParamName)})
		for _, field := range table.Fields {
			res.BusinessTableFields = append(res.BusinessTableFields, &model.APIBusinessObjectTableField{
				ParentTableName: strings.TrimSpace(table.ParamName),
				BusinessObject:  businessObject,
				FieldName:       strings.TrimSpace(field.ParamName),
				TypeName:        strings.TrimSpace(field.TypeName),
				WsParamName:     strings.TrimSpace(field.WsParamName),
				RusName:         strings.TrimSpace(field.RusName),
				Description:     strings.TrimSpace(field.Description),
				ParamOrder:      field.ParamOrder,
				LineNumber:      1,
			})
		}
		for _, index := range table.Indexes {
			indexName := strings.TrimSpace(index.IndexName)
			res.BusinessTableIndexes = append(res.BusinessTableIndexes, &model.APIBusinessObjectTableIndex{
				ParentTableName: strings.TrimSpace(table.ParamName),
				BusinessObject:  businessObject,
				IndexName:       indexName,
				IndexFields:     strings.TrimSpace(index.IndexFields),
				IndexType:       index.IndexType,
				IsClustered:     index.IsClustered != 0,
				LineNumber:      1,
			})
			for fieldOrder, field := range index.Fields {
				res.BusinessIndexFields = append(res.BusinessIndexFields, &model.APIBusinessObjectTableIndexField{
					ParentIndexName: indexName,
					ParentTableName: strings.TrimSpace(table.ParamName),
					BusinessObject:  businessObject,
					FieldName:       strings.TrimSpace(field.FieldName),
					FieldOrder:      fieldOrder,
					LineNumber:      1,
				})
			}
		}
		return res, nil
	case strings.Contains(cleanPath, "/BObject/") && strings.Contains(cleanPath, "/Param/"):
		var param standaloneParamXML
		if err := unmarshalXML(content, &param); err != nil {
			return nil, fmt.Errorf("failed to parse standalone param xml: %w", err)
		}
		res.BusinessObjectParams = append(res.BusinessObjectParams, &model.APIBusinessObjectParam{
			BusinessObject: businessObject,
			ParamName:      strings.TrimSpace(param.ParamName),
			PrmSubObject:   strings.TrimSpace(param.PrmSubObject),
			TypeName:       strings.TrimSpace(param.TypeName),
			WsParamName:    strings.TrimSpace(param.WsParamName),
			RusName:        strings.TrimSpace(param.RusName),
			Description:    strings.TrimSpace(param.Description),
			LineNumber:     1,
		})
		res.Symbols = append(res.Symbols, &model.Symbol{SymbolName: strings.TrimSpace(param.ParamName), SymbolType: "api_param", EntityType: "xml", LineNumber: 1, Signature: strings.TrimSpace(param.ParamName)})
		return res, nil
	default:
		var obj objectXML
		if err := unmarshalXML(content, &obj); err != nil {
			return nil, fmt.Errorf("failed to parse object xml: %w", err)
		}
		if businessObject != "" {
			res.BusinessObjects = append(res.BusinessObjects, &model.APIBusinessObject{BusinessObject: businessObject, ModuleName: ownerModuleFromPath(cleanPath), LineStart: 1, LineEnd: 1})
		}
		contract := &model.APIContract{
			BusinessObject:    businessObject,
			ContractName:      strings.TrimSpace(obj.ObjectName),
			ContractKind:      kind,
			ObjectTypeID:      obj.ObjectTypeID,
			ObjectNameID:      obj.ObjectNameID,
			APIVersion:        obj.APIVersion,
			ArchApproval:      obj.ArchApproval,
			Implemented:       obj.Implemented != 0,
			InternalUse:       obj.InternalUse != 0,
			Deprecated:        obj.Deprecated != 0,
			IsExternal:        obj.IsExternal != 0,
			OwnerModule:       ownerModuleFromPath(cleanPath),
			UsedObjectName:    strings.TrimSpace(obj.UsedObjectName),
			UsedModuleSysName: strings.TrimSpace(obj.UsedModuleSysName),
			ShortDescription:  strings.TrimSpace(obj.ShortDescription),
			FullDescription:   strings.TrimSpace(obj.FullDescription),
			LineStart:         1,
			LineEnd:           1,
		}
		res.Contracts = append(res.Contracts, contract)
		res.Symbols = append(res.Symbols, &model.Symbol{SymbolName: contract.ContractName, SymbolType: kind, EntityType: "xml", LineNumber: 1, Signature: contract.ContractName})
		appendParams := func(direction string, items []paramXML) {
			for _, item := range items {
				res.Params = append(res.Params, &model.APIContractParam{Direction: direction, ParamName: strings.TrimSpace(item.ParamName), PrmSubObject: strings.TrimSpace(item.PrmSubObject), TypeName: strings.TrimSpace(item.TypeName), Required: item.Required != 0, RusName: strings.TrimSpace(item.RusName), Description: strings.TrimSpace(item.Description), WsParamName: strings.TrimSpace(item.WsParamName), ParamOrder: item.ParamOrder, IsVirtualLink: item.IsVirtualLink != 0, LineNumber: 1})
			}
		}
		appendTables := func(direction string, items []tableParamXML) {
			for _, item := range items {
				res.Tables = append(res.Tables, &model.APIContractTable{Direction: direction, TableName: strings.TrimSpace(item.ParamName), WsParamName: strings.TrimSpace(item.WsParamName), Required: item.Required != 0, RusName: strings.TrimSpace(item.RusName), Description: strings.TrimSpace(item.Description), ParamOrder: item.ParamOrder, LineNumber: 1})
				for _, field := range item.Fields {
					res.TableFields = append(res.TableFields, &model.APIContractTableField{ParentTableName: strings.TrimSpace(item.ParamName), ParentDirection: direction, FieldName: strings.TrimSpace(field.ParamName), TypeName: strings.TrimSpace(field.TypeName), Required: field.Required != 0, RusName: strings.TrimSpace(field.RusName), Description: strings.TrimSpace(field.Description), WsParamName: strings.TrimSpace(field.WsParamName), ParamOrder: field.ParamOrder, LineNumber: 1})
				}
			}
		}
		appendParams("input", obj.InputParams)
		appendParams("output", obj.OutputParams)
		appendTables("input", obj.InputTables)
		appendTables("output", obj.OutputTables)
		for _, item := range obj.ReturnValues {
			res.ReturnValues = append(res.ReturnValues, &model.APIContractReturnValue{Value: strings.TrimSpace(item.Value), ReturnType: item.ReturnType, Description: strings.TrimSpace(item.Description), LineNumber: 1})
		}
		for _, item := range obj.Contexts {
			res.Contexts = append(res.Contexts, &model.APIContractContext{ContextName: strings.TrimSpace(item.ContextName), TypeName: strings.TrimSpace(item.TypeName), RusName: strings.TrimSpace(item.RusName), Description: strings.TrimSpace(item.Description), ContextOrder: item.ContextOrder, ContextValue: strings.TrimSpace(item.ContextValue), IsVirtualLink: item.IsVirtualLink != 0, LineNumber: 1})
		}
		return res, nil
	}
}

func classifyPath(path string) (string, string) {
	parts := strings.Split(filepath.ToSlash(path), "/")
	for i, part := range parts {
		switch part {
		case "BObject":
			businessObject := ""
			if i+1 < len(parts) {
				businessObject = parts[i+1]
			}
			if i+2 < len(parts) {
				switch parts[i+2] {
				case "Service":
					return "service", businessObject
				case "Event":
					return "event", businessObject
				case "Table":
					return "api_table", businessObject
				case "Param":
					return "api_param", businessObject
				}
			}
		case "UsedService":
			return "used_service", ""
		case "CallbackEvent":
			return "callback_event", ""
		}
	}
	return "xml", ""
}

func ownerModuleFromPath(path string) string {
	path = filepath.ToSlash(path)
	if idx := strings.Index(path, "/DSArchitectData/"); idx >= 0 {
		prefix := strings.Trim(path[:idx], "/")
		if prefix != "" {
			parts := strings.Split(prefix, "/")
			return parts[len(parts)-1]
		}
	}
	return ""
}

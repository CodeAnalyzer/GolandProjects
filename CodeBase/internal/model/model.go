package model

import "time"

// File файл проекта
type File struct {
	ID         int64
	ScanRunID  int64
	Path       string
	RelPath    string
	Extension  string
	SizeBytes  int64
	HashSHA256 string
	ModifiedAt time.Time
	Encoding   string
	Language   string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ReportForm отчётная форма TPR/RPT
type ReportForm struct {
	ID         int64
	FileID     int64
	ReportName string
	ReportType string
	FormName   string
	FormClass  string
	LineStart  int
	LineEnd    int
}

// ReportField поле отчёта
type ReportField struct {
	ID           int64
	ReportFormID int64
	FieldName    string
	SourceName   string
	FormatMask   string
	Options      []string
	LineNumber   int
	RawText      string
}

// ReportParam параметр отчёта
type ReportParam struct {
	ID            int64
	ReportFormID  int64
	ParamName     string
	ParamKind     string
	ComponentType string
	DataType      string
	LookupForm    string
	LookupTable   string
	LookupColumn  string
	KeyColumn     string
	Required      bool
	DefaultValue  string
	LineNumber    int
	RawText       string
}

// VBFunction VBScript функция/процедура из RPT
type VBFunction struct {
	ID           int64
	ReportFormID int64
	FunctionName string
	FunctionType string
	Signature    string
	BodyText     string
	LineStart    int
	LineEnd      int
	BodyHash     string
}

// SQLProcedure SQL-процедура или скрипт
type SQLProcedure struct {
	ID        int64
	FileID    int64
	ProcName  string
	Params    []SQLParam
	LineStart int
	LineEnd   int
	BodyHash  string
}

// SQLParam параметр SQL-процедуры
type SQLParam struct {
	Name      string
	Type      string
	Direction string // in, out, inout
}

type SQLProcedureCall struct {
	CallerName string
	CalleeName string
	LineNumber int
}

// SQLTable таблица в SQL
type SQLTable struct {
	ID          int64
	FileID      int64
	TableName   string
	Context     string // select, insert, update, delete, unknown
	IsTemporary bool
	LineNumber  int
	ColNumber   int
}

// SQLColumn поле таблицы в SQL
type SQLColumn struct {
	ID         int64
	FileID     int64
	TableName  string
	ColumnName string
	LineNumber int
	ColNumber  int
}

// SQLColumnDefinition определение поля таблицы из CREATE TABLE
type SQLColumnDefinition struct {
	ID          int64
	FileID      int64
	TableName   string
	ColumnName  string
	DataType    string
	DefinitionKind string
	LineNumber  int
	ColumnOrder int
}

// SQLIndexDefinition определение индекса обычной SQL-таблицы.
type SQLIndexDefinition struct {
	ID             int64
	FileID         int64
	TableName      string
	IndexName      string
	IndexFields    string
	IndexType      string
	IsUnique       bool
	DefinitionKind string
	LineNumber     int
}

// SQLIndexDefinitionField поле индекса обычной SQL-таблицы.
type SQLIndexDefinitionField struct {
	ID              int64
	TableIndexID    int64
	ParentIndexName string
	ParentTableName string
	FieldName       string
	FieldOrder      int
	LineNumber      int
}

// PASUnit Pascal/Delphi юнит
type PASUnit struct {
	ID                 int64
	FileID             int64
	UnitName           string
	InterfaceUses      []string
	ImplementationUses []string
	LineStart          int
	LineEnd            int
}

// PASClass класс Pascal/Delphi
type PASClass struct {
	ID          int64
	UnitID      int64
	ClassName   string
	ParentClass string
	DFMFormID   int64
	LineStart   int
	LineEnd     int
}

// PASMethod метод класса
type PASMethod struct {
	ID         int64
	ClassID    int64
	ClassName  string // имя класса для привязки после сохранения
	UnitID     int64
	MethodName string
	Signature  string
	Visibility string
	LineNumber int
}

// PASField поле класса
type PASField struct {
	ID             int64
	ClassID        int64
	ClassName      string // имя класса для привязки после сохранения
	FieldName      string
	FieldType      string
	DFMComponentID int64
	Visibility     string
	LineNumber     int
}

// SQLFragment SQL-фрагмент в PAS/JS коде
type SQLFragment struct {
	ID         int64
	FileID     int64
	MethodID   int64
	ClassName  string
	MethodName string
	QueryText  string
	QueryHash  string
	Context    string // method, property, global
	LineNumber int
}

type QueryFragment struct {
	ID               int64
	FileID           int64
	ParentType       string
	ParentID         int64
	ComponentName    string
	ComponentType    string
	QueryText        string
	QueryHash        string
	TablesReferenced []string
	Context          string
	LineNumber       int
	LineEnd          int
}

// JSFunction JavaScript функция
type JSFunction struct {
	ID           int64
	FileID       int64
	FunctionName string
	Signature    string
	LineStart    int
	LineEnd      int
	ScenarioType string // 'instrument_model', 'mass_operation', 'utility'
	ParentObject string // "ConsCredit", "Instrument", "Sys"
}

// JSScriptObject скриптовый объект (ConsCredit, Sys, Instrument)
type JSScriptObject struct {
	Name string
	Type string
}

// JSProcedureCall вызов хранимой процедуры из JS
type JSProcedureCall struct {
	ObjectName string
	ProcName   string
	LineNumber int
}

// JSQueryCall вызов SQL-запроса из JS
type JSQueryCall struct {
	ObjectName string
	QueryText  string
	LineNumber int
}

// JSConstant константа JS
type JSConstant struct {
	Name  string
	Value string
}

// SMFInstrument модель финансовой операции из SMF
type SMFInstrument struct {
	ID             int64
	FileID         int64
	InstrumentName string
	Brief          string
	DealObjectID   int64
	DsModuleID     int64
	StartState     string
	ScenarioType   string                   // 'instrument_model', 'mass_operation'
	States         []map[string]interface{} // JSONB: [{name, sys_name, state_type}]
	Actions        []map[string]interface{} // JSONB: [{name, sys_name, prop_val, priority}]
	Accounts       []map[string]interface{} // JSONB: [{type, mask}]
}

// DFMForm DFM форма
type DFMForm struct {
	ID        int64
	FileID    int64
	FormName  string
	FormClass string
	Caption   string
	LineStart int
	LineEnd   int
}

// DFMComponent компонент DFM формы
type DFMComponent struct {
	ID            int64
	FileID        int64
	FormID        int64
	FormName      string
	ComponentName string
	ComponentType string
	ParentName    string
	Caption       string
	LineStart     int
	LineEnd       int
}

// DFMQuery SQL-запрос в DFM
type DFMQuery struct {
	ID            int64
	FileID        int64
	FormID        int64
	ComponentName string
	ComponentType string
	QueryText     string
	LineNumber    int
	MethodName    string
	MethodLine    int
}

// HDefine Определение из H-файла
type HDefine struct {
	ID          int64
	FileID      int64
	DefineName  string
	DefineValue string
	DefineType  string // const, macro, include, comment
	LineNumber  int
}

// Relation Связь между сущностями
type Relation struct {
	ID           int64
	SourceType   string
	SourceID     int64
	TargetType   string
	TargetID     int64
	RelationType string // calls_procedure, references_table, selects_from, inserts_into, updates, deletes_from, executes_query, builds_query
	Confidence   string // ast, token, regex
	LineNumber   int
}

// IncludeDirective Директива include
type IncludeDirective struct {
	ID             int64
	FileID         int64
	IncludePath    string
	ResolvedFileID int64
	LineNumber     int
}

// IncludeRef Ссылка на include в исходном тексте
type IncludeRef struct {
	IncludePath string
	LineNumber  int
}

// Symbol Унифицированный символ для поиска
type Symbol struct {
	ID         int64
	FileID     int64
	SymbolName string
	SymbolType string // procedure, function, class, method, table, variable, define
	EntityType string // sql, pas, js, h, dfm
	EntityID   int64
	LineNumber int
	SQLContext string
	Signature  string
}

// ScanStats Статистика сканирования
type ScanStats struct {
	FilesScanned   int
	FilesIndexed   int
	FilesUpdated   int
	FilesAdded     int
	FilesDeleted   int
	SQLFiles       int
	PASFiles       int
	JSFiles        int
	HFiles         int
	DFMFiles       int
	SMFFiles       int
	TPRFiles       int
	RPTFiles       int
	Procedures     int
	Tables         int
	Columns        int
	Units          int
	Classes        int
	Methods        int
	PASFields      int
	JSFunctions    int
	SMFInstruments int
	Forms          int
	ReportFields   int
	ReportParams   int
	VBFunctions    int
	QueryFragments int
	Relations      int
	Errors         int
	XMLFiles       int
	APIContracts   int
	APIParams      int
	APITables      int
	APITableFields int
	APITableIndexes int
}

// APIBusinessObject бизнес-объект DSArchitect.
type APIBusinessObject struct {
	ID             int64
	FileID         int64
	BusinessObject string
	ModuleName     string
	LineStart      int
	LineEnd        int
}

// APIContract XML-контракт сервиса/события/подписки.
type APIContract struct {
	ID                int64
	FileID            int64
	BusinessObjectID  int64
	BusinessObject    string
	ContractName      string
	ContractKind      string // service, event, used_service, callback_event
	ObjectTypeID      int
	ObjectNameID      int64
	APIVersion        int
	ArchApproval      int
	Implemented       bool
	InternalUse       bool
	Deprecated        bool
	IsExternal        bool
	OwnerModule       string
	UsedObjectName    string
	UsedModuleSysName string
	ShortDescription  string
	FullDescription   string
	LineStart         int
	LineEnd           int
}

// APIContractParam параметр контракта.
type APIContractParam struct {
	ID            int64
	ContractID    int64
	Direction     string // input, output, context
	ParamName     string
	PrmSubObject  string
	TypeName      string
	Required      bool
	RusName       string
	Description   string
	WsParamName   string
	ParamOrder    int
	IsVirtualLink bool
	LineNumber    int
}

// APIContractTable табличный параметр контракта.
type APIContractTable struct {
	ID          int64
	ContractID  int64
	Direction   string // input, output
	TableName   string
	WsParamName string
	Required    bool
	RusName     string
	Description string
	ParamOrder  int
	LineNumber  int
}

// APIContractTableField поле табличного параметра.
type APIContractTableField struct {
	ID              int64
	ContractTableID int64
	ParentTableName string
	ParentDirection string
	FieldName       string
	TypeName        string
	Required        bool
	RusName         string
	Description     string
	WsParamName     string
	ParamOrder      int
	LineNumber      int
}

// APIBusinessObjectParam standalone param definition из BObject/.../Param.
type APIBusinessObjectParam struct {
	ID             int64
	FileID         int64
	BusinessObject string
	ParamName      string
	PrmSubObject   string
	TypeName       string
	WsParamName    string
	RusName        string
	Description    string
	LineNumber     int
}

// APIBusinessObjectTable standalone table definition из BObject/.../Table.
type APIBusinessObjectTable struct {
	ID             int64
	FileID         int64
	BusinessObject string
	TableName      string
	TypeName       string
	WsParamName    string
	RusName        string
	Description    string
	LineNumber     int
}

// APIBusinessObjectTableField поле standalone API table.
type APIBusinessObjectTableField struct {
	ID              int64
	BusinessTableID int64
	ParentTableName string
	BusinessObject  string
	FieldName       string
	TypeName        string
	WsParamName     string
	RusName         string
	Description     string
	ParamOrder      int
	LineNumber      int
}

// APIBusinessObjectTableIndex индекс standalone API table из BObject/.../Table.
type APIBusinessObjectTableIndex struct {
	ID              int64
	BusinessTableID int64
	ParentTableName string
	BusinessObject  string
	IndexName       string
	IndexFields     string
	IndexType       int
	IsClustered     bool
	LineNumber      int
}

// APIBusinessObjectTableIndexField поле индекса standalone API table.
type APIBusinessObjectTableIndexField struct {
	ID             int64
	TableIndexID   int64
	ParentIndexName string
	ParentTableName string
	BusinessObject string
	FieldName      string
	FieldOrder     int
	LineNumber     int
}

// APIContractReturnValue коды возврата контракта.
type APIContractReturnValue struct {
	ID          int64
	ContractID  int64
	Value       string
	ReturnType  int
	Description string
	LineNumber  int
}

// APIContractContext context-аргументы контракта.
type APIContractContext struct {
	ID            int64
	ContractID    int64
	ContextName   string
	TypeName      string
	RusName       string
	Description   string
	ContextOrder  int
	ContextValue  string
	IsVirtualLink bool
	LineNumber    int
}

// APIMacroInvocation макросы API_CREATE_PROC/API_INIT_EVENT/API_EXEC и др.
type APIMacroInvocation struct {
	ID            int64
	FileID        int64
	ProcedureName string
	MacroType     string // create_proc, init_event, exec_contract, dispatches_to
	TargetName    string
	TargetKind    string // contract, procedure
	LineNumber    int
	RawText       string
}

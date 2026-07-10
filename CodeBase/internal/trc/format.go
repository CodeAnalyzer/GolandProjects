package trc

// ColumnDataType — грубая категория типа данных колонки трейса, используемая
// для декодирования значений в теле события (Phase 1). Категории "unverified"
// присвоены по документации SQL Server (Data Columns.txt), т.к. ни в одном из
// доступных golden-файлов (DIAPR-391.trc, nbki.trc) колонки этих типов не
// встретились с реальными данными — см. README.md / план Phase 0.
type ColumnDataType int

const (
	TypeUnknown ColumnDataType = iota
	TypeString                // nvarchar (UTF-16LE), по всей видимости length-prefixed
	TypeInt32                 // 4-байтовое целое
	TypeInt64                 // 8-байтовое целое (bigint)
	TypeDateTime              // 8 байт, точная кодировка не подтверждена
	TypeGUID                  // 16 байт, unverified
	TypeBinary                // произвольная длина, unverified
)

// ColumnDef — определение колонки трейса (см. Modifications/Data Columns.txt).
type ColumnDef struct {
	ID       int
	Name     string
	DataType ColumnDataType
}

// columnDefinitions — все 64 колонки из Data Columns.txt.
var columnDefinitions = map[int]ColumnDef{
	1:  {1, "TextData", TypeString},
	2:  {2, "BinaryData", TypeBinary},
	3:  {3, "DatabaseID", TypeInt32},
	4:  {4, "TransactionID", TypeInt64},
	5:  {5, "LineNumber", TypeInt32},
	6:  {6, "NTUserName", TypeString},
	7:  {7, "NTDomainName", TypeString},
	8:  {8, "HostName", TypeString},
	9:  {9, "ClientProcessID", TypeInt32},
	10: {10, "ApplicationName", TypeString},
	11: {11, "LoginName", TypeString},
	12: {12, "SPID", TypeInt32},
	13: {13, "Duration", TypeInt64},
	14: {14, "StartTime", TypeDateTime},
	15: {15, "EndTime", TypeDateTime},
	16: {16, "Reads", TypeInt64},
	17: {17, "Writes", TypeInt64},
	18: {18, "CPU", TypeInt32},
	19: {19, "Permissions", TypeInt64},
	20: {20, "Severity", TypeInt32},
	21: {21, "EventSubClass", TypeInt32},
	22: {22, "ObjectID", TypeInt32},
	23: {23, "Success", TypeInt32},
	24: {24, "IndexID", TypeInt32},
	25: {25, "IntegerData", TypeInt32},
	26: {26, "ServerName", TypeString},
	27: {27, "EventClass", TypeInt32},
	28: {28, "ObjectType", TypeInt32},
	29: {29, "NestLevel", TypeInt32},
	30: {30, "State", TypeInt32},
	31: {31, "Error", TypeInt32},
	32: {32, "Mode", TypeInt32},
	33: {33, "Handle", TypeInt32},
	34: {34, "ObjectName", TypeString},
	35: {35, "DatabaseName", TypeString},
	36: {36, "FileName", TypeString},
	37: {37, "OwnerName", TypeString},
	38: {38, "RoleName", TypeString},
	39: {39, "TargetUserName", TypeString},
	40: {40, "DBUserName", TypeString},
	41: {41, "LoginSid", TypeBinary},
	42: {42, "TargetLoginName", TypeString},
	43: {43, "TargetLoginSid", TypeBinary},
	44: {44, "ColumnPermissions", TypeInt32},
	45: {45, "LinkedServerName", TypeString},
	46: {46, "ProviderName", TypeString},
	47: {47, "MethodName", TypeString},
	48: {48, "RowCounts", TypeInt64},
	49: {49, "RequestID", TypeInt32},
	50: {50, "XactSequence", TypeInt64},
	51: {51, "EventSequence", TypeInt64},
	52: {52, "BigintData1", TypeInt64},
	53: {53, "BigintData2", TypeInt64},
	54: {54, "GUID", TypeGUID},
	55: {55, "IntegerData2", TypeInt32},
	56: {56, "ObjectID2", TypeInt64},
	57: {57, "Type", TypeInt32},
	58: {58, "OwnerID", TypeInt32},
	59: {59, "ParentName", TypeString},
	60: {60, "IsSystem", TypeInt32},
	61: {61, "Offset", TypeInt32},
	62: {62, "SourceDatabaseID", TypeInt32},
	63: {63, "SqlHandle", TypeBinary},
	64: {64, "SessionLoginName", TypeString},
}

// ColumnName возвращает имя колонки по её ColumnID, либо "" если неизвестна.
func ColumnName(id int) string {
	if def, ok := columnDefinitions[id]; ok {
		return def.Name
	}
	return ""
}

// ColumnType возвращает предполагаемую категорию типа данных колонки.
func ColumnType(id int) ColumnDataType {
	if def, ok := columnDefinitions[id]; ok {
		return def.DataType
	}
	return TypeUnknown
}

// eventClassNames — имена классов событий (см. Modifications/Events list.txt).
// Содержит подтверждённые на golden-файлах классы (10-45, 72, 82, 162 и т.д.)
// плюс основной набор документированных классов SQL Server Profiler.
var eventClassNames = map[int]string{
	10:  "RPC:Completed",
	11:  "RPC:Starting",
	12:  "SQL:BatchCompleted",
	13:  "SQL:BatchStarting",
	14:  "Audit Login",
	15:  "Audit Logout",
	16:  "Attention",
	17:  "ExistingConnection",
	18:  "Audit Server Starts and Stops",
	19:  "DTCTransaction",
	20:  "Audit Login Failed",
	21:  "EventLog",
	22:  "ErrorLog",
	23:  "Lock:Released",
	24:  "Lock:Acquired",
	25:  "Lock:Deadlock",
	26:  "Lock:Cancel",
	27:  "Lock:Timeout",
	28:  "Degree of Parallelism Event",
	33:  "Exception",
	34:  "SP:CacheMiss",
	35:  "SP:CacheInsert",
	36:  "SP:CacheRemove",
	37:  "SP:Recompile",
	38:  "SP:CacheHit",
	40:  "SQL:StmtStarting",
	41:  "SQL:StmtCompleted",
	42:  "SP:Starting",
	43:  "SP:Completed",
	44:  "SP:StmtStarting",
	45:  "SP:StmtCompleted",
	46:  "Object:Created",
	47:  "Object:Deleted",
	50:  "SQL Transaction",
	51:  "Scan:Started",
	52:  "Scan:Stopped",
	53:  "CursorOpen",
	54:  "TransactionLog",
	55:  "Hash Warning",
	58:  "Auto Stats",
	59:  "Lock:Deadlock Chain",
	60:  "Lock:Escalation",
	61:  "OLE DB Errors",
	67:  "Execution Warnings",
	68:  "Showplan Text (Unencoded)",
	69:  "Sort Warnings",
	70:  "CursorPrepare",
	71:  "Prepare SQL",
	72:  "Exec Prepared SQL",
	73:  "Unprepare SQL",
	74:  "CursorExecute",
	75:  "CursorRecompile",
	76:  "CursorImplicitConversion",
	77:  "CursorUnprepare",
	78:  "CursorClose",
	79:  "Missing Column Statistics",
	80:  "Missing Join Predicate",
	81:  "Server Memory Change",
	82:  "User Configurable (0)",
	83:  "User Configurable (1)",
	84:  "User Configurable (2)",
	85:  "User Configurable (3)",
	86:  "User Configurable (4)",
	87:  "User Configurable (5)",
	88:  "User Configurable (6)",
	89:  "User Configurable (7)",
	90:  "User Configurable (8)",
	91:  "User Configurable (9)",
	92:  "Data File Auto Grow",
	93:  "Log File Auto Grow",
	94:  "Data File Auto Shrink",
	95:  "Log File Auto Shrink",
	96:  "Showplan Text",
	97:  "Showplan All",
	98:  "Showplan Statistics Profile",
	100: "RPC Output Parameter",
	122: "Showplan XML",
	123: "SQL:FullTextQuery",
	124: "Broker:Conversation",
	125: "Deprecation Announcement",
	126: "Deprecation Final Support",
	127: "Exchange Spill Event",
	146: "Showplan XML Statistics Profile",
	148: "Deadlock Graph",
	150: "Trace File Close",
	162: "User Error Message",
	164: "Object:Altered",
	165: "Performance statistics",
	166: "SQL:StmtRecompile",
	168: "Showplan XML For Query Compile",
	169: "Showplan All For Query Compile",
	190: "Progress Report: Online Index Operation",
	212: "Bitmap Warning",
	213: "Database Suspect Data Page",
	214: "CPU threshold exceeded",
	215: "PreConnect:Starting",
	216: "PreConnect:Completed",
	217: "Plan Guide Successful",
	218: "Plan Guide Unsuccessful",
	235: "Audit Fulltext",
}

// EventClassName возвращает имя класса события по его ID, либо
// "EventClass<N>" если класс не в таблице (не документирован/не встречен).
func EventClassName(id int) string {
	if name, ok := eventClassNames[id]; ok {
		return name
	}
	return ""
}

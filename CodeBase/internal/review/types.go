package review

import "regexp"

type RuleID string

const (
	RuleForeignTablesUsing                RuleID = "foreignTablesUsing"
	RuleForeignPTablesUsing               RuleID = "foreignPTablesUsing"
	RuleForeignProcedureUsing             RuleID = "foreignProcedureUsing"
	RuleExecNotExistsProc                 RuleID = "execNotExistsProc"
	RuleProcDuplicate                     RuleID = "procDuplicate"
	RuleProcParamDefValue                 RuleID = "procParamDefValue"
	RuleProcElseCase                      RuleID = "procElseCase"
	RuleUseSelectAll                      RuleID = "useSelectAll"
	RuleTruncTbl                          RuleID = "truncTbl"
	RuleDatatype                          RuleID = "datatype"
	RuleAnsiInJoin                        RuleID = "ansiInJoin"
	RuleInsertRowLock                     RuleID = "insertRowLock"
	RuleUseEqColumn                       RuleID = "useEqColumn"
	RuleTableFullScan                     RuleID = "tableFullScan"
	RuleTableHintExists                   RuleID = "tableHintExists"
	RuleTableHintIsRight                  RuleID = "tableHintIsRight"
	RuleIndexExistsInDB                   RuleID = "indexExistsInDB"
	RuleIndexWrong                        RuleID = "indexWrong"
	RuleUpdateOnlyVar                     RuleID = "updateOnlyVar"
	RulePTableSpid                        RuleID = "pTableSpid"
	RuleForceOrder2Tbl                    RuleID = "forceOrder2Tbl"
	RuleSaveTran                          RuleID = "saveTran"
	RuleUseDrop                           RuleID = "useDrop"
	RuleMathOperations                    RuleID = "mathOperations"
	RuleExistsWithAndInIf                 RuleID = "existsWithAndInIf"
	RuleNullComparison                    RuleID = "nullComparison"
	RuleShouldBeCP866                     RuleID = "shouldBeCP866"
	RuleTooManyJoins                      RuleID = "tooManyJoins"
	RuleMaxProcParam                      RuleID = "maxProcParam"
	RuleModifyOutProc                     RuleID = "modifyOutProc"
	RuleEmptyReturn                       RuleID = "emptyReturn"
	RuleRawTransactionControl             RuleID = "rawTransactionControl"
	RuleDeferredUpdate                    RuleID = "deferredUpdate"
	RuleInSubQuery                        RuleID = "inSubQuery"
	RuleVarcharSize                       RuleID = "varcharSize"
	RuleColumnInsert                      RuleID = "columnInsert"
	RulePostgreLabelGotoLevel             RuleID = "postgreLabelGotoLevel"
	RuleDateIntoString                    RuleID = "dateIntoString"
	RuleEmptyStringDate                   RuleID = "emptyStringDate"
	RuleVarUseAfterCursor                 RuleID = "varUseAfterCursor"
	RuleExcessProcParams                  RuleID = "excessProcParams"
	RuleDuplicateOutputVariable           RuleID = "duplicateOutputVariable"
	RuleUseOnlyDeclaredCursors            RuleID = "useOnlyDeclaredCursors"
	RuleCursorFetchArguments              RuleID = "cursorFetchArguments"
	RuleUsageVarInSameSelect              RuleID = "usageVarInSameSelect"
	RuleVarAssignInUpdate                 RuleID = "varAssignInUpdate"
	RuleStatementsWithJoinsRequireAliases RuleID = "statementsWithJoinsRequireAliases"
	RuleUseFuncInIndCol                   RuleID = "useFuncInIndCol"
	RuleIsNullSameTypes                   RuleID = "isNullSameTypes"
	RuleDiffTypesComparison               RuleID = "diffTypesComparison"
	RuleFloatToStringConvert              RuleID = "floatToStringConvert"
	RuleSelectAfterSetRowcount            RuleID = "selectAfterSetRowcount"
	RuleAliasWhenUsingUnion               RuleID = "aliasWhenUsingUnion"
)

const (
	SeverityDeployStopper = 1
	SeverityPostgreReq    = 2
	SeverityFineCode      = 3

	MaxJoinsAllowed      = 12
	MaxProcParamsAllowed = 90
)

var (
	// readHints - для SELECT и вспомогательных таблиц в UPDATE/DELETE
	readHints = []string{
		"M_INDEX",
		"M_NOLOCK_INDEX",
		"M_READPAST_INDEX",
		"M_HOLDLOCK_INDEX",
		"M_P_READPAST_INDEX",
		"M_P_HOLDLOCK_INDEX",
	}
	// deleteHints - для целевой таблицы в DELETE
	deleteHints = []string{
		"M_ROWLOCK_INDEX",
		"M_ROWLOCK_READPAST_INDEX",
		"M_P_ROWLOCK_INDEX",
		"M_P_ROWLOCK_READPAST_INDEX",
	}
	// updateHints - для целевой таблицы в UPDATE
	updateHints = []string{
		"M_UPDLOCK_INDEX",
		"M_UPDLOCK_READPAST_INDEX",
		"M_P_UPDLOCK_INDEX",
		"M_P_UPDLOCK_READPAST_INDEX",
	}

	forceOrderMacros = []string{
		"M_FORCEORDER",
		"M_FORCEORDER_NOSPOOL",
		"M_FORCEORDER_FAST",
		"M_FORCEORDER_WO_LOOPJOIN",
	}

	allowedTableHints = []string{
		"M_INDEX",
		"M_NOLOCK_INDEX",
		"M_ROWLOCK_INDEX",
		"M_ROWLOCK_READPAST_INDEX",
		"M_READPAST_INDEX",
		"M_UPDLOCK_INDEX",
		"M_UPDLOCK_READPAST_INDEX",
		"M_HOLDLOCK_INDEX",
		"M_P_ROWLOCK_INDEX",
		"M_P_ROWLOCK_READPAST_INDEX",
		"M_P_READPAST_INDEX",
		"M_P_UPDLOCK_INDEX",
		"M_P_UPDLOCK_READPAST_INDEX",
		"M_P_HOLDLOCK_INDEX",
	}
)

// selectAllRe находит SELECT * (с любыми пробелами между SELECT и *)
var selectAllRe = regexp.MustCompile(`(?i)\bselect\s+\*`)

// truncateTblRe находит TRUNCATE TABLE и имя таблицы (включая схему dbo.table)
var truncateTblRe = regexp.MustCompile(`(?i)\btruncate\s+table\s+(\S+)`)

// nullComparisonBinaryRe ищет сравнения вида: expr =/<>/<=/>=/</>  NULL (не IS NULL / IS NOT NULL)
var nullComparisonBinaryRe = regexp.MustCompile(`(?i)(?:^|[^a-zA-Z_])((?:=|<>|!=|<=|>=|<|>)\s*null\b|\bnull\s*(?:=|<>|!=|<=|>=|<|>))`)

// nullParamDefaultRe соответствует строкам объявления параметра или переменной с дефолтом = null:
// @Name   DSTYPE = null,   или   @Name DSTYPE = null
var nullParamDefaultRe = regexp.MustCompile(`(?i)@\w+\s+\w+\s*=\s*null\s*,?\s*$`)

// nullComparisonInRe ищет IN (..., NULL, ...) или IN (NULL)
var nullComparisonInRe = regexp.MustCompile(`(?i)\bin\s*\([^)]*\bnull\b[^)]*\)`)

// modifyOutProcInsertRe, modifyOutProcUpdateRe, modifyOutProcDeleteRe, modifyOutProcTruncateRe —
// регулярки для детектирования DML-операторов и цели.
var (
	modifyOutProcInsertRe   = regexp.MustCompile(`(?i)^\s*insert\s+(?:into\s+)?([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcUpdateRe   = regexp.MustCompile(`(?i)^\s*update\s+([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcDeleteRe   = regexp.MustCompile(`(?i)^\s*delete\s+(?:from\s+)?([A-Za-z_#][A-Za-z0-9_#]*)`)
	modifyOutProcTruncateRe = regexp.MustCompile(`(?i)^\s*truncate\s+table\s+([A-Za-z_#][A-Za-z0-9_#]*)`)
)

// sqlKeywordsMap — ключевые слова SQL, которые не должны считаться ссылками на столбцы.
var sqlKeywordsMap = map[string]bool{
	"select": true, "from": true, "where": true, "and": true, "or": true,
	"not": true, "null": true, "case": true, "when": true, "then": true,
	"else": true, "end": true, "is": true, "in": true, "like": true,
	"between": true, "as": true, "top": true, "distinct": true, "all": true,
	"join": true, "inner": true, "left": true, "right": true, "outer": true,
	"full": true, "cross": true, "on": true, "group": true, "order": true,
	"by": true, "having": true, "union": true, "insert": true, "into": true,
	"values": true, "update": true, "set": true, "delete": true, "output": true,
	"option": true, "with": true, "exists": true, "while": true, "begin": true,
	"if": true, "return": true, "declare": true, "create": true, "proc": true,
	"procedure": true, "go": true, "print": true, "exec": true, "execute": true,
	"cast": true, "convert": true, "desc": true, "asc": true,
}

// sqlFunctionsMap — встроенные функции SQL, которые не должны считаться ссылками на столбцы.
var sqlFunctionsMap = map[string]bool{
	"isnull": true, "count": true, "sum": true, "max": true, "min": true,
	"avg": true, "convert": true, "cast": true, "substring": true, "len": true,
	"upper": true, "lower": true, "abs": true, "datediff": true, "dateadd": true,
	"getdate": true, "replace": true, "rtrim": true, "ltrim": true,
	"coalesce": true, "charindex": true, "left": true, "right": true,
	"replicate": true, "space": true, "datename": true, "datepart": true,
	"year": true, "month": true, "day": true, "round": true, "floor": true,
	"ceiling": true, "power": true, "sqrt": true, "sign": true,
	"row_number": true, "rank": true, "dense_rank": true,
	"identity": true, "scope_identity": true, "nullif": true,
}

type Finding struct {
	Rule             RuleID `json:"rule"`
	Severity         int    `json:"severity"`
	Message          string `json:"message"`
	File             string `json:"file"`
	Line             int    `json:"line,omitempty"`
	Object           string `json:"object,omitempty"`
	CurrentProductID int64  `json:"current_product_id,omitempty"`
	TargetProductID  int64  `json:"target_product_id,omitempty"`
}

type Summary struct {
	Total      int            `json:"total"`
	ByRule     map[RuleID]int `json:"by_rule,omitempty"`
	BySeverity map[int]int    `json:"by_severity,omitempty"`
}

type Result struct {
	AnalyzedFile string    `json:"analyzed_file"`
	Summary      Summary   `json:"summary"`
	Findings     []Finding `json:"findings"`
}

type Options struct {
	Rules       []RuleID
	MinSeverity int
}

type indexedFile struct {
	ID          int64
	Path        string
	RelPath     string
	DsProductID int64
}

type tableIndexCandidate struct {
	Name   string
	Fields []string
}

type tableFromClause struct {
	TableName string
	Alias     string
	Hint      string // Извлеченный хинт индекса
	IndexName string // Имя индекса из M_*_INDEX(...)
}

type whereAnalysisResult struct {
	Aliases                  []string
	HasUnqualifiedConditions bool
}

type columnRef struct {
	Table  string
	Column string
}

type tableRef struct {
	Name string
	Line int
}

type procedureRef struct {
	Name string
	Line int
}

type updateAssignment struct {
	Target     string
	Expression string
}

type updateSetStatement struct {
	TargetTable string
	TargetAlias string
	Assignments []updateAssignment
	FromClause  string
}

type insertSelectStatement struct {
	TargetTable       string
	TargetColumns     []string
	SelectExpressions []string
	FromClause        string
}

type selectAssignment struct {
	TargetVariable string
	Expression     string
}

type selectAssignStatement struct {
	Assignments []selectAssignment
	FromClause  string
}

type fetchIntoStatement struct {
	CursorName string
	Variables  []string
}

type cursorDeclaration struct {
	CursorName        string
	SelectExpressions []string
	FromClause        string
}

type deallocateStatement struct {
	CursorName string
	Line       int
}

type stmtWithOffset struct {
	stmt   string
	offset int
}
